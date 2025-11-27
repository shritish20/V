import asyncio
import time
import logging
import random
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict
from core.config import IST, ACCOUNT_SIZE, MARKET_HOLIDAYS_2025, MARKET_OPEN_TIME, MARKET_CLOSE_TIME, SAFE_TRADE_END, EXPIRY_FLAT_TIME, MARKET_KEY_INDEX, PAPER_TRADING, STOP_LOSS_MULTIPLE, PROFIT_TARGET_PCT, LOT_SIZE
from core.models import MultiLegTrade, AdvancedMetrics, PortfolioMetrics, EngineStatus, GreeksSnapshot, Position
from core.enums import TradeStatus, ExitReason, MarketRegime, OrderStatus
from database.manager import HybridDatabaseManager
from trading.api_client import HybridUpstoxAPI
from trading.order_manager import SafeOrderManager
from trading.risk_manager import PortfolioRiskManager
from trading.strategy_engine import StrategyEngine
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import EventIntelligence
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from alerts.system import CriticalAlertSystem
from utils.logger import setup_logger
from api.dependencies import set_engine 

logger = setup_logger()

class HybridTradeManager:
    """Complete trade management with safety features"""
    def __init__(self, api: HybridUpstoxAPI, db: HybridDatabaseManager, order_manager: SafeOrderManager, pricing_engine: HybridPricingEngine, risk_manager: PortfolioRiskManager, alert_system: CriticalAlertSystem):
        self.api = api
        self.db = db
        self.om = order_manager
        self.pricing = pricing_engine
        self.risk_mgr = risk_manager
        self.alerts = alert_system

    async def execute_strategy(self, strategy_name: str, legs_spec: List[dict], lots: int, current_spot: float) -> Optional[MultiLegTrade]:
        """Execute a multi-leg strategy with comprehensive safety checks"""
        
        full_legs: List[Position] = []
        try:
            spot = current_spot
            
            # 1. Prepare Positions & Fetch Greeks/Prices
            for spec in legs_spec:
                instrument_key = await self.api.get_instrument_key(
                    symbol=MARKET_KEY_INDEX.split('|')[-1], 
                    expiry=spec['expiry'],
                    strike=spec['strike'],
                    opt_type=spec['type']
                )
                if not instrument_key:
                    logger.error(f"Failed to resolve instrument key for {spec}")
                    return None
                
                # BUG #2 FIX: Fetch real market price before proceeding (or use provided limit)
                market_data = await self.api.get_quotes([instrument_key])
                bid_ask = market_data.get("data", {}).get(instrument_key, {})
                bid = bid_ask.get("bid", 0.0)
                ask = bid_ask.get("ask", 0.0)
                
                entry_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else spec.get('price', 50.0) 
                
                # CRITICAL FIX 3: Fetch Greeks with market validation
                greeks = await self.api.calculate_greeks_with_validation(
                    instrument_key, spot, spec['strike'], spec['type'], spec['expiry']
                )

                quantity_sign = 1 if spec['side'] == 'BUY' else -1
                quantity = quantity_sign * LOT_SIZE * lots
                
                full_legs.append(Position(
                    symbol=MARKET_KEY_INDEX, instrument_key=instrument_key, strike=spec['strike'], option_type=spec['type'], 
                    quantity=quantity, entry_price=entry_price, entry_time=datetime.now(IST), current_price=entry_price, current_greeks=greeks
                ))
            
            # 2. CRITICAL FIX 2: Full Margin Check for all legs
            required_margin = await self.api.calculate_margin_for_basket(full_legs)
            current_funds = self.risk_mgr.portfolio_metrics.equity 
            if required_margin > current_funds:
                 await self.alerts.send_alert("MARGIN_FAIL", f"Margin required: ₹{required_margin:,.0f} exceeds funds: ₹{current_funds:,.0f}", urgent=True)
                 return None

            temp_trade = MultiLegTrade(legs=full_legs, strategy_type=strategy_name, net_premium_per_share=0.0, entry_time=datetime.now(IST), lots=lots, status=TradeStatus.PENDING, expiry_date=legs_spec[0]['expiry'])
            
            # 3. Execute Basket Order
            success, fill_prices = await self.om.execute_basket_order(full_legs)
            if not success:
                logger.error("Basket order failed and rolled back. Aborting trade.")
                return None
            
            # 4. Finalize Trade Object
            net_premium = 0.0
            for leg in full_legs:
                filled_order = next((o for o in self.om.orders.values() if o.instrument_key == leg.instrument_key and o.status == OrderStatus.FILLED), None)
                if filled_order:
                    leg.entry_price = filled_order.average_price
                    leg.current_price = filled_order.average_price
                    net_premium += (leg.entry_price * leg.quantity) 
            
            net_premium_per_share = net_premium / (lots * LOT_SIZE)
            
            final_trade = MultiLegTrade(
                legs=full_legs, strategy_type=strategy_name, net_premium_per_share=net_premium_per_share, entry_time=datetime.now(IST), 
                lots=lots, status=TradeStatus.OPEN, expiry_date=legs_spec[0]['expiry']
            )
            
            # 5. Save to Database
            trade_id = self.db.save_trade(final_trade)
            final_trade.id = trade_id
            logger.info(f"Trade {trade_id} ({strategy_name}) opened successfully. Net Premium: {net_premium_per_share:.2f}")
            return final_trade

        except Exception as e:
            logger.critical(f"Critical error during execute_strategy: {e}")
            return None

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        """Close a trade with safety checks"""
        if trade.status not in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
            logger.warning(f"Trade {trade.id} is already {trade.status.value}. Skipping close.")
            return

        # 1. Create reverse positions for the closing basket
        close_legs: List[Position] = []
        for leg in trade.legs:
            reverse_quantity = -leg.quantity 
            close_price = leg.current_price
            side = "SELL" if reverse_quantity > 0 else "BUY"
            
            close_legs.append(Position(
                symbol=leg.symbol, instrument_key=leg.instrument_key, strike=leg.strike, option_type=leg.option_type, 
                quantity=abs(reverse_quantity), entry_price=close_price, entry_time=datetime.now(IST), current_price=close_price, 
                current_greeks=GreeksSnapshot(timestamp=datetime.now(IST))
            ))
            
        # 2. Execute the closing basket order
        success, fill_prices = await self.om.execute_basket_order(close_legs)
        
        if not success:
            logger.critical(f"CRITICAL: Failed to close trade {trade.id}. Rollback attempted. MANUAL INTERVENTION.")
            await self.alerts.partial_fill_alert("Close Failed", str(trade.id), urgent=True)
            return
        
        # 3. Calculate PnL and Update Database
        trade.status = TradeStatus.CLOSED
        gross_pnl = 0.0
        for leg in trade.legs:
            filled_order = next((o for o in self.om.orders.values() if o.instrument_key == leg.instrument_key and o.status == OrderStatus.FILLED), None)
            exit_price = filled_order.average_price if filled_order else leg.current_price
            
            price_change = exit_price - leg.entry_price
            leg_pnl = price_change * leg.quantity
            gross_pnl += leg_pnl
        
        pnl = gross_pnl - trade.transaction_costs
        
        if trade.id:
            self.db.update_trade_close(trade.id, pnl, reason.value)
        
        logger.info(f"Trade {trade.id} closed for PnL: ₹{pnl:,.2f} | Reason: {reason.value}")

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, real_time_quotes: Dict[str, float]):
        """Update trade prices and Greeks using real-time quotes."""
        
        if trade.status not in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
            return

        for leg in trade.legs:
            last_price = real_time_quotes.get(leg.instrument_key)
            
            if last_price:
                leg.current_price = last_price
                
                greeks = self.pricing.calculate_greeks(
                    spot=spot, strike=leg.strike, opt_type=leg.option_type, expiry=trade.expiry_date
                )
                leg.current_greeks = greeks
        
        trade.calculate_trade_greeks()
        logger.debug(f"Trade {trade.id} updated. PnL: {trade.total_unrealized_pnl():.2f}")


class VolGuardHybridUltimate:
    """THE DEFINITIVE VERSION - Best of Both Worlds"""
    
    def __init__(self):
        # Core Infrastructure
        self.db = HybridDatabaseManager()
        self.api = HybridUpstoxAPI(UPSTOX_ACCESS_TOKEN)
        
        # CRITICAL FIX 3: Inject the pricing engine into the API client for Greek validation
        self.analytics = HybridVolatilityAnalytics()
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.api.set_pricing_engine(self.pricing) 
        
        self.alerts = CriticalAlertSystem()
        self.event_intel = EventIntelligence()

        # Trading & Risk Management
        self.om = SafeOrderManager(self.api)
        self.risk_mgr = PortfolioRiskManager()
        self.strategy_engine = StrategyEngine(self.analytics, self.event_intel)
        self.trade_mgr = HybridTradeManager(self.api, self.db, self.om, self.pricing, self.risk_mgr, self.alerts)
        
        # CRITICAL FIX 5 (Part B): Set global engine reference
        set_engine(self) 
        
        # Real-Time Data Storage
        self.rt_quotes: Dict[str, float] = {}
        self.ws_task: Optional[asyncio.Task] = None

        # State Management
        self.trades: List[MultiLegTrade] = []
        daily_pnl, max_equity, cycle_count, total_trades = self.db.get_daily_state()
        self.daily_pnl = daily_pnl
        self.max_equity = max_equity
        self.cycle_count = cycle_count
        self.total_trades = total_trades
        self.equity = ACCOUNT_SIZE + daily_pnl

        # Engine State
        self.running = False 
        self.circuit_breaker = False
        self.last_metrics: Optional[AdvancedMetrics] = None
        logger.info(f"VolGuard Hybrid Ultimate Initialized. Paper Trading: {PAPER_TRADING}")
        
    async def _startup_reconciliation(self):
        """CRITICAL FIX 2: Loads open positions from the broker and checks for hourly reconciliation."""
        logger.info("Starting broker position reconciliation...")
        
        broker_positions_data = await self.api.get_short_term_positions() 
        
        if broker_positions_data:
            logger.critical(f"Found {len(broker_positions_data)} open positions on the broker that need management.")
            
            self.trades = [t for t in self.trades if t.status != TradeStatus.EXTERNAL]

            for pos_data in broker_positions_data:
                instrument_key = pos_data.get('instrument_key')
                
                position = Position(
                    symbol=pos_data.get('symbol', MARKET_KEY_INDEX),
                    instrument_key=instrument_key,
                    strike=pos_data.get('strike_price', 0.0),
                    option_type=pos_data.get('option_type', 'CE'),
                    quantity=pos_data.get('net_quantity', 0), 
                    entry_price=pos_data.get('average_price', 0.0),
                    current_price=pos_data.get('last_price', 0.0),
                    entry_time=datetime.now(IST),
                    current_greeks=GreeksSnapshot(timestamp=datetime.now(IST))
                )
                
                external_trade = MultiLegTrade(
                    legs=[position],
                    strategy_type="RECONCILED_SINGLE_LEG",
                    net_premium_per_share=0.0,
                    entry_time=position.entry_time,
                    lots=abs(position.quantity) // LOT_SIZE,
                    status=TradeStatus.EXTERNAL, 
                    expiry_date=pos_data.get('expiry_date', '2099-12-31')
                )
                self.trades.append(external_trade)
                
            self.circuit_breaker = True 
            await self.alerts.send_alert(
                "RECONCILIATION_WARNING", 
                f"Broker shows {len(self.trades)} unmanaged open trades. Circuit Breaker engaged.",
                urgent=True
            )
        else:
            logger.info("Reconciliation complete. Broker positions are clean.")

    async def _get_market_metrics(self) -> Tuple[float, AdvancedMetrics]:
        """CRITICAL FIX 4: Implements WebSocket Failure Fallback AND Real SABR Calibration."""
        
        spot_price = self.rt_quotes.get(MARKET_KEY_INDEX, 0.0)
        vix = self.rt_quotes.get("INDICES|INDIA VIX", 15.0)
        
        # Check for data freshness (if timestamp is missing or older than 30s)
        rt_data_stale = (datetime.now(IST).timestamp() - self.rt_quotes.get('timestamp', 0)) > 30

        # 1. Data Source Logic (Fallback)
        if spot_price == 0.0 or rt_data_stale:
             logger.warning("No fresh RT data available. Switching to REST polling.")
             quotes = await self.api.get_quotes([MARKET_KEY_INDEX, "INDICES|INDIA VIX"])
             spot_price = quotes.get("data", {}).get(MARKET_KEY_INDEX, {}).get("last_price", 0.0)
             vix = quotes.get("data", {}).get("INDICES|INDIA VIX", {}).get("last_price", 15.0)
             
             if spot_price == 0.0:
                 # CRITICAL: Total Data Failure
                 logger.critical("TOTAL DATA FAILURE: Cannot retrieve spot price via WS or REST.")
                 self.circuit_breaker = True
                 self.running = False
                 await self.alerts.send_alert("TOTAL_DATA_FAILURE", "No market data available. Flattening and stopping.", urgent=True)
                 await self._emergency_flatten()
                 raise Exception("Total data failure.")
             else:
                 logger.info(f"REST fallback successful. Spot: {spot_price}")
                 
        # 2. SABR Calibration (BUG #3 FIX: Using Real Option Chain Data)
        if not self.sabr.calibrated or (datetime.now() - self.sabr.last_calibration).seconds > 3600:
            expiry = self.strategy_engine._get_weekly_expiry()
            underlying_symbol = MARKET_KEY_INDEX.split('|')[-1]
            option_chain_data = await self.api.get_option_chain_data(underlying_symbol, expiry)
            
            if option_chain_data:
                strikes = []
                ivs = []
                for strike_data in option_chain_data:
                    if strike_data.get('call_options') and strike_data['call_options'].get('option_greeks'):
                        strikes.append(strike_data['strike_price'])
                        ivs.append(strike_data['call_options']['option_greeks'].get('iv', 0.0))
                    if strike_data.get('put_options') and strike_data['put_options'].get('option_greeks'):
                        strikes.append(strike_data['strike_price'])
                        ivs.append(strike_data['put_options']['option_greeks'].get('iv', 0.0))
                
                # Filter valid IVs and calibrate
                valid_pairs = [(s, iv) for s, iv in zip(strikes, ivs) if iv > 0.05 and iv < 1.5]
                
                if len(valid_pairs) >= 5:
                    strikes_clean, ivs_clean = zip(*valid_pairs)
                    self.sabr.calibrate_to_chain(list(strikes_clean), list(ivs_clean), spot_price, 7/365)
                    logger.info("SABR calibrated successfully to live option chain data.")
                else:
                    logger.warning("Insufficient valid IV data for SABR, using synthetic defaults.")
                    self.sabr.calibrate_to_chain([spot_price-200, spot_price, spot_price+200], [0.15, 0.16, 0.17], spot_price, 7/365)
            else:
                 logger.warning("Option chain fetch failed, using synthetic SABR calibration.")
                 self.sabr.calibrate_to_chain([spot_price-200, spot_price, spot_price+200], [0.15, 0.16, 0.17], spot_price, 7/365)
                 
        # 3. Final Metrics Generation
        realized_vol, garch_vol, ivp = self.analytics.get_volatility_metrics(vix)
        event_risk_score = self.event_intel.get_event_risk_score()
        regime = self._determine_market_regime(vix, ivp, realized_vol, event_risk_score)

        metrics = AdvancedMetrics(
            timestamp=datetime.now(IST), spot_price=spot_price, vix=vix, ivp=ivp, realized_vol_7d=realized_vol, 
            garch_vol_7d=garch_vol, iv_rv_spread=vix - realized_vol, pcr=1.0, max_pain=spot_price, 
            event_risk_score=event_risk_score, regime=regime, term_structure_slope=0.0, volatility_skew=0.0, 
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta, sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics
        self.db.save_market_analytics(metrics)
        return spot_price, metrics

    def _determine_market_regime(self, vix: float, ivp: float, realized_vol: float, event_risk: float) -> MarketRegime:
        if event_risk > 2.5: return MarketRegime.DEFENSIVE_EVENT
        if vix > 25 and (vix - realized_vol) > 5.0: return MarketRegime.PANIC
        if vix < 12 and ivp < 20: return MarketRegime.LOW_VOL_COMPRESSION
        if 15 <= vix <= 22 and ivp < 70: return MarketRegime.CALM_COMPRESSION
        return MarketRegime.TRANSITION

    async def _update_portfolio(self, spot: float):
        
        await asyncio.gather(*[self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes) for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]])
        
        self.risk_mgr.update_portfolio_state(self.trades, self.daily_pnl)
        
        trades_to_close = []
        for trade in self.trades:
            if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                exit_reason = self._should_exit_trade(trade)
                if exit_reason:
                    trades_to_close.append((trade, exit_reason))
                    
        if trades_to_close:
            await asyncio.gather(*[self.trade_mgr.close_trade(t, r) for t, r in trades_to_close])
            self.trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
            
        if self.risk_mgr.should_flatten_portfolio():
            self.circuit_breaker = True
            await self.alerts.circuit_breaker_alert(self.risk_mgr.portfolio_metrics.daily_pnl, self.risk_mgr.portfolio_metrics.total_pnl, urgent=True)
            await self._emergency_flatten()

    def _should_exit_trade(self, trade: MultiLegTrade) -> Optional[ExitReason]:
        
        now = datetime.now(IST).time()
        if now >= SAFE_TRADE_END: return ExitReason.EOD_FLATTEN
        if now >= EXPIRY_FLAT_TIME and (datetime.strptime(trade.expiry_date, "%Y-%m-%d").date() == datetime.now(IST).date()): return ExitReason.EXPIRY_FLATTEN
            
        pnl = trade.total_unrealized_pnl()
        max_loss = trade.max_loss_per_lot * trade.lots
        
        if trade.strategy_type in ["SHORT_STRANGLE", "IRON_CONDOR"] and pnl >= (max_loss * PROFIT_TARGET_PCT): return ExitReason.PROFIT_TARGET
        if pnl <= -(max_loss * STOP_LOSS_MULTIPLE): return ExitReason.STOP_LOSS
        if abs(trade.trade_vega) > 500: return ExitReason.VEGA_LIMIT
            
        return None

    async def _consider_new_trade(self, metrics: AdvancedMetrics, spot: float):
        
        if self.circuit_breaker or datetime.now(IST).time() >= SAFE_TRADE_END: return

        strategy_name, legs_spec = self.strategy_engine.select_strategy(metrics, spot)
        if strategy_name == "WAIT": return
            
        simulated_max_loss_per_lot = 50.0 
        lots = self.risk_mgr.get_position_size(simulated_max_loss_per_lot, metrics)
        if lots == 0: return

        sim_trade_vega = 50.0 * lots 
        sim_trade_delta = 10.0 * lots 
        if not self.risk_mgr.can_open_new_trade(sim_trade_vega, sim_trade_delta): return
            
        new_trade = await self.trade_mgr.execute_strategy(strategy_name, legs_spec, lots, spot)
        if new_trade:
            self.trades.append(new_trade)
            self.total_trades += 1

    async def run_cycle(self):
        """Execute one trading cycle (Strategy/Risk management loop)"""
        
        if not self._is_market_open() or self.circuit_breaker:
            await asyncio.sleep(1) 
            return

        try:
            spot, metrics = await self._get_market_metrics()
            
            # CRITICAL FIX 2: Hourly Reconciliation Check
            if self.cycle_count % 240 == 0 and self.cycle_count > 0: # Every 60 minutes
                 await self._startup_reconciliation()
            
            await self._update_portfolio(spot)
            await self._consider_new_trade(metrics, spot)
            
            self.db.save_daily_state(self.risk_mgr.portfolio_metrics.daily_pnl, self.risk_mgr.max_equity, self.cycle_count, self.total_trades)
            self.db.save_portfolio_snapshot(self.risk_mgr.portfolio_metrics)
            
            self.cycle_count += 1
            logger.info(f"Cycle {self.cycle_count} completed. PnL: ₹{self.risk_mgr.portfolio_metrics.total_pnl:,.2f}")
            
        except Exception as e:
            logger.error(f"Error in run_cycle: {e}")
            await self.alerts.send_alert("CYCLE_ERROR", str(e), urgent=True)

    async def run(self, continuous: bool = True):
        """Main engine loop: Starts WS feed and strategy cycles."""
        logger.info(f"Starting VolGuard Hybrid Ultimate main loop (Continuous: {continuous}).")
        
        # 1. CRITICAL: Reconcile positions on startup
        await self._startup_reconciliation()

        # 2. Start WebSocket Feed
        self.ws_task = asyncio.create_task(self.api.ws_connect_and_stream(self.rt_quotes))
        
        # 3. Start Trading Cycles (15-second strategy interval)
        self.running = True
        while self.running:
            if not continuous:
                self.running = False
            
            await self.run_cycle()
            await asyncio.sleep(15) 

    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Initiating graceful shutdown.")
        self.running = False
        if self.ws_task:
            self.ws_task.cancel()
        await self._emergency_flatten()
        await self.api.close()
        logger.info("VolGuard Hybrid Ultimate shut down successfully.")

    def _is_market_open(self) -> bool:
        """Check if market is open for trading"""
        now = datetime.now(IST)
        if now.weekday() >= 5: return False
        if now.strftime("%Y-%m-%d") in MARKET_HOLIDAYS_2025: return False
        current_time = now.time()
        return MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME

    async def _emergency_flatten(self):
        """Emergency flatten all positions"""
        logger.critical("EMERGENCY FLATTEN INITIATED")
        open_trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
        if not open_trades:
            logger.info("No open trades to flatten.")
            return
        await asyncio.gather(*[self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in open_trades])
        self.trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
        logger.critical(f"Emergency flatten complete. {len(self.trades)} trades remaining.")

    def get_status(self) -> EngineStatus:
        """Get current engine status"""
        return EngineStatus(
            running=self.running, circuit_breaker=self.circuit_breaker, cycle_count=self.cycle_count, 
            total_trades=self.total_trades, daily_pnl=self.daily_pnl, max_equity=self.max_equity, 
            last_metrics=self.last_metrics
        )
