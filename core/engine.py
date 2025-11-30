import asyncio
import time
import logging
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple, Any
from core.config import IST, ACCOUNT_SIZE, MARKET_HOLIDAYS_2025, MARKET_OPEN_TIME, MARKET_CLOSE_TIME, SAFE_TRADE_END, EXPIRY_FLAT_TIME, MARKET_KEY_INDEX, PAPER_TRADING, STOP_LOSS_MULTIPLE, PROFIT_TARGET_PCT, LOT_SIZE, UPSTOX_ACCESS_TOKEN
from core.models import MultiLegTrade, AdvancedMetrics, PortfolioMetrics, EngineStatus, GreeksSnapshot, Position, TradeStatus, ExitReason, MarketRegime
from database.manager import HybridDatabaseManager
from trading.api_client import HybridUpstoxAPI
from trading.order_manager import EnhancedOrderManager
from trading.risk_manager import AdvancedRiskManager
from trading.strategy_engine import AdvancedStrategyEngine
from trading.trade_manager import EnhancedTradeManager
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import AdvancedEventIntelligence
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from analytics.chain_metrics import ChainMetricsCalculator
from analytics.performance import PerformanceMetrics
from alerts.system import CriticalAlertSystem
from utils.logger import setup_logger
import prometheus_client
from prometheus_client import Counter, Gauge

logger = setup_logger()

# Prometheus metrics
ENGINE_CYCLES = Counter('volguard_engine_cycles_total', 'Total engine cycles completed')
MARKET_DATA_UPDATES = Counter('volguard_market_data_updates_total', 'Market data updates')

# Global process pool for CPU-intensive tasks
cpu_executor = ProcessPoolExecutor(max_workers=2)

class VolGuard14Engine:
    """VOLGUARD 14.00 - IRONCLAD ARCHITECTURE"""
    
    def __init__(self):
        # Core Infrastructure
        self.db = HybridDatabaseManager()
        self.api = HybridUpstoxAPI(UPSTOX_ACCESS_TOKEN)
        
        # Advanced Analytics Stack
        self.vol_analytics = HybridVolatilityAnalytics()
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.api.set_pricing_engine(self.pricing)
        self.chain_metrics = ChainMetricsCalculator()
        self.performance = PerformanceMetrics(self.db)
        self.event_intel = AdvancedEventIntelligence()

        # Production Infrastructure
        self.alerts = CriticalAlertSystem()
        self.om = EnhancedOrderManager(self.api, self.db, self.alerts)
        self.risk_mgr = AdvancedRiskManager(self.db, self.alerts)
        self.strategy_engine = AdvancedStrategyEngine(self.vol_analytics, self.event_intel)
        self.trade_mgr = EnhancedTradeManager(self.api, self.db, self.om, self.pricing, self.risk_mgr, self.alerts)
        
        # Real-Time Data Storage
        self.rt_quotes: Dict[str, float] = {}
        self.rt_quotes_lock = asyncio.Lock()
        self.ws_task: Optional[asyncio.Task] = None
        self.subscribed_instruments: set = set()

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
        
        logger.info(f"VolGuard 14.00 Engine Initialized. Paper Trading: {PAPER_TRADING}")
        
    async def update_quote(self, instrument_key: str, price: float):
        """Thread-safe quote update"""
        async with self.rt_quotes_lock:
            self.rt_quotes[instrument_key] = price
            self.rt_quotes['timestamp'] = time.time()
            MARKET_DATA_UPDATES.inc()
            
    async def get_quote(self, instrument_key: str) -> Optional[float]:
        """Thread-safe quote retrieval"""
        async with self.rt_quotes_lock:
            return self.rt_quotes.get(instrument_key)
        
    async def _startup_reconciliation(self):
        """Enhanced broker position reconciliation"""
        logger.info("Starting enhanced broker position reconciliation...")
        
        try:
            broker_positions_data = await self.api.get_short_term_positions() 
            
            if broker_positions_data:
                logger.info(f"Found {len(broker_positions_data)} open positions on broker")
                
                # Clear existing external trades
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
                    f"Broker shows {len(broker_positions_data)} unmanaged positions. Circuit Breaker engaged.",
                    urgent=True
                )
            else:
                logger.info("Reconciliation complete. Broker positions are clean.")
                
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}")

    async def _calibrate_sabr_safe(self, spot: float):
        """Non-blocking SABR calibration"""
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                cpu_executor, 
                self.sabr.calibrate_to_chain,
                [spot*0.9, spot, spot*1.1], [0.18, 0.15, 0.17], spot, 7/365
            )
        except Exception as e:
            logger.error(f"SABR calibration error: {e}")

    async def _get_market_metrics(self) -> Tuple[float, AdvancedMetrics]:
        """Enhanced market metrics with comprehensive analytics"""
        
        spot_price = await self.get_quote(MARKET_KEY_INDEX) or 0.0
        vix = await self.get_quote("INDICES|INDIA VIX") or 15.0
        
        # Check data freshness
        rt_data_stale = (datetime.now(IST).timestamp() - self.rt_quotes.get('timestamp', 0)) > 30

        if spot_price == 0.0 or rt_data_stale:
             logger.warning("No fresh RT data available. Switching to REST polling.")
             quotes = await self.api.get_quotes([MARKET_KEY_INDEX, "INDICES|INDIA VIX"])
             spot_price = quotes.get("data", {}).get(MARKET_KEY_INDEX, {}).get("last_price", 0.0)
             vix = quotes.get("data", {}).get("INDICES|INDIA VIX", {}).get("last_price", 15.0)
             
             if spot_price == 0.0:
                 logger.critical("TOTAL DATA FAILURE: Cannot retrieve spot price")
                 self.circuit_breaker = True
                 await self.alerts.send_alert("TOTAL_DATA_FAILURE", "No market data available. Flattening and stopping.", urgent=True)
                 await self._emergency_flatten()
                 return 0.0, AdvancedMetrics(
                     timestamp=datetime.now(IST), spot_price=0.0, vix=15.0, ivp=50.0,
                     realized_vol_7d=15.0, garch_vol_7d=15.0, iv_rv_spread=0.0,
                     pcr=1.0, max_pain=0.0, event_risk_score=0.0,
                     regime=MarketRegime.TRANSITION, term_structure_slope=0.0,
                     volatility_skew=0.0
                 )
             else:
                 logger.info(f"REST fallback successful. Spot: {spot_price}")
                 
        # FIXED: SABR calibration timing - block on first calibration
        if not self.sabr.calibrated:
            await self._calibrate_sabr_safe(spot_price)
        else:
            # Subsequent calibrations can be async
            asyncio.create_task(self._calibrate_sabr_safe(spot_price))
        
        # Comprehensive volatility metrics
        realized_vol, garch_vol, ivp = self.vol_analytics.get_volatility_metrics(vix)
        
        # Event risk scoring
        event_risk_score = self.event_intel.get_event_risk_score()
        
        # Advanced regime detection
        regime = self._determine_market_regime(vix, ivp, realized_vol, event_risk_score, spot_price)

        # Create comprehensive metrics
        metrics = AdvancedMetrics(
            timestamp=datetime.now(IST), 
            spot_price=spot_price, 
            vix=vix, 
            ivp=ivp, 
            realized_vol_7d=realized_vol, 
            garch_vol_7d=garch_vol, 
            iv_rv_spread=vix - realized_vol, 
            pcr=1.0,
            max_pain=spot_price,
            event_risk_score=event_risk_score, 
            regime=regime, 
            term_structure_slope=0.0,
            volatility_skew=0.0,
            sabr_alpha=self.sabr.alpha, 
            sabr_beta=self.sabr.beta, 
            sabr_rho=self.sabr.rho, 
            sabr_nu=self.sabr.nu
        )
        
        self.last_metrics = metrics
        self.db.save_market_analytics(metrics)
        return spot_price, metrics

    def _determine_market_regime(self, vix: float, ivp: float, realized_vol: float, event_risk: float, spot: float) -> MarketRegime:
        """Enhanced regime detection with comprehensive analytics"""
        if event_risk > 2.5: 
            return MarketRegime.DEFENSIVE_EVENT
        if vix > 25 and (vix - realized_vol) > 5.0: 
            return MarketRegime.PANIC
        if vix < 12 and ivp < 20: 
            return MarketRegime.LOW_VOL_COMPRESSION
        if 15 <= vix <= 22 and ivp < 70: 
            return MarketRegime.CALM_COMPRESSION
        if vix > 20 and ivp > 70:
            return MarketRegime.FEAR_BACKWARDATION
        return MarketRegime.TRANSITION

    async def _update_portfolio(self, spot: float):
        """Enhanced portfolio management with analytics"""
        # FIXED: Pass real-time quotes to update_trade_prices
        await asyncio.gather(*[
            self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes) 
            for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]
        ])
        
        # Update risk management
        self.risk_mgr.update_portfolio_state(self.trades, self.daily_pnl)
        
        # Check for trade exits
        trades_to_close = []
        for trade in self.trades:
            if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                exit_reason = await self.trade_mgr.manage_trade_exits(trade, self.last_metrics, spot)
                if exit_reason:
                    trades_to_close.append((trade, exit_reason))
                    
        # Close trades that need exiting
        if trades_to_close:
            await asyncio.gather(*[
                self.trade_mgr.close_trade(t, r) for t, r in trades_to_close
            ])
            self.trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
            
        # Circuit breaker check
        if self.risk_mgr.should_flatten_portfolio():
            self.circuit_breaker = True
            await self.alerts.circuit_breaker_alert(
                self.risk_mgr.portfolio_metrics.daily_pnl, 
                DAILY_LOSS_LIMIT, 
                urgent=True
            )
            await self._emergency_flatten()

    async def _consider_new_trade(self, metrics: AdvancedMetrics, spot: float):
        """Enhanced trade consideration with comprehensive analytics"""
        if self.circuit_breaker or datetime.now(IST).time() >= SAFE_TRADE_END:
            return

        # Get strategy recommendation
        strategy_name, legs_spec = self.strategy_engine.select_strategy(metrics, spot)
        if strategy_name == "WAIT": 
            return
            
        # Calculate position size with event risk multiplier
        event_risk_multiplier = self.event_intel.get_event_aware_multiplier(metrics.event_risk_score)
        simulated_max_loss_per_lot = 1000.0  # Would be calculated from strategy
        lots = self.risk_mgr.get_position_size(simulated_max_loss_per_lot, metrics, event_risk_multiplier)
        
        if lots == 0: 
            return

        # Risk check
        sim_trade_vega = 50.0 * lots 
        sim_trade_delta = 10.0 * lots 
        if not self.risk_mgr.can_open_new_trade(sim_trade_vega, sim_trade_delta, self.trades): 
            return
            
        # Execute trade
        new_trade = await self.trade_mgr.execute_strategy(strategy_name, legs_spec, lots, spot)
        if new_trade:
            self.trades.append(new_trade)
            self.total_trades += 1
            
            # FIXED: Subscribe to option instruments for real-time updates
            await self._subscribe_to_trade_instruments(new_trade)

    async def _subscribe_to_trade_instruments(self, trade: MultiLegTrade):
        """Subscribe to option instruments for real-time updates"""
        instrument_keys = [leg.instrument_key for leg in trade.legs]
        new_instruments = set(instrument_keys) - self.subscribed_instruments
        
        if new_instruments:
            await self.api.subscribe_instruments(list(new_instruments))
            self.subscribed_instruments.update(new_instruments)
            logger.info(f"Subscribed to {len(new_instruments)} new instruments")

    async def run_cycle(self):
        """Execute one trading cycle with comprehensive analytics and safety"""
        
        if not self._is_market_open() or self.circuit_breaker:
            await asyncio.sleep(1) 
            return

        try:
            # Get comprehensive market metrics
            spot, metrics = await self._get_market_metrics()
            
            if spot == 0.0:
                return  # No valid data
            
            # Periodic reconciliation
            if self.cycle_count % 240 == 0 and self.cycle_count > 0:
                 await self._startup_reconciliation()
            
            # Portfolio management
            await self._update_portfolio(spot)
            
            # Strategy consideration
            await self._consider_new_trade(metrics, spot)
            
            # Save state
            self.db.save_daily_state(
                self.risk_mgr.portfolio_metrics.daily_pnl, 
                self.risk_mgr.max_equity, 
                self.cycle_count, 
                self.total_trades
            )
            self.db.save_portfolio_snapshot(self.risk_mgr.portfolio_metrics)
            
            self.cycle_count += 1
            ENGINE_CYCLES.inc()
            
            if self.cycle_count % 10 == 0:
                logger.info(f"Cycle {self.cycle_count} completed. PnL: ₹{self.risk_mgr.portfolio_metrics.total_pnl:,.2f}")
            
        except Exception as e:
            logger.error(f"Error in run_cycle: {e}")
            await self.alerts.send_alert("CYCLE_ERROR", str(e), urgent=False)

    async def run(self, continuous: bool = True):
        """Main engine loop"""
        logger.info(f"Starting VolGuard 14.00 main loop (Continuous: {continuous}).")
        
        # Start background services
        await self.om.start()
        await self._startup_reconciliation()

        # Start WebSocket feed
        self.ws_task = asyncio.create_task(self.api.ws_connect_and_stream(self.rt_quotes))
        
        self.running = True
        while self.running:
            if not continuous:
                self.running = False
            
            start_time = time.time()
            await self.run_cycle()
            
            # Maintain consistent cycle timing
            cycle_time = time.time() - start_time
            sleep_time = max(0.1, 1.0 - cycle_time)  # Target 1-second cycles
            await asyncio.sleep(sleep_time)

    async def shutdown(self):
        """Graceful shutdown of all services"""
        logger.info("Initiating graceful shutdown of VolGuard 14.00.")
        self.running = False
        
        # Stop background tasks
        if self.ws_task:
            self.ws_task.cancel()
        await self.om.stop()
        
        # Emergency flatten if needed
        if any(t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL] for t in self.trades):
            await self._emergency_flatten()
        
        # Close connections
        await self.api.close()
        self.db.close()
        
        logger.info("VolGuard 14.00 shut down successfully.")

    def _is_market_open(self) -> bool:
        """Check if market is open for trading"""
        now = datetime.now(IST)
        if now.weekday() >= 5: 
            return False
        if now.strftime("%Y-%m-%d") in MARKET_HOLIDAYS_2025: 
            return False
        current_time = now.time()
        return MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME

    async def _emergency_flatten(self):
        """Emergency flatten all positions"""
        logger.critical("EMERGENCY FLATTEN INITIATED")
        open_trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
        if not open_trades:
            logger.info("No open trades to flatten.")
            return
            
        await asyncio.gather(*[
            self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in open_trades
        ])
        
        # Update trades list
        self.trades = [t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]]
        logger.critical(f"Emergency flatten complete. {len(open_trades) - len(self.trades)} trades closed.")

    def get_status(self) -> EngineStatus:
        """Get current engine status"""
        return EngineStatus(
            running=self.running, 
            circuit_breaker=self.circuit_breaker, 
            cycle_count=self.cycle_count, 
            total_trades=self.total_trades, 
            daily_pnl=self.daily_pnl, 
            max_equity=self.max_equity, 
            last_metrics=self.last_metrics
        )

    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status"""
        return {
            "engine": {
                "running": self.running,
                "circuit_breaker": self.circuit_breaker,
                "cycle_count": self.cycle_count,
                "active_trades": len([t for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]])
            },
            "analytics": {
                "sabr_calibrated": self.sabr.calibrated,
                "pricing_cache": self.pricing.get_cache_stats(),
                "event_risk": self.event_intel.get_event_risk_score()
            },
            "risk": self.risk_mgr.get_risk_report(),
            "alerts": self.alerts.get_alert_stats()
}
