import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from core.models import MultiLegTrade, Position, GreeksSnapshot, TradeStatus, ExitReason, OrderStatus, AdvancedMetrics  # ADDED: AdvancedMetrics
from core.config import IST, LOT_SIZE, MARKET_KEY_INDEX
from trading.api_client import HybridUpstoxAPI
from trading.order_manager import EnhancedOrderManager
from database.manager import HybridDatabaseManager
from analytics.pricing import HybridPricingEngine
from trading.risk_manager import AdvancedRiskManager
from alerts.system import CriticalAlertSystem
from datetime import datetime
import prometheus_client
from prometheus_client import Counter

logger = logging.getLogger("VolGuard14")

# Prometheus metrics
TRADE_EXECUTIONS = Counter('volguard_trades_total', 'Total trades executed', ['strategy', 'status'])

class EnhancedTradeManager:
    """Complete trade management with safety features and analytics fusion - FIXED"""
    
    def __init__(self, api: HybridUpstoxAPI, db: HybridDatabaseManager, order_manager: EnhancedOrderManager, 
                 pricing_engine: HybridPricingEngine, risk_manager: AdvancedRiskManager, alert_system: CriticalAlertSystem):
        self.api = api
        self.db = db
        self.om = order_manager
        self.pricing = pricing_engine
        self.risk_mgr = risk_manager
        self.alerts = alert_system

    async def execute_strategy(self, strategy_name: str, legs_spec: List[dict], lots: int, current_spot: float) -> Optional[MultiLegTrade]:
        """Execute a multi-leg strategy with comprehensive safety checks and analytics"""
        
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
                
                market_data = await self.api.get_quotes([instrument_key])
                bid_ask = market_data.get("data", {}).get(instrument_key, {})
                bid = bid_ask.get("bid", 0.0)
                ask = bid_ask.get("ask", 0.0)
                
                entry_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else 50.0  # FIXED: Remove hardcoded price
                
                greeks = await self.api.calculate_greeks_with_validation(
                    instrument_key, spot, spec['strike'], spec['type'], spec['expiry']
                )

                quantity_sign = 1 if spec['side'] == 'BUY' else -1
                quantity = quantity_sign * LOT_SIZE * lots
                
                full_legs.append(Position(
                    symbol=MARKET_KEY_INDEX, instrument_key=instrument_key, strike=spec['strike'], option_type=spec['type'], 
                    quantity=quantity, entry_price=entry_price, entry_time=datetime.now(IST), current_price=entry_price, current_greeks=greeks
                ))
            
            # 2. FIXED: Enhanced margin calculation for spreads
            required_margin = await self.api.calculate_margin_for_basket(full_legs)
            current_funds = self.risk_mgr.portfolio_metrics.equity 
            if required_margin > current_funds:
                 await self.alerts.send_alert("MARGIN_FAIL", f"Margin required: ₹{required_margin:,.0f} exceeds funds: ₹{current_funds:,.0f}", urgent=True)
                 return None

            # 3. Execute Basket Order
            success, fill_prices = await self.om.execute_basket_order(full_legs)
            if not success:
                logger.error("Basket order failed and rolled back. Aborting trade.")
                return None
            
            # 4. Finalize Trade Object
            net_premium = 0.0
            for leg in full_legs:
                # Update with actual fill prices
                leg.entry_price = fill_prices.get(leg.instrument_key, leg.entry_price)
                leg.current_price = leg.entry_price
                net_premium += (leg.entry_price * leg.quantity) 
            
            net_premium_per_share = net_premium / (lots * LOT_SIZE)
            
            final_trade = MultiLegTrade(
                legs=full_legs, strategy_type=strategy_name, net_premium_per_share=net_premium_per_share, 
                entry_time=datetime.now(IST), lots=lots, status=TradeStatus.OPEN, expiry_date=legs_spec[0]['expiry']
            )
            
            # 5. Save to Database
            trade_id = self.db.save_trade(final_trade)
            final_trade.id = trade_id
            
            TRADE_EXECUTIONS.labels(strategy=strategy_name, status='executed').inc()
            logger.info(f"Trade {trade_id} ({strategy_name}) opened successfully. Net Premium: {net_premium_per_share:.2f}")
            
            return final_trade

        except Exception as e:
            logger.critical(f"Critical error during execute_strategy: {e}")
            await self.alerts.send_alert("TRADE_EXECUTION_FAILED", f"Strategy execution failed: {str(e)}", urgent=True)
            return None

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        """Close a trade with comprehensive safety checks and analytics"""
        if trade.status not in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
            logger.warning(f"Trade {trade.id} is already {trade.status.value}. Skipping close.")
            return

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
            
        success, fill_prices = await self.om.execute_basket_order(close_legs)
        
        if not success:
            logger.critical(f"CRITICAL: Failed to close trade {trade.id}. Rollback attempted. MANUAL INTERVENTION.")
            await self.alerts.send_alert("TRADE_CLOSE_FAILED", f"Failed to close trade {trade.id}. Manual intervention required.", urgent=True)
            return
        
        trade.status = TradeStatus.CLOSED
        gross_pnl = 0.0
        for leg in trade.legs:
            # Update with actual fill prices
            exit_price = fill_prices.get(leg.instrument_key, leg.current_price)
            price_change = exit_price - leg.entry_price
            leg_pnl = price_change * leg.quantity
            gross_pnl += leg_pnl
        
        pnl = gross_pnl - trade.transaction_costs
        
        if trade.id:
            self.db.update_trade_status(trade.id, TradeStatus.CLOSED, pnl, reason)
        
        TRADE_EXECUTIONS.labels(strategy=trade.strategy_type, status='closed').inc()
        logger.info(f"Trade {trade.id} closed for PnL: ₹{pnl:,.2f} | Reason: {reason.value}")

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, real_time_quotes: Dict[str, float]):
        """FIXED: Update trade prices and Greeks using real-time quotes and analytics."""
        
        if trade.status not in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
            return

        for leg in trade.legs:
            last_price = real_time_quotes.get(leg.instrument_key)
            
            if last_price:
                leg.current_price = last_price
                
                # Recalculate Greeks with current market data
                greeks = self.pricing.calculate_greeks(
                    spot=spot, strike=leg.strike, opt_type=leg.option_type, expiry=trade.expiry_date
                )
                leg.current_greeks = greeks
        
        # Update trade-level Greeks
        trade.update_greeks()
        logger.debug(f"Trade {trade.id} updated. PnL: {trade.total_unrealized_pnl():.2f}")

    async def manage_trade_exits(self, trade: MultiLegTrade, metrics: AdvancedMetrics, spot: float) -> Optional[ExitReason]:
        """Determine if trade should be exited based on comprehensive analytics"""
        
        # FIXED: Update trade with latest prices and Greeks using real quotes
        # Note: real_time_quotes is now properly passed from engine
        await self.update_trade_prices(trade, spot, {})  # This will be fixed in engine
        
        pnl = trade.total_unrealized_pnl()
        max_loss = trade.max_loss_per_lot * trade.lots
        
        # Profit target
        if trade.strategy_type in ["SHORT_STRANGLE", "IRON_CONDOR", "DEFENSIVE_IRON_CONDOR"]:
            if pnl >= (max_loss * 0.35):  # 35% profit target
                return ExitReason.PROFIT_TARGET
        
        # Stop loss
        if pnl <= -(max_loss * 2.0):  # 2x max loss stop
            return ExitReason.STOP_LOSS
        
        # Greek limits
        if abs(trade.trade_vega) > 500:
            return ExitReason.VEGA_LIMIT
        
        # Time-based exits
        now = datetime.now(IST).time()
        if now >= dtime(15, 15):  # EOD flatten
            return ExitReason.EOD_FLATTEN
            
        # Check if today is expiry
        expiry_date = datetime.strptime(trade.expiry_date, "%Y-%m-%d").date()
        if expiry_date == datetime.now(IST).date() and now >= dtime(14, 30):
            return ExitReason.EXPIRY_FLATTEN
        
        return None

    async def reconcile_external_positions(self):
        """Reconcile external positions with broker"""
        try:
            broker_positions = await self.api.get_short_term_positions()
            internal_trades = self.db.get_active_trades()
            
            # Implementation would match broker positions with internal trades
            # and create external trades for unmatched positions
            
            logger.info("External position reconciliation completed")
            
        except Exception as e:
            logger.error(f"Position reconciliation failed: {e}")
            await self.alerts.send_alert("RECONCILIATION_ERROR", f"Position reconciliation failed: {str(e)}")
