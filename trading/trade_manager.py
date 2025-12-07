from core.models import MultiLegTrade, Order, OrderStatus
from core.enums import TradeStatus, ExitReason
from core.config import settings
from utils.logger import setup_logger
from trading.live_order_executor import LiveOrderExecutor
from trading.margin_guard import MarginGuard

logger = setup_logger("TradeMgr")

class EnhancedTradeManager:
    def __init__(self, api, db, om, pricing, risk, alerts, capital):
        self.api = api
        self.db = db
        self.om = om
        self.pricing = pricing
        self.risk = risk
        self.capital = capital
        self.feed = None
        self.executor = LiveOrderExecutor(self.api)
        # FIX: Initialize Margin Guard
        self.margin_guard = MarginGuard()

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        """
        Entry Point: Atomic Batch Execution with Margin Check
        """
        # 1. Pre-Trade Risk Check (Greeks/Exposure)
        if not self.risk.check_pre_trade(trade):
            return False

        # 2. FIX: Margin Check (Buying Power)
        is_sufficient, margin_req = await self.margin_guard.is_margin_ok(trade)
        if not is_sufficient:
            logger.warning(f"🚫 Margin Block: Required {margin_req:.0f} > Available")
            return False

        # 3. Allocate Capital (Internal Buckets)
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if not await self.capital.allocate_capital(trade.capital_bucket.value, val, trade.id):
            return False

        # 4. Atomic Execution
        success = await self.executor.place_multi_leg_batch(trade)
        
        if success:
            # 5. Verify Fills
            filled = await self.executor.verify_fills(trade)
            if filled:
                trade.status = TradeStatus.OPEN
                logger.info(f"✅ Trade {trade.id} Live & Filled")
                return True
            else:
                logger.critical(f"❌ Trade {trade.id} Partial Fill / Timeout! Manual Intervention Needed.")
                return False
        else:
            await self.capital.release_capital(trade.capital_bucket.value, trade.id)
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"Closing Trade {trade.id} Reason: {reason}")
        
        close_trade_obj = trade.copy()
        for leg in close_trade_obj.legs:
            leg.quantity = leg.quantity * -1 
            leg.entry_price = 0.0 
            
        await self.executor.place_multi_leg_batch(close_trade_obj)
        
        trade.status = TradeStatus.CLOSED
        trade.exit_reason = reason
        await self.capital.release_capital(trade.capital_bucket.value, trade.id)

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                leg.current_price = quotes[leg.instrument_key]
                updated = True
        if updated:
            trade.calculate_trade_greeks()

    async def monitor_active_trades(self, trades):
        """
        FIX: Real-time Profit/Loss Monitoring logic.
        """
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue

            # Calculate PnL % based on Premium Captured
            # For selling strategies: Max Profit = Net Premium
            pnl = trade.total_unrealized_pnl()
            max_profit = trade.net_premium_per_share * trade.lots * settings.LOT_SIZE 
            
            # Avoid division by zero
            if max_profit <= 0: max_profit = 1.0 
            
            pnl_pct = (pnl / max_profit) * 100

            # Take Profit (Default 50% of max profit)
            if pnl_pct >= (settings.TAKE_PROFIT_PCT * 100):
                logger.info(f"💰 Target Hit ({pnl_pct:.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.PROFIT_TARGET)
            
            # Stop Loss (Default -200% of max profit)
            elif pnl_pct <= -(settings.STOP_LOSS_PCT * 100):
                logger.warning(f"🛑 Stop Loss Hit ({pnl_pct:.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.STOP_LOSS)
