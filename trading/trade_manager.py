from core.models import MultiLegTrade, Order, OrderStatus
from core.enums import TradeStatus, ExitReason
from core.config import settings
from utils.logger import setup_logger
from trading.live_order_executor import LiveOrderExecutor # Correct Import

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
        
        # INJECT THE NEW EXECUTOR
        self.executor = LiveOrderExecutor(self.api)

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        """
        Entry Point: Uses Atomic Batch Execution
        """
        # 1. Pre-Trade Risk Check
        if not self.risk.check_pre_trade(trade):
            return False

        # 2. Allocate Capital
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if not await self.capital.allocate_capital(trade.capital_bucket.value, val, trade.id):
            return False

        # 3. ATOMIC EXECUTION (The Counter)
        success = await self.executor.place_multi_leg_batch(trade)
        
        if success:
            # 4. Verify Fills
            filled = await self.executor.verify_fills(trade)
            if filled:
                trade.status = TradeStatus.OPEN
                logger.info(f"✅ Trade {trade.id} Live & Filled")
                return True
            else:
                logger.critical(f"❌ Trade {trade.id} Partial Fill / Timeout! Manual Intervention Needed.")
                # Optional: Trigger auto-flatten for this specific trade ID here
                return False
        else:
            # Release capital if rejected
            await self.capital.release_capital(trade.capital_bucket.value, trade.id)
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"Closing Trade {trade.id} Reason: {reason}")
        
        # Create a reverse trade object for closing
        close_trade_obj = trade.copy()
        for leg in close_trade_obj.legs:
            # Flip side for exit
            leg.quantity = leg.quantity * -1 
            # Market order for exit
            leg.entry_price = 0.0 
            
        # Execute closing batch
        await self.executor.place_multi_leg_batch(close_trade_obj)
        
        trade.status = TradeStatus.CLOSED
        trade.exit_reason = reason
        await self.capital.release_capital(trade.capital_bucket.value, trade.id)

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        # ... (Same as before) ...
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                leg.current_price = quotes[leg.instrument_key]
                updated = True
        if updated:
            trade.calculate_trade_greeks()

    async def monitor_active_trades(self, trades):
        # ... (Same as before) ...
        pass

