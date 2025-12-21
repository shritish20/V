import asyncio
from copy import deepcopy
from core.models import MultiLegTrade
from core.enums import TradeStatus, ExitReason, StrategyType
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
        
        # FIX 1: Pass BOTH API and OrderManager
        self.executor = LiveOrderExecutor(self.api, self.om)
        self.margin_guard = MarginGuard(self.api)

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        # 1. Pre-Trade Risk
        if not self.risk.check_pre_trade(trade):
            logger.warning(f"🚫 Risk Check Failed: {trade.id}")
            return False

        # 2. Margin Check
        current_vix = None
        if self.feed and hasattr(self.feed, 'rt_quotes'):
            current_vix = self.feed.rt_quotes.get(settings.MARKET_KEY_VIX)
        
        is_sufficient, margin_req = await self.margin_guard.is_margin_ok(trade, current_vix)
        if not is_sufficient:
            logger.warning(f"🚫 Margin Block: Req {margin_req:,.0f} > Avail")
            return False

        # 3. Allocate Capital (Locks DB Row)
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if not await self.capital.allocate_capital(trade.capital_bucket.value, val, trade.id):
            logger.warning(f"🚫 Capital Lock Failed: {trade.id}")
            return False

        # 4. Execute (Using Hardened Executor)
        logger.info(f"🚀 Executing {trade.strategy_type.value} | ID: {trade.id}")
        
        # FIX 2: Use correct method and unpack tuple result
        success, msg = await self.executor.execute_with_hedge_priority(trade)

        if success:
            trade.status = TradeStatus.OPEN
            logger.info(f"✅ Trade {trade.id} OPENED ({msg})")
            return True
        else:
            # Release capital on failure
            await self.capital.release_capital(trade.capital_bucket.value, trade.id, amount=val)
            logger.error(f"❌ Execution Failed {trade.id}: {msg}")
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"🔐 Closing Trade {trade.id} | Reason: {reason.value}")
        close_obj = deepcopy(trade)
        
        # Reverse positions for closing
        for leg in close_obj.legs:
            leg.quantity = leg.quantity * -1 
            # Note: LiveOrderExecutor will fetch fresh prices for Limits
            
        # FIX 3: Re-use Hardened Executor for Closing
        # This is safe because 'execute_with_hedge_priority' splits by quantity.
        # When closing a short, we Buy (+Qty), so it treats the Buy-back as a Hedge (Priority 1).
        # This correctly prioritizes buying back shorts before selling longs.
        success, msg = await self.executor.execute_with_hedge_priority(close_obj)
        
        if success:
            logger.info(f"✅ Trade {trade.id} Closed Successfully")
            trade.status = TradeStatus.CLOSED
            trade.exit_reason = reason
            
            # Release Capital
            val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
            await self.capital.release_capital(trade.capital_bucket.value, trade.id, amount=val)
        else:
            logger.critical(f"⚠️ Trade {trade.id} Close Failed: {msg} - MANUAL INTERVENTION REQD")

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                if leg.current_price != quotes[leg.instrument_key]:
                    leg.current_price = quotes[leg.instrument_key]
                    updated = True
        if updated:
            # Assuming models.py has this method, otherwise skip
            if hasattr(trade, 'calculate_trade_greeks'):
                trade.calculate_trade_greeks()

    async def monitor_active_trades(self, trades):
        for trade in trades:
            if trade.status != TradeStatus.OPEN: continue
            
            pnl = trade.total_unrealized_pnl()
            basis = self._calculate_basis(trade)
            pnl_pct = (pnl / basis) * 100 if basis > 0 else 0

            # Logging significant moves
            if abs(pnl_pct) > 5:
                logger.info(f"📊 {trade.strategy_type.value} | PnL: {pnl:,.0f} ({pnl_pct:+.1f}%)")

            # Targets
            if pnl_pct >= (settings.TAKE_PROFIT_PCT * 100):
                await self.close_trade(trade, ExitReason.PROFIT_TARGET)
            elif pnl_pct <= -(settings.STOP_LOSS_PCT * 100):
                await self.close_trade(trade, ExitReason.STOP_LOSS)

    def _calculate_basis(self, trade: MultiLegTrade) -> float:
        # Simplified Margin Basis for PnL % Calc
        if trade.strategy_type in [StrategyType.SHORT_STRANGLE, StrategyType.RATIO_SPREAD_PUT]:
            return 150000.0 * trade.lots
        elif trade.strategy_type == StrategyType.JADE_LIZARD:
            return 120000.0 * trade.lots
        return 60000.0 * trade.lots 
