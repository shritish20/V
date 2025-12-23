import asyncio
from copy import deepcopy
from typing import Optional

from core.models import MultiLegTrade
from core.enums import TradeStatus, ExitReason, StrategyType
from core.config import settings
from utils.logger import setup_logger
from trading.live_order_executor import LiveOrderExecutor
from trading.margin_guard import MarginGuard

logger = setup_logger("TradeMgr")

class EnhancedTradeManager:
    """
    Orchestrates trade lifecycle, execution, and risk checks.
    - Uses LiveOrderExecutor for safe execution (Hedge First).
    - Uses MarginGuard (DB-Aware) for safety.
    - Handles Capital Allocation locking.
    """
    def __init__(self, api, db, om, pricing, risk, alerts, capital):
        self.api = api
        self.db = db
        self.om = om
        self.pricing = pricing
        self.risk = risk
        self.capital = capital
        self.feed = None
        
        # --- COMPONENT LINKING ---
        self.executor = LiveOrderExecutor(self.api, self.om)
        
        # CRITICAL FIX: Pass DB to MarginGuard so it can run Sanity Checks
        self.margin_guard = MarginGuard(self.api, self.db)

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        """
        Validates and executes a new strategy.
        Flow: Pre-Trade Risk -> Margin Check -> Capital Lock -> Execution
        """
        # 1. Pre-Trade Risk
        if not self.risk.check_pre_trade(trade):
            logger.warning(f"🚫 Risk Check Failed: {trade.id}")
            return False

        # 2. Margin Check (Now DB-Aware)
        current_vix = None
        if self.feed and hasattr(self.feed, 'rt_quotes'):
            current_vix = self.feed.rt_quotes.get(settings.MARKET_KEY_VIX)
        
        is_sufficient, margin_req = await self.margin_guard.is_margin_ok(trade, current_vix)
        if not is_sufficient:
            logger.warning(f"🚫 Margin Block: Req {margin_req:,.0f} > Avail")
            return False

        # 3. Allocate Capital (Locks DB Row via Allocator)
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if not await self.capital.allocate_capital(trade.capital_bucket.value, val, trade.id):
            logger.warning(f"🚫 Capital Lock Failed: {trade.id}")
            return False

        # 4. Execute (Using Hardened Executor)
        logger.info(f"🚀 Executing {trade.strategy_type.value} | ID: {trade.id}")
        
        success, msg = await self.executor.execute_with_hedge_priority(trade)

        if success:
            trade.status = TradeStatus.OPEN
            logger.info(f"✅ Trade {trade.id} OPENED ({msg})")
            return True
        else:
            # Release capital on failure (Rollback)
            await self.capital.release_capital(trade.capital_bucket.value, trade.id, amount=val)
            logger.error(f"❌ Execution Failed {trade.id}: {msg}")
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        """
        Closes an open trade safely.
        """
        logger.info(f"🔐 Closing Trade {trade.id} | Reason: {reason.value}")
        close_obj = deepcopy(trade)
        
        # Reverse positions for closing (Buy -> Sell, Sell -> Buy)
        for leg in close_obj.legs:
            leg.quantity = leg.quantity * -1 
            # Note: Executor will re-fetch prices, so old prices here don't matter much.
            
        # Re-use Hardened Executor
        # It handles 'Buy' legs first (Short Covering) automatically due to Hedge Priority logic
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
        """Updates internal state with live market data."""
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                if leg.current_price != quotes[leg.instrument_key]:
                    leg.current_price = quotes[leg.instrument_key]
                    updated = True
        
        # If prices changed, recalculate Greeks (if model available)
        if updated and hasattr(trade, 'calculate_trade_greeks'):
            try:
                trade.calculate_trade_greeks()
            except Exception:
                pass

    async def monitor_active_trades(self, trades):
        """Checks PnL limits for active trades."""
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
        """
        Estimates deployed margin for PnL % calculation.
        Used only for logging/display, not for safety checks.
        """
        if trade.strategy_type in [StrategyType.SHORT_STRANGLE, StrategyType.RATIO_SPREAD_PUT]:
            return 150000.0 * trade.lots
        elif trade.strategy_type == StrategyType.JADE_LIZARD:
            return 120000.0 * trade.lots
        return 60000.0 * trade.lots
