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
    """
    PRODUCTION FIXED v2.0:
    - Correct PnL calculation for Iron Condors
    - Proper handling of credit vs debit spreads
    - MarginGuard properly initialized
    - Stateless Capital Release Support
    """
    def __init__(self, api, db, om, pricing, risk, alerts, capital):
        self.api = api
        self.db = db
        self.om = om
        self.pricing = pricing
        self.risk = risk
        self.capital = capital
        self.feed = None
        self.executor = LiveOrderExecutor(self.api)
        self.margin_guard = MarginGuard(self.api)

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        """
        Entry Point: Atomic Batch Execution with Margin Check
        """
        # 1. Pre-Trade Risk Check
        if not self.risk.check_pre_trade(trade):
            logger.warning(f"🚫 Risk Check Failed for {trade.id}")
            return False

        # 2. Margin Check
        current_vix = None
        if self.feed and hasattr(self.feed, 'rt_quotes'):
            current_vix = self.feed.rt_quotes.get(settings.MARKET_KEY_VIX)
        
        is_sufficient, margin_req = await self.margin_guard.is_margin_ok(trade, current_vix)
        if not is_sufficient:
            logger.warning(f"🚫 Margin Block: Required {margin_req:.0f} > Available")
            return False

        # 3. Allocate Capital
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        if not await self.capital.allocate_capital(trade.capital_bucket.value, val, trade.id):
            logger.warning(f"🚫 Capital Allocation Failed for {trade.id}")
            return False

        # 4. Atomic Execution
        logger.info(f"🚀 Executing {trade.strategy_type.value} - Trade ID: {trade.id}")
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
            # Release capital on failure
            # CRITICAL FIX: Pass amount to stateless allocator
            await self.capital.release_capital(
                trade.capital_bucket.value, 
                trade.id, 
                amount=val
            )
            logger.error(f"❌ Trade {trade.id} Execution Failed")
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"🔐 Closing Trade {trade.id} - Reason: {reason.value}")
        
        close_trade_obj = deepcopy(trade)
        for leg in close_trade_obj.legs:
            leg.quantity = leg.quantity * -1
            leg.entry_price = 0.0 # Market order

        success = await self.executor.place_multi_leg_batch(close_trade_obj)
        
        if success:
            await self.executor.verify_fills(close_trade_obj, timeout=15)
            logger.info(f"✅ Trade {trade.id} Closed Successfully")
        else:
            logger.error(f"⚠️ Trade {trade.id} Exit Had Friction - Check Manually")

        trade.status = TradeStatus.CLOSED
        trade.exit_reason = reason
        
        # CRITICAL FIX: Calculate amount for stateless release
        val_to_release = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        
        await self.capital.release_capital(
            trade.capital_bucket.value, 
            trade.id, 
            amount=val_to_release
        )

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                new_price = quotes[leg.instrument_key]
                if new_price != leg.current_price:
                    leg.current_price = new_price
                    updated = True
        
        if updated:
            trade.calculate_trade_greeks()

    async def monitor_active_trades(self, trades):
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue

            pnl = trade.total_unrealized_pnl()
            basis = self._calculate_trade_basis(trade)
            pnl_pct = (pnl / basis) * 100 if basis > 0 else 0.0

            if abs(pnl_pct) > 15:
                logger.info(
                    f"📊 Trade {trade.id[:8]} | {trade.strategy_type.value} | "
                    f"PnL: {pnl:,.0f} ({pnl_pct:+.1f}%) | Basis: {basis:,.0f}"
                )

            if pnl_pct >= (settings.TAKE_PROFIT_PCT * 100):
                logger.info(f"💰 PROFIT TARGET HIT ({pnl_pct:+.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.PROFIT_TARGET)
            
            elif pnl_pct <= -(settings.STOP_LOSS_PCT * 100):
                logger.warning(f"🛑 STOP LOSS HIT ({pnl_pct:+.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.STOP_LOSS)

    def _calculate_trade_basis(self, trade: MultiLegTrade) -> float:
        net_premium_total = trade.net_premium_per_share * trade.lots * settings.LOT_SIZE
        
        if trade.strategy_type == StrategyType.IRON_CONDOR:
            ce_legs = [l for l in trade.legs if l.option_type == "CE"]
            pe_legs = [l for l in trade.legs if l.option_type == "PE"]
            if len(ce_legs) >= 2 and len(pe_legs) >= 2:
                ce_strikes = sorted([l.strike for l in ce_legs])
                pe_strikes = sorted([l.strike for l in pe_legs])
                ce_width = ce_strikes[-1] - ce_strikes[0]
                pe_width = pe_strikes[-1] - pe_strikes[0]
                max_spread_width = max(ce_width, pe_width)
                max_loss_per_share = max_spread_width - abs(net_premium_total / (trade.lots * settings.LOT_SIZE))
                max_loss = max_loss_per_share * trade.lots * settings.LOT_SIZE
                return max(max_loss, 1000.0)
            else:
                return max(abs(net_premium_total), 10000.0)
                
        elif trade.strategy_type in [StrategyType.SHORT_STRANGLE, StrategyType.ATM_STRADDLE]:
            if net_premium_total > 0:
                return net_premium_total
            else:
                return max(abs(net_premium_total), 10000.0)
                
        elif trade.strategy_type in [StrategyType.BULL_PUT_SPREAD]:
            if net_premium_total < 0:
                return abs(net_premium_total)
            else:
                return max(abs(net_premium_total), 10000.0)
        else:
            if abs(net_premium_total) > 0:
                return abs(net_premium_total)
            else:
                return trade.lots * settings.LOT_SIZE * 150.0
