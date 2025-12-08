import asyncio
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
    - Correct PnL calculation for Iron Condors (max loss based on spread width)
    - Proper handling of credit vs debit spreads
    - Enhanced logging for monitoring
    - MarginGuard properly initialized with API client
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
        
        # CRITICAL FIX: Pass API client to MarginGuard
        self.margin_guard = MarginGuard(self.api)

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        """
        Entry Point: Atomic Batch Execution with Margin Check
        """
        # 1. Pre-Trade Risk Check (Greeks/Exposure)
        if not self.risk.check_pre_trade(trade):
            logger.warning(f"🚫 Risk Check Failed for {trade.id}")
            return False

        # 2. FIXED: Margin Check (Buying Power) - Now properly initialized
        # Get current VIX for VIX-aware fallback
        current_vix = None
        if self.feed and hasattr(self.feed, 'rt_quotes'):
            current_vix = self.feed.rt_quotes.get(settings.MARKET_KEY_VIX)
        
        is_sufficient, margin_req = await self.margin_guard.is_margin_ok(trade, current_vix)
        
        if not is_sufficient:
            logger.warning(f"🚫 Margin Block: Required {margin_req:.0f} > Available")
            return False

        # 3. Allocate Capital (Internal Buckets)
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
                # Keep capital allocated but mark trade as problematic
                return False
        else:
            # Release capital on failure
            await self.capital.release_capital(trade.capital_bucket.value, trade.id)
            logger.error(f"❌ Trade {trade.id} Execution Failed")
            return False

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"🔐 Closing Trade {trade.id} - Reason: {reason.value}")
        
        # Create a reverse trade object for closing
        # Use deepcopy to avoid modifying original trade
        from copy import deepcopy
        close_trade_obj = deepcopy(trade)
        
        # CRITICAL: Flip quantities for exit (Buy -> Sell, Sell -> Buy)
        for leg in close_trade_obj.legs:
            leg.quantity = leg.quantity * -1 
            leg.entry_price = 0.0  # Market order for immediate exit
            
        # Execute closing batch
        success = await self.executor.place_multi_leg_batch(close_trade_obj)
        
        if success:
            await self.executor.verify_fills(close_trade_obj, timeout=15)
            logger.info(f"✅ Trade {trade.id} Closed Successfully")
        else:
            logger.error(f"⚠️ Trade {trade.id} Exit Had Friction - Check Manually")
        
        # Always mark closed internally to prevent loops
        trade.status = TradeStatus.CLOSED
        trade.exit_reason = reason
        await self.capital.release_capital(trade.capital_bucket.value, trade.id)

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        """Updates leg prices from live quotes and recalculates Greeks"""
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
        """
        PRODUCTION FIXED v2.0: Real-time Profit/Loss Monitoring
        - Correct Iron Condor max loss calculation
        - Proper handling of credit vs debit spreads
        - Enhanced logging for monitoring
        """
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue

            # Calculate Absolute PnL
            pnl = trade.total_unrealized_pnl()
            
            # CRITICAL FIX: Determine basis for % calculation based on strategy type
            basis = self._calculate_trade_basis(trade)
            
            pnl_pct = (pnl / basis) * 100 if basis > 0 else 0.0

            # Enhanced logging for monitoring
            if abs(pnl_pct) > 15:  # Log if PnL > 15%
                logger.info(
                    f"📊 Trade {trade.id[:8]} | {trade.strategy_type.value} | "
                    f"PnL: ₹{pnl:,.0f} ({pnl_pct:+.1f}%) | "
                    f"Basis: ₹{basis:,.0f}"
                )

            # Take Profit Logic
            if pnl_pct >= (settings.TAKE_PROFIT_PCT * 100):
                logger.info(f"💰 PROFIT TARGET HIT ({pnl_pct:+.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.PROFIT_TARGET)
            
            # Stop Loss Logic
            elif pnl_pct <= -(settings.STOP_LOSS_PCT * 100):
                logger.warning(f"🛑 STOP LOSS HIT ({pnl_pct:+.1f}%). Closing {trade.id}")
                await self.close_trade(trade, ExitReason.STOP_LOSS)

    def _calculate_trade_basis(self, trade: MultiLegTrade) -> float:
        """
        PRODUCTION FIX v2.0: Calculate proper basis for PnL percentage
        
        Credit Spreads (Iron Condor, Short Strangle):
        - Basis = Premium Received (your max profit)
        - Target: % of max profit captured
        
        Debit Spreads (Bull Call Spread, Long Strangle):
        - Basis = Premium Paid (your investment)
        - Target: % ROI on investment
        
        Defined Risk Spreads (Iron Condor):
        - Basis = Max Loss = (Spread Width - Net Credit) × Lot Size
        """
        net_premium_total = trade.net_premium_per_share * trade.lots * settings.LOT_SIZE
        
        # Strategy-specific logic
        if trade.strategy_type == StrategyType.IRON_CONDOR:
            # Calculate true max loss based on spread widths
            ce_legs = [l for l in trade.legs if l.option_type == "CE"]
            pe_legs = [l for l in trade.legs if l.option_type == "PE"]
            
            if len(ce_legs) >= 2 and len(pe_legs) >= 2:
                # Get strikes for each wing
                ce_strikes = sorted([l.strike for l in ce_legs])
                pe_strikes = sorted([l.strike for l in pe_legs])
                
                # Calculate spread widths
                ce_width = ce_strikes[-1] - ce_strikes[0]
                pe_width = pe_strikes[-1] - pe_strikes[0]
                
                # Max loss is the larger spread width minus net credit
                max_spread_width = max(ce_width, pe_width)
                max_loss_per_share = max_spread_width - abs(net_premium_total / (trade.lots * settings.LOT_SIZE))
                max_loss = max_loss_per_share * trade.lots * settings.LOT_SIZE
                
                return max(max_loss, 1000.0)  # Minimum basis to avoid div/0
            else:
                # Fallback if leg structure is unexpected
                return max(abs(net_premium_total), 10000.0)
        
        elif trade.strategy_type in [StrategyType.SHORT_STRANGLE, StrategyType.ATM_STRADDLE]:
            # Credit Strategy: Basis is premium received (max profit)
            if net_premium_total > 0:
                return net_premium_total
            else:
                # If somehow net premium is negative, use absolute value
                return max(abs(net_premium_total), 10000.0)
        
        elif trade.strategy_type in [StrategyType.BULL_PUT_SPREAD]:
            # Debit Strategy: Basis is premium paid (investment)
            if net_premium_total < 0:
                return abs(net_premium_total)
            else:
                # If somehow net premium is positive, use absolute value
                return max(abs(net_premium_total), 10000.0)
        
        else:
            # Generic fallback for other strategies
            if abs(net_premium_total) > 0:
                return abs(net_premium_total)
            else:
                # Conservative fallback if no premium data
                # Use 10% of lot value as basis
                return trade.lots * settings.LOT_SIZE * 150.0
