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
    FIXED: Corrected PnL calculation logic for Debit vs Credit strategies.
    FIXED: MarginGuard now properly initialized with API client.
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
        FIXED: Real-time Profit/Loss Monitoring logic.
        Now correctly handles Debit Spreads vs Credit Spreads calculation.
        """
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue

            # Calculate Absolute PnL
            pnl = trade.total_unrealized_pnl()
            
            # Determine Basis for % Calculation
            net_premium_total = trade.net_premium_per_share * trade.lots * settings.LOT_SIZE
            
            basis = 1.0  # Default to avoid div/0
            
            if net_premium_total > 0:
                # CREDIT Strategy (e.g., Short Strangle, Iron Condor)
                # Target is % of Max Profit (Premium Received)
                # PnL is positive when premium decays (good for seller)
                basis = net_premium_total
            elif net_premium_total < 0:
                # DEBIT Strategy (e.g., Bull Call Spread, Long Strangle)
                # Target is % Return on Investment (ROI on Cost)
                # PnL is positive when spread widens (good for buyer)
                basis = abs(net_premium_total)
            else:
                # Zero Cost (Unlikely, but fallback to conservative margin estimate)
                basis = 10000.0

            pnl_pct = (pnl / basis) * 100

            # Detailed logging for monitoring
            if abs(pnl_pct) > 20:  # Log if PnL > 20%
                logger.debug(
                    f"Trade {trade.id} | {trade.strategy_type.value} | "
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
