from core.config import settings
from core.models import Order, OrderType
from utils.logger import get_logger

logger = get_logger("HedgeManager")

class PortfolioHedgeManager:
    def __init__(self, api, om=None, risk_mgr=None):
        self.api = api
        self.om = om
        self.risk_mgr = risk_mgr

    async def check_and_hedge(self, portfolio_delta: float, current_vix: float):
        # 1. Volatility Filter
        # Only hedge if VIX is high enough to warrant cost
        if current_vix < settings.VIX_MIN_THRESHOLD:
            return

        # 2. Threshold Check
        # Don't hedge small delta drifts
        HEDGE_THRESHOLD = 500
        if abs(portfolio_delta) < HEDGE_THRESHOLD:
            return

        logger.critical(
            f"⚠️ PORTFOLIO SKEWED! Delta: {portfolio_delta:.0f}. Initiating Hedge."
        )
        
        # We aim to neutralize 50% of the delta to avoid over-hedging
        target_reduction = portfolio_delta * 0.5
        lots_needed = int(abs(target_reduction) / settings.LOT_SIZE)
        
        if lots_needed == 0:
            return

        # 3. Determine Side & Symbol
        txn_type = "SELL" if portfolio_delta > 0 else "BUY"
        future_symbol = await self.api.get_current_future_symbol(
            settings.MARKET_KEY_INDEX
        )
        
        # 4. Get LTP for Limit Order Calculation
        # CRITICAL: Avoid market orders during high vol events to prevent slippage
        quotes = await self.api.get_quotes([future_symbol])
        ltp = 0.0
        
        # Handle Upstox response structure
        if "data" in quotes and future_symbol in quotes["data"]:
             ltp = quotes["data"][future_symbol].get("last_price", 0.0)
        
        if ltp == 0.0:
            logger.error("❌ Cannot Hedge: No Future LTP found")
            return

        # Place Marketable Limit Order (LTP +/- 0.5%)
        # This ensures execution but prevents "infinite" slippage in flash crashes
        limit_buffer = ltp * 0.005
        limit_price = ltp + limit_buffer if txn_type == "BUY" else ltp - limit_buffer
        
        order = Order(
            instrument_key=future_symbol, # Passed as instrument_token in API V2
            transaction_type=txn_type,
            quantity=lots_needed * settings.LOT_SIZE,
            price=round(limit_price, 2),
            order_type=OrderType.LIMIT, 
            product="I",
        )
        
        ok, oid = await self.api.place_order(order)
        if ok:
            logger.critical(f"✅ Hedge placed (Limit): {txn_type} {lots_needed} lots @ {limit_price:.2f} ({oid})")
        else:
            logger.error("❌ Hedge order failed.")
