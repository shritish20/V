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
        if current_vix < settings.VIX_MIN_THRESHOLD:
            return

        # 2. Threshold Check
        HEDGE_THRESHOLD = 500
        if abs(portfolio_delta) < HEDGE_THRESHOLD:
            return

        logger.critical(
            f"⚠️ PORTFOLIO SKEWED! Delta: {portfolio_delta:.0f}. Initiating Hedge."
        )
        
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
        # (Avoids market order slippage during high vol)
        quotes = await self.api.get_quotes([future_symbol])
        ltp = quotes.get("data", {}).get(future_symbol, {}).get("last_price", 0.0)
        
        if ltp == 0.0:
            logger.error("❌ Cannot Hedge: No Future LTP found")
            return

        # Place Marketable Limit Order (LTP +/- 0.5%)
        # This ensures execution but prevents "infinite" slippage
        limit_buffer = ltp * 0.005
        limit_price = ltp + limit_buffer if txn_type == "BUY" else ltp - limit_buffer
        
        order = Order(
            instrument_key=future_symbol,
            transaction_type=txn_type,
            quantity=lots_needed * settings.LOT_SIZE,
            price=round(limit_price, 2),
            [span_0](start_span)order_type=OrderType.LIMIT,  # CHANGED FROM MARKET[span_0](end_span)
            product="I",
        )
        
        ok, oid = await self.api.place_order(order)
        if ok:
            logger.critical(f"✅ Hedge placed (Limit): {txn_type} {lots_needed} lots @ {limit_price:.2f} ({oid})")
        else:
            logger.error("❌ Hedge order failed.")
