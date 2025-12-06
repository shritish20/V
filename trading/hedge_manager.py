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
        if current_vix < settings.VIX_MIN_THRESHOLD:
            return

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

        # Simple Futures Hedge
        txn_type = "SELL" if portfolio_delta > 0 else "BUY"
        future_symbol = await self.api.get_current_future_symbol(
            settings.MARKET_KEY_INDEX
        )
        
        order = Order(
            instrument_key=future_symbol,
            transaction_type=txn_type,
            quantity=lots_needed * settings.LOT_SIZE,
            price=0.0,
            order_type=OrderType.MARKET,
            product="I",
        )
        
        ok, oid = await self.api.place_order(order)
        if ok:
            logger.critical(f"✅ Hedge placed via Futures: {txn_type} {lots_needed} lots ({oid})")
        else:
            logger.error("❌ Hedge order failed.")
