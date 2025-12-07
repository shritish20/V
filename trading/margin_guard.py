import aiohttp
from typing import Tuple, Optional, Dict, List, Any
from core.models import MultiLegTrade
from core.config import settings
from utils.logger import get_logger
from trading.api_client import EnhancedUpstoxAPI # CRITICAL IMPORT

logger = get_logger("MarginGuard")

class MarginGuard:
    """
    FIXED: Consolidated Margin Guard.
    - Uses VIX-aware fallback.
    - Delegates network calls to the resilient EnhancedUpstoxAPI.
    """
    
    def __init__(self, api_client: EnhancedUpstoxAPI): # Requires API client injection
        self.api = api_client
        self.available_margin = None 
        self.default_vix_safety = 25.0 

    async def is_margin_ok(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        Check if sufficient margin is available using Upstox API with VIX-aware fallback.
        """
        try:
            # 1. Build Schema-Compliant Payload (MarginRequest)
            instruments_payload = []
            for leg in trade.legs:
                instruments_payload.append({
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "product": "I", # Intraday
                    "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0
                })

            # 2. Get Required Margin via resilient API client
            res_margin = await self.api.get_margin(instruments_payload)
            
            if res_margin.get("status") != "success":
                logger.error(f"Margin calculation failed: {res_margin.get('message', 'Unknown Error')}")
                return await self._fallback_margin_check(trade, current_vix)

            # Extract required margin (Schema: MarginData)
            margin_data = res_margin.get("data", {})
            required_margin = margin_data.get("required_margin", 0.0)

            # 3. Get Available Funds via resilient API client
            funds = await self.api.get_funds()
            available = funds.get("available_margin")

            if available is None:
                # Use cached margin if available, else conservative 40% of settings.ACCOUNT_SIZE
                available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
                logger.warning(f"Live funds fetch failed. Using available estimate: {available:,.0f}")

            # 4. Validation
            required_with_buffer = required_margin * 1.05 # 5% buffer
            is_sufficient = available >= required_with_buffer

            if not is_sufficient:
                logger.warning(f"❌ Margin Shortfall: Req={required_with_buffer:,.0f}, Avail={available:,.0f}")

            return is_sufficient, required_margin

        except Exception as e:
            logger.error(f"Margin Check Exception: {e}")
            return await self._fallback_margin_check(trade, current_vix)

    async def _fallback_margin_check(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        CRITICAL FIX: VIX-aware conservative fallback margin calculation.
        """
        try:
            vix = current_vix if current_vix is not None else self.default_vix_safety
            
            if vix < 15:
                margin_multiplier = 0.20
            elif vix < 20:
                margin_multiplier = 0.25
            elif vix < 30:
                margin_multiplier = 0.35
            else:
                margin_multiplier = 0.50

            estimated_margin = 0.0

            for leg in trade.legs:
                quantity = abs(leg.quantity)
                
                if leg.quantity > 0: # BUY Leg: Risk limited to premium paid
                    estimated_margin += quantity * leg.entry_price
                else:
                    # SELL Leg: Use Strike Price * Qty (Contract Value) * Multiplier
                    ref_price = getattr(leg, 'strike', 0)
                    if ref_price <= 0:
                        ref_price = 24000.0 # Safety reference for underlying
                        logger.warning("Leg missing strike price for fallback. Using safety ref.")
                    
                    leg_margin = (quantity * ref_price) * margin_multiplier
                    estimated_margin += leg_margin

            estimated_margin += settings.ACCOUNT_SIZE * 0.05 # Add buffer

            # Use cached margin if available, else conservative 40%
            available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)

            is_sufficient = available >= estimated_margin

            logger.warning(
                f"⚠ API DOWN. Using FALLBACK Logic (VIX={vix:.1f}, Mult={margin_multiplier}): "
                f"Est.Req={estimated_margin:,.0f}, Est.Avail={available:,.0f}"
            )

            return is_sufficient, estimated_margin

        except Exception as e:
            logger.critical(f"Fallback margin check crashed: {e}")
            return False, float('inf')

    async def refresh_available_margin(self):
        """
        Manually refresh available margin (call periodically)
        """
        funds = await self.api.get_funds()
        self.available_margin = funds.get("available_margin")
        
        if self.available_margin is not None:
            logger.debug(f"Available margin refreshed: ₹{self.available_margin:,.0f}")
