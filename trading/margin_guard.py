import aiohttp
import logging
from typing import Tuple, Optional, Dict, List, Any
from core.models import MultiLegTrade
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("MarginGuard")

class MarginGuard:
    """
    Production Margin Guard.
    - Handles Upstox Maintenance Mode (Error 423) gracefully.
    - Uses safe fallbacks during off-hours (12 AM - 5:30 AM) or API outages.
    """
    
    def __init__(self, api_client: EnhancedUpstoxAPI): 
        self.api = api_client
        self.available_margin = None 
        self.default_vix_safety = 25.0 

    async def is_margin_ok(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        Check margin with automatic fallback for API downtime/maintenance.
        """
        try:
            # 1. Build Payload
            instruments_payload = []
            for leg in trade.legs:
                instruments_payload.append({
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "product": "I", 
                    "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0
                })

            # 2. Get Required Margin
            res_margin = await self.api.get_margin(instruments_payload)
            
            # --- HANDLE UPSTOX OFF-HOURS (Error 423) ---
            # Upstox sends 423 or error code UDAPI100072 during maintenance
            if res_margin.get("code") == 423 or "UDAPI100072" in str(res_margin):
                if settings.SAFETY_MODE == "live":
                    logger.debug("🌙 Upstox Funds API sleeping (Maintenance). Using fallback.")
                return await self._fallback_margin_check(trade, current_vix)

            if res_margin.get("status") != "success":
                logger.error(f"Margin API Error: {res_margin.get('message', 'Unknown')}")
                return await self._fallback_margin_check(trade, current_vix)

            margin_data = res_margin.get("data", {})
            required_margin = margin_data.get("required_margin", 0.0)

            # 3. Get Available Funds
            funds = await self.api.get_funds()
            
            # --- HANDLE UPSTOX OFF-HOURS FOR FUNDS ---
            if funds.get("code") == 423 or "UDAPI100072" in str(funds):
                available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
            else:
                available = funds.get("available_margin")

            if available is None:
                available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
                # Log warning only if we expected live data but failed
                if settings.SAFETY_MODE == "live" and not (funds.get("code") == 423):
                    logger.warning(f"Live funds fetch failed. Using est: {available:,.0f}")

            # 4. Validation
            required_with_buffer = required_margin * 1.05 
            is_sufficient = available >= required_with_buffer
            
            if not is_sufficient:
                logger.warning(f"🚫 Margin Shortfall: Req={required_with_buffer:,.0f}, Avail={available:,.0f}")
            
            return is_sufficient, required_margin

        except Exception as e:
            logger.error(f"Margin Check Exception: {e}")
            return await self._fallback_margin_check(trade, current_vix)

    async def _fallback_margin_check(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        Conservative estimation logic when API is down or sleeping.
        """
        try:
            vix = current_vix if current_vix is not None else self.default_vix_safety
            
            # VIX-based margin multiplier (Higher VIX = Higher Margin req)
            if vix < 15: margin_multiplier = 0.20
            elif vix < 20: margin_multiplier = 0.25
            elif vix < 30: margin_multiplier = 0.35
            else: margin_multiplier = 0.50

            estimated_margin = 0.0
            for leg in trade.legs:
                quantity = abs(leg.quantity)
                if leg.quantity > 0: 
                    # BUY: Full premium
                    estimated_margin += quantity * leg.entry_price
                else:
                    # SELL: Estimate based on strike
                    ref_price = getattr(leg, 'strike', 0)
                    if ref_price <= 0: ref_price = 24000.0 
                    estimated_margin += (quantity * ref_price) * margin_multiplier

            estimated_margin += settings.ACCOUNT_SIZE * 0.05 # Add 5% buffer

            # Conservative Available Margin
            available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
            
            is_sufficient = available >= estimated_margin
            return is_sufficient, estimated_margin

        except Exception as e:
            logger.critical(f"Fallback math crashed: {e}")
            return False, float('inf')

    async def refresh_available_margin(self):
        """Update cached margin when API is awake"""
        funds = await self.api.get_funds()
        if funds.get("status") == "success":
            val = funds.get("data", {}).get("available_margin")
            if val:
                self.available_margin = val
