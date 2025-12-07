import aiohttp
from typing import Tuple, Optional
from core.models import MultiLegTrade
from core.config import settings, get_full_url
from utils.logger import get_logger

logger = get_logger("MarginGuard")

class MarginGuard:
    """
    FIXED: Robust margin validation with VIX-aware fallback logic.
    Addresses Critical Issue #1 from Code Review: "Margin Guard Implementation Incomplete"
    """
    
    def __init__(self):
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.available_margin = None  # Caches the last known good margin value

    async def is_margin_ok(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        Check if sufficient margin is available using Upstox API with VIX-aware fallback.
        
        Args:
            trade (MultiLegTrade): The trade to validate
            current_vix (float, optional): Current market volatility for risk scaling
        
        Returns:
            Tuple[bool, float]: (is_sufficient, required_margin)
        """
        try:
            # Build instruments payload for margin calculation
            instruments = []
            for leg in trade.legs:
                instruments.append({
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "product": "I",  # Intraday
                    "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0
                })

            # Call Upstox margin API
            url = get_full_url("margin")
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            payload = {"instruments": instruments}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    # If API fails (non-200 or error status), trigger fallback immediately
                    if resp.status != 200:
                        logger.error(f"Margin API HTTP error: {resp.status}")
                        return await self._fallback_margin_check(trade, current_vix)

                    data = await resp.json()
                    
                    if data.get("status") != "success":
                        logger.error(f"Margin API logical error: {data}")
                        return await self._fallback_margin_check(trade, current_vix)

                    # Extract required margin
                    margin_data = data.get("data", {})
                    # 'total_margin' is usually the most accurate field for entry requirements
                    required_margin = margin_data.get("total_margin", margin_data.get("required_margin", 0.0))
                    
                    # Fetch available margin
                    available = await self._get_available_margin()
                    
                    if available is None:
                        # If we have a cached value from earlier, use it; otherwise be conservative
                        available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.5)
                        logger.warning(f"Live funds fetch failed. Using available estimate: {available}")

                    # Add 5% buffer for slippage/volatility during execution
                    required_with_buffer = required_margin * 1.05

                    is_sufficient = available >= required_with_buffer

                    if is_sufficient:
                        logger.info(
                            f"✅ Margin OK: Req={required_with_buffer:.0f}, Avail={available:.0f}"
                        )
                    else:
                        logger.warning(
                            f"❌ Insufficient Margin: Req={required_with_buffer:.0f}, "
                            f"Avail={available:.0f}, Shortfall={required_with_buffer - available:.0f}"
                        )

                    return is_sufficient, required_margin

        except Exception as e:
            logger.exception(f"Critical exception in margin check: {e}")
            return await self._fallback_margin_check(trade, current_vix)

    async def _get_available_margin(self) -> Optional[float]:
        """
        Fetch available margin from Upstox. Updates internal cache.
        """
        try:
            url = f"{settings.API_BASE_V2}/user/get-funds-and-margin"
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            }
            params = {"segment": "SEC"}  # Securities segment

            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        logger.error(f"Funds API failed: {resp.status}")
                        return None

                    data = await resp.json()
                    
                    if data.get("status") != "success":
                        return None

                    # Extract available margin from equity segment
                    equity_data = data.get("data", {}).get("equity", {})
                    available_margin = equity_data.get("available_margin", 0.0)
                    
                    # Update cache
                    self.available_margin = available_margin
                    return available_margin

        except Exception as e:
            logger.error(f"Failed to fetch available margin: {e}")
            return None

    async def _fallback_margin_check(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        """
        CRITICAL FIX: VIX-aware conservative fallback margin calculation.
        Replaces the unsafe static 20% multiplier.
        """
        try:
            # 1. Determine Margin Multiplier based on Volatility Regime
            # If VIX is unknown, assume High Volatility (Safety First)
            vix = current_vix if current_vix is not None else 25.0
            
            if vix < 15:
                # Low Volatility: ~20% of contract value
                margin_multiplier = 0.20
            elif vix < 20:
                # Normal Volatility: ~25% of contract value
                margin_multiplier = 0.25
            elif vix < 30:
                # High Volatility: ~35% of contract value
                margin_multiplier = 0.35
            else:
                # Extreme Volatility / Crash Mode: 50%
                margin_multiplier = 0.50

            estimated_margin = 0.0

            for leg in trade.legs:
                quantity = abs(leg.quantity)
                
                if leg.quantity > 0:
                    # BUY Leg: Risk is limited to Premium Paid
                    estimated_margin += quantity * leg.entry_price
                else:
                    # SELL Leg: Risk is high. 
                    # Use Strike Price * Qty (Contract Value) * Multiplier
                    # If strike is unavailable, use entry_price * 100 as a rough heuristic or fail safe
                    ref_price = getattr(leg, 'strike', 0)
                    if ref_price <= 0:
                        # Fallback if strike missing: heavy penalty on premium or hardcoded Nifty level
                        # Assuming Nifty ~24000 for safety if strike is 0/missing
                        ref_price = 24000.0 
                        logger.warning(f"Leg missing strike price for fallback. Using safety ref: {ref_price}")
                    
                    leg_margin = (quantity * ref_price) * margin_multiplier
                    estimated_margin += leg_margin

            # Add general account buffer (5% of total account size)
            estimated_margin += settings.ACCOUNT_SIZE * 0.05

            # Determine Available Capital
            # Use cached margin if available, else conservative 40% of settings.ACCOUNT_SIZE
            available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)

            is_sufficient = available >= estimated_margin

            logger.warning(
                f"⚠ API DOWN. Using FALLBACK Logic (VIX={vix:.1f}, Mult={margin_multiplier}): "
                f"Est.Req={estimated_margin:,.0f}, Est.Avail={available:,.0f}"
            )

            return is_sufficient, estimated_margin

        except Exception as e:
            logger.critical(f"Fallback margin check crashed: {e}")
            # Ultra-conservative: If even the fallback fails, reject the trade to save the account.
            return False, float('inf')

    async def refresh_available_margin(self):
        """
        Manually refresh available margin (call periodically)
        """
        await self._get_available_margin()
        if self.available_margin is not None:
            logger.debug(f"Available margin refreshed: ₹{self.available_margin:,.0f}")
