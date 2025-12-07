import aiohttp
from typing import Tuple
from core.models import MultiLegTrade
from core.config import settings, get_full_url
from utils.logger import get_logger

logger = get_logger("MarginGuard")

class MarginGuard:
    """
    FIXED: Use official Upstox margin calculation API
    """
    
    def __init__(self):
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.available_margin = None  # Will be fetched dynamically

    async def is_margin_ok(self, trade: MultiLegTrade) -> Tuple[bool, float]:
        """
        Check if sufficient margin is available using Upstox API
        
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
                    if resp.status != 200:
                        logger.error(f"Margin API failed: {resp.status}")
                        # Fallback to conservative estimate
                        return await self._fallback_margin_check(trade)

                    data = await resp.json()
                    
                    if data.get("status") != "success":
                        logger.error(f"Margin calculation failed: {data}")
                        return await self._fallback_margin_check(trade)

                    # Extract required margin
                    margin_data = data.get("data", {})
                    required_margin = margin_data.get("required_margin", 0.0)
                    final_margin = margin_data.get("final_margin", required_margin)

                    # Fetch available margin
                    available = await self._get_available_margin()
                    
                    if available is None:
                        logger.warning("Could not fetch available margin, using conservative check")
                        # Assume 50% of account size is available
                        available = settings.ACCOUNT_SIZE * 0.5

                    # Add 10% buffer for safety
                    required_with_buffer = final_margin * 1.10

                    is_sufficient = available >= required_with_buffer

                    if is_sufficient:
                        logger.info(
                            f"✅ Margin OK: Required={required_with_buffer:.0f}, "
                            f"Available={available:.0f}"
                        )
                    else:
                        logger.warning(
                            f"❌ Insufficient Margin: Required={required_with_buffer:.0f}, "
                            f"Available={available:.0f}, "
                            f"Shortfall={required_with_buffer - available:.0f}"
                        )

                    return is_sufficient, final_margin

        except Exception as e:
            logger.error(f"Margin check exception: {e}")
            return await self._fallback_margin_check(trade)

    async def _get_available_margin(self) -> float:
        """
        Fetch available margin from Upstox
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
                    
                    self.available_margin = available_margin
                    return available_margin

        except Exception as e:
            logger.error(f"Failed to fetch available margin: {e}")
            return None

    async def _fallback_margin_check(self, trade: MultiLegTrade) -> Tuple[bool, float]:
        """
        Conservative fallback margin calculation if API fails
        """
        try:
            # Estimate margin as 20% of notional value for options
            total_notional = 0.0
            for leg in trade.legs:
                notional = abs(leg.quantity) * leg.entry_price if leg.entry_price > 0 else 0
                total_notional += notional

            # For option selling, assume 20% margin
            estimated_margin = total_notional * 0.20

            # Add span and exposure margin estimates
            estimated_margin += settings.ACCOUNT_SIZE * 0.05  # 5% buffer

            # Assume 50% of account is available
            available = settings.ACCOUNT_SIZE * 0.5

            is_sufficient = available >= estimated_margin

            logger.warning(
                f"⚠ Using FALLBACK margin calculation: "
                f"Estimated={estimated_margin:.0f}, Available={available:.0f}"
            )

            return is_sufficient, estimated_margin

        except Exception as e:
            logger.error(f"Fallback margin check failed: {e}")
            # Ultra-conservative: reject trade
            return False, float('inf')

    async def refresh_available_margin(self):
        """
        Manually refresh available margin (call periodically)
        """
        await self._get_available_margin()
        if self.available_margin is not None:
            logger.debug(f"Available margin refreshed: ₹{self.available_margin:,.0f}")
