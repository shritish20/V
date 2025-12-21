import aiohttp
from typing import Tuple, Optional, Dict, List, Any
from core.models import MultiLegTrade
from core.config import settings
from utils.logger import setup_logger
from trading.api_client import EnhancedUpstoxAPI 

logger = setup_logger("MarginGuard")

class MarginGuard:
    """
    FIXED v2.0:
    - VIX-aware fallback with exchange margin buffers
    - Handles Upstox Maintenance Mode (Error 423) gracefully
    - Separate logic for PAPER vs LIVE modes
    """
    
    def __init__(self, api_client: EnhancedUpstoxAPI): 
        self.api = api_client
        self.available_margin = None 
        self.default_vix_safety = 25.0
        
        # ENHANCED: NSE margin lookup table (based on historical data)
        # Updated periodically from NSE SPAN calculator
        self.nse_margin_table = {
            "IRON_CONDOR": 55000,      # Per lot (tighter than estimate)
            "IRON_FLY": 52000,
            "JADE_LIZARD": 140000,     # Higher than before
            "SHORT_STRANGLE": 170000,  # Undefined risk
            "RATIO_SPREAD_PUT": 190000,
            "SHORT_STRADDLE": 180000,
            "BULL_PUT_SPREAD": 45000,
            "BEAR_CALL_SPREAD": 45000,
        }

    async def is_margin_ok(
        self, 
        trade: MultiLegTrade, 
        current_vix: Optional[float] = None
    ) -> Tuple[bool, float]:
        """
        Check margin with fallback logic.
        """
        
        # PAPER TRADING MODE
        if settings.SAFETY_MODE != "live":
            return await self._paper_mode_check(trade, current_vix)
        
        # LIVE TRADING MODE
        return await self._live_mode_check(trade, current_vix)

    async def _paper_mode_check(
        self, 
        trade: MultiLegTrade, 
        current_vix: Optional[float]
    ) -> Tuple[bool, float]:
        """Paper trading: Use enhanced fallback with NSE margin table"""
        _, req_margin = await self._enhanced_fallback_margin(trade, current_vix)
        available = settings.ACCOUNT_SIZE
        
        if available >= req_margin:
            return True, req_margin
        else:
            logger.warning(
                f"🚫 [PAPER] Margin Shortfall: "
                f"Req=₹{req_margin:,.0f}, Virtual Avail=₹{available:,.0f}"
            )
            return False, req_margin

    async def _live_mode_check(
        self, 
        trade: MultiLegTrade, 
        current_vix: Optional[float]
    ) -> Tuple[bool, float]:
        """Live trading: Upstox API with graceful 423 handling"""
        try:
            # Step 1: Build payload
            instruments_payload = []
            for leg in trade.legs:
                instruments_payload.append({
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "product": "I",
                    "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0
                })

            # Step 2: Get Required Margin
            res_margin = await self.api._request_with_retry(
                "POST", 
                "margin_calc",  # Uses /v2/charges/margin endpoint
                json={"instruments": instruments_payload}
            )
            
            # Handle Error 423 (Upstox Maintenance Mode: 12 AM - 5:30 AM)
            if res_margin.get("code") == 423 or "UDAPI100072" in str(res_margin):
                logger.info("🌙 Upstox Maintenance Mode - Using Enhanced Fallback")
                return await self._enhanced_fallback_margin(trade, current_vix)

            if res_margin.get("status") != "success":
                logger.warning(
                    f"Margin API failed: {res_margin.get('message', 'Unknown')} - Fallback"
                )
                return await self._enhanced_fallback_margin(trade, current_vix)

            # Extract required margin
            margin_data = res_margin.get("data", {})
            required_margin = margin_data.get("required_margin", 0.0)

            # Step 3: Get Available Funds
            funds = await self.api._request_with_retry("GET", "funds_margin")
            
            if funds.get("code") == 423 or "UDAPI100072" in str(funds):
                # Use cached or conservative estimate
                available = self.available_margin if self.available_margin else (
                    settings.ACCOUNT_SIZE * 0.40
                )
            else:
                fund_data = funds.get("data", {})
                
                # FIX: Handle nested fund structure based on Upstox API schema
                # The API returns: {"data": {"SEC": {...}, "COM": {...}}}
                if isinstance(fund_data, dict):
                    # Default to SEC segment for equity derivatives
                    segment_data = fund_data.get("SEC", fund_data)
                    available = segment_data.get("available_margin")
                else:
                    available = None

            if available is None:
                available = self.available_margin if self.available_margin else (
                    settings.ACCOUNT_SIZE * 0.40
                )
                logger.warning(f"Using estimated available funds: ₹{available:,.0f}")

            # Step 4: Validation with buffer
            required_with_buffer = required_margin * 1.10  # 10% safety buffer
            is_sufficient = available >= required_with_buffer

            if not is_sufficient:
                logger.warning(
                    f"❌ Margin Shortfall: Req=₹{required_with_buffer:,.0f}, "
                    f"Avail=₹{available:,.0f}"
                )

            return is_sufficient, required_margin

        except Exception as e:
            logger.error(f"Live Margin Check Exception: {e}")
            return await self._enhanced_fallback_margin(trade, current_vix)

    async def _enhanced_fallback_margin(
        self, 
        trade: MultiLegTrade, 
        current_vix: Optional[float]
    ) -> Tuple[bool, float]:
        """
        ENHANCED: Uses NSE margin table + VIX multiplier + exchange buffer
        """
        try:
            vix = current_vix if current_vix is not None else self.default_vix_safety
            
            # Step 1: Base margin from lookup table
            strategy_name = trade.strategy_type.value
            base_margin = self.nse_margin_table.get(strategy_name, 150000)  # Default fallback
            
            # Step 2: VIX Multiplier
            if vix < 15:
                vix_multiplier = 1.0
            elif vix < 20:
                vix_multiplier = 1.15
            elif vix < 30:
                vix_multiplier = 1.40
            else:
                vix_multiplier = 1.75  # Extreme volatility

            # Step 3: Calculate per-lot margin
            per_lot_margin = base_margin * vix_multiplier
            
            # Step 4: Total margin for all legs
            total_lots = max(1, abs(trade.legs[0].quantity) // settings.LOT_SIZE)
            estimated_margin = per_lot_margin * total_lots
            
            # Step 5: Add NSE buffer (exchanges can increase margins overnight)
            # Historical observation: NSE increases margins by 20-50% during events
            exchange_buffer = 1.30  # 30% buffer for safety
            final_margin = estimated_margin * exchange_buffer

            # Step 6: Check availability
            available = self.available_margin if self.available_margin else (
                settings.ACCOUNT_SIZE * 0.40
            )

            is_sufficient = available >= final_margin

            logger.info(
                f"⚠️ FALLBACK Margin (VIX={vix:.1f}): "
                f"Strategy={strategy_name}, Base=₹{base_margin:,.0f}/lot, "
                f"VIX×{vix_multiplier:.2f}, Lots={total_lots}, "
                f"Final=₹{final_margin:,.0f}, Avail=₹{available:,.0f}"
            )

            return is_sufficient, final_margin

        except Exception as e:
            logger.critical(f"Fallback margin check crashed: {e}")
            return False, float('inf')

    async def refresh_available_margin(self):
        """
        Manually refresh available margin (call periodically)
        """
        try:
            funds = await self.api._request_with_retry("GET", "funds_margin")
            
            if funds.get("status") == "success":
                fund_data = funds.get("data", {})
                
                # Handle nested structure
                if isinstance(fund_data, dict):
                    segment_data = fund_data.get("SEC", fund_data)
                    val = segment_data.get("available_margin")
                    
                    if val is not None:
                        self.available_margin = val
                        logger.debug(f"Available margin refreshed: ₹{self.available_margin:,.0f}")
        except Exception as e:
            logger.warning(f"Margin refresh failed: {e}")
