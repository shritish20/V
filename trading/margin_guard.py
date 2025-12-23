import aiohttp
import asyncio
from datetime import datetime
from typing import Tuple, Optional, Dict, List, Any
from sqlalchemy import select, desc

from core.models import MultiLegTrade
from core.config import settings
from utils.logger import setup_logger
from trading.api_client import EnhancedUpstoxAPI 
from database.manager import HybridDatabaseManager
from database.models import DbMarginHistory

logger = setup_logger("MarginGuard")

class MarginGuard:
    """
    FORTRESS EDITION v3.0:
    - VIX-aware fallback with historical sanity checks.
    - Records real margin data to DB for future safety.
    - Handles Upstox Maintenance Mode (Error 423) gracefully.
    - Prevents under-estimation during black swan events.
    """
    
    def __init__(self, api_client: EnhancedUpstoxAPI, db_manager: Optional[HybridDatabaseManager] = None): 
        self.api = api_client
        self.db = db_manager
        self.available_margin = None 
        self.default_vix_safety = 25.0
        
        # ENHANCED: NSE margin lookup table (Hard floors per lot)
        self.nse_margin_table = {
            "IRON_CONDOR": 55000,
            "IRON_FLY": 52000,
            "JADE_LIZARD": 140000,
            "SHORT_STRANGLE": 170000,
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
        Check margin with fallback logic and database recording.
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
        """Live trading: Upstox API with graceful 423 handling & DB recording"""
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
            
            # Handle Error 423 (Upstox Maintenance Mode)
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

            # --- NEW: Record Real Margin to DB for future Sanity Checks ---
            if self.db and required_margin > 0:
                try:
                    asyncio.create_task(self._record_margin_history(trade, required_margin, current_vix))
                except Exception as e:
                    logger.error(f"Failed to record margin history: {e}")

            # Step 3: Get Available Funds
            funds = await self.api._request_with_retry("GET", "funds_margin")
            
            if funds.get("code") == 423 or "UDAPI100072" in str(funds):
                available = self.available_margin if self.available_margin else (
                    settings.ACCOUNT_SIZE * 0.40
                )
            else:
                fund_data = funds.get("data", {})
                if isinstance(fund_data, dict):
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
        HARDENED: Uses NSE margin table + VIX multiplier + DB Sanity Check
        """
        try:
            vix = current_vix if current_vix is not None else self.default_vix_safety
            
            # Step 1: Base margin from lookup table
            strategy_name = trade.strategy_type.value
            base_margin = self.nse_margin_table.get(strategy_name, 150000)
            
            # Step 2: VIX Multiplier
            if vix < 15: vix_multiplier = 1.0
            elif vix < 20: vix_multiplier = 1.15
            elif vix < 30: vix_multiplier = 1.40
            else: vix_multiplier = 1.75

            # Step 3: Calculate per-lot margin
            per_lot_margin = base_margin * vix_multiplier
            
            # Step 4: Total margin for all legs
            total_lots = max(1, abs(trade.legs[0].quantity) // settings.LOT_SIZE)
            estimated_margin = per_lot_margin * total_lots
            
            # Step 5: Exchange buffer
            exchange_buffer = 1.30
            fallback_final = estimated_margin * exchange_buffer
            
            # --- NEW: SANITY CHECK vs DB ---
            # Compare against the last REAL margin we successfully got from Upstox
            if self.db:
                last_real = await self._get_last_real_margin(strategy_name)
                if last_real and last_real > 0:
                    # Normalize last_real to current lot size
                    # (Simplified: assumes last_real was for 1 lot, scales up)
                    # Ideally, store margin-per-lot in DB. 
                    # Here we use a conservative 50% check.
                    if fallback_final < (last_real * total_lots * 0.5):
                        logger.critical(
                            f"🚨 FALLBACK SANITY CHECK FAILED: "
                            f"Calc=₹{fallback_final:,.0f} vs Hist=₹{last_real*total_lots:,.0f}"
                        )
                        fallback_final = last_real * total_lots * 1.2  # Use history + 20%

            # Step 6: Absolute floor
            absolute_floor = 100000 * total_lots # Never assume less than 1L/lot for complex strats
            final_margin = max(fallback_final, absolute_floor)

            # Step 7: Check availability
            available = self.available_margin if self.available_margin else (
                settings.ACCOUNT_SIZE * 0.40
            )

            is_sufficient = available >= final_margin

            logger.info(
                f"⚠️ FALLBACK (VIX={vix:.1f}): "
                f"Strat={strategy_name}, Lots={total_lots}, "
                f"Req=₹{final_margin:,.0f}, Avail=₹{available:,.0f}"
            )

            return is_sufficient, final_margin

        except Exception as e:
            logger.critical(f"Fallback margin check crashed: {e}")
            return False, float('inf')

    async def _record_margin_history(self, trade: MultiLegTrade, margin: float, vix: Optional[float]):
        """Background task to save real margin data."""
        if not self.db: return
        try:
            total_lots = max(1, abs(trade.legs[0].quantity) // settings.LOT_SIZE)
            margin_per_lot = margin / total_lots
            
            async with self.db.get_session() as session:
                entry = DbMarginHistory(
                    strategy_type=trade.strategy_type.value,
                    lots=total_lots,
                    required_margin=margin_per_lot, # Normalize to per-lot
                    vix_at_calc=vix if vix else 0.0,
                    timestamp=datetime.utcnow()
                )
                session.add(entry)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"DB Write Error (MarginHistory): {e}")

    async def _get_last_real_margin(self, strategy_type: str) -> Optional[float]:
        """Fetch the most recent real margin-per-lot for this strategy."""
        if not self.db: return None
        try:
            async with self.db.get_session() as session:
                stmt = select(DbMarginHistory.required_margin)\
                    .where(DbMarginHistory.strategy_type == strategy_type)\
                    .order_by(desc(DbMarginHistory.timestamp))\
                    .limit(1)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except Exception:
            return None

    async def refresh_available_margin(self):
        """Manually refresh available margin."""
        try:
            funds = await self.api._request_with_retry("GET", "funds_margin")
            if funds.get("status") == "success":
                fund_data = funds.get("data", {})
                if isinstance(fund_data, dict):
                    segment_data = fund_data.get("SEC", fund_data)
                    val = segment_data.get("available_margin")
                    if val is not None:
                        self.available_margin = val
                        logger.debug(f"Available margin refreshed: ₹{self.available_margin:,.0f}")
        except Exception as e:
            logger.warning(f"Margin refresh failed: {e}")
