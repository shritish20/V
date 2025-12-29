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
    FORTRESS EDITION v3.1:
    - Fixed Sanity Check Math (Per-Lot Comparison).
    - VIX-aware fallback with historical sanity checks.
    - Records real margin data to DB.
    """
    def __init__(self, api_client: EnhancedUpstoxAPI, db_manager: Optional[HybridDatabaseManager] = None):
        self.api = api_client
        self.db = db_manager
        self.available_margin = None
        self.default_vix_safety = 25.0
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

    async def is_margin_ok(self, trade: MultiLegTrade, current_vix: Optional[float] = None) -> Tuple[bool, float]:
        # GUARD: zero legs or zero lot size
        if not trade.legs or settings.LOT_SIZE <= 0:
            logger.error("Margin check aborted: empty legs or LOT_SIZE=0")
            return False, float('inf')
        return await self._live_mode_check(trade, current_vix)

    async def _live_mode_check(
        self, trade: MultiLegTrade, current_vix: Optional[float]
    ) -> Tuple[bool, float]:
        try:
            instruments_payload = []
            for leg in trade.legs:
                instruments_payload.append({
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "product": "I",
                    "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0,
                })
            res_margin = await self.api._request_with_retry(
                "POST", "margin_calc", json={"instruments": instruments_payload}
            )
            if res_margin.get("code") == 423 or "UDAPI100072" in str(res_margin):
                logger.info("🌙 Upstox Maintenance Mode - Using Enhanced Fallback")
                return await self._enhanced_fallback_margin(trade, current_vix)
            if res_margin.get("status") != "success":
                logger.warning(f"Margin API failed: {res_margin.get('message', 'Unknown')} - Fallback")
                return await self._enhanced_fallback_margin(trade, current_vix)
            margin_data = res_margin.get("data", {})
            required_margin = margin_data.get("required_margin", 0.0)
            if self.db and required_margin > 0:
                try:
                    asyncio.create_task(self._record_margin_history(trade, required_margin, current_vix))
                except Exception as e:
                    logger.error(f"Failed to record margin history: {e}")
            funds = await self.api._request_with_retry("GET", "funds_margin")
            if funds.get("code") == 423 or "UDAPI100072" in str(funds):
                available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
            else:
                fund_data = funds.get("data", {})
                if isinstance(fund_data, dict):
                    segment_data = fund_data.get("SEC", fund_data)
                    available = segment_data.get("available_margin")
                else:
                    available = None
                if available is None:
                    available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
                logger.warning(f"Using estimated available funds: ₹{available:,.0f}")
            required_with_buffer = required_margin * 1.10
            is_sufficient = available >= required_with_buffer
            if not is_sufficient:
                logger.warning(
                    f"❌ Margin Shortfall: Req=₹{required_with_buffer:,.0f}, Avail=₹{available:,.0f}"
                )
            return is_sufficient, required_margin
        except Exception as e:
            logger.error(f"Live Margin Check Exception: {e}")
            return await self._enhanced_fallback_margin(trade, current_vix)

    async def _enhanced_fallback_margin(
        self, trade: MultiLegTrade, current_vix: Optional[float]
    ) -> Tuple[bool, float]:
        """
        HARDENED: Uses NSE margin table + VIX multiplier + DB Sanity Check
        CRITICAL FIX: Added validation for empty trades and zero quantities
        """
        # ===== GUARD: Invalid LOT_SIZE =====
        if settings.LOT_SIZE <= 0:
            raise ValueError(f"Invalid LOT_SIZE in settings: {settings.LOT_SIZE}")

        try:
            if not trade.legs or len(trade.legs) == 0:
                logger.error("❌ Cannot calculate margin for trade with no legs")
                return False, float('inf')
            first_leg_qty = abs(trade.legs[0].quantity)
            if first_leg_qty == 0:
                logger.error("❌ Cannot calculate margin for trade with zero quantity")
                return False, float('inf')
            total_lots = max(1, first_leg_qty // settings.LOT_SIZE)
            if total_lots == 0:
                logger.warning("⚠️ Quantity less than 1 lot - rounding up to 1")
                total_lots = 1
            vix = current_vix if current_vix is not None else self.default_vix_safety
            if vix < 15:
                vix_multiplier = 1.0
            elif vix < 20:
                vix_multiplier = 1.15
            elif vix < 30:
                vix_multiplier = 1.40
            else:
                vix_multiplier = 1.75
            strategy_name = trade.strategy_type.value
            base_margin = self.nse_margin_table.get(strategy_name, 150000)
            per_lot_estimate = base_margin * vix_multiplier
            estimated_margin = per_lot_estimate * total_lots
            exchange_buffer = 1.30
            fallback_final = estimated_margin * exchange_buffer
            if self.db:
                last_real_per_lot = await self._get_last_real_margin(strategy_name)
                if last_real_per_lot and last_real_per_lot > 0:
                    fallback_per_lot = fallback_final / total_lots
                    if fallback_per_lot < (last_real_per_lot * 0.7):
                        logger.critical(
                            f"🚨 FALLBACK SANITY CHECK FAILED:\n"
                            f"   Calculated/Lot: ₹{fallback_per_lot:,.0f}\n"
                            f"   Historical/Lot: ₹{last_real_per_lot:,.0f}\n"
                            f"   Difference: {((fallback_per_lot / last_real_per_lot - 1) * 100):.1f}%"
                        )
                        fallback_final = (last_real_per_lot * 1.2) * total_lots
            absolute_floor = 100000 * total_lots
            final_margin = max(fallback_final, absolute_floor)
            available = self.available_margin if self.available_margin else (settings.ACCOUNT_SIZE * 0.40)
            is_sufficient = available >= final_margin
            logger.info(
                f"⚠️ FALLBACK MARGIN CALC:\n"
                f"   Strategy: {strategy_name}\n"
                f"   Lots: {total_lots}\n"
                f"   VIX: {vix:.1f} (Multiplier: {vix_multiplier:.2f})\n"
                f"   Base/Lot: ₹{base_margin:,.0f}\n"
                f"   Required: ₹{final_margin:,.0f}\n"
                f"   Available: ₹{available:,.0f}\n"
                f"   Status: {'✅ OK' if is_sufficient else '❌ INSUFFICIENT'}"
            )
            return is_sufficient, final_margin
        except Exception as e:
            logger.critical(f"Fallback margin check crashed: {e}", exc_info=True)
            return False, float('inf')

    async def _record_margin_history(self, trade: MultiLegTrade, margin: float, vix: Optional[float]):
        if not self.db:
            return
        try:
            total_lots = max(1, abs(trade.legs[0].quantity) // settings.LOT_SIZE)
            if total_lots == 0:
                return
            margin_per_lot = margin / total_lots
            async with self.db.get_session() as session:
                entry = DbMarginHistory(
                    strategy_type=trade.strategy_type.value,
                    lots=total_lots,
                    required_margin=margin_per_lot,
                    vix_at_calc=vix if vix else 0.0,
                    timestamp=datetime.utcnow(),
                )
                session.add(entry)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"DB Write Error (MarginHistory): {e}")

    async def _get_last_real_margin(self, strategy_type: str) -> Optional[float]:
        if not self.db:
            return None
        try:
            async with self.db.get_session() as session:
                stmt = (
                    select(DbMarginHistory.required_margin)
                    .where(DbMarginHistory.strategy_type == strategy_type)
                    .order_by(desc(DbMarginHistory.timestamp))
                    .limit(1)
                )
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except Exception:
            return None

    async def refresh_available_margin(self):
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
