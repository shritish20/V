#!/usr/bin/env python3
"""
VolGuard 20.0 – Position Lifecycle Manager
- Fixed: Returns valid (bool, str) tuple in all paths
"""
import logging
from typing import Tuple, List, Dict
from datetime import datetime, date

from core.models import MultiLegTrade, TradeStatus, ExpiryType
from core.config import settings

logger = logging.getLogger("LifecycleMgr")

class PositionLifecycleManager:
    def __init__(self, trade_manager):
        self.trade_mgr = trade_manager

    async def monitor_lifecycle(self, trades: List[MultiLegTrade]):
        """
        Scans open trades for expiry/exit conditions.
        """
        pass # Placeholder for now

    def can_enter_new_trade(self, expiry_date_str: str, expiry_type: ExpiryType) -> Tuple[bool, str]:
        """
        Determines if a new trade can be entered based on time to expiry.
        Must return (bool, reason_string).
        """
        try:
            # 1. Parse Date
            if isinstance(expiry_date_str, date):
                exp = expiry_date_str
            else:
                exp = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
            
            today = datetime.now(settings.IST).date()
            now = datetime.now(settings.IST).time()

            # 2. Check Expiry Day Logic
            if exp == today:
                # On expiry day, no new trades after SAFE_TRADE_END (e.g., 3:15 PM)
                if now >= settings.SAFE_TRADE_END:
                    return False, f"Too close to expiry cutoff ({settings.SAFE_TRADE_END})"
                
                # Zero-DTE logic (if we wanted to restrict it further)
                # For now, allow it until cutoff.

            # 3. Check Past Dates
            if exp < today:
                return False, "Expiry date is in the past"

            # 4. Success Path
            return True, "OK"

        except Exception as e:
            logger.error(f"Lifecycle Check Error: {e}")
            return False, f"Lifecycle Error: {str(e)}"
