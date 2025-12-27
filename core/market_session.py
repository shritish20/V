from __future__ import annotations
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional
import asyncio
import logging

from core.config import IST, settings
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("MarketSession")

# Upstox EOD candle availability (realistic)
EOD_AVAILABLE_TIME = dtime(19, 0)  # 7:00 PM IST

class MarketSessionManager:
    """
    Institutional-grade market session controller.

    Governs:
    - Market hours
    - Holidays
    - WebSocket permission
    - Trading permission
    - Historical (EOD) data permission
    """

    def __init__(self, api: EnhancedUpstoxAPI):
        self.api = api
        self._holidays: set[date] = set()
        self._last_refresh: Optional[date] = None
        self._lock = asyncio.Lock()

        self.open_time = settings.MARKET_OPEN_TIME
        self.close_time = settings.MARKET_CLOSE_TIME

    async def refresh(self) -> None:
        """
        Refresh holidays ONCE per day.
        Never spam Upstox.
        """
        async with self._lock:
            today = datetime.now(IST).date()
            if self._last_refresh == today:
                return

            try:
                holidays = await self.api.get_market_holidays()
                self._holidays = {
                    datetime.strptime(h["date"], "%Y-%m-%d").date()
                    for h in holidays if "date" in h
                }
                self._last_refresh = today
                logger.info(f"📅 Market holidays loaded: {len(self._holidays)}")
            except Exception as e:
                logger.warning(f"⚠️ Holiday fetch failed: {e}")

    # ---------- BASIC CALENDAR ---------- #

    def is_trading_day(self, d: date) -> bool:
        if d.weekday() >= 5:
            return False
        return d not in self._holidays

    # ---------- LIVE MARKET ---------- #

    def is_market_open_now(self) -> bool:
        now = datetime.now(IST)
        if not self.is_trading_day(now.date()):
            return False
        return self.open_time <= now.time() <= self.close_time

    def can_trade(self) -> bool:
        return self.is_market_open_now()

    def can_use_websocket(self) -> bool:
        return self.is_market_open_now()

    # ---------- POST-MARKET / HISTORICAL ---------- #

    def is_eod_available_today(self) -> bool:
        now = datetime.now(IST)
        if not self.is_trading_day(now.date()):
            return False
        return now.time() >= EOD_AVAILABLE_TIME

    def can_fetch_historical(self, from_dt: date, to_dt: date) -> bool:
        today = datetime.now(IST).date()

        # Never ask future
        if to_dt > today:
            return False

        # If asking for today, wait until EOD publish
        if to_dt == today and not self.is_eod_available_today():
            return False

        # Must include at least one trading day
        cursor = from_dt
        while cursor <= to_dt:
            if self.is_trading_day(cursor):
                return True
            cursor += timedelta(days=1)

        return False

    # ---------- ENGINE MODE ---------- #

    def current_mode(self) -> str:
        if self.is_market_open_now():
            return "LIVE_MARKET"
        if self.is_eod_available_today():
            return "POST_MARKET"
        return "OFF_MARKET"
