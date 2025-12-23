#!/usr/bin/env python3
"""
EnhancedUpstoxAPI 20.0 – V3 Production Hardened (Fortress Edition)
- MIGRATED: All Market Data and Order calls to V3 Endpoints.
- NO RECURSION: Validity checks are isolated from main request logic.
- Hard 5-second timeout per call.
"""
from __future__ import annotations

import asyncio
import logging
import time
import random
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote

import aiohttp
from core.config import settings, UPSTOX_API_ENDPOINTS
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class TokenExpiredError(RuntimeError):
    """Raised when bearer token is rejected."""

class MarginInsaneError(RuntimeError):
    """Raised when available margin is <= 0."""

# ---------------------------------------------------------------------------
# Rate limiter – token bucket
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rate_per_sec: int = 9) -> None:
        self._rate = rate_per_sec
        self._tokens = float(rate_per_sec)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1:
                sleep = (1 - self._tokens) / self._rate
                await asyncio.sleep(sleep)
                self._tokens = 0
            else:
                self._tokens -= 1

# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------
class EnhancedUpstoxAPI:
    def __init__(self, token: str) -> None:
        self._token = token
        self._token_last_updated = time.time()
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0", # Required by Upstox for V3 transitions
        }
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._limiter = RateLimiter()
        self.instrument_master = None

    def set_instrument_master(self, master: Any) -> None:
        self.instrument_master = master

    async def update_token(self, new_token: str) -> None:
        async with self._session_lock:
            self._token = new_token
            self._token_last_updated = time.time()
            self._headers["Authorization"] = f"Bearer {new_token}"
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
        logger.info("🔄 API Client Token Rotated Successfully (V3 Ready)")

    async def check_token_validity(self) -> bool:
        """ISOLATED PROBE: Uses Profile V2 (stable) to check token status."""
        url = "https://api.upstox.com/v2/user/profile"
        try:
            async with aiohttp.ClientSession(headers=self._headers) as temp_session:
                async with temp_session.get(url, timeout=5) as resp:
                    if resp.status == 401:
                        raise TokenExpiredError("Token Probe Failed (401)")
                    return True
        except Exception as e:
            logger.warning(f"Token probe network error: {e}")
            return True

    async def get_v3_market_data_authorize(self) -> Dict[str, Any]:
        """
        CRITICAL V3 HANDSHAKE:
        Retrieves the designated socket endpoint URI for Market updates.
        """
        url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
        return await self._request("GET", dynamic_url=url)

    async def close(self) -> None:
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()

    # ---------------------------------------------------------------------
    # Low-Level Request (Hardened)
    # ---------------------------------------------------------------------
    async def _request(
        self,
        method: str,
        endpoint_key: str = "",
        dynamic_url: str = "",
        *,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        retry: int = 3,
    ) -> Dict[str, Any]:
        
        # Determine URL - prioritizing V3 routes where possible
        if dynamic_url:
            url = dynamic_url
        else:
            # Fallback to config endpoints, ensure we check for V3 mappings
            path = UPSTOX_API_ENDPOINTS.get(endpoint_key, "")
            url = f"https://api.upstox.com{path}"

        request_start_time = time.time()

        for attempt in range(1, retry + 1):
            await self._limiter.acquire()
            try:
                session = await self._get_session()
                async with session.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    body = await resp.text()
                    safe_body = self._redact(body)

                    if resp.status == 200:
                        data = json.loads(body)
                        if endpoint_key == "funds_margin":
                            self._sanity_check_margin(data)
                        return data

                    if resp.status == 401:
                        if self._token_last_updated > request_start_time:
                            logger.info("⚠️ 401 received, token rotated during flight. Retrying...")
                            continue
                        raise TokenExpiredError("Access Token Invalid")

                    if resp.status in (429, 503):
                        await asyncio.sleep((2 ** attempt) + random.uniform(0, 1))
                        continue

                    return {"status": "error", "message": safe_body, "code": resp.status}

            except TokenExpiredError:
                raise
            except Exception as exc:
                if attempt == retry:
                    return {"status": "error", "message": str(exc)}
                await asyncio.sleep(1)

        return {"status": "error", "message": "Max retries reached"}

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(headers=self._headers)
            return self._session

    @staticmethod
    def _sanity_check_margin(data: Dict[str, Any]) -> None:
        try:
            if data.get("status") == "success":
                fund_data = data.get("data", {})
                segment = fund_data.get("SEC", fund_data)
                avail = float(segment.get("available_margin", 0.0))
                if avail <= 0:
                    raise MarginInsaneError(f"Available margin {avail} – HALT TRADING")
        except MarginInsaneError:
            raise
        except:
            pass

    @staticmethod
    def _redact(text: str) -> str:
        patterns = [
            (r"Bearer\s+[a-zA-Z0-9\-._]+", "Bearer [REDACTED]"),
            (r'"access_token"\s*:\s*"[^"]+"', '"access_token":"[REDACTED]"'),
            (r'eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+', "[JWT_REDACTED]"),
        ]
        for pat, repl in patterns:
            text = re.sub(pat, repl, text, flags=re.IGNORECASE)
        return text

    # -------------------------------------------------------------------------
    # High-level Helpers (V3 Order Execution)
    # -------------------------------------------------------------------------
    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(time.time() * 1_000)}"

        # Using HFT/V3 Optimized endpoint
        url = "https://api-hft.upstox.com/v3/order/place"
        payload = {
            "quantity": abs(order.quantity),
            "product": order.product,
            "validity": order.validity,
            "price": float(order.price),
            "trigger_price": float(order.trigger_price),
            "instrument_token": order.instrument_key,
            "order_type": order.order_type,
            "transaction_type": order.transaction_type,
            "disclosed_quantity": 0,
            "is_amo": order.is_amo,
            "tag": "VG20",
        }
        res = await self._request("POST", dynamic_url=url, json_data=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_ids"][0]
        return False, None

    async def get_historical_candles(self, instrument_key: str, interval: str, to_date: str, from_date: str) -> Dict[str, Any]:
        encoded = quote(instrument_key)
        # Migrated to V3 Candle API
        url = f"https://api.upstox.com/v3/historical-candle/{encoded}/{interval}/{to_date}/{from_date}"
        return await self._request("GET", dynamic_url=url)

    async def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        encoded = quote(instrument_key)
        # Migrated to V3 Intraday API
        url = f"https://api.upstox.com/v3/historical-candle/intraday/{encoded}/{interval}"
        return await self._request("GET", dynamic_url=url)

    async def get_market_quote_ohlc(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        # Upstox V3 Market Quote OHLC
        url = "https://api.upstox.com/v3/market-quote/ohlc"
        return await self._request(
            "GET",
            dynamic_url=url,
            params={"instrument_key": instrument_key, "interval": interval},
        )
