#!/usr/bin/env python3
"""
EnhancedUpstoxAPI 20.1 – Production Hardened (Fortress Edition)
- V2 ENDPOINTS only (no 404)
- UDAPI100072 trap
- set_instrument_master restored
- close() method present
- dates always YYYY-MM-DD strings
----------------------------------------------------------
NEW: 00:00-06:00 IST night-mode stub for funds/margin calls
     so you can test without Upstox blocking you.
"""
from __future__ import annotations

import asyncio
import logging
import time
import random
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote

import aiohttp
from core.config import settings, UPSTOX_API_ENDPOINTS
from core.models import Order

logger = logging.getLogger("UpstoxAPI")

# ------------------------------------------------------------------
#  Night-mode helpers
# ------------------------------------------------------------------
def _ist_now() -> datetime:
    """Return current time in IST."""
    from core.config import IST
    return datetime.now(IST)

def _is_night_mode() -> bool:
    """True between 00:00 and 06:00 IST."""
    t = _ist_now().time()
    return t.hour < 6

def _dummy_funds_margin() -> Dict[str, Any]:
    """Return a believable funds/margin payload."""
    fake_avail = 2_000_000.0          # 20 Lakh free
    fake_used  = 150_000.0            # ~1.5 Lakh used
    return {
        "status": "success",
        "data": {
            "equity": {
                "available_margin": fake_avail,
                "used_margin": fake_used,
                "payin": 0,
                "span_margin": 135_000.0,
                "exposure_margin": 15_000.0,
            }
        }
    }

# ------------------------------------------------------------------
#  Existing code untouched below this line
# ------------------------------------------------------------------
class TokenExpiredError(RuntimeError): pass
class MarginInsaneError(RuntimeError): pass

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

class EnhancedUpstoxAPI:
    def __init__(self, token: str) -> None:
        self._token = token
        self._token_last_updated = time.time()
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Api-Version": "2.0",
        }
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._limiter = RateLimiter()
        self.instrument_master = None

    async def update_token(self, new_token: str) -> None:
        async with self._session_lock:
            self._token = new_token
            self._token_last_updated = time.time()
            self._headers["Authorization"] = f"Bearer {new_token}"
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
        logger.info("🔄 API Client Token Rotated Successfully")

    async def check_token_validity(self) -> bool:
        url = settings.API_BASE_URL + "/v2/user/profile"
        try:
            async with aiohttp.ClientSession(headers=self._headers) as temp_session:
                async with temp_session.get(url, timeout=5) as resp:
                    if resp.status == 401:
                        raise TokenExpiredError("Token Probe Failed (401)")
                    return True
        except TokenExpiredError:
            raise
        except Exception as e:
            logger.warning(f"Token probe network error: {e}")
            return True

    def set_instrument_master(self, master) -> None:
        """Engine needs this – do not delete."""
        self.instrument_master = master

    async def close(self) -> None:
        """Closes aiohttp session; Engine calls this on shutdown."""
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
                logger.info("📡 API Session Closed Gracefully")

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(headers=self._headers)
            return self._session

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
        if dynamic_url:
            url = dynamic_url
        else:
            url = settings.API_BASE_URL + UPSTOX_API_ENDPOINTS.get(endpoint_key, "")

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
                        try:
                            data = json.loads(body)
                        except json.JSONDecodeError:
                            logger.error(f"❌ JSON Decode Error: {safe_body}")
                            return {"status": "error", "message": "Invalid JSON"}
                        if endpoint_key == "funds_margin":
                            self._sanity_check_margin(data)
                        return data

                    if resp.status == 401:
                        if self._token_last_updated > request_start_time:
                            logger.info("⚠️ 401 but token was just updated – retrying")
                            continue
                        raise TokenExpiredError("Access Token Invalid")

                    if resp.status in (429, 503):
                        sleep_time = (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(f"⚠️ Rate/Gateway limit – Backing off {round(sleep_time, 2)}s")
                        await asyncio.sleep(sleep_time)
                        continue

                    if resp.status == 423:
                        return {"status": "error", "message": "Upstox Maintenance", "code": 423}

                    logger.error(f"❌ API error: {resp.status} - {url}")
                    return {"status": "error", "message": safe_body, "code": resp.status}

            except TokenExpiredError:
                raise
            except asyncio.TimeoutError:
                logger.error(f"⏰ Request Timeout (5s): {url}")
                return {"status": "error", "message": "timeout"}
            except Exception as exc:
                logger.exception(f"🔥 Request failed: {url}")
                if attempt == retry:
                    return {"status": "error", "message": str(exc)}
                await asyncio.sleep(1)

        return {"status": "error", "message": "Max retries"}

    @staticmethod
    def _sanity_check_margin(data: Dict[str, Any]) -> None:
        try:
            if data.get("status") == "success":
                fund_data = data.get("data", {})
                segment = fund_data.get("SEC", fund_data)
                avail = float(segment.get("available_margin", 0.0))
                if avail <= 0:
                    raise MarginInsaneError(f"Available margin {avail} – HALT TRADING")
        except (KeyError, ValueError):
            pass

    @staticmethod
    def _redact(text: str) -> str:
        text = re.sub(r"Bearer\s+[a-zA-Z0-9\-._]+", "Bearer [REDACTED]", text, flags=re.I)
        text = re.sub(r'"access_token"\s*:\s*"[^"]+"', '"access_token":"[REDACTED]"', text, flags=re.I)
        text = re.sub(r'eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+', "[JWT_REDACTED]", text, flags=re.I)
        return text

    # -------------------------------------------------------------------------
    # High-level wrappers – V2 OFFICIAL ENDPOINTS
    # -------------------------------------------------------------------------
    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        if settings.SAFETY_MODE != "live":
            return True, f"SIM-{int(time.time() * 1_000)}"

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
        res = await self._request("POST", "place_order", json_data=payload)
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict[str, Any]:
        """Official v2 option-chain endpoint."""
        return await self._request("GET", "option_chain", params={"instrument_key": instrument_key, "expiry_date": expiry_date})

    async def get_short_term_positions(self) -> List[Dict[str, Any]]:
        res = await self._request("GET", "positions")
        return res.get("data", []) if res.get("status") == "success" else []

    async def get_funds_and_margin(self) -> Dict[str, Any]:
        """Night-mode stub injected here."""
        if _is_night_mode():
            logger.info("🌙 Night-mode stub active for funds/margin")
            return _dummy_funds_margin()
        return await self._request("GET", "funds_margin")

    async def get_historical_candles(self, instrument_key: str, interval: str, to_date: str, from_date: str) -> Dict[str, Any]:
        """
        V2 historical candles + UDAPI100072 trap + empty guard
        ALWAYS expects YYYY-MM-DD strings
        """
        encoded = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v2/historical-candle/{encoded}/{interval}/{to_date}/{from_date}"
        res = await self._request("GET", dynamic_url=url)

        # ---- Trap expired instrument ----
        if res.get("code") == "UDAPI100072":
            logger.info("Instrument %s expired – returning empty candles", instrument_key)
            return {"status": "success", "data": {"candles": []}}

        # ---- Guard empty payload ----
        if res.get("status") == "success" and not res.get("data", {}).get("candles"):
            logger.warning("No candles for %s – empty frame", instrument_key)
            return {"status": "success", "data": {"candles": []}}

        return res

    async def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        encoded = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v2/historical-candle/intraday/{encoded}/{interval}"
        return await self._request("GET", dynamic_url=url)

    async def get_market_quote_ohlc(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        return await self._request(
            "GET",
            "market_quote_ohlc",
            params={"instrument_key": instrument_key, "interval": interval},
        )
