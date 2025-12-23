#!/usr/bin/env python3
"""
EnhancedUpstoxAPI 20.0 – Production Hardened (Fortress Edition)
- SMART RETRY: Retries on 401 only if token was just updated.
- NO RECURSION: Validity checks are isolated from main request logic.
- Hard 5-second timeout per call.
- Margin sanity check.
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
        # Timestamp of last token update (used to prevent stale retries)
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

    def set_instrument_master(self, master: Any) -> None:
        """Link to Instrument Master for symbol lookups."""
        self.instrument_master = master

    async def update_token(self, new_token: str) -> None:
        """Called by TokenManager when a fresh token is available."""
        async with self._session_lock:
            self._token = new_token
            self._token_last_updated = time.time()
            self._headers["Authorization"] = f"Bearer {new_token}"
            # Force close session to flush old headers
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
        logger.info("🔄 API Client Token Rotated Successfully")

    async def check_token_validity(self) -> bool:
        """
        ISOLATED PROBE: Checks if token is valid without triggering main retry loops.
        """
        url = settings.API_BASE_URL + "/v2/user/profile"
        try:
            # We create a throwaway session for the probe to ensure isolation
            async with aiohttp.ClientSession(headers=self._headers) as temp_session:
                async with temp_session.get(url, timeout=5) as resp:
                    if resp.status == 401:
                        raise TokenExpiredError("Token Probe Failed (401)")
                    return True
        except TokenExpiredError:
            raise
        except Exception as e:
            # Network glitches shouldn't kill the bot, just warn
            logger.warning(f"Token probe network error: {e}")
            return True

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
        """
        Unified request with Smart 401 handling.
        """
        if dynamic_url:
            url = dynamic_url
        else:
            url = settings.API_BASE_URL + UPSTOX_API_ENDPOINTS.get(endpoint_key, "")

        # Snapshot start time to detect if token changed during request
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
                    timeout=aiohttp.ClientTimeout(total=5), # Hard 5s Limit
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

                    # --- SMART 401 HANDLING ---
                    if resp.status == 401:
                        # Case A: Token was updated by TokenManager while we were waiting
                        if self._token_last_updated > request_start_time:
                            logger.info("⚠️ 401 received, but token was just updated. Retrying...")
                            continue # Retry loop will pick up new token/session
                        
                        # Case B: Genuine Expiry
                        logger.error("❌ Token Expired (401). Raising signal for Engine.")
                        raise TokenExpiredError("Access Token Invalid")

                    if resp.status in (429, 503):
                        sleep_time = (2 ** attempt) + random.uniform(0, 1)
                        logger.warning(
                            "⚠️ Rate/Gateway limit – Backing off",
                            extra={"status": resp.status, "sleep": round(sleep_time, 2)},
                        )
                        await asyncio.sleep(sleep_time)
                        continue

                    # Maintenance Mode Handling
                    if resp.status == 423:
                        return {"status": "error", "message": "Upstox Maintenance", "code": 423}

                    logger.error(
                        "❌ API error",
                        extra={"status": resp.status, "url": url, "body": safe_body[:200]},
                    )
                    return {"status": "error", "message": safe_body, "code": resp.status}

            except TokenExpiredError:
                raise # Pass up to Engine
            except asyncio.TimeoutError:
                logger.error("⏰ Request Timeout (5s)", extra={"url": url})
                return {"status": "error", "message": "timeout"}
            except Exception as exc:
                logger.exception("🔥 Request failed", extra={"url": url})
                if attempt == retry:
                    return {"status": "error", "message": str(exc)}
                await asyncio.sleep(1)

        return {"status": "error", "message": "Max retries"}

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(headers=self._headers)
            return self._session

    @staticmethod
    def _sanity_check_margin(data: Dict[str, Any]) -> None:
        """Raise if available_margin <= 0."""
        try:
            if data.get("status") == "success":
                fund_data = data.get("data", {})
                # Handle nested SEC/COM structure
                if isinstance(fund_data, dict):
                    segment = fund_data.get("SEC", fund_data)
                    avail = float(segment.get("available_margin", 0.0))
                    if avail <= 0:
                        raise MarginInsaneError(f"Available margin {avail} – HALT TRADING")
        except (KeyError, ValueError) as exc:
            pass # Don't crash on bad data structure, just log
        except MarginInsaneError:
            raise

    @staticmethod
    def _redact(text: str) -> str:
        """Remove tokens / jwt from logged text."""
        patterns = [
            (r"Bearer\s+[a-zA-Z0-9\-._]+", "Bearer [REDACTED]"),
            (r'"access_token"\s*:\s*"[^"]+"', '"access_token":"[REDACTED]"'),
            (r'eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+', "[JWT_REDACTED]"),
        ]
        for pat, repl in patterns:
            text = re.sub(pat, repl, text, flags=re.IGNORECASE)
        return text

    # -------------------------------------------------------------------------
    # High-level Helpers
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

    async def place_multi_order(self, orders_payload: List[Dict[str, Any]]) -> Dict[str, Any]:
        if settings.SAFETY_MODE != "live":
            return {
                "status": "success",
                "data": [{"order_id": f"SIM-BATCH-{i}"} for i in range(len(orders_payload))],
            }
        return await self._request("POST", "place_multi_order", json_data=orders_payload)

    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict[str, Any]:
        return await self._request("GET", "option_chain", params={"instrument_key": instrument_key, "expiry_date": expiry_date})

    async def get_short_term_positions(self) -> List[Dict[str, Any]]:
        res = await self._request("GET", "positions")
        return res.get("data", []) if res.get("status") == "success" else []

    async def get_funds_and_margin(self) -> Dict[str, Any]:
        """Public wrapper – engine uses this for real margin."""
        return await self._request("GET", "funds_margin")

    async def get_holidays(self) -> Dict[str, Any]:
        return await self._request("GET", "holidays")

    async def get_historical_candles(self, instrument_key: str, interval: str, to_date: str, from_date: str) -> Dict[str, Any]:
        encoded = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v3/historical-candle/{encoded}/{interval}/{to_date}/{from_date}"
        return await self._request("GET", dynamic_url=url)

    async def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        encoded = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v3/historical-candle/intraday/{encoded}/{interval}"
        return await self._request("GET", dynamic_url=url)

    async def get_market_quote_ohlc(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        return await self._request(
            "GET",
            "market_quote_ohlc",
            params={"instrument_key": instrument_key, "interval": interval},
        )
