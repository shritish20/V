#!/usr/bin/env python3
"""
EnhancedUpstoxAPI 20.3 (V3 POWERED)
- V3: Orders, GTT, History (Verified in Pre-Flight).
- V2: Option Chain (Reliable).
- Robust Error Handling & Timed Out Requests.
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

# ------------------------------------------------------
# Night-mode helpers
# ------------------------------------------------------
def _ist_now() -> datetime:
    from core.config import IST
    return datetime.now(IST)

def _is_night_mode() -> bool:
    t = _ist_now().time()
    return t.hour < 6

def _dummy_funds_margin() -> Dict[str, Any]:
    return {
        "status": "success",
        "data": {
            "equity": {
                "available_margin": 2_000_000.0,
                "used_margin": 150_000.0,
                "payin": 0,
                "span_margin": 135_000.0,
                "exposure_margin": 15_000.0,
            }
        }
    }

# ------------------------------------------------------
# Exceptions
# ------------------------------------------------------
class TokenExpiredError(RuntimeError):
    pass

class MarginInsaneError(RuntimeError):
    pass

# ------------------------------------------------------
# Rate limiter
# ------------------------------------------------------
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

# ------------------------------------------------------
# API Client
# ------------------------------------------------------
class EnhancedUpstoxAPI:
    def __init__(self, token: str) -> None:
        self._token = token
        self._token_last_updated = time.time()
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._limiter = RateLimiter()
        self.instrument_master = None

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
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
        # Check against V2 User Profile (Fastest probe)
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

    def set_instrument_master(self, master: Any) -> None:
        self.instrument_master = master

    async def close(self) -> None:
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
                logger.info("📡 API Session Closed Gracefully")

    # ------------------------------------------------------------------
    # Internal request wrapper
    # ------------------------------------------------------------------
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
                    timeout=aiohttp.ClientTimeout(total=8), # Increased for V3 latency
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
                logger.error(f"⏰ Request Timeout (8s): {url}")
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
                segment = fund_data.get("SEC", fund_data) # Handles inconsistent Upstox response structures
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

    # ------------------------------------------------------------------
    # V3 EXECUTION METHODS (Verified)
    # ------------------------------------------------------------------
    async def place_order(self, order: Order) -> Tuple[bool, Optional[str]]:
        """
        Uses V3 Order Endpoint.
        """
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
        
        # Uses 'place_order' key which maps to /v3/order/place in config
        res = await self._request("POST", "place_order", json_data=payload)
        
        if res.get("status") == "success":
            return True, res["data"]["order_id"]
        return False, None

    async def place_gtt_order(self, instrument_key: str, transaction_type: str, 
                              quantity: int, price: float, trigger_price: float) -> Dict:
        """
        Uses V3 GTT Endpoint (Verified payload structure).
        """
        if settings.SAFETY_MODE != "live":
            return {"status": "success", "data": {"gtt_order_id": "SIM-GTT"}}

        rule = {
            "strategy": "SINGLE",
            "trigger_type": "IMMEDIATE",
            "trigger_price": trigger_price,
            "transaction_type": transaction_type,
            "order_type": "LIMIT",
            "quantity": quantity,
            "price": price,
            "product": "D"
        }
        
        payload = {
            "type": "SINGLE",
            "instrument_token": instrument_key,
            "quantity": quantity,
            "product": "D",
            "rules": [rule]
        }
        # Uses 'place_gtt' key mapping to /v3/order/gtt/place
        return await self._request("POST", "place_gtt", json_data=payload)

    # ------------------------------------------------------------------
    # V2 DATA & PORTFOLIO (Stable)
    # ------------------------------------------------------------------
    async def get_option_chain(self, instrument_key: str, expiry_date: str) -> Dict[str, Any]:
        return await self._request("GET", "option_chain", params={"instrument_key": instrument_key, "expiry_date": expiry_date})

    async def get_short_term_positions(self) -> List[Dict[str, Any]]:
        res = await self._request("GET", "positions")
        return res.get("data", []) if res.get("status") == "success" else []

    async def get_funds_and_margin(self) -> Dict[str, Any]:
        if _is_night_mode():
            logger.info("🌙 Night-mode stub active for funds/margin")
            return _dummy_funds_margin()
        return await self._request("GET", "funds_margin")

    # ------------------------------------------------------------------
    # V3 HISTORY (FIXED: Supports /days/1/ structure)
    # ------------------------------------------------------------------
    async def get_historical_candles(
        self, instrument_key: str, interval: str, to_date: str, from_date: str
    ) -> Dict[str, Any]:
        """
        Fetches history using V3 structure.
        Auto-splits 'day' -> 'days/1' to prevent API errors.
        """
        encoded = quote(instrument_key)
        
        # --- V3 FIX: SPLIT INTERVAL INTO UNIT & VALUE ---
        if interval == "day":
            unit, value = "days", "1"
        elif interval == "1minute":
            unit, value = "minutes", "1"
        elif interval == "30minute":
            unit, value = "minutes", "30"
        elif interval == "week":
             unit, value = "weeks", "1"
        elif interval == "month":
             unit, value = "months", "1"
        else:
            # Fallback for manual inputs
            unit, value = "days", "1"

        # Construct V3 URL: /v3/historical-candle/{key}/{unit}/{value}/{to}/{from}
        url = f"{settings.API_BASE_URL}/v3/historical-candle/{encoded}/{unit}/{value}/{to_date}/{from_date}"
        res = await self._request("GET", dynamic_url=url)

        # Handle expiration/empty data gracefully
        if res.get("code") == "UDAPI100072":
            logger.info("Instrument %s expired/invalid – returning empty candles", instrument_key)
            return {"status": "success", "data": {"candles": []}}
            
        if res.get("status") == "success" and not res.get("data", {}).get("candles"):
            logger.warning("No candles for %s – empty frame", instrument_key)
            return {"status": "success", "data": {"candles": []}}
            
        return res

    async def get_intraday_candles(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        # Upstox V3 Intraday URL: /v3/historical-candle/intraday/{key}/{interval}
        encoded = quote(instrument_key)
        url = f"{settings.API_BASE_URL}/v3/historical-candle/intraday/{encoded}/{interval}"
        return await self._request("GET", dynamic_url=url)

    async def get_market_quote_ohlc(self, instrument_key: str, interval: str) -> Dict[str, Any]:
        return await self._request(
            "GET",
            "market_quote_ohlc",
            params={"instrument_key": instrument_key, "interval": interval},
        )
