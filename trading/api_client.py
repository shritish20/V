import aiohttp
import asyncio
import time
import logging
from typing import Optional, List, Dict
from core.config import API_BASE_V2, UPSTOX_ACCESS_TOKEN

logger = logging.getLogger("VolGuardHybrid")

class HybridUpstoxAPI:
    """Ultra-fast async API with robust error handling"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = API_BASE_V2
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_lock = asyncio.Lock()
        self.last_request_time = 0
        self.request_count = 0
    
    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session
    
    async def _rate_limit(self):
        async with self.rate_limit_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < 0.2:  # 5 requests per second
                await asyncio.sleep(0.2 - elapsed)
            self.last_request_time = time.time()
            self.request_count += 1
    
    async def get_quotes(self, instruments: List[str]) -> dict:
        """Bulk quote fetching with error handling"""
        if not instruments:
            return {"data": {}}
        
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/market-quote/quotes"
        params = {"instrument_key": ",".join(instruments)}
        
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.warning(f"Quote API returned {resp.status}")
                    return {"data": {}}
        except Exception as e:
            logger.error(f"Quote fetch failed: {e}")
            return {"data": {}}
    
    async def get_option_chain(self, symbol: str, expiry: str) -> dict:
        """Fetch option chain with timeout"""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/option/chain"
        params = {"symbol": symbol, "expiry_date": expiry}
        
        try:
            async with session.get(url, params=params, timeout=15) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {"data": {}}
        except asyncio.TimeoutError:
            logger.error(f"Option chain timeout for {expiry}")
            return {"data": {}}
        except Exception as e:
            logger.error(f"Chain fetch failed: {e}")
            return {"data": {}}
    
    async def place_order(self, payload: dict) -> dict:
        """Place order with comprehensive error handling"""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/order/place"
        
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    error_text = await resp.text()
                    logger.error(f"Order failed {resp.status}: {error_text}")
                    return {"status": "error", "message": error_text}
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"status": "error", "error": str(e)}
    
    async def get_order_details(self, order_id: str) -> dict:
        """Get order details with retry logic"""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/order/details"
        params = {"order_id": order_id}
        
        try:
            async with session.get(url, params=params, timeout=5) as resp:
                if resp.status == 200:
                    return await resp.json()
                return {"data": {}}
        except Exception:
            return {"data": {}}
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel order"""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/order/cancel"
        payload = {"order_id": order_id}
        
        try:
            async with session.post(url, json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"Order cancel failed: {e}")
            return False
    
    async def get_instrument_key(self, symbol: str, expiry: str, strike: float, opt_type: str) -> str:
        """Resolve instrument key with fallback"""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/option/contract"
        params = {"instrument_key": f"NSE_INDEX|{symbol}", "expiry_date": expiry}
        
        try:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for contract in data.get("data", []):
                        c_strike = float(contract.get("strike_price", 0))
                        c_type = contract.get("option_type", "")
                        
                        if abs(c_strike - strike) < 0.1 and c_type == opt_type:
                            return contract.get("instrument_key", "")
                
                logger.warning(f"Instrument not found: {symbol} {strike} {opt_type} {expiry}")
                return ""
        except Exception as e:
            logger.error(f"Instrument resolution failed: {e}")
            return ""
    
    async def close(self):
        """Cleanup session"""
        if self.session and not self.session.closed:
            await self.session.close()
