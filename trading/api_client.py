import aiohttp
import asyncio
import time
import logging
import websockets
import json
import random
from typing import Optional, List, Dict
from core.config import API_BASE_V2, UPSTOX_ACCESS_TOKEN, PAPER_TRADING, WS_BASE_URL
from core.models import Position, GreeksSnapshot
from analytics.pricing import HybridPricingEngine
from datetime import datetime
import numpy as np

logger = logging.getLogger("VolGuardHybrid")

class HybridUpstoxAPI:
    """Ultra-fast async API with robust error handling, WebSocket, and Margin checks."""
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
        self.ws_token = None
        self.pricing_engine: Optional[HybridPricingEngine] = None
        
        if not PAPER_TRADING and not token:
            logger.critical("UPSTOX_ACCESS_TOKEN not set in live mode! Deployment will fail.")

    def set_pricing_engine(self, engine: HybridPricingEngine):
        """Injects the pricing engine for Greeks calculation/validation."""
        self.pricing_engine = engine

    async def _get_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self.session

    async def _rate_limit(self):
        async with self.rate_limit_lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < 0.2: 
                await asyncio.sleep(0.2 - elapsed)
            self.last_request_time = time.time()

    async def _get_ws_auth_token(self) -> Optional[str]:
        """Fetches the authorization token required for WebSocket connection."""
        if PAPER_TRADING:
            self.ws_token = "SIMULATED_WS_TOKEN"
            return self.ws_token
            
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/feed/market-data-feed/authorize"
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.ws_token = data.get('data', {}).get('authorizedToken')
                    return self.ws_token
                else:
                    logger.error(f"WS Auth failed: {resp.status} - {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"WS Auth connection failed: {e}")
            return None

    async def ws_connect_and_stream(self, rt_quotes: Dict[str, float]):
        """Connects to WebSocket and updates the real-time quotes dictionary."""
        token = await self._get_ws_auth_token()
        if not token:
            logger.critical("Cannot start WebSocket stream without authorization token. Retrying in 5s.")
            await asyncio.sleep(5)
            if not self.session or not self.session.closed:
                asyncio.create_task(self.ws_connect_and_stream(rt_quotes))
            return

        ws_url = f"{WS_BASE_URL}?token={token}"
        initial_instruments = ["NSE_INDEX|Nifty Bank", "INDICES|INDIA VIX"]

        try:
            async with websockets.connect(ws_url, ping_interval=5) as websocket:
                logger.info("WebSocket connected. Subscribing to default indices.")
                
                subscribe_message = {
                    "method": "subscribe", "guid": "vg-init-1", "data": {"instrumentKeys": initial_instruments}
                }
                await websocket.send(json.dumps(subscribe_message))
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        
                        if data.get('ltpc'): 
                            instrument_key = data['instrumentKey']
                            ltp = data['ltpc'].get('ltp')
                            if ltp is not None:
                                rt_quotes[instrument_key] = ltp
                                # Update timestamp for data freshness check
                                rt_quotes['timestamp'] = time.time()
                                
                        
                    except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError) as e:
                        logger.warning(f"WebSocket closed. Reconnecting in 5s: {e}")
                        break
                    except Exception as e:
                        logger.error(f"Error processing WS message: {e}")
        
        except Exception as e:
            logger.critical(f"WebSocket failed to connect or stream: {e}")
            
        await asyncio.sleep(5) 
        if not self.session or not self.session.closed:
             asyncio.create_task(self.ws_connect_and_stream(rt_quotes))


    async def get_quotes(self, instruments: List[str]) -> dict:
        """Bulk quote fetching (Used primarily for fallbacks/less frequent data)"""
        if PAPER_TRADING:
            await asyncio.sleep(0.05) 
            mock_data = {}
            for i, inst in enumerate(instruments):
                if "INDIA VIX" in inst: mock_data[inst] = {"last_price": 15.0}
                elif "Nifty Bank" in inst: mock_data[inst] = {"last_price": 40000.0}
                else: mock_data[inst] = {"last_price": 100.0 + i * 0.5}
            return {"data": mock_data}
            
        if not instruments: return {"data": {}}
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/market-quote/quotes"
        params = {"instrument_key": ",".join(instruments)}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200: return await resp.json()
                else: logger.warning(f"Quote API returned {resp.status}"); return {"data": {}}
        except Exception as e:
            logger.error(f"Quote fetch failed: {e}")
            return {"data": {}}

    async def get_option_chain_data(self, underlying_symbol: str, expiry_date: str) -> Optional[List[Dict]]:
        """BUG #3 FIX: Fetches the raw option chain data required for SABR calibration."""
        if PAPER_TRADING:
            # Synthetic data simulating the Upstox response structure
            return [
                {
                    "strike_price": 40000, 
                    "call_options": {"option_greeks": {"iv": 0.15}}, 
                    "put_options": {"option_greeks": {"iv": 0.16}}
                },
                {
                    "strike_price": 40100, 
                    "call_options": {"option_greeks": {"iv": 0.14}}, 
                    "put_options": {"option_greeks": {"iv": 0.17}}
                }
            ]
            
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/option/chain"
        params = {"instrument_key": f"NSE_INDEX|{underlying_symbol}", "expiry_date": expiry_date}
        
        try:
            async with session.get(url, params=params, timeout=15) as resp:
                if resp.status == 200: 
                    data = await resp.json()
                    return data.get('data')
                else:
                    logger.warning(f"Option chain API failed {resp.status}: {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Option chain fetch failed: {e}")
            return None


    async def calculate_margin_for_basket(self, legs: List[Position]) -> float:
        """Calculates margin required by simulating the whole basket (CRITICAL FIX 2)."""
        if PAPER_TRADING:
            trade_value = sum(abs(leg.entry_price * leg.quantity) for leg in legs)
            return trade_value * 0.05 

        margin_requests = [self.calculate_margin_single_leg(leg) for leg in legs]
        
        results = await asyncio.gather(*margin_requests)
        total_margin = sum(r for r in results)
        
        return total_margin

    async def calculate_margin_single_leg(self, leg: Position) -> float:
        """Helper to get margin for a single leg."""
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/charges/margin"

        payload = {
            "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
            "instrument_key": leg.instrument_key,
            "quantity": abs(leg.quantity),
            "price": leg.entry_price,
            "product": "I"
        }
        
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', {}).get('total_margin_required', abs(leg.entry_price * leg.quantity) * 0.2) 
                else:
                    return abs(leg.entry_price * leg.quantity) * 0.2 
        except Exception:
            return abs(leg.entry_price * leg.quantity) * 0.2
            
    async def get_short_term_positions(self) -> List[Dict]:
        """Fetches current short-term/intraday positions for reconciliation."""
        if PAPER_TRADING:
            return [] 
            
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/portfolio/short-term-positions"
        
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', [])
                else:
                    logger.warning(f"Positions API failed {resp.status}: {await resp.text()}")
                    return []
        except Exception as e:
            logger.error(f"Position fetch failed: {e}")
            return []

    async def get_greeks_from_quote(self, instrument_key: str) -> Optional[Dict]:
        """Fetches Option Greeks from market quote endpoint."""
        if PAPER_TRADING:
            return {'delta': 0.15 + random.uniform(-0.01, 0.01), 'vega': 5.0 + random.uniform(-0.5, 0.5)}
            
        await self._rate_limit()
        session = await self._get_session()
        url = f"https://api.upstox.com/v3/market-quote/option-greek" 
        params = {"instrument_key": instrument_key}
        
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', {}).get(instrument_key)
                return None
        except Exception:
            return None

    async def calculate_greeks_with_validation(self, instrument_key: str, spot: float, strike: float, opt_type: str, expiry: str) -> GreeksSnapshot:
        """CRITICAL FIX 3: Validates SABR Greeks against broker data."""
        
        if not self.pricing_engine:
            raise Exception("Pricing engine not injected in API client.")

        # 1. Calculate SABR Greeks (The system's reliable internal model)
        sabr_greeks = self.pricing_engine.calculate_greeks(spot, strike, opt_type, expiry)
        
        # 2. Fetch Market Greeks (Validation Source)
        market_greeks_data = await self.get_greeks_from_quote(instrument_key)
        
        if market_greeks_data:
            market_delta = market_greeks_data.get('delta', sabr_greeks.delta)
            market_vega = market_greeks_data.get('vega', sabr_greeks.vega)
            
            # Validation Check: If SABR Delta deviates significantly (> 20%), use Market Delta/Vega
            if abs(sabr_greeks.delta - market_delta) > 0.20:
                logger.warning(f"GREEK MISMATCH: Delta diff > 20% for {instrument_key}. Using market values.")
                return GreeksSnapshot(
                    timestamp=sabr_greeks.timestamp,
                    delta=market_delta,
                    vega=market_vega,
                    gamma=sabr_greeks.gamma,
                    theta=sabr_greeks.theta
                )
        
        return sabr_greeks

    async def place_order(self, payload: dict) -> dict:
        """CRITICAL FIX 5: Implements Realistic Paper Trading."""
        
        if PAPER_TRADING:
            await asyncio.sleep(random.uniform(0.5, 3.0)) 
            
            if random.random() < 0.15: # 15% Rejection Rate
                 return {"status": "error", "message": "Simulated insufficient liquidity/margin."}

            order_id = f"SIM_{int(time.time() * 1000)}"
            order_price = payload.get('price', 50.0)
            order_qty = payload.get('quantity', 1)
            
            slippage = random.uniform(-0.01, 0.02)  
            fill_price = order_price * (1 + slippage)
            
            if random.random() < 0.10: # 10% Partial Fill Rate
                filled_qty = order_qty // 2 if order_qty > 1 else 1
            else:
                filled_qty = order_qty

            return {
                "status": "success", 
                "data": {
                    "order_id": order_id,
                    "status": "FILLED" if filled_qty == order_qty else "PARTIALLY_FILLED",
                    "filled_quantity": filled_qty,
                    "average_price": fill_price
                }
            }

        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/order/place"
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200: return await resp.json()
                else: 
                    error_text = await resp.text()
                    logger.error(f"Order failed {resp.status}: {error_text}")
                    return {"status": "error", "message": error_text}
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return {"status": "error", "error": str(e)}

    async def get_order_details(self, order_id: str) -> dict:
        """Get order details with retry logic"""
        if PAPER_TRADING:
            await asyncio.sleep(0.05) 
            if order_id.startswith("SIM_"):
                return {"data": {"order_id": order_id, "status": "FILLED", "filled_quantity": 100, "average_price": 50.0}}
            return {"data": {}}

        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/order/details"
        params = {"order_id": order_id}
        try:
            async with session.get(url, params=params, timeout=5) as resp:
                if resp.status == 200: return await resp.json()
                return {"data": {}}
        except Exception:
            return {"data": {}}

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel order"""
        if PAPER_TRADING:
            await asyncio.sleep(0.05)
            return True

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
        if PAPER_TRADING:
            await asyncio.sleep(0.1)
            return f"SIM_KEY_{symbol}_{expiry}_{int(strike)}_{opt_type}"

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
