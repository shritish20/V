import aiohttp
import asyncio
import time
import logging
import websockets
import json
import random
import uuid
from typing import Optional, List, Dict, Tuple
from core.config import API_BASE_V2, UPSTOX_ACCESS_TOKEN, PAPER_TRADING, WS_BASE_URL, API_BASE_V3
from core.models import Position, GreeksSnapshot, Order, OrderStatus, OrderType
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("VolGuard14")

class HybridUpstoxAPI:
    """Ultra-fast async API with robust error handling, WebSocket, and Margin checks - Enhanced"""
    
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
        
        # WebSocket enhancements
        self.ws_reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.ws_lock = asyncio.Lock()
        self.rt_quotes_lock = asyncio.Lock()
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.ws_connected = False
        
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

    async def subscribe_instruments(self, instrument_keys: List[str]):
        """FIXED: Dynamically subscribe to new instruments"""
        if not self.ws_connected or not self.websocket:
            logger.warning("WebSocket not connected, cannot subscribe")
            return
            
        try:
            subscribe_message = {
                "method": "subscribe",
                "guid": f"vg-sub-{uuid.uuid4().hex[:8]}",
                "data": {"instrumentKeys": instrument_keys}
            }
            await self.websocket.send(json.dumps(subscribe_message))
            logger.info(f"Subscribed to {len(instrument_keys)} instruments")
        except Exception as e:
            logger.error(f"Failed to subscribe to instruments: {e}")

    async def ws_connect_and_stream(self, rt_quotes: Dict[str, float]):
        """Connects to WebSocket and updates the real-time quotes dictionary with thread safety."""
        async with self.ws_lock:  # PREVENT MULTIPLE CONNECTIONS
            if self.ws_reconnect_attempts >= self.max_reconnect_attempts:
                logger.critical("Max WebSocket reconnection attempts reached")
                return
            
            token = await self._get_ws_auth_token()
            if not token:
                self.ws_reconnect_attempts += 1
                await asyncio.sleep(self.reconnect_delay * self.ws_reconnect_attempts)
                asyncio.create_task(self.ws_connect_and_stream(rt_quotes))
                return

        ws_url = f"{WS_BASE_URL}?token={token}"
        initial_instruments = ["NSE_INDEX|Nifty 50", "INDICES|INDIA VIX"]

        try:
            async with websockets.connect(ws_url, ping_interval=5) as websocket:
                self.websocket = websocket
                self.ws_connected = True
                self.ws_reconnect_attempts = 0  # Reset on successful connection
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
                                async with self.rt_quotes_lock:
                                    rt_quotes[instrument_key] = ltp
                                    rt_quotes['timestamp'] = time.time()
                                
                        
                    except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError) as e:
                        logger.warning(f"WebSocket closed. Reconnecting in 5s: {e}")
                        break
                    except Exception as e:
                        logger.error(f"Error processing WS message: {e}")
        
        except Exception as e:
            logger.critical(f"WebSocket failed to connect or stream: {e}")
            self.ws_connected = False
            self.ws_reconnect_attempts += 1
            delay = min(self.reconnect_delay * (2 ** self.ws_reconnect_attempts), 300)  # Exponential backoff
            await asyncio.sleep(delay)
            if self.ws_reconnect_attempts < self.max_reconnect_attempts:
                asyncio.create_task(self.ws_connect_and_stream(rt_quotes))
            
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
                elif "Nifty 50" in inst: mock_data[inst] = {"last_price": 40000.0}
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
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error fetching quotes: {e}")
            return {"data": {}}
        except Exception as e:
            logger.error(f"Unexpected error in get_quotes: {e}")
            return {"data": {}}

    async def get_option_chain_data(self, underlying_symbol: str, expiry_date: str) -> Optional[List[Dict]]:
        """Fetches the raw option chain data required for SABR calibration."""
        if PAPER_TRADING:
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
        """FIXED: Enhanced margin calculation for multi-leg strategies"""
        if PAPER_TRADING:
            # FIXED: Better margin calculation for spreads
            if len(legs) >= 2:
                # For spreads/condors, use max spread width
                strikes = sorted([leg.strike for leg in legs])
                max_spread = strikes[-1] - strikes[0]
                return max_spread * 0.3  # 30% of max spread as margin
            else:
                # For single legs, use traditional calculation
                trade_value = sum(abs(leg.entry_price * leg.quantity) for leg in legs)
                return trade_value * 0.05 

        # FIXED: Use Upstox basket margin API for accurate calculation
        await self._rate_limit()
        session = await self._get_session()
        url = f"{self.base_url}/charges/margin/basket"

        positions = []
        for leg in legs:
            positions.append({
                "instrument_key": leg.instrument_key,
                "quantity": abs(leg.quantity),
                "price": leg.entry_price,
                "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                "product": "I"
            })
        
        payload = {"positions": positions}
        
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('data', {}).get('total_margin_required', 
                        sum(abs(leg.entry_price * leg.quantity) for leg in legs) * 0.2)
                else:
                    logger.warning(f"Basket margin API failed, using fallback: {resp.status}")
                    return sum(abs(leg.entry_price * leg.quantity) for leg in legs) * 0.2
        except Exception as e:
            logger.error(f"Basket margin calculation failed: {e}")
            return sum(abs(leg.entry_price * leg.quantity) for leg in legs) * 0.2

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
        url = f"{API_BASE_V3}/market-quote/option-greek"
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
        """Validates SABR Greeks against broker data."""
        
        if not self.pricing_engine:
            raise Exception("Pricing engine not injected in API client.")

        sabr_greeks = self.pricing_engine.calculate_greeks(spot, strike, opt_type, expiry)
        
        market_greeks_data = await self.get_greeks_from_quote(instrument_key)
        
        if market_greeks_data:
            market_delta = market_greeks_data.get('delta', sabr_greeks.delta)
            market_vega = market_greeks_data.get('vega', sabr_greeks.vega)
            
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

    async def place_order_safe(self, order: Order) -> Tuple[bool, Optional[str]]:
        """Enhanced order placement with ghost order recovery"""
        short_uuid = str(uuid.uuid4())[:4]
        order_tag = f"VG14_{order.parent_trade_id}_{order.instrument_key[-4:]}_{order.retry_count}_{short_uuid}"
        
        if PAPER_TRADING:
            await asyncio.sleep(random.uniform(0.1, 0.5))
            return True, f"SIM_{short_uuid}_{int(time.time())}"

        payload = {
            "quantity": abs(order.quantity),
            "product": order.product,
            "validity": order.validity,
            "price": round(order.price, 2),
            "tag": order_tag,
            "instrument_key": order.instrument_key,
            "order_type": order.order_type.value,
            "transaction_type": order.transaction_type,
            "disclosed_quantity": order.disclosed_quantity,
            "trigger_price": order.trigger_price
        }

        try:
            await self._rate_limit()
            session = await self._get_session()
            url = f"{self.base_url}/order/place"
            
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success":
                        return True, data["data"]["order_id"]
                    else:
                        logger.error(f"Order Rejected: {data}")
                        return False, None
                else:
                    logger.warning(f"Order HTTP {resp.status}. Checking Orderbook...")
                    return await self._recover_ghost_order(order_tag)

        except Exception as e:
            logger.error(f"Network error placing order: {e}. Checking Orderbook...")
            return await self._recover_ghost_order(order_tag)

    async def _recover_ghost_order(self, tag: str) -> Tuple[bool, Optional[str]]:
        """Recover ghost orders from order book"""
        try:
            await asyncio.sleep(1.0)
            session = await self._get_session()
            url = f"{self.base_url}/order/retrieve-all"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for o in data.get("data", []):
                        if o.get("tag") == tag:
                            logger.info(f"✅ Ghost Order Found! ID: {o['order_id']}")
                            return True, o['order_id']
        except Exception as e:
            logger.error(f"Failed to scan order book: {e}")
        return False, None

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
        self.ws_connected = False
        if self.websocket:
            await self.websocket.close()
        if self.session and not self.session.closed:
            await self.session.close()
