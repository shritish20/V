import asyncio
import logging
from typing import Dict, Any, Union
from datetime import datetime
import aiohttp

from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("GreekValidator")

class GreekValidator:
    """
    FIXED: Added robust error handling and type safety for broker Greek data.
    """
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, refresh_sec: int = 15, tolerance_pct: float = 15.0):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        self.tolerance_pct = tolerance_pct
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        self.subscribed = set()
        self.instrument_master = None
        
        # FIX: Async Lock for Cache Safety
        self._update_lock = asyncio.Lock()

    def set_instrument_master(self, master):
        self.instrument_master = master

    def subscribe(self, instrument_keys: set):
        self.subscribed.update(instrument_keys)

    async def start(self):
        logger.info("Starting Greek validation loop")
        while True:
            try:
                await self._validate_once()
            except Exception as e:
                logger.error(f"Greek validation loop error: {e}")
            await asyncio.sleep(self.refresh_sec)

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        """
        CRITICAL FIX: Safely convert broker response values to float.
        Handles None, 'null', strings, and malformed data to prevent crashes.
        """
        try:
            if val in (None, "", "null"):
                return default
            return float(val)
        except (ValueError, TypeError):
            return default

    async def _validate_once(self):
        if not self.subscribed: return

        broker_data = await self._fetch_broker_greeks()
        sabr_data = self._compute_sabr_greeks(broker_data.keys())

        # FIX: Locked Update with Type Validation
        async with self._update_lock:
            for key, broker in broker_data.items():
                # Ensure broker data is a valid dictionary
                if not isinstance(broker, dict):
                    continue

                sabr = sabr_data.get(key)
                
                # If SABR failed or not ready, fallback to sanitized broker data
                if not sabr:
                    sanitized_broker = {}
                    for g in ("delta", "theta", "gamma", "vega", "iv"):
                        sanitized_broker[g] = self._safe_float(broker.get(g))
                    sanitized_broker["timestamp"] = datetime.now(IST)
                    self.cache[key] = sanitized_broker
                    continue

                trusted = {}
                for g in ("delta", "theta", "gamma", "vega", "iv"):
                    # Use safe float conversion
                    b = self._safe_float(broker.get(g))
                    s = self._safe_float(sabr.get(g))
                    
                    denom = max(abs(s), 1e-6)
                    disc = abs(b - s) * 100 / denom

                    # If discrepancy is high, trust the Broker (Market Maker) values
                    # If discrepancy is low, trust our SABR model (smoothness)
                    if disc > self.tolerance_pct:
                        trusted[g] = b
                    else:
                        trusted[g] = s
                
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        chunk_size = 500
        keys_list = list(self.subscribed)
        chunks = [keys_list[i:i + chunk_size] for i in range(0, len(keys_list), chunk_size)]
        
        results = {}
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}

        async with aiohttp.ClientSession() as session:
            for chunk in chunks:
                params = {"instrument_key": ",".join(chunk)}
                try:
                    async with session.get(self.url, headers=headers, params=params) as r:
                        if r.status == 200:
                            data = await r.json()
                            if data.get("status") == "success":
                                results.update(data.get("data", {}))
                except Exception as e:
                    logger.error(f"Broker Greek fetch error: {e}")
        return results

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        if not self.sabr.calibrated: return {}
        engine = HybridPricingEngine(self.sabr)
        out = {}
        spot = 25000.0 # Should ideally use RT quotes
        
        for key in keys:
            try:
                if self.instrument_master and self.instrument_master.df is not None:
                    row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == key]
                    if row.empty: continue
                    
                    strike = float(row.iloc[0]['strike'])
                    opt_type = row.iloc[0]['option_type']
                    # Ensure date parsing is robust
                    try:
                        expiry_raw = row.iloc[0]['expiry']
                        expiry = expiry_raw.strftime("%Y-%m-%d") if hasattr(expiry_raw, 'strftime') else str(expiry_raw)
                    except Exception:
                        continue
                    
                    gsnap = engine.calculate_greeks(spot, strike, opt_type, expiry)
                    
                    out[key] = {
                        "delta": gsnap.delta, "theta": gsnap.theta,
                        "gamma": gsnap.gamma, "vega": gsnap.vega, "iv": gsnap.iv
                    }
            except Exception:
                continue
        return out



