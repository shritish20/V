import asyncio
import logging
from typing import Dict
from datetime import datetime
import aiohttp

from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel
from analytics.pricing import HybridPricingEngine

logger = logging.getLogger("GreekValidator")

class GreekValidator:
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, refresh_sec: int = 15, tolerance_pct: float = 15.0):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        self.tolerance_pct = tolerance_pct
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        self.subscribed = set()
        self.instrument_master = None # Injected by engine

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

    async def _validate_once(self):
        if not self.subscribed:
            return

        broker_data = await self._fetch_broker_greeks()
        sabr_data = self._compute_sabr_greeks(broker_data.keys())

        for key, broker in broker_data.items():
            sabr = sabr_data.get(key)
            if not sabr:
                self.cache[key] = broker # Fallback to broker
                continue

            trusted = {}
            for g in ("delta", "theta", "gamma", "vega", "iv"):
                b = broker.get(g, 0)
                s = sabr.get(g, 0)
                
                # Avoid division by zero
                denominator = max(abs(s), 1e-6)
                disc = abs(b - s) * 100 / denominator

                if disc > self.tolerance_pct:
                    # logger.warning(f"{key} {g} Diff: {disc:.1f}% (Using Broker)")
                    trusted[g] = b
                else:
                    trusted[g] = s
            
            trusted["timestamp"] = datetime.now(IST)
            self.cache[key] = trusted

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        # Chunking for API limits
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
                            results.update(data.get("data", {}))
                except Exception as e:
                    logger.error(f"Broker Greek fetch error: {e}")
        return results

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        if not self.sabr.calibrated:
            return {}
            
        engine = HybridPricingEngine(self.sabr)
        out = {}
        # Fetch spot from cache or settings (Assuming engine updates this)
        spot = 25000.0 # Placeholder, ideally inject RT quotes
        
        for key in keys:
            try:
                # FIX: Use InstrumentMaster for safe lookup
                if self.instrument_master and self.instrument_master.df is not None:
                    row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == key]
                    if row.empty: continue
                    
                    strike = float(row.iloc[0]['strike'])
                    opt_type = row.iloc[0]['option_type']
                    expiry = row.iloc[0]['expiry'].strftime("%Y-%m-%d")
                    
                    gsnap = engine.calculate_greeks(spot, strike, opt_type, expiry)
                    
                    out[key] = {
                        "delta": gsnap.delta,
                        "theta": gsnap.theta,
                        "gamma": gsnap.gamma,
                        "vega": gsnap.vega,
                        "iv": gsnap.iv
                    }
            except Exception:
                continue
        return out

