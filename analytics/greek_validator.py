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
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, refresh_sec: int = 15):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        self.tolerance_map = {'delta': 8.0, 'gamma': 15.0, 'theta': 15.0, 'vega': 15.0, 'iv': 12.0}
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.url = f"{settings.API_BASE_URL}/v3/market-quote/option-greek"
        self.subscribed = set()
        self.instrument_master = None
        self._update_lock = asyncio.Lock()
        self._token_lock = asyncio.Lock()

    def set_instrument_master(self, master):
        self.instrument_master = master

    def subscribe(self, instrument_keys: set):
        self.subscribed.update(instrument_keys)

    async def update_token(self, new_token: str):
        async with self._token_lock:
            self.token = new_token
            logger.info("Greek Validator token updated")

    async def start(self):
        logger.info("Starting Hardened Greek Validation Loop")
        while True:
            try:
                await self._validate_once()
            except Exception as e:
                logger.error(f"Greek validation loop error: {e}")
            await asyncio.sleep(self.refresh_sec)

    async def _validate_once(self):
        if not self.subscribed: return
        broker_data = await self._fetch_broker_greeks()
        sabr_data = self._compute_sabr_greeks(broker_data.keys())

        async with self._update_lock:
            for key, broker in broker_data.items():
                if not isinstance(broker, dict): continue
                sabr = sabr_data.get(key)
                if not sabr:
                    # Defusal: Hard 0.0 confidence if SABR fails to avoid "marketing" scores
                    self.cache[key] = {"confidence_score": 0.0, "timestamp": datetime.now(IST)}
                    continue

                trusted = self._smart_greek_selection(key, broker, sabr)
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    def _smart_greek_selection(self, instrument_key: str, broker: Dict, sabr: Dict) -> Dict[str, float]:
        trusted = {}
        penalty = 0.0
        for g in ("delta", "theta", "gamma", "vega", "iv"):
            b, s = float(broker.get(g, 0)), float(sabr.get(g, 0))
            # [span_2](start_span)Defusal: Relative tolerance math to prevent the "OTM 50,000% divergence" trap[span_2](end_span).
            # Using max(abs(s), 0.01) prevents freezing on tiny decimal divergences.
            denom = max(abs(s), 0.01)
            disc = abs(b - s) * 100 / denom
            
            if disc > self.tolerance_map.get(g, 15.0) * 2: penalty += 0.25
            trusted[g] = b if disc < self.tolerance_map.get(g, 15.0) else s

        trusted["confidence_score"] = max(0.0, round(1.0 - penalty, 2))
        return trusted

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        if not self.subscribed: return {}
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        async with aiohttp.ClientSession() as session:
            params = {"instrument_key": ",".join(list(self.subscribed)[:500])}
            try:
                async with session.get(self.url, headers=headers, params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        return data.get("data", {})
            except Exception: pass
        return {}

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        if not self.sabr.calibrated: return {}
        engine = HybridPricingEngine(self.sabr)
        out = {}
        spot = 25000.0 # Standard Nifty reference
        for key in keys:
            try:
                if self.instrument_master and self.instrument_master.df is not None:
                    row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == key]
                    if row.empty: continue
                    gsnap = engine.calculate_greeks(spot, float(row.iloc[0]['strike']), row.iloc[0]['option_type'], str(row.iloc[0]['expiry']))
                    out[key] = {"delta": gsnap.delta, "theta": gsnap.theta, "gamma": gsnap.gamma, "vega": gsnap.vega, "iv": gsnap.iv}
            except: continue
        return out
