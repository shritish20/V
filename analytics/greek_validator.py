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

    def subscribe(self, instrument_keys: set):
        self.subscribed.update(instrument_keys)
        logger.debug(f"Greek validator subscribed to {len(self.subscribed)} instruments")

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
                continue
            trusted = {}
            for g in ("delta", "theta", "gamma", "vega", "iv"):
                b = broker.get(g, 0)
                s = sabr.get(g, 0)
                disc = abs(b - s) * 100 / max(abs(s), 1e-6)
                if disc > self.tolerance_pct:
                    logger.warning(f"{key} {g} SABR={s:.3f} broker={b:.3f} disc={disc:.1f}% – using broker")
                    trusted[g] = b
                else:
                    trusted[g] = s
            trusted["timestamp"] = datetime.now(IST)
            self.cache[key] = trusted

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        chunks = [list(self.subscribed)[i:i + 500] for i in range(0, len(self.subscribed), 500)]
        results = {}
        headers = {"Authorization": f"Bearer {self.token}"}
        async with aiohttp.ClientSession() as session:
            for chunk in chunks:
                params = {"instrument_key": ",".join(chunk)}
                async with session.get(self.url, headers=headers, params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        results.update(data.get("data", {}))
                    else:
                        logger.warning(f"Broker Greek fetch failed: {r.status}")
        return results

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        engine = HybridPricingEngine(self.sabr)
        out = {}
        spot = self.cache.get(settings.MARKET_KEY_INDEX, {}).get("last_price", 25000)
        for key in keys:
            try:
                parts = key.split("|")[1].split("")
                strike = float(parts[1])
                opt_type = parts[2]
                expiry_raw = parts[3]
                expiry = f"{expiry_raw[:4]}-{expiry_raw[4:6]}-{expiry_raw[6:8]}"

                class MockPosition:
                    instrument_key = key
                    strike = strike
                    option_type = opt_type
                    expiry_date = expiry
                    spot_price = spot

                gsnap = engine.calculate_greeks(MockPosition())
                out[key] = {
                    "delta": gsnap.delta,
                    "theta": gsnap.theta,
                    "gamma": gsnap.gamma,
                    "vega": gsnap.vega,
                    "iv": gsnap.iv
                }
            except Exception as e:
                logger.debug(f"SABR greek failed for {key}: {e}")
        return out
