import asyncio
import logging
from typing import Dict, Any
from datetime import datetime
import aiohttp
from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel

logger = logging.getLogger("GreekValidator")

class GreekValidator:
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, refresh_sec: int = 15):
        self.cache      = validated_cache
        self.sabr       = sabr_model
        self.refresh_sec= refresh_sec
        self.tolerance  = {'delta': 8.0, 'gamma': 15.0, 'theta': 15.0, 'vega': 15.0, 'iv': 12.0}
        self.token      = settings.UPSTOX_ACCESS_TOKEN
        self.url        = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        self.subscribed : set[str] = set()
        self.instrument_master = None
        self._update_lock = asyncio.Lock()
        self._token_lock  = asyncio.Lock()

    # ---------- public API ----------
    def set_instrument_master(self, master): self.instrument_master = master

    async def update_token(self, new_token: str):
        async with self._token_lock:
            self.token = new_token
            logger.info("🔐 GreekValidator token rotated")

    def subscribe(self, keys: set[str]): self.subscribed.update(keys)

    async def start(self):
        logger.info("🚀 GreekValidator loop started (cold-math safe)")
        while True:
            try:
                await self._validate_once()
            except Exception as e:
                logger.error(f"Validator cycle error: {e}")
            await asyncio.sleep(self.refresh_sec)

    # ---------- internal ----------
    async def _validate_once(self):
        if not self.subscribed: return
        broker = await self._fetch_broker_greeks()
        sabr   = self._compute_sabr_greeks(broker.keys())

        async with self._update_lock:
            for key, b_dict in broker.items():
                sabr_dict = sabr.get(key)
                if not sabr_dict:                      # FIX 1: SABR failed → 0 confidence
                    self.cache[key] = {"confidence_score": 0.0, "timestamp": datetime.now(IST)}
                    continue
                trusted = self._smart_select(key, b_dict, sabr_dict)
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    def _smart_select(self, key: str, b: Dict, s: Dict) -> Dict[str, float]:
        trusted: Dict[str, float] = {}
        penalty = 0.0
        for g in ("delta", "gamma", "theta", "vega", "iv"):
            bv, sv = float(b.get(g, 0)), float(s.get(g, 0))
            # FIX 2: relative tolerance – prevents 50 000 % divergence on tiny IV
            denom  = max(abs(sv), 0.01)
            disc   = abs(bv - sv) * 100 / denom
            tol    = self.tolerance.get(g, 15.0)
            if disc > tol * 2:      penalty += 0.25
            elif disc > tol:        penalty += 0.10
            trusted[g] = bv if disc < tol else sv
        trusted["confidence_score"] = max(0.0, round(1.0 - penalty, 2))
        if trusted["confidence_score"] < 0.5:
            logger.warning(f"⚠️ Low confidence {trusted['confidence_score']} for {key}")
        return trusted

    # ---------- data ----------
    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        if not self.token: return {}
        chunks = [list(self.subscribed)[i:i+500] for i in range(0, len(self.subscribed), 500)]
        out = {}
        headers = {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}
        async with aiohttp.ClientSession() as sess:
            for c in chunks:
                try:
                    async with sess.get(self.url, headers=headers, params={"instrument_key": ",".join(c)}) as r:
                        if r.status == 200:
                            data = await r.json()
                            if data.get("status") == "success":
                                out.update(data.get("data", {}))
                        elif r.status == 401:
                            logger.error("🔐 Token rejected – engine must rotate")
                            break
                except Exception as e:
                    logger.debug(f"Greek chunk fetch err: {e}")
        return out

    def _compute_sabr_greeks(self, keys):
        if not self.sabr.calibrated: return {}
        # minimal stub – real implementation same as showcase
        return {}
