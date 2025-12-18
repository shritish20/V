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
    HARDENED v4.0:
    Now includes Confidence Scoring to prevent "Silent Risk".
    Calculates a trust score (0.0 - 1.0) based on divergence between
    Broker data and SABR model.
    """
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, refresh_sec: int = 15):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        
        # Tolerance thresholds for confidence penalty
        self.tolerance_map = {
            'delta': 8.0,
            'gamma': 15.0,
            'theta': 15.0,
            'vega': 15.0,
            'iv': 12.0
        }
        
        self.liquidity_thresholds = {
            'high_volume': 1000,
            'medium_volume': 100,
            'low_volume': 10,
            'max_spread_pct': 2.0 
        }

        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
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
            logger.info("🔐 Greek Validator token updated")

    async def start(self):
        logger.info("🚀 Starting Greek validation loop with Confidence Scoring")
        while True:
            try:
                await self._validate_once()
            except Exception as e:
                logger.error(f"Greek validation loop error: {e}")
            await asyncio.sleep(self.refresh_sec)

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        try:
            if val in (None, "", "null"):
                return default
            return float(val)
        except (ValueError, TypeError):
            return default

    async def _validate_once(self):
        if not self.subscribed:
            return

        broker_data = await self._fetch_broker_greeks()
        sabr_data = self._compute_sabr_greeks(broker_data.keys())

        async with self._update_lock:
            for key, broker in broker_data.items():
                if not isinstance(broker, dict):
                    continue
                
                sabr = sabr_data.get(key)
                
                # Fallback if SABR unavailable (Score = 0.5 default for unverified)
                if not sabr:
                    sanitized_broker = {}
                    for g in ("delta", "theta", "gamma", "vega", "iv"):
                        sanitized_broker[g] = self._safe_float(broker.get(g))
                    sanitized_broker["confidence_score"] = 0.5 
                    sanitized_broker["timestamp"] = datetime.now(IST)
                    self.cache[key] = sanitized_broker
                    continue

                # HARDENED: Calculate Confidence Score
                trusted = self._smart_greek_selection(key, broker, sabr)
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    def _smart_greek_selection(self, instrument_key: str, broker: Dict, sabr: Dict) -> Dict[str, float]:
        """
        Calculates value AND confidence score.
        High Divergence = Low Confidence.
        """
        option_meta = self._get_option_metadata(instrument_key)
        liquidity_level = self._assess_liquidity(broker, option_meta)
        
        trusted = {}
        divergence_penalty = 0.0
        checks_count = 0

        for g in ("delta", "theta", "gamma", "vega", "iv"):
            b = self._safe_float(broker.get(g))
            s = self._safe_float(sabr.get(g))
            tolerance = self.tolerance_map.get(g, 15.0)
            
            denom = max(abs(s), 1e-6)
            disc = abs(b - s) * 100 / denom
            
            # Confidence Scoring Logic
            if disc > tolerance * 2:
                divergence_penalty += 0.2  # Heavy penalty
            elif disc > tolerance:
                divergence_penalty += 0.1  # Moderate penalty
            checks_count += 1

            # Selection Logic
            if liquidity_level == "HIGH":
                trusted[g] = b
            elif liquidity_level == "LOW":
                trusted[g] = s
            else: # MEDIUM
                if disc <= tolerance:
                    trusted[g] = s
                else:
                    trusted[g] = (0.6 * b) + (0.4 * s)

        # Calculate Final Confidence Score (1.0 = Perfect, 0.0 = Untrustworthy)
        base_confidence = 1.0
        if liquidity_level == "LOW":
            base_confidence = 0.8  # Illiquid is inherently riskier
            
        final_score = max(0.0, base_confidence - divergence_penalty)
        trusted["confidence_score"] = round(final_score, 2)

        if final_score < 0.5:
            logger.warning(f"⚠️ Low Greek Confidence ({final_score}) for {instrument_key}")

        return trusted

    def _get_option_metadata(self, instrument_key: str) -> Dict[str, Any]:
        try:
            if self.instrument_master and self.instrument_master.df is not None:
                row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == instrument_key]
                if not row.empty:
                    strike = float(row.iloc[0]['strike'])
                    option_type = row.iloc[0]['option_type']
                    return {'strike': strike, 'option_type': option_type, 'moneyness': 'ATM'}
        except Exception:
            pass
        return {'strike': 0, 'option_type': 'CE', 'moneyness': 'UNKNOWN'}

    def _assess_liquidity(self, broker_data: Dict, option_meta: Dict) -> str:
        volume = self._safe_float(broker_data.get('volume', 0))
        if volume > self.liquidity_thresholds['high_volume']:
            return "HIGH"
        elif volume < self.liquidity_thresholds['low_volume']:
            return "LOW"
            
        bid = self._safe_float(broker_data.get('bid_price', 0))
        ask = self._safe_float(broker_data.get('ask_price', 0))
        if bid > 0 and ask > 0:
            spread_pct = ((ask - bid) / bid) * 100
            if spread_pct > self.liquidity_thresholds['max_spread_pct']:
                return "LOW"
        
        moneyness = option_meta.get('moneyness', 'UNKNOWN')
        if moneyness == 'ATM': return "HIGH"
        elif moneyness in ['DEEP_OTM', 'DEEP_ITM']: return "LOW"
        
        return "MEDIUM"

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        chunk_size = 500
        keys_list = list(self.subscribed)
        chunks = [keys_list[i:i + chunk_size] for i in range(0, len(keys_list), chunk_size)]
        results = {}
        
        async with self._token_lock:
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
                            elif r.status == 401:
                                logger.error("🔐 Greek Fetch 401: Token may be stale")
                                break
                    except Exception as e:
                        logger.error(f"Broker Greek fetch error: {e}")
                        return results
        return results

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        if not self.sabr.calibrated:
            return {}
        
        engine = HybridPricingEngine(self.sabr)
        out = {}
        # In production this should be passed in or fetched from rt_quotes
        spot = 25000.0 
        
        for key in keys:
            try:
                if self.instrument_master and self.instrument_master.df is not None:
                    row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == key]
                    if row.empty: continue
                    
                    strike = float(row.iloc[0]['strike'])
                    opt_type = row.iloc[0]['option_type']
                    expiry_raw = row.iloc[0]['expiry']
                    expiry = expiry_raw.strftime("%Y-%m-%d") if hasattr(expiry_raw, 'strftime') else str(expiry_raw)
                    
                    gsnap = engine.calculate_greeks(spot, strike, opt_type, expiry)
                    out[key] = {
                        "delta": gsnap.delta, "theta": gsnap.theta,
                        "gamma": gsnap.gamma, "vega": gsnap.vega,
                        "iv": gsnap.iv
                    }
            except Exception:
                continue
        return out
