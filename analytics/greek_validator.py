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
    PRODUCTION FIXED:
    - Tighter tolerance for Delta (8% vs 15%)
    - Robust error handling for broker Greek data
    - Token update lock to prevent race conditions
    """
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, 
                 refresh_sec: int = 15):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        
        # PRODUCTION FIX: Greek-specific tolerance thresholds
        self.tolerance_map = {
            'delta': 8.0,    # Tighter for Delta (critical for hedging)
            'gamma': 15.0,
            'theta': 15.0,
            'vega': 15.0,
            'iv': 12.0
        }
        
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.url = "https://api-v2.upstox.com/v3/market-quote/option-greek"
        self.subscribed = set()
        self.instrument_master = None
        
        # Async Lock for Cache Safety
        self._update_lock = asyncio.Lock()
        
        # CRITICAL FIX: Token Update Lock
        self._token_lock = asyncio.Lock()

    def set_instrument_master(self, master):
        self.instrument_master = master

    def subscribe(self, instrument_keys: set):
        self.subscribed.update(instrument_keys)

    async def update_token(self, new_token: str):
        """Thread-safe token update"""
        async with self._token_lock:
            self.token = new_token
            logger.info("✅ Greek Validator token updated")

    async def start(self):
        logger.info("Starting Greek validation loop")
        while True:
            try:
                await self._validate_once()
            except Exception as e:
                logger.error(f"Greek validation loop error: {e}")
            await asyncio.sleep(self.refresh_sec)

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        """CRITICAL FIX: Safely convert broker response to float"""
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

        # Locked Update with Type Validation
        async with self._update_lock:
            for key, broker in broker_data.items():
                if not isinstance(broker, dict):
                    continue

                sabr = sabr_data.get(key)
                
                # Fallback to sanitized broker data if SABR unavailable
                if not sabr:
                    sanitized_broker = {}
                    for g in ("delta", "theta", "gamma", "vega", "iv"):
                        sanitized_broker[g] = self._safe_float(broker.get(g))
                    sanitized_broker["timestamp"] = datetime.now(IST)
                    self.cache[key] = sanitized_broker
                    continue

                # PRODUCTION FIX: Use greek-specific tolerance
                trusted = {}
                for g in ("delta", "theta", "gamma", "vega", "iv"):
                    b = self._safe_float(broker.get(g))
                    s = self._safe_float(sabr.get(g))
                    
                    # Get tolerance for this greek
                    tolerance = self.tolerance_map.get(g, 15.0)
                    
                    denom = max(abs(s), 1e-6)
                    disc = abs(b - s) * 100 / denom

                    # Trust broker if discrepancy high, else trust SABR
                    if disc > tolerance:
                        trusted[g] = b
                        if g == "delta" and disc > 20:
                            # Log significant delta discrepancies
                            logger.warning(
                                f"⚠️ Large Delta Discrepancy: {key[:20]}... "
                                f"Broker={b:.3f}, SABR={s:.3f}, Diff={disc:.1f}%"
                            )
                    else:
                        trusted[g] = s
                
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        """FIXED: Uses token lock for consistent headers"""
        chunk_size = 500
        keys_list = list(self.subscribed)
        chunks = [keys_list[i:i + chunk_size] for i in range(0, len(keys_list), chunk_size)]
        
        results = {}
        
        # Acquire token lock for consistent headers
        async with self._token_lock:
            headers = {
                "Authorization": f"Bearer {self.token}", 
                "Accept": "application/json"
            }

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
                            logger.error("❌ Greek Fetch 401: Token may be stale")
                            break
                except Exception as e:
                    logger.error(f"Broker Greek fetch error: {e}")
        
        return results

    def _compute_sabr_greeks(self, keys) -> Dict[str, dict]:
        if not self.sabr.calibrated: 
            return {}
        
        engine = HybridPricingEngine(self.sabr)
        out = {}
        spot = 25000.0  # Should use RT quotes
        
        for key in keys:
            try:
                if self.instrument_master and self.instrument_master.df is not None:
                    row = self.instrument_master.df[self.instrument_master.df['instrument_key'] == key]
                    if row.empty: 
                        continue
                    
                    strike = float(row.iloc[0]['strike'])
                    opt_type = row.iloc[0]['option_type']
                    
                    try:
                        expiry_raw = row.iloc[0]['expiry']
                        expiry = expiry_raw.strftime("%Y-%m-%d") if hasattr(expiry_raw, 'strftime') else str(expiry_raw)
                    except Exception:
                        continue
                    
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
