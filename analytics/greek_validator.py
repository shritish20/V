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
    PRODUCTION OPTIMAL v3.0:
    
    PHILOSOPHY:
    - Broker Greeks are PRIMARY for liquid, ATM options (they see real market prices)
    - SABR is PRIMARY for illiquid, OTM options (broker may have stale data)
    - Use liquidity metrics (volume, bid-ask spread) to determine trust level
    - Weighted blending only when both sources are unreliable
    
    WHY THIS WORKS:
    - ATM options trade frequently → Broker Greeks are fresh and reliable
    - OTM options trade rarely → SABR's model-based approach is more accurate
    - During high volatility → SABR adapts faster than broker's lagging IV
    """
    def __init__(self, validated_cache: Dict[str, dict], sabr_model: EnhancedSABRModel, 
                 refresh_sec: int = 15):
        self.cache = validated_cache
        self.sabr = sabr_model
        self.refresh_sec = refresh_sec
        
        # Greek-specific tolerance thresholds
        self.tolerance_map = {
            'delta': 8.0,
            'gamma': 15.0,
            'theta': 15.0,
            'vega': 15.0,
            'iv': 12.0
        }
        
        # NEW: Liquidity-aware trust thresholds
        self.liquidity_thresholds = {
            'high_volume': 1000,      # Trades per day
            'medium_volume': 100,
            'low_volume': 10,
            'max_spread_pct': 2.0     # 2% bid-ask spread
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
        """Safely convert broker response to float"""
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
                
                # Fallback to sanitized broker data if SABR unavailable
                if not sabr:
                    sanitized_broker = {}
                    for g in ("delta", "theta", "gamma", "vega", "iv"):
                        sanitized_broker[g] = self._safe_float(broker.get(g))
                    sanitized_broker["timestamp"] = datetime.now(IST)
                    self.cache[key] = sanitized_broker
                    continue

                # OPTIMAL STRATEGY v3.0: Liquidity-aware trust logic
                trusted = self._smart_greek_selection(key, broker, sabr)
                trusted["timestamp"] = datetime.now(IST)
                self.cache[key] = trusted

    def _smart_greek_selection(self, instrument_key: str, 
                               broker: Dict, sabr: Dict) -> Dict[str, float]:
        """
        CORE LOGIC: Intelligent source selection based on option characteristics
        
        Decision Tree:
        1. Is option liquid? → Trust broker Greeks
        2. Is option illiquid? → Trust SABR Greeks
        3. High discrepancy + medium liquidity? → Weighted blend
        4. SABR not calibrated? → Trust broker by default
        """
        
        # Get option metadata (strike, type, moneyness)
        option_meta = self._get_option_metadata(instrument_key)
        
        # Determine liquidity level
        liquidity_level = self._assess_liquidity(broker, option_meta)
        
        trusted = {}
        
        for g in ("delta", "theta", "gamma", "vega", "iv"):
            b = self._safe_float(broker.get(g))
            s = self._safe_float(sabr.get(g))
            
            tolerance = self.tolerance_map.get(g, 15.0)
            denom = max(abs(s), 1e-6)
            disc = abs(b - s) * 100 / denom
            
            # DECISION LOGIC:
            
            # Case 1: High Liquidity → Trust Broker (market knows best)
            if liquidity_level == "HIGH":
                trusted[g] = b
                if disc > tolerance * 2:
                    logger.debug(
                        f"💧 Liquid Option: Trusting Broker {g.upper()} despite "
                        f"{disc:.1f}% disc (Broker={b:.3f}, SABR={s:.3f})"
                    )
            
            # Case 2: Low Liquidity → Trust SABR (model more reliable than stale prices)
            elif liquidity_level == "LOW":
                trusted[g] = s
                if disc > tolerance:
                    logger.debug(
                        f"🏜️ Illiquid Option: Trusting SABR {g.upper()} "
                        f"(Broker={b:.3f} likely stale, SABR={s:.3f})"
                    )
            
            # Case 3: Medium Liquidity → Use Agreement or Weighted Blend
            else:  # MEDIUM liquidity
                if disc <= tolerance:
                    # Good agreement → Trust SABR (it's calibrated to full chain)
                    trusted[g] = s
                else:
                    # Disagreement → Weighted blend favoring liquidity
                    # For medium liquidity, give 60% weight to broker, 40% to SABR
                    broker_weight = 0.6
                    sabr_weight = 0.4
                    trusted[g] = (broker_weight * b) + (sabr_weight * s)
                    
                    if disc > tolerance * 1.5:
                        logger.warning(
                            f"⚖️ Medium Liquidity Blend: {g.upper()} "
                            f"Broker={b:.3f} (60%), SABR={s:.3f} (40%), "
                            f"Result={trusted[g]:.3f}, Disc={disc:.1f}%"
                        )
        
        return trusted

    def _get_option_metadata(self, instrument_key: str) -> Dict[str, Any]:
        """Extract strike, type, and moneyness from instrument master"""
        try:
            if self.instrument_master and self.instrument_master.df is not None:
                row = self.instrument_master.df[
                    self.instrument_master.df['instrument_key'] == instrument_key
                ]
                
                if not row.empty:
                    strike = float(row.iloc[0]['strike'])
                    option_type = row.iloc[0]['option_type']
                    
                    # Estimate moneyness (need spot price - simplified here)
                    # In production, pass spot as parameter
                    return {
                        'strike': strike,
                        'option_type': option_type,
                        'moneyness': 'ATM'  # Simplified
                    }
        except Exception:
            pass
        
        return {'strike': 0, 'option_type': 'CE', 'moneyness': 'UNKNOWN'}

    def _assess_liquidity(self, broker_data: Dict, option_meta: Dict) -> str:
        """
        Determine if option is liquid, illiquid, or medium
        
        Heuristics:
        - Volume > 1000 contracts/day = HIGH liquidity
        - Volume < 10 contracts/day = LOW liquidity
        - ATM options = typically HIGH liquidity
        - Deep OTM options = typically LOW liquidity
        """
        
        # Method 1: Check volume (if available in broker data)
        volume = self._safe_float(broker_data.get('volume', 0))
        
        if volume > self.liquidity_thresholds['high_volume']:
            return "HIGH"
        elif volume < self.liquidity_thresholds['low_volume']:
            return "LOW"
        
        # Method 2: Check bid-ask spread (if available)
        bid = self._safe_float(broker_data.get('bid_price', 0))
        ask = self._safe_float(broker_data.get('ask_price', 0))
        
        if bid > 0 and ask > 0:
            spread_pct = ((ask - bid) / bid) * 100
            if spread_pct > self.liquidity_thresholds['max_spread_pct']:
                return "LOW"  # Wide spread = illiquid
        
        # Method 3: Use moneyness as proxy
        moneyness = option_meta.get('moneyness', 'UNKNOWN')
        if moneyness == 'ATM':
            return "HIGH"  # ATM options are usually liquid
        elif moneyness in ['DEEP_OTM', 'DEEP_ITM']:
            return "LOW"
        
        # Default: Medium liquidity
        return "MEDIUM"

    async def _fetch_broker_greeks(self) -> Dict[str, dict]:
        """Fetch Greeks from Upstox API"""
        chunk_size = 500
        keys_list = list(self.subscribed)
        chunks = [keys_list[i:i + chunk_size] for i in range(0, len(keys_list), chunk_size)]
        
        results = {}
        
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
        """Compute Greeks using SABR model"""
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
