import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date, time as dtime

from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel

logger = logging.getLogger("PricingEngine")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MAX_IV_PCT        = 5.0          # 500% Max IV (Sanity Check)
CALIB_TIMEOUT_SEC = 15           # Max time allowed for Math Optimization
MIN_POINTS        = 5            # Min strikes required for a valid fit

class DataIntegrityError(RuntimeError): pass
class CalibrationError(RuntimeError): pass

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel) -> None:
        self.sabr = sabr_model
        self.api: Any = None
        self.instrument_master: Any = None

    def set_api(self, api: Any) -> None:
        self.api = api
        self.instrument_master = api.instrument_master

    async def get_market_structure(self, spot: float) -> Dict[str, Any]:
        """Calculates Skew, Term Structure, and Efficiency for Strategy Engine."""
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            # 1. Fetch Expiries
            expiries = self.instrument_master.get_all_expiries(settings.UNDERLYING_SYMBOL)
            if len(expiries) < 2:
                raise DataIntegrityError("Need ≥ 2 expiries for Term Structure")

            now = datetime.now(IST)
            near_exp, far_exp = self._select_expiries(expiries, now)
            dte = max(0.001, self._calculate_dte(near_exp, now))

            # 2. Parallel Data Fetch
            chain_near, chain_far = await asyncio.gather(
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_exp.isoformat()),
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_exp.isoformat()),
            )

            if not chain_near.get("data"):
                raise DataIntegrityError("Received Empty Near Chain")

            # 3. Process Metrics
            atm_strike = round(spot / 50) * 50
            near_metrics = self._extract_atm_metrics(chain_near["data"], atm_strike)
            far_metrics  = self._extract_atm_metrics(chain_far.get("data", []), atm_strike)

            # --- 🔥 QUANT UPGRADE: SKEW & TERM STRUCTURE ---
            
            # Skew: Difference between 5% OTM Put IV and 5% OTM Call IV
            # Positive skew = Puts are expensive (Fear)
            skew_index = self._calculate_skew(chain_near["data"], spot)
            
            # Term Structure Slope: % difference between near and far IV
            # If Slope > 0: Backwardation (Panic - Weekly IV > Monthly IV)
            slope = 0.0
            if far_metrics["iv"] > 0:
                slope = (near_metrics["iv"] - far_metrics["iv"]) / far_metrics["iv"]

            # 4. Calibration & Efficiency
            await self._calibrate_if_needed(chain_near["data"], atm_strike, spot, dte)
            eff_table = self._build_efficiency_table(chain_near["data"], spot)

            return {
                "atm_iv"                : near_metrics["iv"],
                "monthly_iv"            : far_metrics["iv"],
                "term_structure_spread" : near_metrics["iv"] - far_metrics["iv"],
                "term_structure_slope"  : round(slope, 4),
                "skew_index"            : round(skew_index, 2),
                "straddle_price"        : near_metrics["ltp"],
                "straddle_price_monthly": far_metrics["ltp"],
                "atm_theta"             : near_metrics["theta"],
                "atm_vega"              : near_metrics["vega"],
                "atm_delta"             : near_metrics["delta"],
                "atm_gamma"             : near_metrics["gamma"],
                "atm_pop"               : near_metrics["pop"],
                "days_to_expiry"        : float(dte),
                "near_expiry"           : near_exp.isoformat(),
                "confidence"            : 1.0 if near_metrics["iv"] > 0 else 0.0,
                "efficiency_table"      : eff_table,
            }

        except Exception as exc:
            logger.exception("Pricing Engine Critical Failure")
            return {"confidence": 0.0}

    def _calculate_skew(self, chain: List[Dict], spot: float) -> float:
        """Calculates IV Skew: (OTM Put IV / OTM Call IV) - 1."""
        try:
            target_put = round((spot * 0.95) / 50) * 50
            target_call = round((spot * 1.05) / 50) * 50
            
            put_iv = self._find_iv_at_strike(chain, target_put, "PE")
            call_iv = self._find_iv_at_strike(chain, target_call, "CE")
            
            if call_iv > 0.01:
                return (put_iv / call_iv - 1.0) * 100
            return 0.0
        except: return 0.0

    def _find_iv_at_strike(self, chain: List[Dict], strike: float, opt_type: str) -> float:
        row = next((c for c in chain if c.get("strike_price") == strike), None)
        if not row: return 0.0
        greeks = row.get("call_options" if opt_type == "CE" else "put_options", {}).get("option_greeks", {})
        iv = float(greeks.get("iv", 0))
        return iv / 100 if iv > 5.0 else iv

    def _select_expiries(self, expiries: List[date], now: datetime) -> tuple[date, date]:
        today = now.date()
        near = expiries[0]
        if near == today and now.time() > dtime(15, 15):
            near = expiries[1] if len(expiries) > 1 else expiries[0]
        far = next((e for e in expiries if 25 <= (e - near).days <= 45), expiries[-1])
        return near, far

    def _calculate_dte(self, expiry: date, now: datetime) -> float:
        today = now.date()
        if expiry > today: return (expiry - today).days
        return 0.001

    def _extract_atm_metrics(self, chain: List[Dict[str, Any]], atm_strike: float) -> Dict[str, float]:
        row = next((c for c in chain if c.get("strike_price") == atm_strike), None)
        if not row:
            return {"iv": 0.0, "ltp": 0.0, "theta": 0.0, "vega": 0.0, "delta": 0.0, "gamma": 0.0, "pop": 0.0}

        ce = row.get("call_options", {})
        pe = row.get("put_options", {})
        ce_g = ce.get("option_greeks", {})
        pe_g = pe.get("option_greeks", {})

        def iv_clamp(iv: float) -> float:
            if iv > 5.0: iv /= 100.0
            return min(iv, MAX_IV_PCT)

        iv = iv_clamp((float(ce_g.get("iv", 0)) + float(pe_g.get("iv", 0))) / 2)
        ltp = float(ce.get("market_data", {}).get("ltp", 0)) + float(pe.get("market_data", {}).get("ltp", 0))

        return {
            "iv"   : iv,
            "ltp"  : ltp,
            "theta": float(ce_g.get("theta", 0)) + float(pe_g.get("theta", 0)),
            "vega" : float(ce_g.get("vega", 0)) + float(pe_g.get("vega", 0)),
            "delta": float(ce_g.get("delta", 0)) + float(pe_g.get("delta", 0)),
            "gamma": float(ce_g.get("gamma", 0)) + float(pe_g.get("gamma", 0)),
            "pop"  : (float(ce_g.get("pop", 0)) + float(pe_g.get("pop", 0))) / 2,
        }

    def _build_efficiency_table(self, chain: List[Dict[str, Any]], spot: float) -> List[Dict[str, float]]:
        """Finds strikes with best decay vs volatility risk."""
        table = []
        for item in chain:
            strike = float(item.get("strike_price", 0))
            if abs(strike - spot) > 500: continue
            ce_g = item.get("call_options", {}).get("option_greeks", {})
            pe_g = item.get("put_options", {}).get("option_greeks", {})
            vega = float(ce_g.get("vega", 0)) + float(pe_g.get("vega", 0))
            theta = float(ce_g.get("theta", 0)) + float(pe_g.get("theta", 0))
            if vega > 0.1:
                table.append({
                    "strike": strike, 
                    "efficiency": round(abs(theta) / vega, 4)
                })
        return sorted(table, key=lambda x: x["efficiency"], reverse=True)[:10]

    async def _calibrate_if_needed(self, chain: List[Dict[str, Any]], atm_strike: float, spot: float, dte: float) -> None:
        strikes, ivs = [], []
        for item in chain:
            strike = float(item.get("strike_price", 0))
            if abs(strike - atm_strike) > 500: continue
            iv = self._find_iv_at_strike(chain, strike, "CE")
            if iv > 0.01:
                strikes.append(strike)
                ivs.append(iv)
        if len(strikes) < MIN_POINTS: return
        
        loop = asyncio.get_running_loop()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, self.sabr.calibrate_to_chain, strikes, ivs, spot, dte/365.25),
                timeout=CALIB_TIMEOUT_SEC,
            )
        except: pass
