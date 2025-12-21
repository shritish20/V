#!/usr/bin/env python3
"""
HybridPricingEngine 20.0 – Production Hardened
- Real SABR fit to market IV
- IV sanity clamp (max 500%)
- 15-second timeout protection (Prevents Engine Freeze)
- Fallback to last-good params on math failure
"""
from __future__ import annotations

import asyncio
import logging
import time
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

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class DataIntegrityError(RuntimeError):
    """Chain is garbage – engine must skip this expiry."""

class CalibrationError(RuntimeError):
    """SABR fit failed – use last good params."""

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel) -> None:
        self.sabr = sabr_model
        self.api: Any = None
        self.instrument_master: Any = None

    def set_api(self, api: Any) -> None:
        self.api = api
        self.instrument_master = api.instrument_master

    # -------------------------------------------------------------------------
    # Public – Main Entry Point
    # -------------------------------------------------------------------------
    async def get_market_structure(self, spot: float) -> Dict[str, Any]:
        """
        Return full expiry structure + ATM metrics.
        If anything smells bad → confidence = 0.
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            # 1. Fetch Expiries
            expiries = self.instrument_master.get_all_expiries(settings.UNDERLYING_SYMBOL)
            if len(expiries) < 2:
                raise DataIntegrityError("Need ≥ 2 expiries for Term Structure")

            now = datetime.now(IST)
            near_exp, far_exp = self._select_expiries(expiries, now)

            # 2. Calculate DTE
            dte = max(0.001, self._calculate_dte(near_exp, now))

            # 3. Parallel Data Fetch
            chain_near, chain_far = await asyncio.gather(
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_exp.isoformat()),
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_exp.isoformat()),
            )

            if not chain_near.get("data"):
                raise DataIntegrityError("Received Empty Near Chain")

            # 4. Process ATM Metrics
            atm_strike = round(spot / 50) * 50
            near_metrics = self._extract_atm_metrics(chain_near["data"], atm_strike)
            far_metrics  = self._extract_atm_metrics(chain_far.get("data", []), atm_strike)

            # 5. Sanity Checks
            if near_metrics["iv"] > MAX_IV_PCT:
                raise DataIntegrityError(f"IV exploded > {MAX_IV_PCT*100}%")

            # 6. Real SABR Calibration (The Heavy Math)
            await self._calibrate_if_needed(chain_near["data"], atm_strike, spot, dte)

            # 7. Build Output
            eff_table = self._build_efficiency_table(chain_near["data"], spot)

            return {
                "atm_iv"                : near_metrics["iv"],
                "monthly_iv"            : far_metrics["iv"],
                "term_structure_spread" : near_metrics["iv"] - far_metrics["iv"],
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
                "pcr"                   : 1.0,  # placeholder
                "max_pain"              : spot,  # placeholder
                "efficiency_table"      : eff_table,
                "skew_index"            : 0.0,  # placeholder
            }

        except (DataIntegrityError, CalibrationError):
            # Engine will skip this expiry and retry next cycle
            raise
        except Exception as exc:
            logger.exception("Pricing Engine Critical Failure")
            return {"confidence": 0.0}

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    def _select_expiries(self, expiries: List[date], now: datetime) -> tuple[date, date]:
        """Pick near (weekly) and far (monthly) expiry."""
        today = now.date()
        near = expiries[0]
        # Roll to next expiry if today is expiry and market is near close
        if near == today and now.time() > dtime(15, 15):
            near = expiries[1] if len(expiries) > 1 else expiries[0]
            
        # Find a monthly expiry approx 25-45 days out
        far = next((e for e in expiries if 25 <= (e - near).days <= 45), expiries[-1])
        return near, far

    def _calculate_dte(self, expiry: date, now: datetime) -> float:
        """Days to expiry with intraday fraction precision."""
        today = now.date()
        if expiry > today:
            return (expiry - today).days
        if expiry == today:
            market_close = datetime.combine(today, dtime(15, 30))
            seconds_left = (market_close - now).total_seconds()
            return max(0.001, seconds_left / 86_400)
        return 0.001

    def _extract_atm_metrics(self, chain: List[Dict[str, Any]], atm_strike: float) -> Dict[str, float]:
        """Pull ATM greeks + IV + LTP."""
        row = next((c for c in chain if c.get("strike_price") == atm_strike), None)
        if not row:
            return {"iv": 0.0, "ltp": 0.0, "theta": 0.0, "vega": 0.0, "delta": 0.0, "gamma": 0.0, "pop": 0.0}

        ce = row.get("call_options", {})
        pe = row.get("put_options", {})
        ce_g = ce.get("option_greeks", {})
        pe_g = pe.get("option_greeks", {})

        def greek(key: str) -> float:
            return float(ce_g.get(key, 0)) + float(pe_g.get(key, 0))

        def iv_clamp(iv: float) -> float:
            # Fix percentage vs decimal inconsistency from Upstox
            if iv > 5.0:
                iv /= 100.0
            return min(iv, MAX_IV_PCT)

        iv = iv_clamp((float(ce_g.get("iv", 0)) + float(pe_g.get("iv", 0))) / 2)
        ltp = float(ce.get("market_data", {}).get("ltp", 0)) + float(pe.get("market_data", {}).get("ltp", 0))

        return {
            "iv"   : iv,
            "ltp"  : ltp,
            "theta": greek("theta"),
            "vega" : greek("vega"),
            "delta": greek("delta"),
            "gamma": greek("gamma"),
            "pop"  : (float(ce_g.get("pop", 0)) + float(pe_g.get("pop", 0))) / 2,
        }

    def _build_efficiency_table(self, chain: List[Dict[str, Any]], spot: float) -> List[Dict[str, float]]:
        """Return top 5 theta/vega ratio strikes."""
        table = []
        for item in chain:
            strike = float(item.get("strike_price", 0))
            if abs(strike - spot) > 500:
                continue
            ce_g = item.get("call_options", {}).get("option_greeks", {})
            pe_g = item.get("put_options", {}).get("option_greeks", {})
            vega = float(ce_g.get("vega", 0)) + float(pe_g.get("vega", 0))
            theta = float(ce_g.get("theta", 0)) + float(pe_g.get("theta", 0))
            
            if vega > 0.1:
                table.append({
                    "strike": strike, 
                    "theta": round(theta, 2), 
                    "vega": round(vega, 2), 
                    "ratio": round(abs(theta) / vega, 2)
                })
        return sorted(table, key=lambda x: x["ratio"], reverse=True)[:5]

    # -------------------------------------------------------------------------
    # SABR Calibration – The Quantitative Core
    # -------------------------------------------------------------------------
    async def _calibrate_if_needed(self, chain: List[Dict[str, Any]], atm_strike: float, spot: float, dte: float) -> None:
        """Fit SABR to market IV – timeout protected."""
        strikes, ivs = [], []
        
        # 1. Filter Strikes for Calibration
        for item in chain:
            strike = float(item.get("strike_price", 0))
            # Only use strikes near ATM (+/- 500 points)
            if abs(strike - atm_strike) > 500:
                continue
                
            ce_iv = float(item.get("call_options", {}).get("option_greeks", {}).get("iv", 0))
            pe_iv = float(item.get("put_options", {}).get("option_greeks", {}).get("iv", 0))
            
            # Normalize IVs
            iv = ((ce_iv if ce_iv < 5 else ce_iv / 100) + (pe_iv if pe_iv < 5 else pe_iv / 100)) / 2
            
            if iv > 0.01:
                strikes.append(strike)
                ivs.append(iv)

        if len(strikes) < MIN_POINTS:
            raise CalibrationError(f"Too few strikes ({len(strikes)}) for SABR fit")

        tte = max(0.001, dte / 365.25)

        # 2. Run Calibration in ThreadPool with Timeout
        loop = asyncio.get_running_loop()
        try:
            ok = await asyncio.wait_for(
                loop.run_in_executor(None, self.sabr.calibrate_to_chain, strikes, ivs, spot, tte),
                timeout=CALIB_TIMEOUT_SEC,
            )
            if ok:
                logger.info(f"✨ SABR Calibrated | Alpha: {self.sabr.alpha:.3f} | Rho: {self.sabr.rho:.3f}")
            else:
                raise CalibrationError("Fit converged to bad params")
                
        except asyncio.TimeoutError as exc:
            logger.warning("⏳ SABR Calibration Timeout – using previous params")
            raise CalibrationError("Timeout") from exc
        except Exception as exc:
            logger.warning("⚠️ SABR Calibration Failed – using previous params", exc_info=exc)
            raise CalibrationError("Fit failed") from exc
