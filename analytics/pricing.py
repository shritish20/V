#!/usr/bin/env python3
"""
VolGuard 20.0 – Pricing Engine (Non-Blocking)
Uses ProcessPoolExecutor to bypass GIL during SABR calibration.
"""
import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date, time as dtime

from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel, _worker_calibrate_sabr

logger = logging.getLogger("PricingEngine")

# Config
MAX_IV_PCT = 5.0
CALIB_TIMEOUT_SEC = 20  # Reduced - ProcessPool is much faster than threads for math
MIN_POINTS = 5

class DataIntegrityError(RuntimeError): pass
class CalibrationError(RuntimeError): pass

class HybridPricingEngine:
    """
    Computes market microstructure metrics:
    - Implied Volatility Surface (via SABR)
    - Term Structure Slope (Backwardation/Contango)
    - Skew Index (Put vs Call Demand)
    """
    def __init__(self, sabr_model: EnhancedSABRModel) -> None:
        self.sabr = sabr_model
        self.api: Any = None
        self.instrument_master: Any = None
        
        # CRITICAL: Single-worker process pool
        # Why 1 worker? SABR calibration is CPU-intensive serial work.
        # Multiple workers would compete for CPU and slow everything down.
        # We process in a separate core to avoid blocking the main Event Loop.
        self.process_pool = ProcessPoolExecutor(max_workers=1)
        
        # Track calibration state to prevent overlap/spam
        self._calibration_in_progress = False
        self._last_calibration_time = 0.0

    def set_api(self, api: Any) -> None:
        self.api = api

    async def shutdown(self):
        """Cleanup on engine shutdown"""
        self.process_pool.shutdown(wait=False)
        logger.info("🛑 ProcessPool shutdown")

    async def get_market_structure(self, spot: float) -> Dict[str, Any]:
        """Calculates Skew, Term Structure, and Efficiency for Strategy Engine."""
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            expiries = self.instrument_master.get_all_expiries(settings.UNDERLYING_SYMBOL)
            if len(expiries) < 2:
                logger.warning("Not enough expiries for Term Structure analysis")
                return {"confidence": 0.0}

            now = datetime.now(IST)
            near_exp, far_exp = self._select_expiries(expiries, now)
            dte = max(0.001, self._calculate_dte(near_exp, now))

            # Fetch both chains in parallel
            chain_near, chain_far = await asyncio.gather(
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_exp.isoformat()),
                self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_exp.isoformat()),
                return_exceptions=True
            )

            if isinstance(chain_near, Exception) or not chain_near.get("data"):
                logger.error("Failed to fetch Near Chain")
                return {"confidence": 0.0}
            
            far_data = []
            if not isinstance(chain_far, Exception) and chain_far.get("data"):
                far_data = chain_far["data"]

            atm_strike = round(spot / 50) * 50
            near_metrics = self._extract_atm_metrics(chain_near["data"], atm_strike)
            far_metrics  = self._extract_atm_metrics(far_data, atm_strike)

            skew_index = self._calculate_skew(chain_near["data"], spot)
            
            slope = 0.0
            if far_metrics["iv"] > 0:
                slope = (near_metrics["iv"] - far_metrics["iv"]) / far_metrics["iv"]

            # NON-BLOCKING CALIBRATION
            # Fire-and-forget: we launch the task but don't wait for it
            asyncio.create_task(
                self._calibrate_if_needed(chain_near["data"], atm_strike, spot, dte)
            )
            
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

    async def calibrate_sabr(self, spot: float) -> None:
        """Public method for Engine to trigger calibration manually."""
        if not self.api or not self.instrument_master: return
        try:
            expiries = self.instrument_master.get_all_expiries(settings.UNDERLYING_SYMBOL)
            if not expiries: return
            
            now = datetime.now(IST)
            near_exp, _ = self._select_expiries(expiries, now)
            dte = max(0.001, self._calculate_dte(near_exp, now))
            
            chain = await self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_exp.isoformat())
            if not chain or not chain.get("data"): return
            
            atm_strike = round(spot / 50) * 50
            await self._calibrate_if_needed(chain["data"], atm_strike, spot, dte)
            
        except Exception:
            pass

    async def _calibrate_if_needed(self, chain: List[Dict[str, Any]], atm_strike: float, spot: float, dte: float) -> None:
        """Runs calibration in process pool - NEVER blocks main thread."""
        
        # 1. Rate Limiting (Don't run more than once per 5 mins)
        import time
        now = time.time()
        if now - self._last_calibration_time < 300:
            return
            
        # 2. Concurrency Check
        if self._calibration_in_progress:
            return

        try:
            self._calibration_in_progress = True
            
            strikes, ivs = [], []
            for item in chain:
                strike = float(item.get("strike_price", 0))
                # Filter points too far away (noise)
                if abs(strike - atm_strike) > 500: continue
                
                iv = self._find_iv_at_strike(chain, strike, "CE")
                if iv > 0.01:
                    strikes.append(strike)
                    ivs.append(iv)
            
            if len(strikes) < MIN_POINTS: return
            
            # Prepare inputs for the worker process
            current_params = self.sabr.get_current_params()
            bounds = [
                settings.SABR_BOUNDS['alpha'],
                settings.SABR_BOUNDS['beta'],
                settings.SABR_BOUNDS['rho'],
                settings.SABR_BOUNDS['nu']
            ]
            
            # 3. Offload to Process Pool
            loop = asyncio.get_running_loop()
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    self.process_pool,
                    _worker_calibrate_sabr,  # The standalone function
                    current_params,
                    bounds,
                    strikes,
                    ivs,
                    spot,
                    dte / 365.25
                ),
                timeout=CALIB_TIMEOUT_SEC,
            )
            
            # 4. Update Model if successful
            if result:
                params, error = result
                self.sabr.update_params(params, error)
                self._last_calibration_time = now
            else:
                self.sabr.use_cached_params()

        except asyncio.TimeoutError:
            logger.warning(f"SABR Calibration Timed Out ({CALIB_TIMEOUT_SEC}s) - using cache")
            self.sabr.use_cached_params()
        except Exception as e: 
            logger.error(f"SABR Worker Error: {e}")
            self.sabr.use_cached_params()
        finally:
            self._calibration_in_progress = False

    # -------------------------------------------------------------------------
    # Internal Helpers (Unchanged Logic)
    # -------------------------------------------------------------------------

    def _calculate_skew(self, chain: List[Dict], spot: float) -> float:
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

    def _select_expiries(self, expiries: List[date], now: datetime) -> Tuple[date, date]:
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
