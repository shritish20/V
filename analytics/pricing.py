import numpy as np
import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta
import logging
from core.config import settings, IST
from core.models import GreeksSnapshot
from analytics.sabr_model import EnhancedSABRModel
from analytics.chain_metrics import ChainMetricsCalculator

logger = logging.getLogger("PricingEngine")

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self.api = None
        self.instrument_master = None
        self.metrics_calc = ChainMetricsCalculator()

    def set_api(self, api):
        self.api = api
        self.instrument_master = api.instrument_master

    async def get_market_structure(self, spot: float) -> Dict:
        """
        Returns DEEP Market Structure.
        Fetches Weekly chain for ATM metrics and Monthly for Term Structure.
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            # 1. Get Expiries
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 2:
                return {"confidence": 0.0}

            near_expiry = expiries[0]
            far_expiry = expiries[-1]

            # Find a valid monthly expiry (25-45 days out)
            for e in expiries:
                if 25 <= (e - datetime.now(IST).date()).days <= 45:
                    far_expiry = e
                    break

            today = datetime.now(IST).date()
            dte = max(1, (near_expiry - today).days)

            # 2. FETCH DATA IN PARALLEL (Weekly + Monthly)
            task_weekly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_monthly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))

            chain_res_w, chain_res_m = await asyncio.gather(task_weekly, task_monthly)

            if not chain_res_w or not chain_res_w.get("data"):
                logger.warning(f"Weekly Chain Empty for {near_expiry}")
                return {"confidence": 0.0}

            chain_w = chain_res_w["data"]
            chain_m = chain_res_m.get("data", [])

            # 3. Analyze Weekly Chain (Near Term)
            # Calculate PCR and Max Pain using the ChainMetricsCalculator
            seller_metrics = self.metrics_calc.extract_seller_metrics(chain_w, spot)
            
            # Extract ATM/OTM Data
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50

            row_w_atm = next((x for x in chain_w if abs(x['strike_price'] - atm_strike) < 10), None)
            row_w_otm = next((x for x in chain_w if abs(x['strike_price'] - otm_strike) < 10), None)

            if not row_w_atm:
                return {"confidence": 0.0}

            # --- IV SCALING FIX ---
            # Standard: Always convert to Percentage (e.g. 15.5) for Dashboard consistency
            def normalize_iv(val):
                if val is None or val == 0: return 0.0
                # If decimal (0.15), convert to 15.0. If already percent (15.0), keep it.
                if val < 2.0: 
                    return val * 100.0
                return val

            w_ce_iv = normalize_iv(row_w_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0))
            w_pe_iv = normalize_iv(row_w_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0))

            # ATM IV is average of Call and Put IV
            w_iv = (w_ce_iv + w_pe_iv) / 2 if (w_ce_iv > 0 and w_pe_iv > 0) else max(w_ce_iv, w_pe_iv)

            # Calculate Real Straddle Price (LTP)
            w_ce_ltp = row_w_atm.get("call_options", {}).get("market_data", {}).get("ltp", 0)
            w_pe_ltp = row_w_atm.get("put_options", {}).get("market_data", {}).get("ltp", 0)
            straddle_price = w_ce_ltp + w_pe_ltp

            # Skew (OTM Put IV vs ATM IV)
            otm_iv = 0.0
            if row_w_otm:
                otm_iv_raw = row_w_otm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
                otm_iv = normalize_iv(otm_iv_raw)

            # 4. Analyze Monthly Chain (Far Term)
            m_iv = w_iv # Default
            if chain_m:
                row_m_atm = next((x for x in chain_m if abs(x['strike_price'] - atm_strike) < 10), None)
                if row_m_atm:
                    m_ce = normalize_iv(row_m_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0))
                    m_pe = normalize_iv(row_m_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0))
                    if m_ce > 0 and m_pe > 0:
                        m_iv = (m_ce + m_pe) / 2
                    elif m_ce > 0:
                        m_iv = m_ce

            # 5. Final Calculation
            # Skew Index: Positive means OTM Puts are expensive (Fear)
            skew_index = (otm_iv - w_iv) if otm_iv > 0 else 0.0
            
            # Term Structure: Positive means Monthly > Weekly (Contango/Normal)
            term_structure = m_iv - w_iv

            return {
                "atm_iv": w_iv,             # Percentage (e.g. 15.5)
                "monthly_iv": m_iv,         # Percentage
                "term_structure": term_structure,
                "skew_index": skew_index,
                "straddle_price": straddle_price,
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "pcr": seller_metrics.get("pcr", 1.0),       # REAL PCR
                "max_pain": seller_metrics.get("max_pain", spot), # REAL MAX PAIN
                "confidence": 1.0
            }

        except Exception as e:
            logger.error(f"Structure Scan Error: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        return GreeksSnapshot(timestamp=datetime.now(IST))
