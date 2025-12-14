# File: analytics/pricing.py

import numpy as np
import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta
import logging
from core.config import settings, IST
from core.models import GreeksSnapshot
from analytics.sabr_model import EnhancedSABRModel

logger = logging.getLogger("PricingEngine")

class HybridPricingEngine:
    def __init__(self, sabr_model: EnhancedSABRModel):
        self.sabr = sabr_model
        self.api = None
        self.instrument_master = None

    def set_api(self, api):
        self.api = api
        self.instrument_master = api.instrument_master

    async def get_market_structure(self, spot: float) -> Dict:
        """
        Returns DEEP Market Structure.
        Fetches BOTH Weekly and Monthly chains to calculate Term Structure.
        Calculates Real Straddle Price from LTP.
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
            dte = (near_expiry - today).days
            
            # 2. FETCH DATA IN PARALLEL (Weekly + Monthly)
            # We need both chains to calculate Term Structure (Contango/Backwardation)
            task_weekly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_monthly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            
            chain_res_w, chain_res_m = await asyncio.gather(task_weekly, task_monthly)
            
            if not chain_res_w or not chain_res_w.get("data"):
                logger.warning(f"⛔ Weekly Chain Empty for {near_expiry}")
                return {"confidence": 0.0}
                
            chain_w = chain_res_w["data"]
            chain_m = chain_res_m.get("data", [])
            
            # 3. Analyze Weekly Chain (Near Term)
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50
            
            # Find Rows
            row_w_atm = next((x for x in chain_w if abs(x['strike_price'] - atm_strike) < 2), None)
            row_w_otm = next((x for x in chain_w if abs(x['strike_price'] - otm_strike) < 2), None)
            
            if not row_w_atm:
                return {"confidence": 0.0}

            # Extract Weekly Metrics
            w_ce_iv = row_w_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
            w_pe_iv = row_w_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
            
            # Fix: Calculate Real Straddle Price (LTP)
            w_ce_ltp = row_w_atm.get("call_options", {}).get("market_data", {}).get("ltp", 0)
            w_pe_ltp = row_w_atm.get("put_options", {}).get("market_data", {}).get("ltp", 0)
            straddle_price = w_ce_ltp + w_pe_ltp
            
            # Skew
            otm_iv = 0.0
            if row_w_otm:
                otm_iv = row_w_otm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)

            # Weekly IV Avg
            w_iv = (w_ce_iv + w_pe_iv) / 2 if (w_ce_iv > 0 and w_pe_iv > 0) else max(w_ce_iv, w_pe_iv)

            # 4. Analyze Monthly Chain (Far Term)
            m_iv = w_iv # Default if monthly fails
            if chain_m:
                row_m_atm = next((x for x in chain_m if abs(x['strike_price'] - atm_strike) < 2), None)
                if row_m_atm:
                    m_ce_iv = row_m_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
                    m_pe_iv = row_m_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
                    if m_ce_iv > 0 and m_pe_iv > 0:
                        m_iv = (m_ce_iv + m_pe_iv) / 2
                    elif m_ce_iv > 0: m_iv = m_ce_iv
                    elif m_pe_iv > 0: m_iv = m_pe_iv

            # 5. Normalization (Handle Percentage vs Decimal)
            # If IV is > 50, it's definitely scaled wrong.
            if w_iv > 50.0: w_iv /= 100.0
            if m_iv > 50.0: m_iv /= 100.0
            if otm_iv > 50.0: otm_iv /= 100.0
            
            if w_iv > 3.0: w_iv /= 100.0 # Catch 14.5 cases
            if m_iv > 3.0: m_iv /= 100.0
            if otm_iv > 3.0: otm_iv /= 100.0

            if w_iv <= 0.001: return {"confidence": 0.0}

            # 6. Final Pack
            return {
                "atm_iv": w_iv,
                "monthly_iv": m_iv,
                "term_structure": (m_iv - w_iv) * 100, # Positive = Contango
                "skew_index": (otm_iv - w_iv) * 100,
                "straddle_price": straddle_price, # REAL PRICE
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0
            }

        except Exception as e:
            logger.error(f"Structure Scan Error: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        return GreeksSnapshot(timestamp=datetime.now(IST))
