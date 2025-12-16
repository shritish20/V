# File: analytics/pricing.py

import numpy as np
import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta, time
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
        SMART LOGIC: Automatically rolls to next expiry if today > 15:30 IST.
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            # 1. Get All Expiries
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 3:
                return {"confidence": 0.0}
            
            # --- SMART ROLLOVER LOGIC ---
            now = datetime.now(IST)
            today_date = now.date()
            market_close_time = time(15, 30)
            
            # Default: Nearest is index 0
            near_expiry = expiries[0]
            next_expiry = expiries[1]
            far_expiry = expiries[-1] # Default Monthly/Far

            # CHECK: Is the nearest expiry "Dead"?
            # If today is the expiry date AND it is past 3:30 PM
            if near_expiry == today_date and now.time() > market_close_time:
                # ROLL FORWARD
                near_expiry = expiries[1] # Next Week
                next_expiry = expiries[2] # Week after that
                # Recalculate monthly target
                for e in expiries:
                    if 25 <= (e - near_expiry).days <= 50:
                        far_expiry = e
                        break
            else:
                # Standard logic for monthly
                for e in expiries:
                    if 25 <= (e - near_expiry).days <= 50:
                        far_expiry = e
                        break
            
            # Calculate Valid DTE (Days To Expiry)
            # We add a small epsilon (1/365) to avoid DivisionByZero if we check exactly at open on expiry day
            delta = near_expiry - today_date
            dte = max(0.01, delta.days + (0.5 if now.time() < market_close_time else 0.0))

            # 2. FETCH DATA IN PARALLEL (Smart Expiry vs Monthly)
            task_weekly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_monthly = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            
            chain_res_w, chain_res_m = await asyncio.gather(task_weekly, task_monthly)
            
            if not chain_res_w or not chain_res_w.get("data"):
                return {"confidence": 0.0}

            chain_w = chain_res_w["data"]
            chain_m = chain_res_m.get("data", [])

            # 3. Analyze Weekly Chain (Near Term)
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50

            row_w_atm = next((x for x in chain_w if abs(x['strike_price'] - atm_strike) < 2), None)
            row_w_otm = next((x for x in chain_w if abs(x['strike_price'] - otm_strike) < 2), None)

            if not row_w_atm:
                return {"confidence": 0.0}

            # Extract IVs (Upstox gives 14.5 for 14.5%)
            w_ce_iv = row_w_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
            w_pe_iv = row_w_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
            
            # Real Straddle Price
            w_ce_ltp = row_w_atm.get("call_options", {}).get("market_data", {}).get("ltp", 0)
            w_pe_ltp = row_w_atm.get("put_options", {}).get("market_data", {}).get("ltp", 0)
            straddle_price = w_ce_ltp + w_pe_ltp

            # Skew
            otm_iv = 0.0
            if row_w_otm:
                otm_iv = row_w_otm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)

            # Weekly IV Avg
            w_iv = (w_ce_iv + w_pe_iv) / 2 if (w_ce_iv > 0 and w_pe_iv > 0) else max(w_ce_iv, w_pe_iv)

            # 4. Analyze Monthly Chain
            m_iv = w_iv 
            if chain_m:
                row_m_atm = next((x for x in chain_m if abs(x['strike_price'] - atm_strike) < 2), None)
                if row_m_atm:
                    m_ce_iv = row_m_atm.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
                    m_pe_iv = row_m_atm.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
                    if m_ce_iv > 0 and m_pe_iv > 0:
                        m_iv = (m_ce_iv + m_pe_iv) / 2
                    elif m_ce_iv > 0: m_iv = m_ce_iv
                    elif m_pe_iv > 0: m_iv = m_pe_iv

            # 5. DATA SANITIZATION (Strict Logic)
            def sanitize_iv(val):
                if val <= 0: return 0.0
                # If IV is > 500% (5.0) or massive, it's garbage/expired
                if val > 5.0 and val < 500.0: val = val / 100.0 
                # If still crazy high (> 200%), clamp it. NIFTY never hits 200% IV.
                if val > 2.0: return 0.0
                return val

            w_iv = sanitize_iv(w_iv)
            m_iv = sanitize_iv(m_iv)
            otm_iv = sanitize_iv(otm_iv)

            # Force decimal if API returns whole numbers (e.g. 15.0 -> 0.15)
            if w_iv > 2.0: w_iv /= 100.0
            if m_iv > 2.0: m_iv /= 100.0
            if otm_iv > 2.0: otm_iv /= 100.0

            if w_iv <= 0.001: return {"confidence": 0.0}

            # 6. Final Pack
            return {
                "atm_iv": w_iv,  # Returns 0.14
                "monthly_iv": m_iv,
                "term_structure": (m_iv - w_iv) * 100, # e.g., (0.16 - 0.14)*100 = 2.0
                "skew_index": (otm_iv - w_iv) * 100,
                "straddle_price": straddle_price,
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"), # Will show Next Week's Date
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0,
                "pcr": 1.0, # Placeholder or fetch real
                "max_pain": spot # Placeholder or fetch real
            }

        except Exception as e:
            logger.error(f"Structure Scan Error: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        return GreeksSnapshot(timestamp=datetime.now(IST))
