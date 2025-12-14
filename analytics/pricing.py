# File: analytics/pricing.py

import numpy as np
from typing import Dict, Optional, Tuple
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
        Returns structure metrics + confidence score.
        Includes ROBUST FALLBACK for Paper Trading on Weekends/No-Data.
        """
        # --- PAPER MODE SIMULATION (Sunday/Offline Handling) ---
        if settings.SAFETY_MODE != "live":
            # Check if we have real data first
            has_real_data = False
            if self.instrument_master:
                expiries = self.instrument_master.get_all_expiries("NIFTY")
                if len(expiries) >= 2:
                    has_real_data = True
            
            # If no real data (e.g. Sunday), create Mock Expiries to verify logic
            if not has_real_data:
                today = datetime.now(IST).date()
                # Find next Thursday
                days_ahead = (3 - today.weekday()) % 7
                if days_ahead == 0: days_ahead = 7
                sim_weekly = today + timedelta(days=days_ahead)
                sim_monthly = sim_weekly + timedelta(days=28)
                
                return {
                    "atm_iv": 0.12, # 12% IV (Mock)
                    "monthly_iv": 0.13,
                    "term_structure": 1.0, # Normal Contango
                    "skew_index": 2.5,     # Normal Skew
                    "days_to_expiry": float(days_ahead),
                    "near_expiry": sim_weekly.strftime("%Y-%m-%d"),
                    "far_expiry": sim_monthly.strftime("%Y-%m-%d"),
                    "confidence": 0.8 # High enough to show on dashboard
                }

        # --- LIVE MODE LOGIC ---
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50
            
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 2: 
                return {"confidence": 0.0}
            
            near_expiry = expiries[0]
            
            # Find next monthly (25-45 days out)
            far_expiry = expiries[-1]
            for e in expiries:
                if 25 <= (e - datetime.now(IST).date()).days <= 45:
                    far_expiry = e
                    break
            
            today = datetime.now(IST).date()
            dte = (near_expiry - today).days
            
            # Get Tokens
            w_ce = self.instrument_master.get_option_token("NIFTY", atm_strike, "CE", near_expiry)
            w_pe = self.instrument_master.get_option_token("NIFTY", atm_strike, "PE", near_expiry)
            w_otm = self.instrument_master.get_option_token("NIFTY", otm_strike, "PE", near_expiry)
            m_ce = self.instrument_master.get_option_token("NIFTY", atm_strike, "CE", far_expiry)
            m_pe = self.instrument_master.get_option_token("NIFTY", atm_strike, "PE", far_expiry)

            tokens = [t for t in [w_ce, w_pe, w_otm, m_ce, m_pe] if t]
            if not tokens: return {"confidence": 0.0}
            
            greeks = await self.api.get_option_greeks(tokens)
            
            # Check for Empty/Zero Data (Closed Market)
            is_empty = not greeks or all(g.get("iv", 0) == 0 for g in greeks.values())
            if is_empty: 
                logger.warning("⛔ Market Data Empty (Closed Market).")
                return {"confidence": 0.0}

            def get_iv(tok): return greeks.get(tok, {}).get("iv", 0)

            w_iv = (get_iv(w_ce) + get_iv(w_pe)) / 2
            m_iv = (get_iv(m_ce) + get_iv(m_pe)) / 2
            otm_iv = get_iv(w_otm)

            if w_iv == 0: return {"confidence": 0.0}

            return {
                "atm_iv": w_iv,
                "monthly_iv": m_iv,
                "term_structure": (m_iv - w_iv) * 100,
                "skew_index": (otm_iv - w_iv) * 100,
                "days_to_expiry": float(dte),
                # Pouring in the Dates explicitly
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0
            }

        except Exception as e:
            logger.error(f"Structure Scan Failed: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        return GreeksSnapshot(timestamp=datetime.now(IST))
