# File: analytics/pricing.py

import numpy as np
from typing import Dict, Optional, Tuple
from datetime import datetime
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
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50
            
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 2: 
                return {"confidence": 0.0}
            
            near_expiry = expiries[0]
            far_expiry = expiries[-1]
            # Find next monthly (25-45 days out)
            for e in expiries:
                if 25 <= (e - datetime.now(IST).date()).days <= 45:
                    far_expiry = e
                    break
            
            today = datetime.now(IST).date()
            dte = (near_expiry - today).days
            
            # Tokens
            w_ce = self.instrument_master.get_option_token("NIFTY", atm_strike, "CE", near_expiry)
            w_pe = self.instrument_master.get_option_token("NIFTY", atm_strike, "PE", near_expiry)
            w_otm = self.instrument_master.get_option_token("NIFTY", otm_strike, "PE", near_expiry)
            m_ce = self.instrument_master.get_option_token("NIFTY", atm_strike, "CE", far_expiry)
            m_pe = self.instrument_master.get_option_token("NIFTY", atm_strike, "PE", far_expiry)

            # API Call
            tokens = [t for t in [w_ce, w_pe, w_otm, m_ce, m_pe] if t]
            if not tokens: return {"confidence": 0.0}
            
            greeks = await self.api.get_option_greeks(tokens)
            if not greeks: return {"confidence": 0.0}

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
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0 # Success
            }

        except Exception as e:
            logger.error(f"Structure Scan Failed: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        return GreeksSnapshot(timestamp=datetime.now(IST))
