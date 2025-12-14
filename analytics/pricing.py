# File: analytics/pricing.py

import numpy as np
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
        Returns REAL Market Structure.
        Uses Option Chain API to fetch valid data even on weekends.
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            # 1. Get Expiries from Real Instrument Data
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 2: 
                logger.warning("⛔ No Expiries found. Instrument Master may be empty.")
                return {"confidence": 0.0}
            
            near_expiry = expiries[0]
            far_expiry = expiries[-1]
            
            # Find a valid monthly expiry (approx 25-45 days out)
            for e in expiries:
                if 25 <= (e - datetime.now(IST).date()).days <= 45:
                    far_expiry = e
                    break
            
            today = datetime.now(IST).date()
            dte = (near_expiry - today).days
            
            # 2. FETCH REAL DATA (Option Chain)
            # This API returns persistent data (Closing Price/IV) on weekends.
            chain_res = await self.api.get_option_chain(
                settings.MARKET_KEY_INDEX, 
                near_expiry.strftime("%Y-%m-%d")
            )
            
            if not chain_res or not chain_res.get("data"):
                logger.warning(f"⛔ Option Chain API returned no data for {near_expiry}")
                return {"confidence": 0.0}
                
            chain_data = chain_res["data"]
            
            # 3. Find ATM Strike Data
            atm_strike = round(spot / 50) * 50
            otm_strike = round((spot * 0.95) / 50) * 50
            
            atm_data = next((x for x in chain_data if abs(x['strike_price'] - atm_strike) < 2), None)
            otm_data = next((x for x in chain_data if abs(x['strike_price'] - otm_strike) < 2), None)
            
            if not atm_data:
                logger.warning(f"⛔ ATM Strike {atm_strike} not found in Option Chain.")
                return {"confidence": 0.0}

            # 4. Extract Real Greeks
            # Upstox returns 0 for IV on Sunday in some endpoints, but Option Chain usually retains it.
            # If 0, we trust it is 0 (Data Reset) and do NOT trade.
            ce_iv = atm_data.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
            pe_iv = atm_data.get("put_options", {}).get("option_greeks", {}).get("iv", 0)
            
            otm_iv = 0.0
            if otm_data:
                otm_iv = otm_data.get("put_options", {}).get("option_greeks", {}).get("iv", 0)

            # Valid IV Logic
            if ce_iv > 0 and pe_iv > 0:
                w_iv = (ce_iv + pe_iv) / 2
            elif ce_iv > 0:
                w_iv = ce_iv
            elif pe_iv > 0:
                w_iv = pe_iv
            else:
                # If both are 0, Data is missing. Stop.
                logger.warning("⛔ Broker IV is 0.0. Market Data not available.")
                return {"confidence": 0.0}

            # 5. Return Real Structure
            return {
                "atm_iv": w_iv,
                "monthly_iv": w_iv, # Using weekly as proxy to avoid 2nd API call latency
                "term_structure": 0.0,
                "skew_index": (otm_iv - w_iv) * 100,
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "far_expiry": far_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0
            }

        except Exception as e:
            logger.error(f"Structure Scan Failed: {e}")
            return {"confidence": 0.0}

    def calculate_greeks(self, *args, **kwargs):
        # Stub for legacy calls
        return GreeksSnapshot(timestamp=datetime.now(IST))
