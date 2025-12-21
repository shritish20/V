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
        Determines Term Structure (Backwardation/Contango) and Skew.
        Handles Expiry Day Rollover.
        """
        if not self.api or not self.instrument_master:
            return {"confidence": 0.0}

        try:
            expiries = self.instrument_master.get_all_expiries("NIFTY")
            if len(expiries) < 3: return {"confidence": 0.0}

            now = datetime.now(IST)
            today_date = now.date()
            
            near_expiry = expiries[0]
            if near_expiry == today_date and now.time() > time(15, 15):
                near_expiry = expiries[1]
                
            far_expiry = expiries[-1]
            for e in expiries:
                if 25 <= (e - near_expiry).days <= 45:
                    far_expiry = e
                    break
            
            dte = max(0.01, (near_expiry - today_date).days)

            task_w = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_m = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            res_w, res_m = await asyncio.gather(task_w, task_m)
            
            if not res_w.get("data"): return {"confidence": 0.0}
            
            atm_strike = round(spot / 50) * 50
            chain_w = res_w["data"]
            chain_m = res_m.get("data", [])
            
            def get_iv(chain, strike, type_):
                row = next((x for x in chain if x['strike_price'] == strike), None)
                if not row: return 0.0
                opt = row['call_options'] if type_ == 'CE' else row['put_options']
                return opt.get('option_greeks', {}).get('iv', 0.0)

            w_atm_iv = (get_iv(chain_w, atm_strike, 'CE') + get_iv(chain_w, atm_strike, 'PE')) / 2
            m_atm_iv = (get_iv(chain_m, atm_strike, 'CE') + get_iv(chain_m, atm_strike, 'PE')) / 2 if chain_m else w_atm_iv
            
            otm_put_iv = get_iv(chain_w, atm_strike - 200, 'PE')
            otm_call_iv = get_iv(chain_w, atm_strike + 200, 'CE')
            skew = otm_put_iv - otm_call_iv if (otm_put_iv and otm_call_iv) else 0.0

            if w_atm_iv > 2.0: w_atm_iv /= 100.0
            if m_atm_iv > 2.0: m_atm_iv /= 100.0
            if skew > 2.0: skew /= 100.0

            row_atm = next((x for x in chain_w if x['strike_price'] == atm_strike), None)
            straddle = 0.0
            if row_atm:
                straddle = row_atm['call_options']['market_data']['ltp'] + row_atm['put_options']['market_data']['ltp']

            return {
                "atm_iv": w_atm_iv,
                "monthly_iv": m_atm_iv,
                "term_structure": (w_atm_iv / m_atm_iv) if m_atm_iv > 0 else 1.0, 
                "skew_index": skew * 100,
                "straddle_price": straddle,
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0,
                "pcr": 1.0,
                "max_pain": spot
            }

        except Exception as e:
            logger.error(f"Structure Scan Error: {e}")
            return {"confidence": 0.0}
