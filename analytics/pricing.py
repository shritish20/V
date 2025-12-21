import asyncio
from typing import Dict, Optional, List
from datetime import datetime, timedelta, time
import logging
from core.config import settings, IST
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

            # Fetch Option Chains
            task_w = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_m = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            res_w, res_m = await asyncio.gather(task_w, task_m)
            
            if not res_w.get("data"): return {"confidence": 0.0}
            
            chain_w = res_w["data"]
            chain_m = res_m.get("data", [])
            
            atm_strike = round(spot / 50) * 50
            
            # --- HELPER FUNCTIONS ---
            def get_iv(chain, strike, type_):
                row = next((x for x in chain if x['strike_price'] == strike), None)
                if not row: return 0.0
                opt = row['call_options'] if type_ == 'CE' else row['put_options']
                return opt.get('option_greeks', {}).get('iv', 0.0)

            def get_straddle(chain, strike):
                row = next((x for x in chain if x['strike_price'] == strike), None)
                if not row: return 0.0
                return row['call_options']['market_data']['ltp'] + row['put_options']['market_data']['ltp']

            # --- METRICS CALCULATION ---
            # 1. IVs
            w_atm_iv = (get_iv(chain_w, atm_strike, 'CE') + get_iv(chain_w, atm_strike, 'PE')) / 2
            m_atm_iv = (get_iv(chain_m, atm_strike, 'CE') + get_iv(chain_m, atm_strike, 'PE')) / 2 if chain_m else w_atm_iv
            
            # 2. Skew
            otm_put_iv = get_iv(chain_w, atm_strike - 200, 'PE')
            otm_call_iv = get_iv(chain_w, atm_strike + 200, 'CE')
            skew = otm_put_iv - otm_call_iv if (otm_put_iv and otm_call_iv) else 0.0

            # 3. Term Structure
            term_structure = (w_atm_iv / m_atm_iv) if m_atm_iv > 0 else 1.0

            # 4. Straddles
            straddle_w = get_straddle(chain_w, atm_strike)
            straddle_m = get_straddle(chain_m, atm_strike)

            # 5. Efficiency Table (Theta/Vega)
            eff_table = []
            for item in chain_w:
                strike = item['strike_price']
                if abs(strike - spot) > 500: continue # Filter far OTM
                
                ce = item['call_options']['option_greeks']
                pe = item['put_options']['option_greeks']
                
                total_theta = ce.get('theta', 0) + pe.get('theta', 0)
                total_vega = ce.get('vega', 0) + pe.get('vega', 0)
                
                if total_vega > 0.1:
                    ratio = abs(total_theta) / total_vega
                    eff_table.append({
                        "strike": strike,
                        "theta": total_theta,
                        "vega": total_vega,
                        "ratio": ratio
                    })
            
            # Sort by ratio descending and take top 5
            eff_table.sort(key=lambda x: x['ratio'], reverse=True)
            top_5_eff = eff_table[:5]
            
            # 6. Max Pain & PCR
            pain_map = {}
            pcr_num, pcr_den = 0, 0
            for item in chain_w:
                strike = item['strike_price']
                ce_oi = item['call_options']['market_data']['oi']
                pe_oi = item['put_options']['market_data']['oi']
                pcr_num += pe_oi
                pcr_den += ce_oi
                
                # Simplified Max Pain
                pain = 0
                # Full pain calc is expensive O(N^2), doing local approximation or skip for speed
                # For dashboard speed, let's use highest OI Put - Highest OI Call center point or just skip exact calculation
                # Using Spot for now as placeholder or Max OI strike
                pain_map[strike] = ce_oi + pe_oi # Just total OI map for now
            
            max_pain = max(pain_map, key=pain_map.get) if pain_map else spot
            pcr = pcr_num / pcr_den if pcr_den > 0 else 1.0

            # Sanitize
            if w_atm_iv > 2.0: w_atm_iv /= 100.0
            if m_atm_iv > 2.0: m_atm_iv /= 100.0
            if skew > 2.0: skew /= 100.0

            return {
                "atm_iv": w_atm_iv,
                "monthly_iv": m_atm_iv,
                "term_structure": term_structure, 
                "skew_index": skew * 100,
                "straddle_price": straddle_w,
                "straddle_price_monthly": straddle_m,
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0 if w_atm_iv > 0 else 0.0,
                "pcr": round(pcr, 2),
                "max_pain": max_pain,
                "efficiency_table": top_5_eff
            }

        except Exception as e:
            logger.error(f"Structure Scan Error: {e}")
            return {"confidence": 0.0}
