import asyncio
from typing import Dict, Optional, List
from datetime import datetime, timedelta, time
import logging
from core.config import settings, IST
from analytics.sabr_model import EnhancedSABRModel

logger = logging.getLogger("PricingEngine")

class DataIntegrityError(Exception):
    pass

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
            
            # --- KILL SWITCH 1: 0DTE SAFETY FLOOR ---
            # Never allow T < 0.001 (approx 5 minutes) to prevent Gamma explosion
            dte_raw = self._calculate_dte(near_expiry, now)
            dte = max(0.001, dte_raw)

            # Parallel Fetch
            task_w = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_m = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            res_w, res_m = await asyncio.gather(task_w, task_m)
            
            # --- KILL SWITCH 2: DATA INTEGRITY ---
            if not res_w.get("data"): 
                logger.critical("🚨 EMPTY OPTION CHAIN RECEIVED")
                raise DataIntegrityError("Option Chain Data is Empty")
            
            chain_w = res_w["data"]
            chain_m = res_m.get("data", [])
            atm_strike = round(spot / 50) * 50
            
            # --- METRICS CALCULATION (Lite Script Logic) ---
            atm_row = next((x for x in chain_w if x['strike_price'] == atm_strike), None)
            atm_metrics = {"theta": 0, "vega": 0, "delta": 0, "gamma": 0, "pop": 0, "iv": 0, "ltp": 0}
            
            if atm_row:
                ce, pe = atm_row['call_options'], atm_row['put_options']
                atm_metrics["theta"] = ce['option_greeks'].get('theta', 0) + pe['option_greeks'].get('theta', 0)
                atm_metrics["vega"] = ce['option_greeks'].get('vega', 0) + pe['option_greeks'].get('vega', 0)
                atm_metrics["delta"] = ce['option_greeks'].get('delta', 0) + pe['option_greeks'].get('delta', 0)
                atm_metrics["gamma"] = ce['option_greeks'].get('gamma', 0) + pe['option_greeks'].get('gamma', 0)
                atm_metrics["pop"] = (ce['option_greeks'].get('pop', 0) + pe['option_greeks'].get('pop', 0)) / 2
                
                iv_c, iv_p = ce['option_greeks'].get('iv', 0), pe['option_greeks'].get('iv', 0)
                if iv_c < 2.0: iv_c *= 100
                if iv_p < 2.0: iv_p *= 100
                atm_metrics["iv"] = (iv_c + iv_p) / 2
                atm_metrics["ltp"] = ce['market_data']['ltp'] + pe['market_data']['ltp']

            # Monthly & Skew Logic
            m_atm_iv, m_straddle = 0.0, 0.0
            if chain_m:
                row_m = next((x for x in chain_m if x['strike_price'] == atm_strike), None)
                if row_m:
                    iv_c = row_m['call_options']['option_greeks'].get('iv', 0)
                    iv_p = row_m['put_options']['option_greeks'].get('iv', 0)
                    if iv_c < 2.0: iv_c *= 100
                    if iv_p < 2.0: iv_p *= 100
                    m_atm_iv = (iv_c + iv_p) / 2
                    m_straddle = row_m['call_options']['market_data']['ltp'] + row_m['put_options']['market_data']['ltp']

            # Efficiency Table
            eff_table = []
            for item in chain_w:
                strike = item['strike_price']
                if abs(strike - spot) > 500: continue
                ce_g, pe_g = item['call_options']['option_greeks'], item['put_options']['option_greeks']
                tot_vega = ce_g.get('vega', 0) + pe_g.get('vega', 0)
                tot_theta = ce_g.get('theta', 0) + pe_g.get('theta', 0)
                
                if tot_vega > 0.1:
                    eff_table.append({
                        "strike": strike,
                        "theta": round(tot_theta, 2), 
                        "vega": round(tot_vega, 2),
                        "ratio": round(abs(tot_theta)/tot_vega, 2)
                    })
            eff_table.sort(key=lambda x: x['ratio'], reverse=True)

            return {
                "atm_iv": atm_metrics["iv"],
                "monthly_iv": m_atm_iv,
                "term_structure_spread": atm_metrics["iv"] - m_atm_iv,
                "straddle_price": atm_metrics["ltp"],
                "straddle_price_monthly": m_straddle,
                "atm_theta": atm_metrics["theta"],
                "atm_vega": atm_metrics["vega"],
                "atm_delta": atm_metrics["delta"],
                "atm_gamma": atm_metrics["gamma"],
                "atm_pop": atm_metrics["pop"],
                "days_to_expiry": float(dte),
                "near_expiry": near_expiry.strftime("%Y-%m-%d"),
                "confidence": 1.0 if atm_metrics["iv"] > 0 else 0.0,
                "pcr": 1.0, # Placeholder, calculated in main engine usually
                "max_pain": spot, # Placeholder
                "efficiency_table": eff_table[:5],
                "skew_index": 0.0 # Placeholder
            }

        except DataIntegrityError:
            raise # Let Engine handle critical failure
        except Exception as e:
            logger.error(f"Pricing Engine Error: {e}")
            return {"confidence": 0.0}

    def _calculate_dte(self, expiry_date, now: datetime) -> float:
        today = now.date()
        if expiry_date > today:
            return (expiry_date - today).days
        elif expiry_date == today:
            market_close = time(15, 30)
            current_time = now.time()
            close_dt = datetime.combine(today, market_close)
            current_dt = datetime.combine(today, current_time)
            hours = (close_dt - current_dt).total_seconds() / 3600
            return max(0.001, hours / 24) # Safety floor applied here too
        else:
            return 0.001
