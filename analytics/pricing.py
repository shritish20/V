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
            if len(expiries) < 3: 
                return {"confidence": 0.0}

            now = datetime.now(IST)
            today_date = now.date()
            
            # Intelligent Expiry Selection
            near_expiry = expiries[0]
            if near_expiry == today_date and now.time() > time(15, 15):
                near_expiry = expiries[1] if len(expiries) > 1 else expiries[0]
                
            far_expiry = expiries[-1]
            for e in expiries:
                if 25 <= (e - near_expiry).days <= 45:
                    far_expiry = e
                    break
            
            # CRITICAL FIX: 0DTE Time-to-Expiry Calculation
            dte = self._calculate_dte(near_expiry, now)

            # Parallel Fetch
            task_w = self.api.get_option_chain(settings.MARKET_KEY_INDEX, near_expiry.strftime("%Y-%m-%d"))
            task_m = self.api.get_option_chain(settings.MARKET_KEY_INDEX, far_expiry.strftime("%Y-%m-%d"))
            res_w, res_m = await asyncio.gather(task_w, task_m)
            
            if not res_w.get("data"): 
                return {"confidence": 0.0}
            
            chain_w = res_w["data"]
            chain_m = res_m.get("data", [])
            atm_strike = round(spot / 50) * 50
            
            # ATM STRADDLE ANALYSIS
            atm_row = next((x for x in chain_w if x['strike_price'] == atm_strike), None)
            
            atm_metrics = {
                "theta": 0.0, "vega": 0.0, "delta": 0.0, "gamma": 0.0, 
                "pop": 0.0, "iv": 0.0, "ltp": 0.0
            }
            
            if atm_row:
                ce = atm_row['call_options']
                pe = atm_row['put_options']
                
                atm_metrics["theta"] = ce['option_greeks'].get('theta', 0) + pe['option_greeks'].get('theta', 0)
                atm_metrics["vega"] = ce['option_greeks'].get('vega', 0) + pe['option_greeks'].get('vega', 0)
                atm_metrics["delta"] = ce['option_greeks'].get('delta', 0) + pe['option_greeks'].get('delta', 0)
                atm_metrics["gamma"] = ce['option_greeks'].get('gamma', 0) + pe['option_greeks'].get('gamma', 0)
                
                pop_c = ce['option_greeks'].get('pop', 0)
                pop_p = pe['option_greeks'].get('pop', 0)
                atm_metrics["pop"] = (pop_c + pop_p) / 2
                
                iv_c = ce['option_greeks'].get('iv', 0)
                iv_p = pe['option_greeks'].get('iv', 0)
                # Sanitize IV (0.15 -> 15.0)
                if iv_c < 2.0: iv_c *= 100
                if iv_p < 2.0: iv_p *= 100
                atm_metrics["iv"] = (iv_c + iv_p) / 2
                
                atm_metrics["ltp"] = ce['market_data']['ltp'] + pe['market_data']['ltp']

            # MONTHLY COMPARISON
            m_atm_iv = 0.0
            m_straddle = 0.0
            if chain_m:
                row_m = next((x for x in chain_m if x['strike_price'] == atm_strike), None)
                if row_m:
                    iv_c = row_m['call_options']['option_greeks'].get('iv', 0)
                    iv_p = row_m['put_options']['option_greeks'].get('iv', 0)
                    if iv_c < 2.0: iv_c *= 100
                    if iv_p < 2.0: iv_p *= 100
                    m_atm_iv = (iv_c + iv_p) / 2
                    m_straddle = row_m['call_options']['market_data']['ltp'] + row_m['put_options']['market_data']['ltp']

            # SKEW & EFFICIENCY TABLE
            eff_table = []
            skew = 0.0
            
            row_otm_p = next((x for x in chain_w if x['strike_price'] == atm_strike - 200), None)
            row_otm_c = next((x for x in chain_w if x['strike_price'] == atm_strike + 200), None)
            
            if row_otm_p and row_otm_c:
                p_iv = row_otm_p['put_options']['option_greeks'].get('iv', 0)
                c_iv = row_otm_c['call_options']['option_greeks'].get('iv', 0)
                if p_iv < 2.0: p_iv *= 100
                if c_iv < 2.0: c_iv *= 100
                skew = p_iv - c_iv

            for item in chain_w:
                strike = item['strike_price']
                if abs(strike - spot) > 500: continue
                
                ce_g = item['call_options']['option_greeks']
                pe_g = item['put_options']['option_greeks']
                
                tot_theta = ce_g.get('theta', 0) + pe_g.get('theta', 0)
                tot_vega = ce_g.get('vega', 0) + pe_g.get('vega', 0)
                
                if tot_vega > 0.1:
                    ratio = abs(tot_theta) / tot_vega
                    eff_table.append({
                        "strike": strike,
                        "theta": round(tot_theta, 2),
                        "vega": round(tot_vega, 2),
                        "ratio": round(ratio, 2)
                    })
            
            eff_table.sort(key=lambda x: x['ratio'], reverse=True)
            
            # MAX PAIN & PCR
            pain_map = {}
            pcr_num, pcr_den = 0, 0
            for item in chain_w:
                strike = item['strike_price']
                ce_oi = item['call_options']['market_data']['oi']
                pe_oi = item['put_options']['market_data']['oi']
                pcr_num += pe_oi
                pcr_den += ce_oi
                pain_map[strike] = ce_oi + pe_oi 
                
            max_pain = max(pain_map, key=pain_map.get) if pain_map else spot
            pcr = pcr_num / pcr_den if pcr_den > 0 else 1.0

            return {
                "atm_iv": atm_metrics["iv"],
                "monthly_iv": m_atm_iv,
                "term_structure_spread": atm_metrics["iv"] - m_atm_iv,
                "skew_index": skew,
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
                "pcr": round(pcr, 2),
                "max_pain": max_pain,
                "efficiency_table": eff_table[:5]
            }

        except Exception as e:
            logger.error(f"Pricing Engine Error: {e}")
            return {"confidence": 0.0}

    def _calculate_dte(self, expiry_date, now: datetime) -> float:
        """
        CRITICAL FIX: Proper 0DTE time-to-expiry calculation.
        Uses HOURS on expiry day, not days.
        """
        today = now.date()
        
        # Case 1: Future expiry
        if expiry_date > today:
            return (expiry_date - today).days
        
        # Case 2: Expiry is today (0DTE)
        elif expiry_date == today:
            market_close = time(15, 30)  # 3:30 PM IST
            current_time = now.time()
            
            # Calculate remaining hours until market close
            close_dt = datetime.combine(today, market_close)
            current_dt = datetime.combine(today, current_time)
            
            hours_remaining = (close_dt - current_dt).total_seconds() / 3600
            
            # Convert hours to fraction of a day
            # Minimum 0.01 days (14.4 minutes) to prevent division by zero
            dte_fraction = max(0.01, hours_remaining / 24)
            
            logger.info(
                f"0DTE Calculation: {hours_remaining:.1f}h remaining = {dte_fraction:.4f} days"
            )
            
            return dte_fraction
        
        # Case 3: Expired (should not happen in production)
        else:
            logger.warning(f"Expired contract detected: {expiry_date} < {today}")
            return 0.01  # Minimum safety value
