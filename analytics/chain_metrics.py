import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
import logging
from datetime import datetime
from scipy.stats import linregress

logger = logging.getLogger("VolGuard14")

class ChainMetricsCalculator:
    """Advanced option chain metrics calculator with quant intelligence"""
    
    @staticmethod
    def extract_seller_metrics(option_chain: List[Dict[str, Any]], spot_price: float) -> Dict[str, Any]:
        """Extracts key metrics for option sellers from the option chain."""
        try:
            valid_options = [opt for opt in option_chain if opt.get("call_options") and opt.get("put_options") and \
                opt["call_options"].get("market_data") and opt["put_options"].get("market_data") and \
                opt["call_options"]["market_data"].get("ltp") is not None and opt["put_options"]["market_data"].get("ltp") is not None and \
                opt["call_options"].get("option_greeks") and opt["put_options"].get("option_greeks") and \
                opt["call_options"]["option_greeks"].get("iv") is not None and opt["put_options"]["option_greeks"].get("iv") is not None]

            if not valid_options:
                logger.warning("No valid options found in chain for seller metrics extraction.")
                return {}

            atm_strike_info = min(valid_options, key=lambda x: abs(x["strike_price"] - spot_price))
            call_atm = atm_strike_info["call_options"]
            put_atm = atm_strike_info["put_options"]

            return {
                "atm_strike": atm_strike_info["strike_price"],
                "straddle_price": call_atm["market_data"]["ltp"] + put_atm["market_data"]["ltp"],
                "avg_iv": (call_atm["option_greeks"]["iv"] + put_atm["option_greeks"]["iv"]) / 2,
                "theta": (call_atm["option_greeks"].get("theta", 0.0) or 0.0) + (put_atm["option_greeks"].get("theta", 0.0) or 0.0),
                "vega": (call_atm["option_greeks"].get("vega", 0.0) or 0.0) + (put_atm["option_greeks"].get("vega", 0.0) or 0.0),
                "delta": (call_atm["option_greeks"].get("delta", 0.0) or 0.0) + (put_atm["option_greeks"].get("delta", 0.0) or 0.0),
                "gamma": (call_atm["option_greeks"].get("gamma", 0.0) or 0.0) + (put_atm["option_greeks"].get("gamma", 0.0) or 0.0),
                "pop": ((call_atm["option_greeks"].get("pop", 0.0) or 0.0) + (put_atm["option_greeks"].get("pop", 0.0) or 0.0)) / 2,
            }
        except Exception as e:
            logger.error(f"Exception in extract_seller_metrics for spot {spot_price}: {e}")
            return {}

    @staticmethod
    def calculate_market_metrics(option_chain: List[Dict[str, Any]], expiry_date: str) -> Dict[str, Any]:
        """Calculates broader market metrics like Days to Expiry, PCR, and Max Pain."""
        try:
            expiry_dt = datetime.strptime(expiry_date, "%Y-%m-%d")
            days_to_expiry = (expiry_dt - datetime.now()).days
            days_to_expiry = max(0, days_to_expiry)

            call_oi = sum(opt["call_options"]["market_data"]["oi"] for opt in option_chain if opt.get("call_options") and opt["call_options"].get("market_data") and opt["call_options"]["market_data"].get("oi") is not None)
            put_oi = sum(opt["put_options"]["market_data"]["oi"] for opt in option_chain if opt.get("put_options") and opt["put_options"].get("market_data") and opt["put_options"]["market_data"].get("oi") is not None)

            pcr = put_oi / call_oi if call_oi != 0 else 0

            strikes = sorted(list(set(opt["strike_price"] for opt in option_chain)))
            max_pain_strike = 0
            min_pain = float('inf')

            valid_strikes_for_pain = [opt for opt in option_chain if \
                opt.get("call_options") and opt["call_options"].get("market_data") and opt["call_options"]["market_data"].get("oi") is not None and \
                opt.get("put_options") and opt["put_options"].get("market_data") and opt["put_options"]["market_data"].get("oi") is not None]

            for strike in strikes:
                pain_at_strike = 0
                for opt in valid_strikes_for_pain:
                    pain_at_strike += max(0, strike - opt["strike_price"]) * opt["call_options"]["market_data"]["oi"]
                    pain_at_strike += max(0, opt["strike_price"] - strike) * opt["put_options"]["market_data"]["oi"]
                
                if pain_at_strike < min_pain:
                    min_pain = pain_at_strike
                    max_pain_strike = strike

            return {"days_to_expiry": days_to_expiry, "pcr": round(pcr, 2), "max_pain": max_pain_strike}
        except Exception as e:
            logger.error(f"Exception in market_metrics: {e}")
            return {"days_to_expiry": 0, "pcr": 0, "max_pain": 0}

    @staticmethod
    def calculate_iv_skew_slope(full_chain_df: pd.DataFrame) -> float:
        """Calculates the slope of the IV skew."""
        try:
            if full_chain_df.empty or "Strike" not in full_chain_df.columns or "IV Skew" not in full_chain_df.columns:
                logger.warning("Full chain DataFrame is empty or missing required columns for IV skew slope calculation. Returning 0.0.")
                return 0.0

            df_filtered = full_chain_df[["Strike", "IV Skew"]].dropna()
            if len(df_filtered) < 2:
                logger.warning("Not enough valid data points for linear regression on IV Skew. Returning 0.0.")
                return 0.0

            slope, _, _, _, _ = linregress(df_filtered["Strike"], df_filtered["IV Skew"])
            return round(slope, 4)
        except Exception as e:
            logger.error(f"Exception in calculate_iv_skew_slope: {e}")
            return 0.0

    @staticmethod
    def calculate_regime_score(atm_iv: float, ivp: float, realized_vol: float, garch_vol: float, 
                             straddle_price: float, spot_price: float, pcr: float, vix: float, 
                             iv_skew_slope: float) -> Tuple[float, str, str, str]:
        """Determines the current market volatility regime based on various metrics."""
        expected_move = (straddle_price / spot_price) * 100 if spot_price else 0
        vol_spread = atm_iv - realized_vol

        regime_score = 0
        regime_score += 10 if ivp > 80 else (-10 if ivp < 20 else 0)
        regime_score += 10 if vol_spread > 10 else (-10 if vol_spread < -10 else 0)
        regime_score += 10 if vix > 20 else (-10 if vix < 10 else 0)
        regime_score += 5 if pcr > 1.2 else (-5 if pcr < 0.8 else 0)
        regime_score += 5 if abs(iv_skew_slope) > 0.001 else 0
        regime_score += 10 if expected_move > 0.05 else (-10 if expected_move < 0.02 else 0)
        regime_score += 5 if garch_vol > realized_vol * 1.2 else (-5 if garch_vol < realized_vol * 0.8 else 0)

        if regime_score > 20:
            return regime_score, "High Vol Trend 🔥", "Market in high volatility — ideal for premium selling.", "High IVP, elevated VIX, and wide straddle suggest strong premium opportunities."
        elif regime_score > 10:
            return regime_score, "Elevated Volatility ⚡", "Above-average volatility — favor range-bound strategies.", "Moderate IVP and IV-RV spread indicate potential for mean-reverting moves."
        elif regime_score > -10:
            return regime_score, "Neutral Volatility 🙂", "Balanced market — flexible strategy selection.", "IV and RV aligned, with moderate PCR and skew."
        else:
            return regime_score, "Low Volatility 📉", "Low volatility — cautious selling or long vega plays.", "Low IVP, tight straddle, and low VIX suggest limited movement."
