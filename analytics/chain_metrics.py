import pandas as pd
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger("VolGuardMetrics")

class ChainMetricsCalculator:
    def __init__(self):
        pass

    def extract_seller_metrics(self, chain_data: List[Dict], spot: float) -> Dict[str, float]:
        """
        Calculates institutional metrics from the Option Chain.
        """
        try:
            if not chain_data:
                return self._get_default_metrics(spot)

            df = self._chain_to_dataframe(chain_data, spot)
            if df.empty:
                return self._get_default_metrics(spot)

            atm_strike = self._find_atm_strike(df, spot)
            
            ce_oi = self._calculate_total_oi(df, "CE")
            pe_oi = self._calculate_total_oi(df, "PE")
            
            # PCR Calculation (Volume Weighted)
            pcr = pe_oi / ce_oi if ce_oi > 0 else 1.0
            
            return {
                "atm_strike": atm_strike,
                "max_pain": self._calculate_max_pain(df),
                "pcr": round(pcr, 2),
                "call_oi": ce_oi,
                "put_oi": pe_oi,
                "avg_iv": self._calculate_average_iv(df)
            }

        except Exception as e:
            logger.error(f"Seller metrics extraction failed: {e}")
            return self._get_default_metrics(spot)

    def _chain_to_dataframe(self, chain_data: List[Dict], spot: float) -> pd.DataFrame:
        rows = []
        for item in chain_data:
            strike = item.get("strike_price", 0)
            ce_data = item.get("call_options", {})
            pe_data = item.get("put_options", {})

            if not ce_data or not pe_data: continue

            rows.append({
                "strike": strike,
                "ce_oi": ce_data.get("market_data", {}).get("oi", 0),
                "pe_oi": pe_data.get("market_data", {}).get("oi", 0),
                "ce_iv": ce_data.get("option_greeks", {}).get("iv", 0),
                "pe_iv": pe_data.get("option_greeks", {}).get("iv", 0),
            })
        return pd.DataFrame(rows)

    def _find_atm_strike(self, df: pd.DataFrame, spot: float) -> float:
        # Strict Nifty 50 rounding logic
        return round(spot / 50) * 50

    def _calculate_total_oi(self, df: pd.DataFrame, option_type: str) -> int:
        if option_type == "CE": return int(df["ce_oi"].sum())
        if option_type == "PE": return int(df["pe_oi"].sum())
        return 0

    def _calculate_average_iv(self, df: pd.DataFrame) -> float:
        # Simple average of non-zero IVs
        valid_ivs = df[df["ce_iv"] > 0]["ce_iv"]
        val = valid_ivs.mean() if not valid_ivs.empty else 0.0
        # Normalize: if decimal < 2.0 (e.g. 0.15), make it 15.0
        if val > 0 and val < 2.0:
            return val * 100
        return val

    def _calculate_max_pain(self, df: pd.DataFrame) -> float:
        # Optimized Max Pain Calculation
        pain_data = []
        for strike in df["strike"]:
            # Intrinsic value calculation
            # If market ends at 'strike':
            # Calls lose: Max(0, market - strike) * Call OI
            # Puts lose: Max(0, strike - market) * Put OI
            
            # Using vectorization for speed
            ce_loss = (df["strike"] - strike).clip(lower=0) * df["ce_oi"] # ITM Calls 
            pe_loss = (strike - df["strike"]).clip(lower=0) * df["pe_oi"] # ITM Puts 
            
            # But wait, Max Pain is where *Options Writers* lose the LEAST.
            # Writers lose when options are ITM.
            # If price settles at `strike`:
            # Calls at `k < strike` are ITM (Writer loses `strike - k`)
            # Puts at `k > strike` are ITM (Writer loses `k - strike`)
            
            itm_calls_loss = (strike - df["strike"]).clip(lower=0) * df["ce_oi"]
            itm_puts_loss = (df["strike"] - strike).clip(lower=0) * df["pe_oi"]
            
            total_pain = itm_calls_loss.sum() + itm_puts_loss.sum()
            pain_data.append((strike, total_pain))
        
        if not pain_data: return df["strike"].mean()
        # Return strike with minimum pain
        return min(pain_data, key=lambda x: x[1])[0]

    def _get_default_metrics(self, spot: float) -> Dict[str, float]:
        return {
            "atm_strike": spot,
            "max_pain": spot,
            "pcr": 1.0,
            "call_oi": 0,
            "put_oi": 0,
            "avg_iv": 15.0
        }
