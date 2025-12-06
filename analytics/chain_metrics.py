import pandas as pd
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger("VolGuard18")

class ChainMetricsCalculator:
    def __init__(self):
        self.metrics_cache: Dict[str, Dict] = {}

    def extract_seller_metrics(self, chain_data: List[Dict], spot: float) -> Dict[str, float]:
        try:
            if not chain_data:
                return self._get_default_metrics(spot)

            df = self._chain_to_dataframe(chain_data, spot)
            atm_strike = self._find_atm_strike(df, spot)
            straddle_price = self._calculate_straddle_price(df, atm_strike)
            greeks = self._calculate_atm_greeks(df, atm_strike)
            pop = self._calculate_pop(df, atm_strike, spot)
            avg_iv = self._calculate_average_iv(df)

            return {
                "atm_strike": atm_strike,
                "straddle_price": straddle_price,
                "theta": greeks.get("theta", 0),
                "vega": greeks.get("vega", 0),
                "delta": greeks.get("delta", 0),
                "gamma": greeks.get("gamma", 0),
                "pop": pop,
                "avg_iv": avg_iv,
                "call_oi": self._calculate_total_oi(df, "CE"),
                "put_oi": self._calculate_total_oi(df, "PE"),
                "max_pain": self._calculate_max_pain(df),
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
            if not ce_data or not pe_data:
                continue
            row = {
                "strike": strike,
                "ce_ltp": ce_data.get("market_data", {}).get("ltp", 0),
                "ce_iv": ce_data.get("option_greeks", {}).get("iv", 0),
                "ce_delta": ce_data.get("option_greeks", {}).get("delta", 0),
                "ce_theta": ce_data.get("option_greeks", {}).get("theta", 0),
                "ce_vega": ce_data.get("option_greeks", {}).get("vega", 0),
                "ce_oi": ce_data.get("market_data", {}).get("oi", 0),
                "pe_ltp": pe_data.get("market_data", {}).get("ltp", 0),
                "pe_iv": pe_data.get("option_greeks", {}).get("iv", 0),
                "pe_delta": pe_data.get("option_greeks", {}).get("delta", 0),
                "pe_theta": pe_data.get("option_greeks", {}).get("theta", 0),
                "pe_vega": pe_data.get("option_greeks", {}).get("vega", 0),
                "pe_oi": pe_data.get("market_data", {}).get("oi", 0),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    def _find_atm_strike(self, df: pd.DataFrame, spot: float) -> float:
        df["strike_distance"] = abs(df["strike"] - spot)
        closest = df.loc[df["strike_distance"].idxmin()]
        return closest["strike"]

    def _calculate_straddle_price(self, df: pd.DataFrame, atm_strike: float) -> float:
        atm_row = df[df["strike"] == atm_strike]
        if atm_row.empty:
            return 0.0
        ce_price = atm_row["ce_ltp"].values[0]
        pe_price = atm_row["pe_ltp"].values[0]
        return ce_price + pe_price

    def _calculate_atm_greeks(self, df: pd.DataFrame, atm_strike: float) -> Dict[str, float]:
        atm_row = df[df["strike"] == atm_strike]
        if atm_row.empty:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}
        row = atm_row.iloc[0]
        return {
            "delta": (abs(row["ce_delta"]) + abs(row["pe_delta"])) / 2,
            "gamma": (row["ce_gamma"] + row["pe_gamma"]) / 2,
            "theta": (row["ce_theta"] + row["pe_theta"]) / 2,
            "vega": (row["ce_vega"] + row["pe_vega"]) / 2,
        }

    def _calculate_pop(self, df: pd.DataFrame, atm_strike: float, spot: float) -> float:
        straddle_price = self._calculate_straddle_price(df, atm_strike)
        if straddle_price == 0:
            return 0.5
        pop = 1 - (straddle_price / (atm_strike * 0.05))
        return max(0.1, min(0.9, pop))

    def _calculate_average_iv(self, df: pd.DataFrame) -> float:
        ce_iv = df["ce_iv"].mean()
        pe_iv = df["pe_iv"].mean()
        return (ce_iv + pe_iv) / 2

    def _calculate_total_oi(self, df: pd.DataFrame, option_type: str = "ALL") -> int:
        if option_type == "CE":
            return int(df["ce_oi"].sum())
        elif option_type == "PE":
            return int(df["pe_oi"].sum())
        else:
            return int(df["ce_oi"].sum() + df["pe_oi"].sum())

    def _calculate_max_pain(self, df: pd.DataFrame) -> float:
        pain_points = []
        for strike in df["strike"]:
            total_pain = 0
            for _, row in df.iterrows():
                if row["strike"] < strike:
                    total_pain += row["ce_oi"] * (strike - row["strike"])
                elif row["strike"] > strike:
                    total_pain += row["pe_oi"] * (row["strike"] - strike)
            pain_points.append((strike, total_pain))
        if not pain_points:
            return df["strike"].median()
        min_pain_point = min(pain_points, key=lambda x: x[1])
        return min_pain_point[0]

    def _get_default_metrics(self, spot: float) -> Dict[str, float]:
        return {
            "atm_strike": spot,
            "straddle_price": spot * 0.015,
            "theta": -0.5,
            "vega": 5.0,
            "delta": 0.0,
            "gamma": 0.001,
            "pop": 0.5,
            "avg_iv": 0.15,
            "call_oi": 100000,
            "put_oi": 100000,
            "max_pain": spot,
        }
