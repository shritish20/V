import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Dict, List
import logging
from arch import arch_model
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher

logger = logging.getLogger("VolGuard18")

class HybridVolatilityAnalytics:
    def __init__(self):
        self.data_fetcher = DashboardDataFetcher()
        self.vol_cache: Dict[str, Tuple[float, datetime]] = {}
        self.cache_ttl = 300  # 5 minutes

    def get_volatility_metrics(self, current_vix: float) -> Tuple[float, float, float]:
        try:
            realized_vol = self._calculate_realized_volatility()
            garch_vol = self._calculate_garch_forecast()
            iv_percentile = self._calculate_iv_percentile(current_vix)
            logger.debug(f"Vol Metrics - Realized: {realized_vol:.2f}%, GARCH: {garch_vol:.2f}%, IVP: {iv_percentile:.2f}%")
            return realized_vol, garch_vol, iv_percentile
        except Exception as e:
            logger.error(f"Volatility metrics calculation failed: {e}")
            return 15.0, 15.0, 50.0

    def _calculate_realized_volatility(self, window: int = 7) -> float:
        cache_key = f"realized_vol_{window}"
        if cache_key in self.vol_cache:
            value, timestamp = self.vol_cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return value
        try:
            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna().tail(window)
            realized_vol = returns.std() * np.sqrt(252) * 100
            self.vol_cache[cache_key] = (realized_vol, datetime.now(IST))
            return realized_vol
        except Exception as e:
            logger.error(f"Realized vol calculation failed: {e}")
            return 15.0

    def _calculate_garch_forecast(self, horizon: int = 1) -> float:
        cache_key = f"garch_forecast_{horizon}"
        if cache_key in self.vol_cache:
            value, timestamp = self.vol_cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return value
        try:
            returns = self.data_fetcher.nifty_data['Log_Returns'].dropna().tail(252)
            if len(returns) < 100:
                return 15.0
            model = arch_model(returns * 100, vol='Garch', p=1, q=1)
            fitted_model = model.fit(disp='off')
            forecast = fitted_model.forecast(horizon=horizon)
            garch_vol = np.sqrt(forecast.variance.values[-1, -1]) / 100
            garch_vol_annual = garch_vol * np.sqrt(252) * 100
            self.vol_cache[cache_key] = (garch_vol_annual, datetime.now(IST))
            return garch_vol_annual
        except Exception as e:
            logger.error(f"GARCH forecast failed: {e}")
            return 15.0

    def _calculate_iv_percentile(self, current_vix: float, lookback_days: int = 252) -> float:
        cache_key = f"iv_percentile_{current_vix:.2f}"
        if cache_key in self.vol_cache:
            value, timestamp = self.vol_cache[cache_key]
            if (datetime.now(IST) - timestamp).total_seconds() < self.cache_ttl:
                return value
        try:
            iv_percentile = self.data_fetcher.calculate_iv_percentile(current_vix, lookback_days)
            self.vol_cache[cache_key] = (iv_percentile, datetime.now(IST))
            return iv_percentile
        except Exception as e:
            logger.error(f"IV percentile calculation failed: {e}")
            if current_vix < 12:
                return 20.0
            elif current_vix < 18:
                return 50.0
            elif current_vix < 25:
                return 70.0
            else:
                return 90.0

    def calculate_volatility_regime(self, vix: float, ivp: float, realized_vol: float) -> str:
        iv_rv_spread = vix - realized_vol
        if vix > 25 and iv_rv_spread > 5.0:
            return "PANIC"
        elif vix > 20 and ivp > 70:
            return "FEAR_BACKWARDATION"
        elif ivp < 30:
            return "LOW_VOL_COMPRESSION"
        elif 15 <= vix <= 22 and ivp < 70:
            return "CALM_COMPRESSION"
        else:
            return "TRANSITION"

    def calculate_expected_move(self, spot: float, straddle_price: float) -> Tuple[float, float, float]:
        expected_move_pct = (straddle_price / spot) * 100
        lower_band = spot - straddle_price
        upper_band = spot + straddle_price
        return expected_move_pct, lower_band, upper_band

    def calculate_volatility_surface(self, chain_data: List[Dict], spot: float) -> List[Dict]:
        surface_points = []
        try:
            for item in chain_data:
                strike = item.get('strike_price', 0)
                ce_data = item.get('call_options', {})
                pe_data = item.get('put_options', {})
                if not ce_data or not pe_data:
                    continue
                ce_iv = ce_data.get('option_greeks', {}).get('iv', 0)
                pe_iv = pe_data.get('option_greeks', {}).get('iv', 0)
                if ce_iv == 0 or pe_iv == 0:
                    continue
                moneyness = ((strike - spot) / spot) * 100
                surface_points.append({
                    'strike': strike,
                    'moneyness': moneyness,
                    'call_iv': ce_iv,
                    'put_iv': pe_iv,
                    'avg_iv': (ce_iv + pe_iv) / 2,
                    'iv_skew': ce_iv - pe_iv,
                    'call_oi': ce_data.get('market_data', {}).get('oi', 0),
                    'put_oi': pe_data.get('market_data', {}).get('oi', 0)
                })
            return surface_points
        except Exception as e:
            logger.error(f"Volatility surface calculation failed: {e}")
            return surface_points

    def calculate_term_structure(self, chains_by_expiry: Dict[str, List[Dict]], spot: float) -> List[Dict]:
        term_structure = []
        try:
            for expiry, chain in chains_by_expiry.items():
                if not chain:
                    continue
                atm_item = min(chain, key=lambda x: abs(x.get('strike_price', 0) - spot))
                ce = atm_item.get('call_options', {})
                pe = atm_item.get('put_options', {})
                if ce and pe:
                    ce_iv = ce.get('option_greeks', {}).get('iv', 0)
                    pe_iv = pe.get('option_greeks', {}).get('iv', 0)
                    if ce_iv > 0 and pe_iv > 0:
                        avg_iv = (ce_iv + pe_iv) / 2
                        try:
                            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
                            days_to_expiry = max(1, (expiry_dt - datetime.now(IST)).days)
                        except:
                            days_to_expiry = 7
                        term_structure.append({
                            'expiry': expiry,
                            'days_to_expiry': days_to_expiry,
                            'atm_iv': avg_iv,
                            'strike': atm_item.get('strike_price', spot)
                        })
            term_structure.sort(key=lambda x: x['days_to_expiry'])
            return term_structure
        except Exception as e:
            logger.error(f"Term structure calculation failed: {e}")
            return []

    def clear_cache(self):
        self.vol_cache.clear()
        logger.debug("Volatility cache cleared")
