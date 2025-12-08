import matplotlib
# CRITICAL FIX: Set backend to Agg before importing pyplot to prevent Docker crashes
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from datetime import datetime
import logging
from core.config import settings, IST
from core.models import DashboardData

logger = logging.getLogger("VolGuardVisualizer")

class DashboardVisualizer:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2)
        try:
            plt.style.use('seaborn-v0_8-darkgrid')
        except:
            plt.style.use('bmh')  # Fallback style
        sns.set_palette("husl")
        self.figure_cache: Dict[str, tuple] = {}

    async def generate_dashboard_summary(self, dashboard_data: DashboardData) -> Dict[str, str]:
        """Generates all charts in parallel without blocking the main loop"""
        try:
            loop = asyncio.get_running_loop()
            # Offload heavy plotting to thread pool
            tasks = [
                loop.run_in_executor(self.executor, self._plot_term_structure, dashboard_data),
                loop.run_in_executor(self.executor, self._plot_straddle_price, dashboard_data),
                loop.run_in_executor(self.executor, self._plot_greek_exposure, dashboard_data),
                loop.run_in_executor(self.executor, self._plot_market_regime, dashboard_data)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            visualizations = {}
            for res in results:
                if isinstance(res, dict):
                    visualizations.update(res)
            
            logger.info(f"Generated {len(visualizations)} dashboard visualizations")
            return visualizations
        except Exception as e:
            logger.error(f"Dashboard summary generation failed: {e}")
            return {}

    def _plot_term_structure(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(10, 5))
            days = [7, 14, 21, 28, 35]
            # Mock term structure curve based on ATM IV + skew
            ivs = [dashboard_data.atm_iv + (0.005 * i) for i in range(len(days))]
            
            ax.plot(days, ivs, marker='o', linewidth=2, color='blue')
            ax.set_title("Volatility Term Structure")
            ax.set_xlabel("Days to Expiry")
            ax.set_ylabel("Implied Volatility")
            return self._save_figure(fig, "term_structure")
        except Exception:
            return {}

    def _plot_straddle_price(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(10, 5))
            strikes = np.linspace(dashboard_data.spot_price * 0.9, dashboard_data.spot_price * 1.1, 20)
            prices = [dashboard_data.straddle_price * np.exp(-10 * abs(k - dashboard_data.spot_price) / dashboard_data.spot_price) for k in strikes]
            
            ax.plot(strikes, prices, color='green')
            ax.axvline(x=dashboard_data.spot_price, color='red', linestyle='--', label='ATM')
            ax.set_title("Straddle Price Simulation")
            return self._save_figure(fig, "straddle_prices")
        except Exception:
            return {}

    def _plot_greek_exposure(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(8, 6))
            greeks = ['Delta', 'Gamma', 'Theta', 'Vega']
            values = [dashboard_data.delta, dashboard_data.gamma, dashboard_data.total_theta, dashboard_data.total_vega]
            colors = ['blue', 'green', 'red', 'purple']
            
            ax.bar(greeks, values, color=colors)
            ax.set_title("Portfolio Greek Exposure")
            ax.grid(True, alpha=0.3)
            return self._save_figure(fig, "greek_exposure")
        except Exception:
            return {}

    def _plot_market_regime(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(10, 2))
            ax.text(0.5, 0.5, f"REGIME: {dashboard_data.regime}", 
                   ha='center', va='center', fontsize=20, fontweight='bold')
            ax.axis('off')
            return self._save_figure(fig, "market_regime")
        except Exception:
            return {}

    def _save_figure(self, fig, name: str) -> Dict[str, str]:
        try:
            timestamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
            filename = f"{settings.DASHBOARD_DATA_DIR}/{name}_{timestamp}.png"
            fig.savefig(filename, dpi=100, bbox_inches='tight')
            plt.close(fig) # CRITICAL FIX: Close figure to prevent memory leak
            return {name: filename}
        except Exception as e:
            logger.error(f"Save Figure Failed: {e}")
            plt.close(fig)
            return {}

    def clear_cache(self):
        self.figure_cache.clear()
