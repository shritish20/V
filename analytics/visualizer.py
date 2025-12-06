import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import io
import base64
from core.config import settings, IST
from core.models import DashboardData

logger = logging.getLogger("VolGuard18")

class DashboardVisualizer:
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2)
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        self.figure_cache: Dict[str, tuple] = {}

    async def generate_dashboard_summary(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            tasks = [
                self.generate_term_structure_plot(dashboard_data),
                self.generate_straddle_price_plot(dashboard_data),
                self.generate_capital_allocation_chart(dashboard_data),
                self.generate_greek_exposure_chart(dashboard_data),
                self.generate_market_regime_chart(dashboard_data)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            visualizations = {}
            for result in results:
                if result and not isinstance(result, Exception):
                    visualizations.update(result)
            logger.info(f"Generated {len(visualizations)} dashboard visualizations")
            return visualizations
        except Exception as e:
            logger.error(f"Dashboard summary generation failed: {e}")
            return {}

    async def generate_3d_vol_surface(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig = plt.figure(figsize=(14, 10))
            ax = fig.add_subplot(111, projection='3d')

            strikes = np.linspace(dashboard_data.spot_price * 0.9, dashboard_data.spot_price * 1.1, 20)
            expiries = np.array([7, 14, 21, 28, 35])
            X, Y = np.meshgrid(strikes, expiries)
            Z = dashboard_data.atm_iv * np.exp(-0.5 * ((X - dashboard_data.spot_price) / (dashboard_data.spot_price * 0.1)) ** 2)
            Z = Z * (1 - 0.1 * (Y / 35))

            surf = ax.plot_surface(X, Y, Z, cmap='RdYlGn_r', edgecolor='none', alpha=0.8)
            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Days to Expiry')
            ax.set_zlabel('Implied Volatility (%)')
            ax.set_title('3D Volatility Surface: Nifty 50 Options')

            cbar = fig.colorbar(surf, ax=ax, shrink=0.7)
            cbar.set_label('IV (%)')
            ax.view_init(elev=25, azim=45)

            filename = await self._save_figure(fig, "vol_surface_3d")
            plt.close(fig)
            return {"vol_surface_3d": filename}
        except Exception as e:
            logger.error(f"3D surface generation failed: {e}")
            return {}

    async def generate_iv_heatmap(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(14, 6))
            strikes = np.linspace(dashboard_data.spot_price * 0.9, dashboard_data.spot_price * 1.1, 10)
            expiries = ['7D', '14D', '21D', '28D', '35D']
            heatmap_data = []
            for expiry in expiries:
                row = []
                for strike in strikes:
                    moneyness = (strike - dashboard_data.spot_price) / dashboard_data.spot_price
                    base_iv = dashboard_data.atm_iv
                    skew = -0.1 * moneyness * 100
                    term_decay = 0.05 if expiry == '7D' else 0.1 if expiry == '14D' else 0.15
                    iv = base_iv + skew - term_decay
                    row.append(max(0.05, min(0.50, iv)))
                heatmap_data.append(row)
            heatmap_data = np.array(heatmap_data)

            im = ax.imshow(heatmap_data * 100, cmap='RdYlGn_r', aspect='auto')
            ax.set_xticks(range(len(strikes)))
            ax.set_xticklabels([f'{s:.0f}' for s in strikes], rotation=45)
            ax.set_yticks(range(len(expiries)))
            ax.set_yticklabels(expiries)
            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Days to Expiry')
            ax.set_title('IV Heatmap: Strike vs Expiry')

            cbar = ax.figure.colorbar(im, ax=ax)
            cbar.set_label('IV (%)')

            for i in range(len(expiries)):
                for j in range(len(strikes)):
                    text = ax.text(j, i, f'{heatmap_data[i, j] * 100:.1f}',
                                   ha="center", va="center", color="black", fontsize=8)

            filename = await self._save_figure(fig, "iv_heatmap")
            plt.close(fig)
            return {"iv_heatmap": filename}
        except Exception as e:
            logger.error(f"Heatmap generation failed: {e}")
            return {}

    async def generate_iv_skew_plot(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            strikes = np.linspace(dashboard_data.spot_price * 0.85, dashboard_data.spot_price * 1.15, 20)
            call_ivs = []
            put_ivs = []
            for strike in strikes:
                moneyness = (strike - dashboard_data.spot_price) / dashboard_data.spot_price
                base_iv = dashboard_data.atm_iv
                call_iv = base_iv - 0.05 * moneyness * 100
                put_iv = base_iv + 0.05 * moneyness * 100
                call_ivs.append(max(0.05, min(0.50, call_iv)))
                put_ivs.append(max(0.05, min(0.50, put_iv)))

            ax.plot(strikes, call_ivs, marker='o', label='Call IV', linewidth=2, color='blue')
            ax.plot(strikes, put_ivs, marker='s', label='Put IV', linewidth=2, color='red')
            skew = np.array(call_ivs) - np.array(put_ivs)
            ax.plot(strikes, skew, marker='^', label='IV Skew', linewidth=2, color='purple', linestyle='--', alpha=0.7)
            ax.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
            ax.axvline(x=dashboard_data.spot_price, color='green', linestyle='--', alpha=0.5, label='ATM')
            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Implied Volatility')
            ax.set_title('IV Skew Analysis')
            ax.legend()
            ax.grid(True, alpha=0.3)

            filename = await self._save_figure(fig, "iv_skew")
            plt.close(fig)
            return {"iv_skew": filename}
        except Exception as e:
            logger.error(f"IV skew plot generation failed: {e}")
            return {}

    async def generate_term_structure_plot(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            days = [1, 7, 14, 21, 28, 35, 42, 49, 56]
            ivs = [dashboard_data.atm_iv + 0.03, dashboard_data.atm_iv + 0.02,
                   dashboard_data.atm_iv + 0.01, dashboard_data.atm_iv,
                   dashboard_data.atm_iv - 0.01, dashboard_data.atm_iv - 0.02,
                   dashboard_data.atm_iv - 0.03, dashboard_data.atm_iv - 0.04,
                   dashboard_data.atm_iv - 0.05]

            ax.plot(days, ivs, marker='o', linewidth=2, markersize=8, color='darkblue')
            for day, iv in zip(days, ivs):
                ax.text(day, iv + 0.002, f'{iv * 100:.1f}%', ha='center', fontsize=9)

            ax.set_xlabel('Days to Expiry')
            ax.set_ylabel('ATM Implied Volatility (%)')
            ax.set_title('Volatility Term Structure')
            ax.grid(True, alpha=0.3)
            ax.set_xticks(days)

            filename = await self._save_figure(fig, "term_structure")
            plt.close(fig)
            return {"term_structure": filename}
        except Exception as e:
            logger.error(f"Term structure plot generation failed: {e}")
            return {}

    async def generate_straddle_price_plot(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(12, 6))
            strikes = np.linspace(dashboard_data.spot_price * 0.85, dashboard_data.spot_price * 1.15, 20)
            straddle_prices = []
            for strike in strikes:
                distance = abs(strike - dashboard_data.spot_price) / dashboard_data.spot_price
                price = dashboard_data.straddle_price * np.exp(-10 * distance ** 2)
                straddle_prices.append(price)

            ax.plot(strikes, straddle_prices, marker='o', linewidth=2, markersize=6, color='darkgreen')
            ax.axvline(x=dashboard_data.spot_price, color='red', linestyle='--', alpha=0.5, label='ATM')
            ax.axvline(x=dashboard_data.breakeven_lower, color='orange', linestyle='--', alpha=0.5, label='Lower Breakeven')
            ax.axvline(x=dashboard_data.breakeven_upper, color='orange', linestyle='--', alpha=0.5, label='Upper Breakeven')
            ax.axvspan(dashboard_data.breakeven_lower, dashboard_data.breakeven_upper,
                       alpha=0.2, color='green', label='Profit Zone')

            ax.set_xlabel('Strike Price')
            ax.set_ylabel('Straddle Price (₹)')
            ax.set_title('Straddle Premium Across Strikes')
            ax.legend()
            ax.grid(True, alpha=0.3)

            filename = await self._save_figure(fig, "straddle_prices")
            plt.close(fig)
            return {"straddle_prices": filename}
        except Exception as e:
            logger.error(f"Straddle price plot generation failed: {e}")
            return {}

    async def generate_capital_allocation_chart(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            buckets = ['weekly_expiries', 'monthly_expiries', 'intraday_adjustments']
            allocated = [dashboard_data.capital_allocation[b] for b in buckets]
            used = [dashboard_data.capital_used.get(b, 0) for b in buckets]
            display_names = [b.replace('_', ' ').title() for b in buckets]

            wedges1, texts1, autotexts1 = ax1.pie(allocated, labels=display_names, autopct='%1.1f%%', startangle=90)
            ax1.set_title('Capital Allocation (%)', fontweight='bold')

            wedges2, texts2, autotexts2 = ax2.pie(used, labels=display_names,
                                                   autopct=lambda p: f'₹{sum(used) * p / 100:,.0f}',
                                                   startangle=90)
            ax2.set_title('Capital Usage (₹)', fontweight='bold')

            filename = await self._save_figure(fig, "capital_allocation")
            plt.close(fig)
            return {"capital_allocation": filename}
        except Exception as e:
            logger.error(f"Capital allocation chart generation failed: {e}")
            return {}

    async def generate_greek_exposure_chart(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            axes = axes.flatten()
            greeks = ['Delta', 'Gamma', 'Theta', 'Vega']
            values = [dashboard_data.delta, dashboard_data.gamma,
                      dashboard_data.total_theta, dashboard_data.total_vega]
            colors = ['blue', 'green', 'red', 'purple']

            for idx, (greek, value, color) in enumerate(zip(greeks, values, colors)):
                ax = axes[idx]
                bars = ax.bar([greek], [abs(value)], color=color, alpha=0.7)
                ax.text(0, abs(value) * 0.5, f'{value:,.0f}',
                        ha='center', va='center', fontweight='bold', fontsize=10)
                ax.set_ylim(0, max(abs(value) * 1.2, 10))
                ax.set_title(f'{greek} Exposure', fontweight='bold')
                ax.grid(True, alpha=0.3, axis='y')
                ax.set_xticks([])

            plt.tight_layout()
            filename = await self._save_figure(fig, "greek_exposure")
            plt.close(fig)
            return {"greek_exposure": filename}
        except Exception as e:
            logger.error(f"Greek exposure chart generation failed: {e}")
            return {}

    async def generate_market_regime_chart(self, dashboard_data: DashboardData) -> Dict[str, str]:
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            regimes = {
                'PANIC': {'vix_min': 25, 'color': 'red', 'alpha': 0.3},
                'FEAR': {'vix_min': 20, 'vix_max': 25, 'color': 'orange', 'alpha': 0.3},
                'NORMAL': {'vix_min': 12, 'vix_max': 20, 'color': 'yellow', 'alpha': 0.2},
                'CALM': {'vix_min': 8, 'vix_max': 12, 'color': 'lightgreen', 'alpha': 0.2},
                'COMPLACENT': {'vix_max': 8, 'color': 'darkgreen', 'alpha': 0.3}
            }

            current_vix = dashboard_data.vix
            for name, data in regimes.items():
                if 'vix_min' in data and 'vix_max' in data:
                    ax.axhspan(data['vix_min'], data['vix_max'], alpha=data['alpha'],
                               color=data['color'], label=name)
                elif 'vix_min' in data:
                    ax.axhspan(data['vix_min'], 50, alpha=data['alpha'], color=data['color'], label=name)
                elif 'vix_max' in data:
                    ax.axhspan(0, data['vix_max'], alpha=data['alpha'], color=data['color'], label=name)

            ax.axhline(y=current_vix, color='black', linewidth=3,
                       label=f'Current VIX: {current_vix:.1f}')
            ax.text(0.5, current_vix + 1, f'Regime: {dashboard_data.regime}',
                    ha='center', fontweight='bold', fontsize=12,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="white"))

            ax.set_xlabel('Time')
            ax.set_ylabel('India VIX')
            ax.set_title('Market Regime Analysis')
            ax.set_ylim(0, 50)
            ax.set_xlim(0, 1)
            ax.legend(loc='upper right')
            ax.grid(True, alpha=0.3)
            ax.set_xticks([])

            filename = await self._save_figure(fig, "market_regime")
            plt.close(fig)
            return {"market_regime": filename}
        except Exception as e:
            logger.error(f"Market regime chart generation failed: {e}")
            return {}

    async def _save_figure(self, fig, name: str) -> str:
        try:
            timestamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
            filename = f"{settings.DASHBOARD_DATA_DIR}/{name}_{timestamp}.png"
            fig.savefig(filename, dpi=150, bbox_inches='tight')
            logger.debug(f"Saved figure: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Failed to save figure {name}: {e}")
            return ""

    def clear_cache(self):
        self.figure_cache.clear()
        logger.debug("Figure cache cleared")
