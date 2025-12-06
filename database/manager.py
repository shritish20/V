import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from core.config import settings, IST
from core.models import AdvancedMetrics, MultiLegTrade, TradeStatus

logger = logging.getLogger("VolGuard18")

class HybridDatabaseManager:
    def __init__(self):
        self.engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self):
        try:
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL.replace("postgresql+asyncpg", "postgresql"),
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error(f"Failed to create DB pool: {e}")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def save_market_analytics(self, metrics: AdvancedMetrics):
        if not self.pool:
            return
        try:
            await self.pool.execute("""
                INSERT INTO market_analytics (
                    timestamp, spot_price, vix, ivp, realized_vol_7d, garch_vol_7d,
                    iv_rv_spread, pcr, max_pain, event_risk_score, regime,
                    term_structure_slope, volatility_skew,
                    sabr_alpha, sabr_beta, sabr_rho, sabr_nu
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
                )
            """, metrics.timestamp, metrics.spot_price, metrics.vix, metrics.ivp,
                metrics.realized_vol_7d, metrics.garch_vol_7d, metrics.iv_rv_spread,
                metrics.pcr, metrics.max_pain, metrics.event_risk_score, metrics.regime.value,
                metrics.term_structure_slope, metrics.volatility_skew,
                metrics.sabr_alpha, metrics.sabr_beta, metrics.sabr_rho, metrics.sabr_nu)
        except Exception as e:
            logger.error(f"Failed to save market analytics: {e}")

    async def save_trade(self, trade: MultiLegTrade):
        if not self.pool:
            return
        try:
            await self.pool.execute("""
                INSERT INTO trades (
                    id, strategy_type, net_premium_per_share, entry_time, lots, status,
                    expiry_date, expiry_type, capital_bucket, max_loss_per_lot,
                    max_profit_per_lot, breakeven_lower, breakeven_upper,
                    trade_delta, trade_gamma, trade_theta, trade_vega,
                    transaction_costs, exit_reason, exit_time
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
                )
            """, trade.id, trade.strategy_type.value, trade.net_premium_per_share,
                trade.entry_time, trade.lots, trade.status.value,
                trade.expiry_date, trade.expiry_type.value, trade.capital_bucket.value,
                trade.max_loss_per_lot, trade.max_profit_per_lot,
                trade.breakeven_lower, trade.breakeven_upper,
                trade.trade_delta, trade.trade_gamma, trade.trade_theta, trade.trade_vega,
                trade.transaction_costs,
                trade.exit_reason.value if trade.exit_reason else None,
                trade.exit_time)
        except Exception as e:
            logger.error(f"Failed to save trade: {e}")

    async def save_daily_state(self, daily_pnl: float, max_equity: float,
                               cycle_count: int, total_trades: int):
        if not self.pool:
            return
        try:
            await self.pool.execute("""
                INSERT INTO daily_state (date, daily_pnl, max_equity, cycle_count, total_trades)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (date) DO UPDATE SET
                    daily_pnl = EXCLUDED.daily_pnl,
                    max_equity = EXCLUDED.max_equity,
                    cycle_count = EXCLUDED.cycle_count,
                    total_trades = EXCLUDED.total_trades
            """, datetime.now(IST).date(), daily_pnl, max_equity, cycle_count, total_trades)
        except Exception as e:
            logger.error(f"Failed to save daily state: {e}")

    async def save_portfolio_snapshot(self, metrics: PortfolioMetrics):
        if not self.pool:
            return
        try:
            await self.pool.execute("""
                INSERT INTO portfolio_snapshots (
                    timestamp, delta, gamma, theta, vega, unrealized_pnl, margin_used
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, datetime.now(IST), metrics.delta, metrics.gamma, metrics.theta,
                metrics.vega, metrics.unrealized_pnl, metrics.margin_used)
        except Exception as e:
            logger.error(f"Failed to save portfolio snapshot: {e}")

    async def get_trade_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        if not self.pool:
            return []
        try:
            rows = await self.pool.fetch("""
                SELECT * FROM trades
                ORDER BY entry_time DESC
                LIMIT $1
            """, limit)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch trade history: {e}")
            return []

    async def get_latest_market_analytics(self) -> Optional[Dict[str, Any]]:
        if not self.pool:
            return None
        try:
            row = await self.pool.fetchrow("""
                SELECT * FROM market_analytics
                ORDER BY timestamp DESC
                LIMIT 1
            """)
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Failed to fetch latest market analytics: {e}")
            return None
