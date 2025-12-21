#!/usr/bin/env python3
"""
VolGuard 20.0 – Production-Grade Configuration (Corrected)
- Real-time broker margin as ACCOUNT_SIZE
- Corrected Freeze Limits (Shares)
- Intraday draw-down on *ledger* balance
- Env-driven circuit-breaker knobs
- Secrets never leak into logs
"""
from __future__ import annotations

import os
import pytz
from datetime import time as dtime
from typing import Dict, Tuple, Any
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_core import MultiHostUrl


# ---------------------------------------------------------------------------
# Upstox route map – unchanged
# ---------------------------------------------------------------------------
UPSTOX_API_ENDPOINTS: Dict[str, str] = {
    # Auth
    "authorization_token": "/v2/login/authorization/token",
    # Orders
    "place_order": "/v2/order/place",
    "place_multi_order": "/v2/order/multi/place",
    "modify_order": "/v2/order/modify",
    "cancel_order": "/v2/order/cancel",
    "order_details": "/v2/order/details",
    "retrieve_orders": "/v2/order/retrieve-all",
    # GTT (V3)
    "place_gtt": "/v3/order/gtt/place",
    "modify_gtt": "/v3/order/gtt/modify",
    "cancel_gtt": "/v3/order/gtt/cancel",
    "gtt_details": "/v3/order/gtt",
    # Market Data
    "market_quote": "/v2/market-quote/quotes",
    "market_quote_ohlc": "/v2/market-quote/ohlc",
    "option_chain": "/v2/option/chain",
    "option_greek": "/v3/market-quote/option-greek",
    # Funds & Portfolio
    "funds_margin": "/v2/user/get-funds-and-margin",
    "positions": "/v2/portfolio/short-term-positions",
    "margin_calc": "/v2/charges/margin",
    # Accounting & Holidays
    "profit_loss_charges": "/v2/trade/profit-loss/charges",
    "holidays": "/v2/market/holidays",
}


# ---------------------------------------------------------------------------
# Base settings – with env override
# ---------------------------------------------------------------------------
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---------------------------------------------------------------------
    # Generic
    # ---------------------------------------------------------------------
    ENV: str = Field(default="production", env="ENV")
    PORT: int = Field(default=8000, env="PORT")
    IST: Any = pytz.timezone("Asia/Kolkata")

    # ---------------------------------------------------------------------
    # Runtime flags
    # ---------------------------------------------------------------------
    PAPER_TRADING: bool = Field(default=True, env="PAPER_TRADING")
    SAFETY_MODE: str = Field(default="paper", env="SAFETY_MODE")  # paper / live

    # ---------------------------------------------------------------------
    # Database
    # ---------------------------------------------------------------------
    POSTGRES_SERVER: str = Field(default="postgres", env="POSTGRES_SERVER")
    POSTGRES_USER: str = Field(default="volguard_user", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="secure_trading_password", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field(default="volguard_db", env="POSTGRES_DB")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")

    @computed_field
    @property
    def DATABASE_URL(self) -> str:
        return str(
            MultiHostUrl.build(
                scheme="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD,
                host=self.POSTGRES_SERVER,
                port=self.POSTGRES_PORT,
                path=self.POSTGRES_DB,
            )
        )

    # ---------------------------------------------------------------------
    # Broker
    # ---------------------------------------------------------------------
    UPSTOX_ACCESS_TOKEN: str = Field(..., env="UPSTOX_ACCESS_TOKEN")
    API_BASE_URL: str = "https://api-v2.upstox.com"

    # ---------------------------------------------------------------------
    # AI
    # ---------------------------------------------------------------------
    GEMINI_API_KEY: str = Field(default="", env="GEMINI_API_KEY")

    # ---------------------------------------------------------------------
    # Instrument universe
    # ---------------------------------------------------------------------
    UNDERLYING_SYMBOL: str = Field(default="NIFTY", env="UNDERLYING_SYMBOL")
    LOT_SIZE: int = Field(default=75, env="LOT_SIZE")

    # ---------------------------------------------------------------------
    # Capital – **real margin** refreshed at runtime
    # ---------------------------------------------------------------------
    # Fallback only – real value is fetched from broker
    ACCOUNT_SIZE_FALLBACK: float = Field(default=2_000_000.0, env="ACCOUNT_SIZE")

    # Refresh interval seconds
    MARGIN_REFRESH_SEC: int = Field(default=30, env="MARGIN_REFRESH_SEC")

    # ---------------------------------------------------------------------
    # Freeze limits – Fixed Logic
    # ---------------------------------------------------------------------
    # Nifty Freeze Limit is 1800 SHARES (Quantity), not contracts.
    # We map this to the specific key used by LiveOrderExecutor
    NIFTY_FREEZE_QTY: int = Field(default=1800, env="NIFTY_FREEZE_QTY")
    BANKNIFTY_FREEZE_QTY: int = Field(default=900, env="BANKNIFTY_FREEZE_QTY")

    # ---------------------------------------------------------------------
    # Position sizing
    # ---------------------------------------------------------------------
    MAX_LOTS: int = Field(default=10, env="MAX_LOTS")
    CAPITAL_ALLOCATION: Dict[str, float] = {
        "weekly_expiries": 0.40,
        "monthly_expiries": 0.50,
        "intraday_adjustments": 0.10,
    }

    # ---------------------------------------------------------------------
    # Risk – draw-down on **ledger balance**
    # ---------------------------------------------------------------------
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03, env="DAILY_LOSS_LIMIT_PCT")
    TAKE_PROFIT_PCT: float = 0.50
    STOP_LOSS_PCT: float = 1.0

    WEEKLY_MAX_RISK: float = Field(default=8_000.0, env="WEEKLY_MAX_RISK")
    MONTHLY_MAX_RISK: float = Field(default=10_000.0, env="MONTHLY_MAX_RISK")
    INTRADAY_MAX_RISK: float = Field(default=4_000.0, env="INTRADAY_MAX_RISK")

    # Greeks limits
    MAX_PORTFOLIO_VEGA: float = Field(default=1_000.0, env="MAX_VEGA")
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0, env="MAX_DELTA")
    MAX_PORTFOLIO_THETA: float = Field(default=-1_500.0, env="MAX_THETA")
    MAX_PORTFOLIO_GAMMA: float = Field(default=50.0, env="MAX_GAMMA")

    # ---------------------------------------------------------------------
    # Circuit-breaker knobs
    # ---------------------------------------------------------------------
    MAX_ERROR_COUNT: int = Field(default=5, env="MAX_ERROR_COUNT")
    MAX_SLIPPAGE_PCT: float = Field(default=0.05, env="MAX_SLIPPAGE_PCT")  # 5 %
    SMART_BUFFER_PCT: float = Field(default=0.03, env="SMART_BUFFER_PCT")  # 3 %

    # ---------------------------------------------------------------------
    # SABR
    # ---------------------------------------------------------------------
    SABR_BOUNDS: Dict[str, Tuple[float, float]] = {
        "alpha": (0.01, 2.0),
        "beta": (0.1, 1.0),
        "rho": (-0.99, 0.99),
        "nu": (0.01, 5.0),
    }

    # ---------------------------------------------------------------------
    # Runtime
    # ---------------------------------------------------------------------
    PERSISTENT_DATA_DIR: str = "./data"
    DASHBOARD_DATA_DIR: str = "dashboard_data"
    TRADING_LOOP_INTERVAL: int = Field(default=5, env="TRADING_LOOP_INTERVAL")

    GREEK_VALIDATION: bool = Field(default=True, env="GREEK_VALIDATION")
    GREEK_REFRESH_SEC: int = Field(default=15, env="GREEK_REFRESH_SEC")
    GREEK_TOLERANCE_PCT: float = Field(default=15.0, env="GREEK_TOLERANCE_PCT")

    MARKET_KEY_INDEX: str = Field(default="NSE_INDEX|Nifty 50", env="MARKET_KEY_INDEX")
    MARKET_KEY_VIX: str = Field(default="NSE_INDEX|India VIX", env="MARKET_KEY_VIX")
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)


# -------------------------------------------------------------------------
# singleton
# -------------------------------------------------------------------------
settings = Settings()
IST = settings.IST
