#!/usr/bin/env python3
"""
VolGuard 20.0 – Configuration (Hardened & Test-Ready)
- Corrected Variable Names for Integration
- Default Values for Safe Testing
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
# Upstox route map
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
# Base settings
# ---------------------------------------------------------------------------
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---------------------------------------------------------------------
    # Generic
    # ---------------------------------------------------------------------
    ENV: str = Field(default="production") # validation_alias="ENV" in v2
    PORT: int = Field(default=8000)
    IST: Any = pytz.timezone("Asia/Kolkata")

    # ---------------------------------------------------------------------
    # Runtime flags
    # ---------------------------------------------------------------------
    PAPER_TRADING: bool = Field(default=True)
    SAFETY_MODE: str = Field(default="paper")  # paper / live

    # ---------------------------------------------------------------------
    # Database
    # ---------------------------------------------------------------------
    POSTGRES_SERVER: str = Field(default="localhost")
    POSTGRES_USER: str = Field(default="volguard_user")
    POSTGRES_PASSWORD: str = Field(default="secure_trading_password")
    POSTGRES_DB: str = Field(default="volguard_db")
    POSTGRES_PORT: int = Field(default=5432)

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
    UPSTOX_ACCESS_TOKEN: str = Field(default="")
    API_BASE_URL: str = "https://api-v2.upstox.com"

    # ---------------------------------------------------------------------
    # AI
    # ---------------------------------------------------------------------
    GEMINI_API_KEY: str = Field(default="")

    # ---------------------------------------------------------------------
    # Instrument universe
    # ---------------------------------------------------------------------
    UNDERLYING_SYMBOL: str = Field(default="NIFTY")
    LOT_SIZE: int = Field(default=75)

    # ---------------------------------------------------------------------
    # Capital
    # ---------------------------------------------------------------------
    # RENAMED from ACCOUNT_SIZE_FALLBACK to ACCOUNT_SIZE to fix AttributeError
    ACCOUNT_SIZE: float = Field(default=2_000_000.0)

    # Refresh interval seconds
    MARGIN_REFRESH_SEC: int = Field(default=30)

    # ---------------------------------------------------------------------
    # Freeze limits
    # ---------------------------------------------------------------------
    NIFTY_FREEZE_QTY: int = Field(default=1800)
    BANKNIFTY_FREEZE_QTY: int = Field(default=900)

    # ---------------------------------------------------------------------
    # Position sizing
    # ---------------------------------------------------------------------
    MAX_LOTS: int = Field(default=10)
    CAPITAL_ALLOCATION: Dict[str, float] = {
        "WEEKLY": 0.40,
        "MONTHLY": 0.50,
        "INTRADAY": 0.10,
    }

    # ---------------------------------------------------------------------
    # Risk
    # ---------------------------------------------------------------------
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03)
    TAKE_PROFIT_PCT: float = 0.50
    STOP_LOSS_PCT: float = 1.0

    WEEKLY_MAX_RISK: float = Field(default=8_000.0)
    MONTHLY_MAX_RISK: float = Field(default=10_000.0)
    INTRADAY_MAX_RISK: float = Field(default=4_000.0)

    # Greeks limits
    MAX_PORTFOLIO_VEGA: float = Field(default=1_000.0)
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0)
    MAX_PORTFOLIO_THETA: float = Field(default=-1_500.0)
    MAX_PORTFOLIO_GAMMA: float = Field(default=50.0)

    # ---------------------------------------------------------------------
    # Circuit-breaker knobs
    # ---------------------------------------------------------------------
    MAX_ERROR_COUNT: int = Field(default=5)
    MAX_SLIPPAGE_PCT: float = Field(default=0.05)
    SMART_BUFFER_PCT: float = Field(default=0.03)

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
    TRADING_LOOP_INTERVAL: int = Field(default=5)

    GREEK_VALIDATION: bool = Field(default=True)
    GREEK_REFRESH_SEC: int = Field(default=15)
    GREEK_TOLERANCE_PCT: float = Field(default=15.0)

    MARKET_KEY_INDEX: str = Field(default="NSE_INDEX|Nifty 50")
    MARKET_KEY_VIX: str = Field(default="NSE_INDEX|India VIX")
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)


# -------------------------------------------------------------------------
# singleton
# -------------------------------------------------------------------------
settings = Settings()
IST = settings.IST
