#!/usr/bin/env python3
"""
VolGuard 20.0 - Configuration (HYBRID V3/V2 FORTRESS)
- V3: Orders, GTT, History (Verified)
- V2: Option Chain, User Margin (Stability)
- REMOVED: AI/CIO
- OPTIMIZED: AWS RDS Singleton
"""
from __future__ import annotations
import os
import pytz
from datetime import time as dtime
from typing import Dict, Tuple, Any
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_core import MultiHostUrl

# ----------------------------------------------------------------
# UPSTOX API MAP (HYBRID V3/V2)
# ----------------------------------------------------------------
UPSTOX_API_ENDPOINTS: Dict[str, str] = {
    # AUTH (V2)
    "authorization_token": "/v2/login/authorization/token",
    
    # ORDERS (V3 - ENABLED)
    "place_order": "/v3/order/place",
    "modify_order": "/v3/order/modify",
    "cancel_order": "/v3/order/cancel",
    "place_multi_order": "/v2/order/multi/place", # V3 Multi-order might not be standard yet, fallback V2
    "cancel_multi_order": "/v2/order/multi/cancel",
    "order_details": "/v3/order/details",
    "retrieve_orders": "/v3/order/retrieve-all",
    
    # GTT (V3 - NEW)
    "place_gtt": "/v3/order/gtt/place",
    "modify_gtt": "/v3/order/gtt/modify",
    "cancel_gtt": "/v3/order/gtt/cancel",
    "retrieve_gtt": "/v3/order/gtt",

    # PORTFOLIO (V2 - Stable)
    "positions": "/v2/portfolio/short-term-positions",
    "holdings": "/v2/portfolio/long-term-holdings",
    
    # USER (V2 - Stable)
    "funds_margin": "/v2/user/get-funds-and-margin",
    
    # MARKET DATA (V2/V3 Mixed)
    "market_quote_ohlc": "/v2/market-quote/ohlc",
    "market_quote_ltp": "/v2/market-quote/ltp",
    "option_chain": "/v2/option/chain", # V2 is more reliable for simple chains
    
    # HISTORY (V3 - ENABLED)
    "historical_candle": "/v3/historical-candle",
    
    # HOLIDAYS (V2)
    "holidays": "/v2/market/holidays",
}

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Generic ---
    ENV: str = Field(default="production")
    PORT: int = Field(default=8000)
    IST: Any = pytz.timezone("Asia/Kolkata")

    # --- Runtime Flags ---
    PAPER_TRADING: bool = Field(default=True)
    SAFETY_MODE: str = Field(default="paper") # Change to 'live' for Real Money

    # --- Database (AWS RDS Optimized) ---
    POSTGRES_SERVER: str = Field(default="db")
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

    # --- Broker Credentials ---
    UPSTOX_API_KEY: str = Field(default="")
    UPSTOX_API_SECRET: str = Field(default="")
    UPSTOX_ACCESS_TOKEN: str = Field(default="")
    REDIRECT_URI: str = Field(default="http://localhost:8000/callback")
    API_BASE_URL: str = "https://api.upstox.com"

    # --- Instrument Universe ---
    UNDERLYING_SYMBOL: str = Field(default="NIFTY")
    LOT_SIZE: int = Field(default=75)

    # --- Capital ---
    ACCOUNT_SIZE: float = Field(default=2_000_000.0)
    MARGIN_REFRESH_SEC: int = Field(default=30)
    
    # --- Freeze Limits ---
    NIFTY_FREEZE_QTY: int = Field(default=1800)
    BANKNIFTY_FREEZE_QTY: int = Field(default=900)

    # --- Position Sizing ---
    MAX_LOTS: int = Field(default=10)
    CAPITAL_ALLOCATION: Dict[str, float] = {
        "WEEKLY": 0.40,
        "MONTHLY": 0.50,
        "INTRADAY": 0.10,
    }

    # --- Risk Parameters ---
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03)
    TAKE_PROFIT_PCT: float = 0.50
    STOP_LOSS_PCT: float = 1.0
    
    WEEKLY_MAX_RISK: float = Field(default=8_000.0)
    MONTHLY_MAX_RISK: float = Field(default=10_000.0)
    INTRADAY_MAX_RISK: float = Field(default=4_000.0)
    
    # Greeks Limits
    MAX_PORTFOLIO_VEGA: float = Field(default=1000.0)
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0)
    MAX_PORTFOLIO_THETA: float = Field(default=-1500.0)
    MAX_PORTFOLIO_GAMMA: float = Field(default=50.0)

    # --- Circuit Breakers ---
    MAX_ERROR_COUNT: int = Field(default=5)
    MAX_SLIPPAGE_PCT: float = Field(default=0.05)
    SMART_BUFFER_PCT: float = Field(default=0.03)

    # --- SABR Model ---
    SABR_BOUNDS: Dict[str, Tuple[float, float]] = {
        "alpha": (0.01, 2.0),
        "beta": (0.1, 1.0),
        "rho": (-0.99, 0.99),
        "nu": (0.01, 5.0),
    }

    # --- Runtime ---
    PERSISTENT_DATA_DIR: str = "./data"
    DASHBOARD_DATA_DIR: str = "dashboard_data"
    TRADING_LOOP_INTERVAL: int = Field(default=5)
    GREEK_VALIDATION: bool = Field(default=True)
    GREEK_REFRESH_SEC: int = Field(default=15)
    GREEK_TOLERANCE_PCT: float = Field(default=15.0)
    
    MARKET_KEY_INDEX: str = Field(default="NSE_INDEX|Nifty 50")
    MARKET_KEY_VIX: str = Field(default="NSE_INDEX|India VIX")
    
    # Timings
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)

settings = Settings()
IST = settings.IST
