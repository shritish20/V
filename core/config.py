import os
import pytz
from datetime import time as dtime
from typing import Dict, Any, List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from pydantic_core import MultiHostUrl
from pathlib import Path

UPSTOX_API_ENDPOINTS = {
    "authorization_token": "/v2/login/authorization/token",
    "logout": "/v2/logout",
    "user_profile": "/v2/user/profile",
    "user_funds": "/v2/user/get-funds-and-margin",
    "place_order": "/v2/order/place",
    "place_multi_order": "/v2/order/multi/place",
    "modify_order": "/v2/order/modify",
    "cancel_order": "/v2/order/cancel",
    "order_details": "/v2/order/details",
    "order_book": "/v2/order/retrieve-all",
    "trades": "/v2/order/trades",
    "gtt_place": "/v3/order/gtt/place",
    "gtt_cancel": "/v3/order/gtt/cancel",
    "market_quote": "/v2/market-quote/quotes",
    "option_chain": "/v2/option/chain",
    "option_greek": "/v3/market-quote/option-greek",
    "margin": "/v2/charges/margin",
    "ws_auth": "/v2/feed/market-data-feed/authorize"
}

class Settings(BaseSettings):
    ENV: str = Field(default="production", env="ENV")
    PORT: int = Field(default=8000, env="PORT")
    IST: pytz.timezone = pytz.timezone('Asia/Kolkata')

    SAFETY_MODE: str = Field(default="paper", env="SAFETY_MODE")
    PAPER_TRADING: bool = Field(default=True, env="PAPER_TRADING")
    ENABLE_LIVE_TRADING: bool = Field(default=False, env="ENABLE_LIVE_TRADING")

    POSTGRES_SERVER: str = Field(default="postgres", env="POSTGRES_SERVER")
    POSTGRES_USER: str = Field(default="volguard_user", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="secure_trading_password", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field(default="volguard_db", env="POSTGRES_DB")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")

    @property
    def DATABASE_URL(self) -> str:
        return str(MultiHostUrl.build(
            scheme="postgresql+asyncpg",
            username=self.POSTGRES_USER,
            password=self.POSTGRES_PASSWORD,
            host=self.POSTGRES_SERVER,
            port=self.POSTGRES_PORT,
            path=self.POSTGRES_DB
        ))

    UPSTOX_ACCESS_TOKEN: str = Field(..., env="UPSTOX_ACCESS_TOKEN")
    API_BASE_V2: str = "https://api-v2.upstox.com"
    API_BASE_V3: str = "https://api-v2.upstox.com"
    WS_BASE_URL: str = "wss://ws-api.upstox.com"

    ACCOUNT_SIZE: float = Field(default=2_000_000.0, env="ACCOUNT_SIZE")
    LOT_SIZE: int = Field(default=75, env="LOT_SIZE")

    CAPITAL_ALLOCATION: Dict[str, float] = {
        "weekly_expiries": 0.4,
        "monthly_expiries": 0.5,
        "intraday_adjustments": 0.1
    }

    @field_validator('CAPITAL_ALLOCATION')
    @classmethod
    def validate_capital_allocation(cls, v):
        total = sum(v.values())
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Capital allocation must sum to 100%, got {total * 100:.1f}%")
        return v

    WEEKLY_MAX_RISK: float = Field(default=8_000.0, env="WEEKLY_MAX_RISK")
    MONTHLY_MAX_RISK: float = Field(default=10_000.0, env="MONTHLY_MAX_RISK")
    INTRADAY_MAX_RISK: float = Field(default=4_000.0, env="INTRADAY_MAX_RISK")

    MAX_PORTFOLIO_VEGA: float = Field(default=1_000.0, env="MAX_VEGA")
    MAX_PORTFOLIO_DELTA: float = Field(default=200.0, env="MAX_DELTA")
    MAX_PORTFOLIO_THETA: float = Field(default=-1_000.0, env="MAX_THETA")
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03, env="DAILY_LOSS_LIMIT_PCT")
    MAX_SLIPPAGE_PERCENT: float = Field(default=0.02, env="MAX_SLIPPAGE_PERCENT")
    PROFIT_TARGET_PCT: float = Field(default=0.35, env="PROFIT_TARGET_PCT")
    STOP_LOSS_MULTIPLE: float = Field(default=2.0, env="STOP_LOSS_MULTIPLE")

    TRADING_DAYS: int = 252
    RISK_FREE_RATE: float = 0.05
    BROKERAGE_PER_ORDER: float = 20.0
    STT_RATE: float = 0.0005
    GST_RATE: float = 0.18
    EXCHANGE_CHARGES: float = 0.00005
    STAMP_DUTY: float = 0.00003

    MARKET_KEY_INDEX: str = Field(default="NSE_INDEX|Nifty 50", env="MARKET_KEY_INDEX")
    MARKET_KEY_VIX: str = "NSE_INDEX|India VIX"
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_START: dtime = dtime(9, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)
    EXPIRY_FLAT_TIME: dtime = dtime(14, 30)
    WEEKLY_EXPIRY_DAYS: int = 7
    MONTHLY_EXPIRY_DAYS: int = 30

    DASHBOARD_DATA_URLS: Dict[str, str] = {
        "nifty_hist": "https://raw.githubusercontent.com/shritish20/VolGuard/main/nifty_50.csv",
        "ivp_data": "https://raw.githubusercontent.com/shritish20/VolGuard/main/ivp.csv",
        "vix_history": "https://raw.githubusercontent.com/shritish20/VolGuard/main/atmiv.csv",
        "events_calendar": "https://raw.githubusercontent.com/shritish20/VolGuard/main/events_calendar.csv",
        "upcoming_events": "https://raw.githubusercontent.com/shritish20/VolGuard/main/upcoming_events.csv"
    }

    PERSISTENT_DATA_DIR: str = Field(default="./data", env="PERSISTENT_DATA_DIR")
    DASHBOARD_DATA_DIR: str = "dashboard_data"
    DASHBOARD_UPDATE_INTERVAL: int = 60

    ENABLE_3D_VISUALIZATION: bool = Field(default=True, env="ENABLE_3D_VISUALIZATION")
    ENABLE_GTT_ORDERS: bool = Field(default=False, env="ENABLE_GTT_ORDERS")
    ENABLE_ML_PREDICTIONS: bool = Field(default=False, env="ENABLE_ML_PREDICTIONS")
    ENABLE_ADVANCED_GREEKS: bool = Field(default=True, env="ENABLE_ADVANCED_GREEKS")

    SABR_BOUNDS: Dict[str, tuple] = {
        'alpha': (0.05, 0.8),
        'beta': (0.1, 0.9),
        'rho': (-0.95, 0.95),
        'nu': (0.05, 0.8)
    }

    @field_validator('ACCOUNT_SIZE')
    @classmethod
    def validate_account_size(cls, v):
        if v < 100_000:
            raise ValueError("Account size must be at least ₹1,00,000")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"

settings = Settings()
DAILY_LOSS_LIMIT = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT

os.makedirs(settings.PERSISTENT_DATA_DIR, exist_ok=True)
os.makedirs(settings.DASHBOARD_DATA_DIR, exist_ok=True)

def get_full_url(endpoint_key: str) -> str:
    if endpoint_key not in UPSTOX_API_ENDPOINTS:
        raise ValueError(f"Unknown API endpoint: {endpoint_key}")
    endpoint = UPSTOX_API_ENDPOINTS[endpoint_key]
    base_url = settings.API_BASE_V3 if endpoint_key in {"gtt_place", "gtt_cancel", "option_greek"} else settings.API_BASE_V2
    return f"{base_url}{endpoint}"
