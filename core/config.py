import os
import pytz
from datetime import time as dtime
from typing import Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from pydantic_core import MultiHostUrl

UPSTOX_API_ENDPOINTS = {
    "authorization_token": "/v2/login/authorization/token",
    "place_order": "/v2/order/place",
    "place_multi_order": "/v2/order/multi/place",
    "modify_order": "/v2/order/modify",
    "cancel_order": "/v2/order/cancel",
    "order_details": "/v2/order/details",
    "market_quote": "/v2/market-quote/quotes",
    "margin": "/v2/charges/margin",
    "ws_auth": "/v2/feed/market-data-feed/authorize",
    "option_greek": "/v3/market-quote/option-greek",
    "user_profile": "/v2/user/profile",
    "positions": "/v2/portfolio/short-term-positions"
}

class Settings(BaseSettings):
    # --- Environment ---
    ENV: str = Field(default="production", env="ENV")
    PORT: int = Field(default=8000, env="PORT")
    IST: Any = pytz.timezone("Asia/Kolkata")

    # --- Safety & Modes ---
    # "paper" / "shadow" / "live"
    SAFETY_MODE: str = Field(default="paper", env="SAFETY_MODE")
    PAPER_TRADING: bool = Field(default=True, env="PAPER_TRADING") # Legacy flag

    # --- Database (PostgreSQL) ---
    POSTGRES_SERVER: str = Field(default="postgres", env="POSTGRES_SERVER")
    POSTGRES_USER: str = Field(default="volguard_user", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="secure_trading_password", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field(default="volguard_db", env="POSTGRES_DB")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")

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

    # --- Upstox API ---
    UPSTOX_ACCESS_TOKEN: str = Field(..., env="UPSTOX_ACCESS_TOKEN")
    API_BASE_V2: str = "https://api.upstox.com/v2"
    API_BASE_V3: str = "https://api-v2.upstox.com/v3"

    # --- Capital Allocation ---
    ACCOUNT_SIZE: float = Field(default=2000000.0, env="ACCOUNT_SIZE")
    LOT_SIZE: int = Field(default=75, env="LOT_SIZE")
    CAPITAL_ALLOCATION: Dict[str, float] = {
        "weekly_expiries": 0.40,
        "monthly_expiries": 0.50,
        "intraday_adjustments": 0.10
    }

    @field_validator('CAPITAL_ALLOCATION')
    @classmethod
    def validate_capital_allocation(cls, v):
        total = sum(v.values())
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Capital allocation must total 100%, got {total*100:.1f}%")
        return v

    # --- Risk Management ---
    WEEKLY_MAX_RISK: float = Field(default=8000.0, env="WEEKLY_MAX_RISK")
    MONTHLY_MAX_RISK: float = Field(default=10000.0, env="MONTHLY_MAX_RISK")
    INTRADAY_MAX_RISK: float = Field(default=4000.0, env="INTRADAY_MAX_RISK")
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03, env="DAILY_LOSS_LIMIT_PCT")
    
    # Portfolio Limits
    MAX_PORTFOLIO_VEGA: float = Field(default=1000.0, env="MAX_VEGA")
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0, env="MAX_PORTFOLIO_DELTA")
    MAX_PORTFOLIO_THETA: float = Field(default=-1000.0, env="MAX_THETA")
    
    MAX_ERROR_COUNT: int = 5

    # Exit Rules
    TAKE_PROFIT_PCT: float = 0.50
    STOP_LOSS_PCT: float = 2.0

    # Runtime / Data
    PERSISTENT_DATA_DIR: str = "./data"
    DASHBOARD_DATA_DIR: str = "dashboard_data"
    TRADING_LOOP_INTERVAL: int = 5
    DASHBOARD_UPDATE_INTERVAL: int = 60
    
    GREEK_VALIDATION: bool = True
    GREEK_REFRESH_SEC: int = 15
    GREEK_TOLERANCE_PCT: float = 15.0

    MARKET_KEY_INDEX: str = "NSE_INDEX|Nifty 50"
    MARKET_KEY_VIX: str = "INDICES|INDIA VIX"

    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)
    
    # SABR Bounds
    SABR_BOUNDS: Dict[str, tuple] = {
        'alpha': (0.05, 0.8),
        'beta': (0.1, 0.9),
        'rho': (-0.95, 0.95),
        'nu': (0.05, 0.8)
    }

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
DAILY_LOSS_LIMIT = settings.ACCOUNT_SIZE * settings.DAILY_LOSS_LIMIT_PCT
IST = settings.IST

def get_full_url(endpoint_key: str) -> str:
    if endpoint_key not in UPSTOX_API_ENDPOINTS:
        raise ValueError(f"Unknown API endpoint: {endpoint_key}")
    endpoint = UPSTOX_API_ENDPOINTS[endpoint_key]
    base_url = settings.API_BASE_V3 if endpoint_key in {"option_greek"} else settings.API_BASE_V2
    return f"{base_url}{endpoint}"
