import os
import pytz
from datetime import time as dtime
from typing import Dict, Any, Tuple
from pydantic_settings import BaseSettings
from pydantic import Field
from pydantic_core import MultiHostUrl

# --- UPSTOX API MAP (Verified against OpenAPI 3.1.0 Spec) ---
UPSTOX_API_ENDPOINTS = {
    # Auth
    "authorization_token": "/v2/login/authorization/token",
    
    # Orders (V2 for Batching, V3 for GTT)
    "place_order": "/v2/order/place",
    "place_multi_order": "/v2/order/multi/place", # Spec Source: 1669
    "modify_order": "/v2/order/modify",
    "cancel_order": "/v2/order/cancel",
    "order_details": "/v2/order/details",
    "retrieve_orders": "/v2/order/retrieve-all",
    
    # GTT (Must be V3 per Spec Source: 1669)
    "place_gtt": "/v3/order/gtt/place",
    "modify_gtt": "/v3/order/gtt/modify",
    "cancel_gtt": "/v3/order/gtt/cancel",
    "gtt_details": "/v3/order/gtt",
    
    # Market Data
    "market_quote": "/v2/market-quote/quotes",
    "option_chain": "/v2/option/chain",           # Spec Source: 1757 (Includes Greeks)
    "option_greek": "/v3/market-quote/option-greek", # Spec Source: 1676
    
    # Funds & Portfolio
    "funds_margin": "/v2/user/get-funds-and-margin",
    "positions": "/v2/portfolio/short-term-positions",
    "margin_calc": "/v2/charges/margin"           # Spec Source: 1675
}

class Settings(BaseSettings):
    ENV: str = Field(default="production", env="ENV")
    PORT: int = Field(default=8000, env="PORT")
    IST: Any = pytz.timezone("Asia/Kolkata")

    # --- FLAGS ---
    PAPER_TRADING: bool = Field(default=True, env="PAPER_TRADING")
    SAFETY_MODE: str = Field(default="paper", env="SAFETY_MODE")

    # --- DATABASE ---
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

    # --- UPSTOX ---
    UPSTOX_ACCESS_TOKEN: str = Field(..., env="UPSTOX_ACCESS_TOKEN")
    API_BASE_V2: str = "https://api.upstox.com/v2"
    API_BASE_V3: str = "https://api-v2.upstox.com/v3"
    
    # --- CAPITAL & RISK ---
    ACCOUNT_SIZE: float = Field(default=2_000_000.0, env="ACCOUNT_SIZE")
    LOT_SIZE: int = Field(default=75, env="LOT_SIZE")
    MAX_LOTS: int = Field(default=10, env="MAX_LOTS")
    
    # Freeze Limits (NSE Standards)
    NIFTY_FREEZE_QTY: int = Field(default=1800, env="NIFTY_FREEZE_QTY")
    BANKNIFTY_FREEZE_QTY: int = Field(default=900, env="BANKNIFTY_FREEZE_QTY")

    CAPITAL_ALLOCATION: Dict[str, float] = {
        "weekly_expiries": 0.40,
        "monthly_expiries": 0.50,
        "intraday_adjustments": 0.10,
    }

    # Loss Limits
    DAILY_LOSS_LIMIT_PCT: float = Field(default=0.03, env="DAILY_LOSS_LIMIT_PCT")
    TAKE_PROFIT_PCT: float = 0.50     
    STOP_LOSS_PCT: float = 1.0        

    # Greeks Limits (Portfolio Level)
    MAX_PORTFOLIO_VEGA: float = Field(default=1000.0, env="MAX_VEGA")
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0, env="MAX_DELTA")
    MAX_PORTFOLIO_THETA: float = Field(default=-1500.0, env="MAX_THETA")
    MAX_PORTFOLIO_GAMMA: float = Field(default=50.0, env="MAX_GAMMA")

    # SABR
    SABR_BOUNDS: Dict[str, Tuple[float, float]] = {
        'alpha': (0.01, 2.0),
        'beta': (0.1, 1.0),
        'rho': (-0.99, 0.99),
        'nu': (0.01, 5.0)
    }

    # Runtime
    PERSISTENT_DATA_DIR: str = "./data"
    DASHBOARD_DATA_DIR: str = "dashboard_data"
    TRADING_LOOP_INTERVAL: int = 5
    
    MARKET_KEY_INDEX: str = "NSE_INDEX|Nifty 50"
    MARKET_KEY_VIX: str = "NSE_INDEX|India VIX"
    
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
IST = settings.IST
