import os
import pytz
from datetime import time as dtime
from typing import Dict, Any, Tuple
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from pydantic_core import MultiHostUrl

# UPSTOX API MAP (Verified against Schema)
UPSTOX_API_ENDPOINTS = {
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
    "market_quote_ohlc": "/v2/market-quote/ohlc", # For Live Stitch
    "option_chain": "/v2/option/chain",
    "option_greek": "/v3/market-quote/option-greek",
    # Funds & Portfolio
    "funds_margin": "/v2/user/get-funds-and-margin",
    "positions": "/v2/portfolio/short-term-positions",
    "margin_calc": "/v2/charges/margin",
    # Accounting & Holidays
    "profit_loss_charges": "/v2/trade/profit-loss/charges",
    "holidays": "/v2/market/holidays"
}

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = Field(default="production", env="ENV")
    PORT: int = Field(default=8000, env="PORT")
    IST: Any = pytz.timezone("Asia/Kolkata")

    # FLAGS
    PAPER_TRADING: bool = Field(default=True, env="PAPER_TRADING")
    SAFETY_MODE: str = Field(default="paper", env="SAFETY_MODE")

    # DATABASE
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

    # UPSTOX API
    UPSTOX_ACCESS_TOKEN: str = Field(..., env="UPSTOX_ACCESS_TOKEN")
    API_BASE_URL: str = "https://api-v2.upstox.com"

    # AI CONFIGURATION
    GEMINI_API_KEY: str = Field(default="", env="GEMINI_API_KEY")

    # CAPITAL & RISK
    ACCOUNT_SIZE: float = Field(default=2_000_000.0, env="ACCOUNT_SIZE")
    LOT_SIZE: int = Field(default=75, env="LOT_SIZE")
    MAX_LOTS: int = Field(default=10, env="MAX_LOTS")
    
    # Freeze Limits
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

    # Risk Matrix
    WEEKLY_MAX_RISK: float = Field(default=8000.0, env="WEEKLY_MAX_RISK")
    MONTHLY_MAX_RISK: float = Field(default=10000.0, env="MONTHLY_MAX_RISK")
    INTRADAY_MAX_RISK: float = Field(default=4000.0, env="INTRADAY_MAX_RISK")

    # Greeks Limits
    MAX_PORTFOLIO_VEGA: float = Field(default=1000.0, env="MAX_VEGA")
    MAX_PORTFOLIO_DELTA: float = Field(default=300.0, env="MAX_DELTA")
    MAX_PORTFOLIO_THETA: float = Field(default=-1500.0, env="MAX_THETA")
    MAX_PORTFOLIO_GAMMA: float = Field(default=50.0, env="MAX_GAMMA")
    MAX_ERROR_COUNT: int = 5

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
    GREEK_VALIDATION: bool = True
    GREEK_REFRESH_SEC: int = 15
    GREEK_TOLERANCE_PCT: float = 15.0

    # Market Keys & Symbols
    # NEW: Added for Kimi's Hardened Engine Compatibility
    UNDERLYING_SYMBOL: str = Field(default="NIFTY", env="UNDERLYING_SYMBOL")
    
    MARKET_KEY_INDEX: str = "NSE_INDEX|Nifty 50"
    MARKET_KEY_VIX: str = "NSE_INDEX|India VIX"
    MARKET_OPEN_TIME: dtime = dtime(9, 15)
    MARKET_CLOSE_TIME: dtime = dtime(15, 30)
    SAFE_TRADE_END: dtime = dtime(15, 15)

settings = Settings()
IST = settings.IST
