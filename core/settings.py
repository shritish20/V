import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # AUTH
    UPSTOX_API_KEY: str
    UPSTOX_API_SECRET: str
    UPSTOX_ACCESS_TOKEN: str
    REDIRECT_URI: str = "http://localhost:8000/callback"

    # TRADING CONFIG
    ALGO_TAG: str = "VOLGUARD_PROD"
    MARKET_KEYS: list[str] = ["NSE_INDEX|Nifty 50", "NSE_INDEX|India VIX"]
    ACCOUNT_SIZE: float = 2000000.0  # 20 Lakhs
    
    # LIMITS
    MAX_CAPITAL_PER_TRADE: float = 500000.0
    DAILY_LOSS_LIMIT: float = 50000.0  # Hard stop at 50k loss
    
    # TIMING (Seconds)
    ANALYTICS_INTERVAL: int = 60
    MONITOR_INTERVAL: int = 5

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
