import os
import pytz 
from datetime import time as dtime

# ============================================================
# CONFIGURATION - VOLGUARD 14.00 (Ironclad Edition)
# ============================================================

ENV = os.getenv("ENV", "production").lower()
IST = pytz.timezone("Asia/Kolkata")

# APIs
API_BASE_V2 = "https://api.upstox.com/v2"
API_BASE_V3 = "https://api.upstox.com/v3"
WS_BASE_URL = "wss://api-v2.upstox.com/feed/market-data-feed"

# 🔑 Credentials
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "")
PAPER_TRADING = os.getenv("PAPER_TRADING", "True").lower() in ('true', '1', 't')
MARKET_KEY_INDEX = os.getenv("MARKET_KEY_INDEX", "NSE_INDEX|Nifty 50")

# Enhanced Environment Validation
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

# URLs for Enhanced Analytics
XG_BOOST_MODEL_URL = os.getenv("XG_BOOST_MODEL_URL", "https://raw.githubusercontent.com/shritish20/VolGuard-Pro/main/xgb_vol_model_v2.pkl")
NIFTY_HIST_URL = os.getenv("NIFTY_HIST_URL", "https://raw.githubusercontent.com/shritish20/VolGuard/main/nifty_50.csv")
IVP_HIST_URL = os.getenv("IVP_HIST_URL", "https://raw.githubusercontent.com/shritish20/VolGuard/main/ivp.csv")
UPCOMING_EVENTS_URL = os.getenv("UPCOMING_EVENTS_URL", "https://raw.githubusercontent.com/shritish20/VolGuard/main/upcoming_events.csv")
VIX_HISTORY_URL = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/atmiv.csv"
EVENTS_CALENDAR_URL = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/events_calendar.csv"

# 🛡️ Enhanced Risk Management with bounds
ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "2000000.0"))
LOT_SIZE = int(os.getenv("LOT_SIZE", "75"))
SYSTEMATIC_MAX_RISK_PERCENT = min(0.05, float(os.getenv("MAX_RISK_PERCENT", "0.01")))
MAX_PORTFOLIO_VEGA = float(os.getenv("MAX_VEGA", "1000.0"))
MAX_PORTFOLIO_DELTA = float(os.getenv("MAX_DELTA", "200.0"))
DAILY_LOSS_LIMIT = ACCOUNT_SIZE * min(0.05, float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.03")))
MAX_SLIPPAGE_PERCENT = float(os.getenv("MAX_SLIPPAGE_PERCENT", "0.02"))
PROFIT_TARGET_PCT = float(os.getenv("PROFIT_TARGET_PCT", "0.35"))
STOP_LOSS_MULTIPLE = float(os.getenv("STOP_LOSS_MULTIPLE", "2.0"))
ROLLBACK_SLIPPAGE_PERCENT = float(os.getenv("ROLLBACK_SLIPPAGE_PERCENT", "0.015")) 

# 📈 Trading Constants
TRADING_DAYS = 252
RISK_FREE_RATE = 0.05
BROKERAGE_PER_ORDER = 20.0
STT_RATE = 0.0005
GST_RATE = 0.18
EXCHANGE_CHARGES = 0.00005
STAMP_DUTY = 0.00003

# 📁 Files & Database - RENDER COMPATIBLE
PERSISTENT_DATA_DIR = os.getenv("PERSISTENT_DATA_DIR", "/tmp/data")

DB_FILE = os.path.join(PERSISTENT_DATA_DIR, "volguard_14.db")
TRADE_LOG_FILE = os.path.join(PERSISTENT_DATA_DIR, "volguard_14_log.txt")
JOURNAL_FILE = os.path.join(PERSISTENT_DATA_DIR, "volguard_14_journal.csv")

# 📊 Market Configuration
MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)
SAFE_TRADE_START = dtime(9, 30)
SAFE_TRADE_END = dtime(15, 15)
EXPIRY_FLAT_TIME = dtime(14, 30)

# SABR Model Bounds (CRITICAL FIX)
SABR_BOUNDS = {
    'alpha': (0.05, 0.8),
    'beta': (0.1, 0.9), 
    'rho': (-0.95, 0.95),
    'nu': (0.05, 0.8)
}

# 🗓️ Holidays
MARKET_HOLIDAYS_2025 = [
    "2025-01-26", "2025-03-07", "2025-03-25", "2025-04-11", "2025-04-14", "2025-04-17",
    "2025-05-01", "2025-06-26", "2025-08-15", "2025-09-05", "2025-10-02", "2025-10-22",
    "2025-11-04", "2025-11-14", "2025-12-25"
]
