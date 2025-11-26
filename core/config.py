import os
import pytz
from datetime import time as dtime

# ============================================================
# CONFIGURATION - VOLGUARD ULTIMATE HYBRID
# ============================================================

IST = pytz.timezone("Asia/Kolkata")
API_BASE_V2 = "https://api.upstox.com/v2"
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "YOUR_TOKEN_HERE")

LIVE_FLAG = os.getenv("VOLGUARD_LIVE", "0") == "1"
PAPER_TRADING = not LIVE_FLAG or ("YOUR_TOKEN_HERE" in UPSTOX_ACCESS_TOKEN)

# Risk Management
ACCOUNT_SIZE = 500_000.0
LOT_SIZE = 50
SYSTEMATIC_MAX_RISK_PERCENT = 0.01
MAX_PORTFOLIO_VEGA = 1000.0
MAX_PORTFOLIO_DELTA = 200.0
DAILY_LOSS_LIMIT = ACCOUNT_SIZE * 0.03
MAX_SLIPPAGE_PERCENT = 0.02
PROFIT_TARGET_PCT = 0.35
STOP_LOSS_MULTIPLE = 2.0

# Trading Constants
TRADING_DAYS = 252
RISK_FREE_RATE = 0.05
BROKERAGE_PER_ORDER = 20.0
STT_RATE = 0.0005
GST_RATE = 0.18
EXCHANGE_CHARGES = 0.00005
STAMP_DUTY = 0.00003

# Files & Database
DB_FILE = "volguard_hybrid.db"
TRADE_LOG_FILE = "volguard_hybrid_log.txt"
JOURNAL_FILE = "volguard_hybrid_journal.csv"

# Market Configuration
MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)
SAFE_TRADE_START = dtime(9, 30)
SAFE_TRADE_END = dtime(15, 15)
EXPIRY_FLAT_TIME = dtime(14, 30)

# Data Sources
VIX_HISTORY_URL = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/atmiv.csv"
NIFTY_HISTORY_URL = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/nifty_50.csv"
EVENTS_CALENDAR_URL = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/events_calendar.csv"

MARKET_HOLIDAYS_2025 = [
    "2025-01-26", "2025-03-07", "2025-03-25", "2025-04-11",
    "2025-04-14", "2025-04-17", "2025-05-01", "2025-06-26",
    "2025-08-15", "2025-09-05", "2025-10-02", "2025-10-22",
    "2025-11-04", "2025-11-14", "2025-12-25"
]
