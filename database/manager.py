import sqlite3
from datetime import datetime
from typing import Tuple, Optional
from core.models import MultiLegTrade, PortfolioMetrics, AdvancedMetrics
from core.config import IST, DB_FILE, ACCOUNT_SIZE

class HybridDatabaseManager:
    """ACID-compliant storage with advanced analytics (SQLite, as requested)"""
    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Trades table
            cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_type TEXT,
                status TEXT,
                entry_time TEXT,
                exit_time TEXT,
                lots INTEGER,
                net_premium REAL,
                pnl REAL,
                exit_reason TEXT,
                expiry_date TEXT,
                trade_vega REAL,
                trade_delta REAL,
                max_loss REAL
            )''')
            # Daily state
            cursor.execute('''CREATE TABLE IF NOT EXISTS daily_state (
                date TEXT PRIMARY KEY,
                daily_pnl REAL,
                max_equity REAL,
                cycle_count INTEGER,
                total_trades INTEGER
            )''')
            # Portfolio snapshots
            cursor.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                timestamp TEXT PRIMARY KEY,
                total_pnl REAL,
                total_delta REAL,
                total_gamma REAL,
                total_theta REAL,
                total_vega REAL,
                open_trades INTEGER
            )''')
            # Market analytics
            cursor.execute('''CREATE TABLE IF NOT EXISTS market_analytics (
                timestamp TEXT PRIMARY KEY,
                spot_price REAL,
                vix REAL,
                ivp REAL,
                regime TEXT,
                event_risk REAL
            )''')
            conn.commit()

    def save_trade(self, trade: MultiLegTrade) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO trades (strategy_type, status, entry_time, lots, net_premium, expiry_date, trade_vega, trade_delta, max_loss, pnl) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                           (trade.strategy_type, trade.status.value, trade.entry_time.isoformat(), trade.lots, trade.net_premium_per_share, trade.expiry_date, trade.trade_vega, trade.trade_delta, trade.max_loss_per_lot, 0.0))
            return cursor.lastrowid

    def update_trade_close(self, trade_id: int, pnl: float, reason: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''UPDATE trades SET status=?, pnl=?, exit_reason=?, exit_time=?
                         WHERE id=?''', 
                         ("CLOSED", pnl, reason, datetime.now(IST).isoformat(), trade_id))

    def save_portfolio_snapshot(self, metrics: PortfolioMetrics):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT INTO portfolio_snapshots (timestamp, total_pnl, total_delta, total_gamma, total_theta, total_vega, open_trades) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                         (metrics.timestamp.isoformat(), metrics.total_pnl, metrics.total_delta, metrics.total_gamma, metrics.total_theta, metrics.total_vega, metrics.open_trades))

    def save_market_analytics(self, metrics: AdvancedMetrics):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT INTO market_analytics (timestamp, spot_price, vix, ivp, regime, event_risk) 
                         VALUES (?, ?, ?, ?, ?, ?)''', 
                         (metrics.timestamp.isoformat(), metrics.spot_price, metrics.vix, metrics.ivp, metrics.regime.value, metrics.event_risk_score))

    def get_daily_state(self) -> Tuple[float, float, int, int]:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT daily_pnl, max_equity, cycle_count, total_trades FROM daily_state WHERE date=?", 
                (today,)
            )
            row = cursor.fetchone()
            if row:
                return row[0], row[1], row[2], row[3]
            return 0.0, ACCOUNT_SIZE, 0, 0

    def save_daily_state(self, daily_pnl: float, max_equity: float, cycle_count: int, total_trades: int):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT OR REPLACE INTO daily_state (date, daily_pnl, max_equity, cycle_count, total_trades) 
                         VALUES (?, ?, ?, ?, ?)''', 
                         (today, daily_pnl, max_equity, cycle_count, total_trades))
