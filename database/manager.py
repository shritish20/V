import sqlite3
import asyncio
import os
from datetime import datetime
from typing import Tuple, Optional, List, Dict
from core.models import MultiLegTrade, PortfolioMetrics, AdvancedMetrics, TradeStatus, ExitReason, Order
from core.config import IST, DB_FILE, ACCOUNT_SIZE
import logging

logger = logging.getLogger("VolGuard14")

class HybridDatabaseManager:
    """ACID-compliant storage with state management and audit trail - RENDER COMPATIBLE"""
    
    def __init__(self, db_path: str = None):
        # Use environment variable or default to /tmp/ directory
        if db_path is None:
            db_path = DB_FILE
            
        self.db_path = db_path
        self.conn = None
        self._db_lock = asyncio.Lock()
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self._init_db()

    def _init_db(self):
        """Initialize database with comprehensive schema"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = self.conn.cursor()
            
            # Enhanced trades table
            cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_type TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_time TIMESTAMP NOT NULL,
                exit_time TIMESTAMP,
                lots INTEGER NOT NULL,
                net_premium REAL NOT NULL,
                pnl REAL DEFAULT 0.0,
                exit_reason TEXT,
                expiry_date TEXT NOT NULL,
                trade_vega REAL DEFAULT 0.0,
                trade_delta REAL DEFAULT 0.0,
                max_loss REAL DEFAULT 0.0,
                transaction_costs REAL DEFAULT 0.0,
                basket_order_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # Trade legs table
            cursor.execute('''CREATE TABLE IF NOT EXISTS trade_legs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                instrument_key TEXT NOT NULL,
                strike REAL NOT NULL,
                option_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                current_price REAL NOT NULL,
                delta REAL DEFAULT 0.0,
                gamma REAL DEFAULT 0.0,
                theta REAL DEFAULT 0.0,
                vega REAL DEFAULT 0.0,
                FOREIGN KEY (trade_id) REFERENCES trades (id) ON DELETE CASCADE
            )''')
            
            # Orders table
            cursor.execute('''CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                instrument_key TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                order_type TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                status TEXT NOT NULL,
                product TEXT DEFAULT 'I',
                validity TEXT DEFAULT 'DAY',
                disclosed_quantity INTEGER DEFAULT 0,
                trigger_price REAL DEFAULT 0,
                placed_time TIMESTAMP NOT NULL,
                last_updated TIMESTAMP NOT NULL,
                filled_quantity INTEGER DEFAULT 0,
                average_price REAL DEFAULT 0.0,
                remaining_quantity INTEGER DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                parent_trade_id INTEGER,
                error_message TEXT
            )''')
            
            # Portfolio snapshots for time series analysis
            cursor.execute('''CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                timestamp TIMESTAMP PRIMARY KEY,
                total_pnl REAL NOT NULL,
                total_delta REAL NOT NULL,
                total_vega REAL NOT NULL,
                open_trades INTEGER NOT NULL,
                daily_pnl REAL NOT NULL,
                equity REAL NOT NULL,
                drawdown REAL NOT NULL
            )''')
            
            # Market analytics for regime detection
            cursor.execute('''CREATE TABLE IF NOT EXISTS market_analytics (
                timestamp TIMESTAMP PRIMARY KEY,
                spot_price REAL NOT NULL,
                vix REAL NOT NULL,
                ivp REAL NOT NULL,
                regime TEXT NOT NULL,
                event_risk REAL NOT NULL,
                sabr_alpha REAL,
                sabr_beta REAL,
                sabr_rho REAL,
                sabr_nu REAL,
                realized_vol_7d REAL,
                garch_vol_7d REAL,
                iv_rv_spread REAL,
                pcr REAL,
                max_pain REAL,
                term_structure_slope REAL,
                volatility_skew REAL
            )''')
            
            # Daily state table
            cursor.execute('''CREATE TABLE IF NOT EXISTS daily_state (
                date TEXT PRIMARY KEY,
                daily_pnl REAL DEFAULT 0.0,
                max_equity REAL DEFAULT 0.0,
                cycle_count INTEGER DEFAULT 0,
                total_trades INTEGER DEFAULT 0
            )''')
            
            # Alert log for audit trail
            cursor.execute('''CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                message TEXT NOT NULL,
                severity TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                acknowledged BOOLEAN DEFAULT FALSE
            )''')
            
            # Performance metrics
            cursor.execute('''CREATE TABLE IF NOT EXISTS performance_metrics (
                date TEXT PRIMARY KEY,
                sharpe_ratio REAL,
                max_drawdown REAL,
                win_rate REAL,
                total_pnl REAL,
                daily_pnl REAL,
                total_trades INTEGER
            )''')
            
            # Create indexes for performance
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)''')
            cursor.execute('''CREATE INDEX IF NOT EXISTS idx_trade_legs_trade_id ON trade_legs(trade_id)''')
            
            self.conn.commit()
            logger.info(f"Enhanced database initialized successfully at: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    async def save_trade(self, trade: MultiLegTrade) -> int:
        """Save trade with full audit trail and thread safety"""
        async with self._db_lock:
            try:
                cursor = self.conn.cursor()
                
                # Insert main trade
                cursor.execute('''INSERT INTO trades 
                    (strategy_type, status, entry_time, lots, net_premium, expiry_date, 
                     trade_vega, trade_delta, max_loss, transaction_costs, basket_order_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (trade.strategy_type, trade.status.value, trade.entry_time.isoformat(), 
                     trade.lots, trade.net_premium_per_share, trade.expiry_date,
                     trade.trade_vega, trade.trade_delta, trade.max_loss_per_lot,
                     trade.transaction_costs, trade.basket_order_id))
                
                trade_id = cursor.lastrowid
                
                # Insert trade legs
                for leg in trade.legs:
                    cursor.execute('''INSERT INTO trade_legs 
                        (trade_id, instrument_key, strike, option_type, quantity, 
                         entry_price, current_price, delta, gamma, theta, vega)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (trade_id, leg.instrument_key, leg.strike, leg.option_type, leg.quantity,
                         leg.entry_price, leg.current_price, leg.current_greeks.delta,
                         leg.current_greeks.gamma, leg.current_greeks.theta, leg.current_greeks.vega))
                
                self.conn.commit()
                logger.info(f"Trade {trade_id} saved to database")
                return trade_id
                
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Failed to save trade: {e}")
                raise

    async def update_trade_status(self, trade_id: int, status: TradeStatus, 
                              pnl: float = None, exit_reason: ExitReason = None):
        """Update trade status with comprehensive tracking and thread safety"""
        async with self._db_lock:
            try:
                cursor = self.conn.cursor()
                update_time = datetime.now().isoformat()
                
                if status == TradeStatus.CLOSED:
                    cursor.execute('''UPDATE trades SET status=?, pnl=?, exit_reason=?, exit_time=?, updated_at=?
                                 WHERE id=?''', 
                                 (status.value, pnl, exit_reason.value if exit_reason else None, 
                                  update_time, update_time, trade_id))
                else:
                    cursor.execute('''UPDATE trades SET status=?, updated_at=? WHERE id=?''',
                                 (status.value, update_time, trade_id))
                
                self.conn.commit()
                logger.info(f"Trade {trade_id} status updated to {status.value}")
                
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Failed to update trade status: {e}")
                raise

    def get_active_trades(self) -> List[Dict]:
        """Get all active trades with their legs"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''SELECT * FROM trades WHERE status IN ('OPEN', 'EXTERNAL')''')
            trades = cursor.fetchall()
            
            result = []
            for trade in trades:
                cursor.execute('''SELECT * FROM trade_legs WHERE trade_id=?''', (trade[0],))
                legs = cursor.fetchall()
                result.append({
                    'trade': trade,
                    'legs': legs
                })
            
            return result
        except Exception as e:
            logger.error(f"Failed to get active trades: {e}")
            return []

    async def save_order(self, order: Order):
        """Save order to database with thread safety"""
        async with self._db_lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute('''INSERT OR REPLACE INTO orders 
                    (order_id, instrument_key, quantity, price, order_type, transaction_type,
                     status, product, validity, disclosed_quantity, trigger_price, placed_time,
                     last_updated, filled_quantity, average_price, remaining_quantity,
                     retry_count, parent_trade_id, error_message)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (order.order_id, order.instrument_key, order.quantity, order.price,
                     order.order_type.value, order.transaction_type, order.status.value,
                     order.product, order.validity, order.disclosed_quantity, order.trigger_price,
                     order.placed_time.isoformat(), order.last_updated.isoformat(),
                     order.filled_quantity, order.average_price, order.remaining_quantity,
                     order.retry_count, order.parent_trade_id, order.error_message))
                
                self.conn.commit()
            except Exception as e:
                logger.error(f"Failed to save order: {e}")
                raise

    async def save_portfolio_snapshot(self, metrics: PortfolioMetrics):
        """Save portfolio snapshot for time series analysis with thread safety"""
        async with self._db_lock:
            try:
                self.conn.execute('''INSERT INTO portfolio_snapshots 
                    (timestamp, total_pnl, total_delta, total_vega, open_trades, daily_pnl, equity, drawdown)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (metrics.timestamp.isoformat(), metrics.total_pnl, metrics.total_delta,
                     metrics.total_vega, metrics.open_trades, metrics.daily_pnl,
                     metrics.equity, metrics.drawdown))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Failed to save portfolio snapshot: {e}")

    async def save_market_analytics(self, metrics: AdvancedMetrics):
        """Save comprehensive market analytics with thread safety"""
        async with self._db_lock:
            try:
                self.conn.execute('''INSERT INTO market_analytics 
                    (timestamp, spot_price, vix, ivp, regime, event_risk, sabr_alpha, sabr_beta, 
                     sabr_rho, sabr_nu, realized_vol_7d, garch_vol_7d, iv_rv_spread, pcr, 
                     max_pain, term_structure_slope, volatility_skew)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (metrics.timestamp.isoformat(), metrics.spot_price, metrics.vix, metrics.ivp,
                     metrics.regime.value, metrics.event_risk_score, metrics.sabr_alpha,
                     metrics.sabr_beta, metrics.sabr_rho, metrics.sabr_nu, metrics.realized_vol_7d,
                     metrics.garch_vol_7d, metrics.iv_rv_spread, metrics.pcr, metrics.max_pain,
                     metrics.term_structure_slope, metrics.volatility_skew))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Failed to save market analytics: {e}")

    def get_daily_state(self) -> Tuple[float, float, int, int]:
        """Get daily state with error handling"""
        today = datetime.now(IST).strftime("%Y-%m-%d")
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT daily_pnl, max_equity, cycle_count, total_trades FROM daily_state WHERE date=?", 
                (today,)
            )
            row = cursor.fetchone()
            if row:
                return row[0], row[1], row[2], row[3]
            return 0.0, ACCOUNT_SIZE, 0, 0
        except Exception as e:
            logger.error(f"Database error getting daily state: {e}")
            return 0.0, ACCOUNT_SIZE, 0, 0

    async def save_daily_state(self, daily_pnl: float, max_equity: float, cycle_count: int, total_trades: int):
        """Save daily state with thread safety"""
        async with self._db_lock:
            today = datetime.now(IST).strftime("%Y-%m-%d")
            try:
                self.conn.execute('''INSERT OR REPLACE INTO daily_state (date, daily_pnl, max_equity, cycle_count, total_trades) 
                             VALUES (?, ?, ?, ?, ?)''', 
                             (today, daily_pnl, max_equity, cycle_count, total_trades))
                self.conn.commit()
            except Exception as e:
                logger.error(f"Database error saving daily state: {e}")
                self.conn.rollback()

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
