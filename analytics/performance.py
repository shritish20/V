import numpy as np
from typing import List, Dict
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("VolGuard14")

class PerformanceMetrics:
    """Advanced performance analytics for trading system"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        
    def _get_recent_trades(self, days: int = 30) -> List[Dict]:
        """Get recent trades from database"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            with self.db.conn:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    "SELECT pnl, entry_time, exit_time FROM trades WHERE exit_time >= ? AND status = 'CLOSED'",
                    (cutoff_date,)
                )
                trades = [{'pnl': row[0], 'entry_time': row[1], 'exit_time': row[2]} for row in cursor.fetchall()]
                return trades
        except Exception as e:
            logger.error(f"Error fetching recent trades: {e}")
            return []

    def _get_equity_curve(self, days: int = 30) -> List[float]:
        """Get equity curve from portfolio snapshots"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            with self.db.conn:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    "SELECT total_pnl FROM portfolio_snapshots WHERE timestamp >= ? ORDER BY timestamp",
                    (cutoff_date,)
                )
                equity_points = [500000 + row[0] for row in cursor.fetchall()]  # Starting equity + PnL
                return equity_points
        except Exception as e:
            logger.error(f"Error fetching equity curve: {e}")
            return []

    def calculate_sharpe_ratio(self, days: int = 30) -> float:
        """Calculate Sharpe ratio for recent period"""
        trades = self._get_recent_trades(days)
        if len(trades) < 5:
            return 0.0
            
        returns = [trade['pnl'] / 10000 for trade in trades]  # Normalized returns
        if not returns:
            return 0.0
            
        avg_return = np.mean(returns)
        std_return = np.std(returns)
        
        return avg_return / std_return if std_return > 0 else 0.0
        
    def calculate_max_drawdown(self, days: int = 30) -> float:
        """Calculate maximum drawdown"""
        equity_curve = self._get_equity_curve(days)
        if len(equity_curve) < 2:
            return 0.0
            
        peak = equity_curve[0]
        max_dd = 0.0
        
        for value in equity_curve:
            if value > peak:
                peak = value
            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)
            
        return max_dd
        
    def calculate_win_rate(self, days: int = 30) -> float:
        """Calculate win rate"""
        trades = self._get_recent_trades(days)
        if not trades:
            return 0.0
            
        winning_trades = [t for t in trades if t['pnl'] > 0]
        return len(winning_trades) / len(trades) if trades else 0.0

    def calculate_profit_factor(self, days: int = 30) -> float:
        """Calculate profit factor (gross profits / gross losses)"""
        trades = self._get_recent_trades(days)
        if not trades:
            return 0.0
            
        gross_profits = sum(t['pnl'] for t in trades if t['pnl'] > 0)
        gross_losses = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
        
        return gross_profits / gross_losses if gross_losses > 0 else float('inf')

    def calculate_avg_trade_duration(self, days: int = 30) -> float:
        """Calculate average trade duration in hours"""
        trades = self._get_recent_trades(days)
        if not trades:
            return 0.0
            
        durations = []
        for trade in trades:
            try:
                entry = datetime.fromisoformat(trade['entry_time'])
                exit_time = datetime.fromisoformat(trade['exit_time'])
                duration = (exit_time - entry).total_seconds() / 3600  # Convert to hours
                durations.append(duration)
            except:
                continue
                
        return np.mean(durations) if durations else 0.0
