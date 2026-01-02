from threading import Lock
from datetime import datetime

class WebSocketState:
    def __init__(self):
        self._lock = Lock()
        self.market = {}
        self.positions = {}
        
        # Health Timestamps
        self.last_market_tick = datetime.min
        self.last_portfolio_tick = datetime.min

    def update_market(self, data: dict):
        with self._lock:
            self.market.update(data)
            # Fix 5: Update timestamp for health check
            self.last_market_tick = datetime.utcnow()

    def update_positions(self, data: dict):
        with self._lock:
            # Assuming data is a dictionary of positions keyed by token
            self.positions.update(data)
            self.last_portfolio_tick = datetime.utcnow()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "market": dict(self.market),
                "positions": dict(self.positions),
                "last_market_tick": self.last_market_tick,
                "last_portfolio_tick": self.last_portfolio_tick
            }
