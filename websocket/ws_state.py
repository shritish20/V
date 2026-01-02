from threading import Lock
from datetime import datetime

class WebSocketState:
    def __init__(self):
        self._lock = Lock()
        self.market = {}
        self.positions = {}
        self.last_tick = datetime.utcnow()

    def update_market(self, data: dict):
        with self._lock:
            self.market.update(data)
            self.last_tick = datetime.utcnow()

    def snapshot(self) -> dict:
        with self._lock:
            return {"market": dict(self.market), "last_tick": self.last_tick}
