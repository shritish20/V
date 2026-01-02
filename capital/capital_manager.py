import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger("CapitalManager")

@dataclass
class TradeState:
    id: str
    strategy: str
    legs: list
    entry_time: datetime
    pnl: float = 0.0

class CapitalManager:
    def __init__(self, settings):
        self.settings = settings
        self.deployed_capital = 0.0
        self.daily_pnl = 0.0
        self.active_trade = None 
        self._system_health = {"ws_market": False, "ws_portfolio": False, "latency_ms": 0}

    def current_trade(self) -> TradeState | None:
        return self.active_trade

    def system_health(self) -> dict:
        return self._system_health

    def can_allocate(self, regime_exposure_cap: float) -> bool:
        if self.daily_pnl < -self.settings.DAILY_LOSS_LIMIT:
            logger.warning("🚫 Daily Loss Limit Hit.")
            return False
        if self.active_trade is not None:
            return False
        return True

    def build_strategy_orders(self, market_state, regime) -> list:
        # Placeholder for Strategy Matrix
        return []

    def register_trade(self, filled_orders: list):
        self.active_trade = TradeState(
            id=f"TRD-{int(datetime.now().timestamp())}",
            strategy="AUTO",
            legs=filled_orders,
            entry_time=datetime.now()
        )

    def update_trade_from_positions(self, positions: dict):
        pass # Update PnL logic here
