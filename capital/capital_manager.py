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
    max_drawdown: float = 0.0

class CapitalManager:
    def __init__(self, settings):
        self.settings = settings
        self.deployed_capital = 0.0
        self.daily_pnl = 0.0
        self.active_trade = None 
        
        # Internal health state, updated by workers via update_health()
        self._system_health = {
            "ws_market": False, 
            "ws_portfolio": False, 
            "latency_ms": 0,
            "last_tick_time": 0
        }

    def current_trade(self) -> TradeState | None:
        return self.active_trade

    def system_health(self) -> dict:
        return self._system_health
    
    def update_health(self, key: str, value):
        self._system_health[key] = value

    def can_allocate(self, regime_exposure_cap: float) -> bool:
        """
        Fix 3: BINDING Capital Checks
        """
        # 1. Hard Daily Stop
        if self.daily_pnl < -self.settings.DAILY_LOSS_LIMIT:
            logger.error(f"🚫 DAILY LOSS LIMIT HIT: {self.daily_pnl}")
            return False
        
        # 2. Existing Trade Block
        if self.active_trade is not None:
            return False

        # 3. Real Exposure Calculation
        # We use ACCOUNT_SIZE from settings as the denominator
        max_allowed_deployment = self.settings.ACCOUNT_SIZE * regime_exposure_cap
        
        # Assuming we are checking if we *can* enter a new trade.
        # Since we block if active_trade exists, this checks if we have room 
        # in a multi-trade system, or just general sanity check.
        if self.deployed_capital > max_allowed_deployment:
            logger.warning(f"🚫 Exposure Limit Breached: {self.deployed_capital} > {max_allowed_deployment}")
            return False

        return True

    def build_strategy_orders(self, market_state, regime) -> list:
        # Placeholder for Strategy Matrix
        # Returns [] if no trade setup exists
        return []

    def register_trade(self, filled_orders: list):
        capital_used = 0.0
        for o in filled_orders:
            # Simple approximation: Price * Qty
            # In prod, fetch margin requirements from broker
            if o.get('transaction_type') == 'BUY':
                capital_used += (float(o.get('price', 0) or 0) * int(o.get('quantity', 0)))

        self.deployed_capital += capital_used
        
        self.active_trade = TradeState(
            id=f"TRD-{int(datetime.now().timestamp())}",
            strategy="AUTO",
            legs=filled_orders,
            entry_time=datetime.now()
        )
        logger.info(f"✅ Trade Registered: {self.active_trade.id} | Cap Used: {capital_used}")

    def update_trade_from_positions(self, positions: dict):
        if not self.active_trade:
            return
        
        # Simple PnL Summation
        current_pnl = sum(float(p.get('pnl', 0)) for p in positions.values())
        self.active_trade.pnl = current_pnl
