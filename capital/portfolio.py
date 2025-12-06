from typing import List, Dict
from core.models import Position, PortfolioMetrics
from utils.logger import setup_logger

logger = setup_logger()

class PortfolioManager:
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.metrics: PortfolioMetrics = PortfolioMetrics()

    def update_position(self, position: Position):
        self.positions[position.symbol] = position
        logger.debug(f"Position updated for {position.symbol}. Quantity: {position.quantity}")
        self.recalculate_metrics()

    def get_all_positions(self) -> List[Position]:
        return list(self.positions.values())

    def recalculate_metrics(self):
        total_delta = 0.0
        total_vega = 0.0
        total_theta = 0.0
        total_pnl = 0.0

        for pos in self.positions.values():
            total_delta += pos.delta * pos.quantity
            total_vega += pos.vega * pos.quantity
            total_theta += pos.theta * pos.quantity
            total_pnl += pos.unrealized_pnl()

        self.metrics = PortfolioMetrics(
            delta=total_delta,
            vega=total_vega,
            theta=total_theta,
            unrealized_pnl=total_pnl,
            margin_used=sum(p.margin_required for p in self.positions.values())
        )

        logger.info(f"Portfolio Metrics Recalculated: Delta={self.metrics.delta:.2f}, Vega={self.metrics.vega:.2f}")

    def get_metrics(self) -> PortfolioMetrics:
        return self.metrics
