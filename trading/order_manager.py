import asyncio
import logging
from core.models import Order, OrderStatus
from trading.api_client import EnhancedUpstoxAPI
from database.manager import HybridDatabaseManager

logger = logging.getLogger("OrderManager")

class EnhancedOrderManager:
    def __init__(self, api: EnhancedUpstoxAPI, db: HybridDatabaseManager, alerts=None):
        self.api = api
        self.db = db
        self.running = False
        self.alerts = alerts

    async def start(self):
        self.running = True

    async def place_and_monitor(self, order: Order, timeout: int = 30) -> Order:
        success, order_id = await self.api.place_order(order)
        if not success or not order_id:
            order.status = OrderStatus.REJECTED
            return order
        
        order.order_id = order_id
        order.status = OrderStatus.PENDING
        
        start_time = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            status_data = await self.api.get_order_details(order_id)
            status = status_data.get("status")
            filled_qty = status_data.get("filled_quantity", 0)
            
            if status == "complete":
                order.status = OrderStatus.FILLED
                order.average_price = status_data.get("average_price", 0.0)
                order.filled_quantity = filled_qty
                return order
            elif status in ["cancelled", "rejected"]:
                if filled_qty > 0:
                    order.status = OrderStatus.FILLED
                    order.filled_quantity = filled_qty
                    logger.warning(
                        f"Order {order_id} Partially Filled: {filled_qty}"
                    )
                else:
                    order.status = (
                        OrderStatus.CANCELLED if status == "cancelled" else OrderStatus.REJECTED
                    )
                return order
            
            await asyncio.sleep(1)
        
        logger.warning(f"⏱️ Order {order_id} timed out. Cancelling...")
        await self.api.cancel_order(order_id)
        order.status = OrderStatus.CANCELLED
        return order
