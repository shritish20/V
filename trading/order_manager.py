import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any

from core.models import Order
from core.enums import OrderStatus, TradeStatus # <--- FIXED IMPORT
from database.models import DbOrder
from core.config import settings

logger = logging.getLogger("OrderManager")

class EnhancedOrderManager:
    """
    Central Handler for Single Leg Orders & DB Persistence.
    Used by TradeManager for management and LiveExecutor for tracking.
    """
    def __init__(self, api_client, db_manager):
        self.api = api_client
        self.db = db_manager
        self.running = False

    async def start(self):
        """
        Initialize any background monitoring if needed.
        Called by Engine on startup.
        """
        self.running = True
        logger.info("✅ Order Manager Online")

    async def place_order(self, order: Order, strategy_id: str, tag: str = None) -> Optional[str]:
        """
        Places a single order and logs it to the Database.
        """
        # 1. Place Order via API
        success, order_id = await self.api.place_order(order)
        
        if success and order_id:
            # 2. Persist to DB
            await self._persist_order(
                order_id=order_id,
                strategy_id=strategy_id,
                order_details=order,
                tag=tag or "MANUAL"
            )
            return order_id
        else:
            logger.error(f"❌ Order Placement Failed: {tag}")
            return None

    async def modify_order(self, order_id: str, new_price: float, new_qty: int = None) -> bool:
        """
        Modifies an open pending order.
        """
        try:
            # Construct payload (Upstox specific)
            req = {
                "order_id": order_id,
                "price": float(new_price),
                "order_type": "LIMIT",
                "validity": "DAY"
            }
            if new_qty:
                req["quantity"] = int(new_qty)

            res = await self.api._request_with_retry("PUT", "modify_order", json=req)
            if res.get("status") == "success":
                logger.info(f"✏️ Order {order_id} Modified -> {new_price}")
                # Update DB
                await self._update_db_status(order_id, "MODIFIED", price=new_price)
                return True
            return False
        except Exception as e:
            logger.error(f"Modify Failed: {e}")
            return False

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancels a pending order.
        """
        try:
            res = await self.api._request_with_retry(
                "DELETE", 
                "cancel_order", 
                params={"order_id": order_id}
            )
            if res.get("status") == "success":
                logger.info(f"🚫 Order {order_id} Cancelled")
                await self._update_db_status(order_id, "CANCELLED")
                return True
            return False
        except Exception as e:
            logger.error(f"Cancel Failed: {e}")
            return False

    async def _persist_order(self, order_id: str, strategy_id: str, order_details: Order, tag: str):
        """Saves initial order state to Postgres"""
        try:
            async with self.db.get_session() as session:
                db_order = DbOrder(
                    order_id=str(order_id),
                    strategy_id=str(strategy_id),
                    instrument_token=order_details.instrument_key,
                    transaction_type=order_details.transaction_type,
                    quantity=order_details.quantity,
                    price=order_details.price,
                    status=OrderStatus.PENDING.value,
                    tag=tag,
                    placed_at=datetime.now()
                )
                session.add(db_order)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"DB Order Persist Failed: {e}")

    async def _update_db_status(self, order_id: str, status: str, price: float = None):
        """Updates order status in DB"""
        try:
            async with self.db.get_session() as session:
                db_order = await session.get(DbOrder, order_id)
                if db_order:
                    db_order.status = status
                    if price:
                        db_order.price = price
                    await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"DB Update Failed: {e}")
