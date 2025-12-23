import logging
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List

from core.models import Order
from core.enums import OrderStatus, TradeStatus 
from database.models import DbOrder
from core.config import settings

logger = logging.getLogger("OrderManager")

class EnhancedOrderManager:
    """
    VolGuard 20.0 – Production-Hardened Order Manager (V3 HFT Edition)
    - Central Handler for Single Leg Orders & DB Persistence.
    - UPDATED: Uses V3 HFT Endpoints and handles list-based Order ID returns.
    - PERSISTENCE: Maintains full DbOrder logging for the Quant Journal.
    """
    def __init__(self, api_client, db_manager):
        self.api = api_client
        self.db = db_manager
        self.running = False

    async def start(self):
        """
        Initialize background monitoring if needed.
        Called by Engine on startup.
        """
        self.running = True
        logger.info("✅ V3 Order Manager Online")

    async def place_order(self, order: Order, strategy_id: str, tag: str = None) -> Optional[str]:
        """
        Places a single order via V3 HFT and logs it to the Database.
        - V3 returns a list of order_ids. We take the first one for single orders.
        """
        # 1. Place Order via API (Now calling the V3 HFT compatible method)
        success, order_id = await self.api.place_order(order)
        
        if success and order_id:
            # 2. Persist to DB (Full Logic Restored)
            await self._persist_order(
                order_id=order_id,
                strategy_id=strategy_id,
                order_details=order,
                tag=tag or "MANUAL"
            )
            return order_id
        else:
            logger.error(f"❌ V3 Order Placement Failed for strategy: {strategy_id} | Tag: {tag}")
            return None

    async def modify_order(self, order_id: str, new_price: float, new_qty: int = None) -> bool:
        """
        Modifies an open pending order using V3 parameters.
        """
        try:
            # Construct payload (Upstox 2025 specific)
            req = {
                "order_id": order_id,
                "price": float(new_price),
                "order_type": "LIMIT",
                "validity": "DAY"
            }
            if new_qty:
                req["quantity"] = int(new_qty)

            # Note: api_client now uses dynamic_url for V3 routes
            url = "https://api.upstox.com/v3/order/modify"
            res = await self.api._request("PUT", dynamic_url=url, json_data=req)
            
            if res.get("status") == "success":
                logger.info(f"✏️ Order {order_id} Modified -> Price: {new_price}")
                # Update DB
                await self._update_db_status(order_id, "MODIFIED", price=new_price)
                return True
            else:
                logger.error(f"❌ V3 Modify Failed for {order_id}: {res.get('message')}")
                return False
        except Exception as e:
            logger.error(f"Modify Exception: {e}")
            return False

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancels a pending order via V3 endpoint.
        """
        try:
            url = "https://api.upstox.com/v3/order/cancel"
            res = await self.api._request(
                "DELETE", 
                dynamic_url=url, 
                params={"order_id": order_id}
            )
            if res.get("status") == "success":
                logger.info(f"🚫 Order {order_id} Cancelled")
                await self._update_db_status(order_id, "CANCELLED")
                return True
            else:
                logger.error(f"❌ V3 Cancel Failed for {order_id}: {res.get('message')}")
                return False
        except Exception as e:
            logger.error(f"Cancel Exception: {e}")
            return False

    async def _persist_order(self, order_id: str, strategy_id: str, order_details: Order, tag: str):
        """Saves initial order state to Postgres (Restored Original Logic)"""
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
            logger.error(f"DB Order Persist Failed for {order_id}: {e}")

    async def _update_db_status(self, order_id: str, status: str, price: float = None):
        """Updates order status in DB (Restored Original Logic)"""
        try:
            async with self.db.get_session() as session:
                # Search for order_id which is the primary key in your DbOrder model
                db_order = await session.get(DbOrder, order_id)
                if db_order:
                    db_order.status = status
                    if price:
                        db_order.price = price
                    await self.db.safe_commit(session)
                else:
                    logger.warning(f"⚠️ Could not find order {order_id} in DB to update status to {status}")
        except Exception as e:
            logger.error(f"DB Update Status Failed for {order_id}: {e}")
