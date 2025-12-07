import asyncio
from typing import Optional
import aiohttp
from core.models import MultiLegTrade
from core.config import settings
from trading.margin_guard import MarginGuard
from utils.logger import get_logger
from database.manager import HybridDatabaseManager
from database.models import DbStrategy

logger = get_logger("OrderExecutor")

class LiveOrderExecutor:
    def __init__(self, db: HybridDatabaseManager):
        self.token = settings.UPSTOX_ACCESS_TOKEN
        self.base = settings.API_BASE_V2
        self.mg = MarginGuard()
        self.db = db

    async def place_multi_leg(self, trade: MultiLegTrade) -> Optional[str]:
        """
        FIXED: Place multi-leg order with proper correlation_id and slice parameters
        """
        # Safety Hard-Lock
        if settings.SAFETY_MODE != "live":
            logger.warning(
                f"⚠ SAFETY MODE {settings.SAFETY_MODE}: Skipping Live Order"
            )
            return f"SIM-BASKET-{int(asyncio.get_event_loop().time())}"

        # Check margin before placing
        is_ok, required_margin = await self.mg.is_margin_ok(trade)
        if not is_ok:
            logger.warning(
                f"Margin insufficient – trade aborted. Required: {required_margin:.0f}"
            )
            return None

        # Build orders payload with FIXED parameters
        orders_payload = []
        for idx, leg in enumerate(trade.legs):
            order = {
                "instrument_token": leg.instrument_key,
                "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                "quantity": abs(leg.quantity),
                "price": float(leg.entry_price) if leg.entry_price > 0 else 0.0,
                "order_type": "LIMIT" if leg.entry_price > 0 else "MARKET",
                "product": "I",  # Intraday
                "validity": "DAY",
                "disclosed_quantity": 0,
                "trigger_price": 0.0,
                "is_amo": False,
                "tag": f"VG19-{trade.id}",
                "slice": False,  # FIXED: Required field for multi-order API
                "correlation_id": f"{trade.id}-LEG{idx}",  # FIXED: Required unique ID per leg
            }
            orders_payload.append(order)

        url = f"{self.base}/order/multi/place"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=orders_payload, headers=headers
                ) as resp:
                    data = await resp.json()
                    
                    if resp.status == 200 and data.get("status") == "success":
                        # Extract order IDs from response
                        order_data = data.get("data", [])
                        order_ids = [o.get("order_id") for o in order_data if o.get("order_id")]
                        
                        if not order_ids:
                            logger.error("No order IDs returned from multi-order placement")
                            return None
                        
                        basket_id = order_ids[0]  # Use first order ID as reference
                        
                        # Store order details
                        trade.basket_order_id = basket_id
                        trade.gtt_order_ids = order_ids
                        
                        # Save to database
                        async with self.db.get_session() as db_sess:
                            db_strat = await db_sess.get(DbStrategy, str(trade.id))
                            if db_strat:
                                db_strat.broker_ref_id = basket_id
                                db_strat.metadata_json = db_strat.metadata_json or {}
                                db_strat.metadata_json["order_ids"] = order_ids
                                await db_sess.merge(db_strat)
                                await db_sess.commit()
                        
                        logger.info(
                            f"✅ Multi-leg order placed successfully. "
                            f"Basket ID: {basket_id}, "
                            f"Orders: {len(order_ids)}"
                        )
                        return basket_id
                    
                    elif resp.status == 207:  # Partial success
                        logger.warning(f"⚠ Partial multi-order success: {data}")
                        # Handle partial fills
                        order_data = data.get("data", [])
                        errors = data.get("errors", [])
                        
                        successful_orders = [
                            o.get("order_id") for o in order_data if o.get("order_id")
                        ]
                        
                        if successful_orders:
                            logger.warning(
                                f"Partial fill: {len(successful_orders)}/{len(orders_payload)} legs filled"
                            )
                            # TODO: Handle partial fills - may need to cancel successful legs
                            return successful_orders[0]
                        return None
                    
                    else:
                        logger.error(f"Multi-order placement failed: {data}")
                        return None

        except aiohttp.ClientError as e:
            logger.error(f"Network error during multi-order placement: {e}")
            return None
        except Exception as e:
            logger.error(f"Multi-order exception: {e}")
            return None

    async def verify_order_fills(self, order_ids: list, timeout: int = 30) -> dict:
        """
        ADDED: Verify all legs of multi-order are filled
        Returns dict with fill status for each order
        """
        url = f"{self.base}/order/details"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        fill_status = {}
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            all_filled = True
            
            async with aiohttp.ClientSession() as session:
                for order_id in order_ids:
                    try:
                        async with session.get(
                            url, headers=headers, params={"order_id": order_id}
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                order_data = data.get("data", {})
                                status = order_data.get("status", "")
                                
                                fill_status[order_id] = {
                                    "status": status,
                                    "filled_qty": order_data.get("filled_quantity", 0),
                                    "avg_price": order_data.get("average_price", 0.0),
                                }
                                
                                if status != "complete":
                                    all_filled = False
                    except Exception as e:
                        logger.error(f"Error checking order {order_id}: {e}")
                        all_filled = False
            
            if all_filled:
                logger.info("✅ All multi-leg orders filled")
                return fill_status
            
            await asyncio.sleep(2)  # Check every 2 seconds
        
        logger.warning(f"⏱ Order fill verification timeout after {timeout}s")
        return fill_status
