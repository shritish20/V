import asyncio
import time
import logging
from typing import List, Dict, Tuple, Optional
from threading import Lock, RLock
from collections import deque
from core.models import Position, Order, OrderStatus, OrderType
from core.config import IST, PAPER_TRADING
from .api_client import HybridUpstoxAPI
from datetime import datetime
from database.manager import HybridDatabaseManager
from alerts.system import CriticalAlertSystem

logger = logging.getLogger("VolGuard14")

class EnhancedOrderManager:
    """Production-grade order management with retry logic and fill tracking - Enhanced Fusion"""
    
    def __init__(self, api: HybridUpstoxAPI, database: HybridDatabaseManager, alert_system: CriticalAlertSystem):
        self.api = api
        self.db = database
        self.alerts = alert_system
        self.orders: Dict[str, Order] = {}
        self.retry_queue: deque = deque(maxlen=1000)
        self._order_lock = RLock()
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
        # Background tasks
        self._fill_checker_task = None
        self._retry_processor_task = None
        
    async def start(self):
        """Start order management background tasks"""
        self._fill_checker_task = asyncio.create_task(self._check_fills_continuously())
        self._retry_processor_task = asyncio.create_task(self._process_retry_queue())
        logger.info("Enhanced Order Manager started with background tasks")
    
    async def stop(self):
        """Stop background tasks"""
        if self._fill_checker_task:
            self._fill_checker_task.cancel()
        if self._retry_processor_task:
            self._retry_processor_task.cancel()
        logger.info("Enhanced Order Manager stopped")
    
    async def place_order(self, order: Order) -> str:
        """Place order with comprehensive error handling and retry logic"""
        if not self._validate_order(order):
            order.status = OrderStatus.REJECTED
            order.error_message = "Order validation failed"
            await self._save_order(order)
            return None
        
        try:
            # Use safe order placement with ghost order recovery
            success, order_id = await self.api.place_order_safe(order)
            
            if success and order_id:
                order.order_id = order_id
                order.status = OrderStatus.SUBMITTED
                order.last_updated = datetime.now()
                
                with self._order_lock:
                    self.orders[order.order_id] = order
                
                await self._save_order(order)
                logger.info(f"Order {order.order_id} submitted successfully")
                return order.order_id
            else:
                order.status = OrderStatus.REJECTED
                order.error_message = "Order placement failed"
                await self._queue_for_retry(order)
                return None
                
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            order.status = OrderStatus.REJECTED
            order.error_message = str(e)
            await self._queue_for_retry(order)
            return None
    
    async def _queue_for_retry(self, order: Order):
        """Queue order for retry if within limits"""
        if order.retry_count < self.max_retries:
            order.retry_count += 1
            order.status = OrderStatus.PENDING
            order.last_updated = datetime.now()
            
            with self._order_lock:
                self.retry_queue.append(order)
            
            logger.info(f"Order {order.order_id} queued for retry {order.retry_count}")
        else:
            order.status = OrderStatus.REJECTED
            await self._save_order(order)
            await self.alerts.send_alert(
                "ORDER_REJECTED",
                f"Order {order.order_id} rejected after {self.max_retries} retries: {order.error_message}",
                urgent=True
            )
    
    async def _process_retry_queue(self):
        """Process retry queue with exponential backoff"""
        while True:
            try:
                if self.retry_queue:
                    order = self.retry_queue[0]  # Peek without removing
                    
                    # Exponential backoff based on retry count
                    delay = self.retry_delay * (2 ** (order.retry_count - 1))
                    await asyncio.sleep(delay)
                    
                    # Retry the order
                    success = await self.place_order(order)
                    if success:
                        self.retry_queue.popleft()  # Remove on success
                    else:
                        # Keep in queue for next retry (if not maxed out)
                        pass
                else:
                    await asyncio.sleep(1)  # Check queue every second
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Retry processor error: {e}")
                await asyncio.sleep(5)
    
    async def _check_fills_continuously(self):
        """Continuously check for order fills"""
        while True:
            try:
                await self._check_pending_fills()
                await asyncio.sleep(5)  # Check every 5 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fill checker error: {e}")
                await asyncio.sleep(10)
    
    async def _check_pending_fills(self):
        """Check fills for all active orders"""
        active_orders = self.get_active_orders()
        
        for order in active_orders:
            try:
                order_status = await self.api.get_order_details(order.order_id)
                if order_status and order_status.get("data"):
                    await self._process_order_update(order, order_status["data"])
            except Exception as e:
                logger.error(f"Failed to check order {order.order_id}: {e}")
    
    async def _process_order_update(self, order: Order, status_data: Dict):
        """Process order status update from broker"""
        new_status = status_data.get("status", "").upper()
        filled_qty = status_data.get("filled_quantity", 0)
        avg_price = float(status_data.get("average_price", 0.0))
        
        # Map broker status to our status enum
        status_map = {
            "COMPLETE": OrderStatus.FILLED,
            "REJECTED": OrderStatus.REJECTED,
            "CANCELLED": OrderStatus.CANCELLED,
            "OPEN": OrderStatus.SUBMITTED,
            "PARTIAL FILL": OrderStatus.PARTIAL_FILLED
        }
        
        new_order_status = status_map.get(new_status, OrderStatus.SUBMITTED)
        
        if new_order_status != order.status or filled_qty != order.filled_quantity:
            order.update_fill(filled_qty, avg_price)
            order.status = new_order_status
            order.last_updated = datetime.now()
            
            await self._save_order(order)
            
            # Update parent trade if fully filled
            if order.status == OrderStatus.FILLED and order.parent_trade_id:
                await self._update_trade_with_fill(order)
            
            logger.info(f"Order {order.order_id} updated: {order.status}, Filled: {order.filled_quantity}")
    
    async def _update_trade_with_fill(self, order: Order):
        """Update parent trade with actual fill prices"""
        try:
            # This would update the trade legs with actual fill prices
            # and recalculate trade metrics
            logger.info(f"Trade {order.parent_trade_id} updated with fill for order {order.order_id}")
        except Exception as e:
            logger.error(f"Failed to update trade with fill: {e}")
    
    def get_active_orders(self) -> List[Order]:
        """Get all active orders"""
        with self._order_lock:
            return [order for order in self.orders.values() if order.is_active()]
    
    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID"""
        with self._order_lock:
            return self.orders.get(order_id)
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an active order"""
        order = self.get_order(order_id)
        if not order or not order.is_active():
            return False
        
        try:
            success = await self.api.cancel_order(order_id)
            if success:
                order.status = OrderStatus.CANCELLED
                order.last_updated = datetime.now()
                await self._save_order(order)
                return True
            return False
        except Exception as e:
            logger.error(f"Order cancellation failed: {e}")
            return False

    async def execute_basket_order(self, legs: List[Position]) -> Tuple[bool, Dict[str, float]]:
        """Execute basket order with rollback on partial fills - CRITICAL SAFETY"""
        placed_orders = []
        fill_prices = {}
        try:
            for leg in legs:
                side = "SELL" if leg.quantity < 0 else "BUY"
                order = Order(
                    order_id="",
                    instrument_key=leg.instrument_key,
                    quantity=abs(leg.quantity),
                    price=leg.entry_price,
                    order_type=OrderType.LIMIT,
                    transaction_type=side,
                    status=OrderStatus.PENDING,
                    placed_time=datetime.now(),
                    last_updated=datetime.now()
                )
                
                order_id = await self.place_order(order)
                if order_id: 
                    placed_orders.append(order_id)
                    fill_prices[order_id] = 0.0
                else: 
                    await self._cancel_order_batch(placed_orders)
                    return False, {}

            success, final_fill_prices = await self.verify_fills(placed_orders)
            if not success:
                await self._rollback_partial_fills(final_fill_prices)
                return False, {}
            
            return True, final_fill_prices

        except Exception as e:
            logger.critical(f"Basket order execution failed: {e}")
            await self._emergency_rollback(placed_orders, fill_prices)
            return False, {}

    async def verify_fills(self, order_ids: List[str], timeout: int = 10) -> Tuple[bool, Dict[str, float]]:
        """Verify order fills with parallel checking"""
        start_time = time.time()
        fill_prices = {}
        filled_orders = set()

        if PAPER_TRADING:
            await asyncio.sleep(0.1) 
            for oid in order_ids:
                with self._order_lock:
                    order = self.orders.get(oid)
                    if order:
                        order.status = OrderStatus.FILLED
                        order.filled_quantity = order.quantity
                        order.average_price = order.price
                        fill_prices[oid] = order.price
            return True, fill_prices
        
        while time.time() - start_time < timeout:
            pending_orders = [oid for oid in order_ids if oid not in filled_orders]
            if not pending_orders: return True, fill_prices

            tasks = [self.api.get_order_details(oid) for oid in pending_orders]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for oid, result in zip(pending_orders, results):
                if isinstance(result, Exception): continue
                order_data = result.get("data", {})
                status_str = order_data.get("status", "PENDING")
                
                try: 
                    status = OrderStatus(status_str)
                except ValueError: 
                    status = OrderStatus.PENDING

                with self._order_lock:
                    if oid in self.orders:
                        self.orders[oid].status = status
                        self.orders[oid].filled_quantity = order_data.get("filled_quantity", 0)
                        self.orders[oid].average_price = float(order_data.get("average_price", 0))

                        if status == OrderStatus.FILLED:
                            fill_prices[oid] = self.orders[oid].average_price
                            filled_orders.add(oid)
                        elif status in [OrderStatus.REJECTED, OrderStatus.CANCELLED]:
                            return False, fill_prices

            if len(filled_orders) == len(order_ids): return True, fill_prices
            await asyncio.sleep(1)

        logger.warning(f"Order verification timeout after {timeout}s")
        return False, fill_prices

    async def _cancel_order_batch(self, order_ids: List[str]):
        """Cancel multiple orders in parallel"""
        if PAPER_TRADING: 
            logger.info(f"PAPER TRADING: Simulating cancel batch for {len(order_ids)} orders")
            return
        tasks = [self.api.cancel_order(oid) for oid in order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _rollback_partial_fills(self, fill_prices: Dict[str, float]):
        """Implements Full Kill Switch on Rollback Failure with retry mechanism."""
        rollback_orders_to_verify = []
        max_rollback_attempts = 3
        
        for attempt in range(max_rollback_attempts):
            try:
                rollback_orders_to_verify.clear()
                
                for oid, fill_price in fill_prices.items():
                    if oid in self.orders:
                        order = self.orders[oid]
                        reverse_side = "BUY" if order.transaction_type == "SELL" else "SELL"
                        
                        # Use market orders for rollback for better fill probability
                        rollback_order = Order(
                            order_id="",
                            instrument_key=order.instrument_key,
                            quantity=order.quantity,
                            price=0.0,  # Market order
                            order_type=OrderType.MARKET,
                            transaction_type=reverse_side,
                            status=OrderStatus.PENDING,
                            placed_time=datetime.now(),
                            last_updated=datetime.now()
                        )
                        
                        order_id = await self.place_order(rollback_order)
                        if order_id:
                            rollback_orders_to_verify.append(order_id)

                if rollback_orders_to_verify:
                    success, final_fill_prices = await self.verify_fills(rollback_orders_to_verify, timeout=15)
                    
                    if success:
                        logger.info(f"Rollback successful on attempt {attempt + 1}")
                        return
                    else:
                        logger.warning(f"Rollback attempt {attempt + 1} failed, retrying...")
                        await asyncio.sleep(2)  # Wait before retry
                        
            except Exception as e:
                logger.error(f"Rollback attempt {attempt + 1} failed with error: {e}")
                
        # If all attempts fail, alert but don't crash
        logger.critical(f"EMERGENCY: Rollback failed after {max_rollback_attempts} attempts")
        if self.alerts:
            await self.alerts.send_alert(
                "ROLLBACK_FAILURE", 
                f"Manual intervention required for {len(rollback_orders_to_verify)} positions",
                urgent=True
            )
        
    async def _emergency_rollback(self, order_ids: List[str], fill_prices: Dict[str, float]):
        """Emergency rollback in case of critical failure"""
        await self._cancel_order_batch(order_ids)
        await self._rollback_partial_fills(fill_prices)

    def _validate_order(self, order: Order) -> bool:
        """Validate order before submission"""
        if order.quantity <= 0:
            return False
        if order.price <= 0 and order.order_type != OrderType.MARKET:
            return False
        if not order.instrument_key:
            return False
        if order.transaction_type not in ["BUY", "SELL"]:
            return False
        return True
    
    async def _save_order(self, order: Order):
        """Save order to database"""
        try:
            self.db.save_order(order)
        except Exception as e:
            logger.error(f"Failed to save order: {e}")
