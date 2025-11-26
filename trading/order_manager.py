import asyncio
import time
import logging
from typing import List, Dict, Tuple, Optional
from threading import Lock
from core.models import Position, Order
from core.enums import OrderStatus
from core.config import IST, PAPER_TRADING
from .api_client import HybridUpstoxAPI

logger = logging.getLogger("VolGuardHybrid")

class SafeOrderManager:
    """Order manager with partial fill rollback protection"""
    
    def __init__(self, api: HybridUpstoxAPI):
        self.api = api
        self.orders: Dict[str, Order] = {}
        self._lock = Lock()
    
    async def place_order(self, instrument_key: str, quantity: int, price: float, side: str) -> Optional[Order]:
        """Place order with validation"""
        payload = {
            "quantity": abs(quantity),
            "product": "I",
            "validity": "DAY",
            "price": round(price, 2),
            "tag": "VOLGUARD_HYBRID",
            "instrument_key": instrument_key,
            "order_type": "LIMIT",
            "transaction_type": side,
            "disclosed_quantity": 0,
            "trigger_price": 0
        }
        
        response = await self.api.place_order(payload)
        order_id = response.get("data", {}).get("order_id")
        
        if order_id:
            order = Order(
                order_id=order_id,
                instrument_key=instrument_key,
                quantity=quantity,
                price=price,
                side=side,
                status=OrderStatus.PLACED,
                placed_time=datetime.now(IST)
            )
            with self._lock:
                self.orders[order_id] = order
            logger.info(f"Order placed: {order_id} at ₹{price:.2f}")
            return order
        
        logger.error(f"Order placement failed: {response}")
        return None
    
    async def execute_basket_order(self, legs: List[Position]) -> Tuple[bool, Dict[str, float]]:
        """Execute basket order with rollback on partial fills - CRITICAL SAFETY"""
        placed_orders = []
        fill_prices = {}
        
        try:
            # Phase 1: Place all orders
            for leg in legs:
                side = "SELL" if leg.quantity < 0 else "BUY"
                order = await self.place_order(
                    leg.instrument_key, 
                    abs(leg.quantity), 
                    leg.entry_price, 
                    side
                )
                if order:
                    placed_orders.append(order.order_id)
                else:
                    # Failed to place one order - cancel all placed orders
                    await self._cancel_order_batch(placed_orders)
                    return False, {}
            
            # Phase 2: Verify all fills
            success, fill_prices = await self.verify_fills(placed_orders)
            
            if not success:
                # CRITICAL: Rollback any filled orders
                await self._rollback_partial_fills(fill_prices)
                return False, {}
                
            return True, fill_prices
            
        except Exception as e:
            logger.critical(f"Basket order execution failed: {e}")
            await self._emergency_rollback(placed_orders, fill_prices)
            return False, {}
    
    async def verify_fills(self, order_ids: List[str], timeout: int = 10) -> Tuple[bool, Dict[str, float]]:
        """Verify order fills with parallel checking"""
        start_time = time.time()
        fill_prices = {}
        filled_orders = set()
        
        while time.time() - start_time < timeout:
            pending_orders = [oid for oid in order_ids if oid not in filled_orders]
            
            if not pending_orders:
                return True, fill_prices
            
            # Check all pending orders in parallel
            tasks = [self.api.get_order_details(oid) for oid in pending_orders]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for oid, result in zip(pending_orders, results):
                if isinstance(result, Exception):
                    continue
                    
                order_data = result.get("data", {})
                status_str = order_data.get("status", "PENDING")
                
                try:
                    status = OrderStatus(status_str)
                except ValueError:
                    status = OrderStatus.PENDING
                
                # Update order status
                with self._lock:
                    if oid in self.orders:
                        self.orders[oid].status = status
                        self.orders[oid].filled_quantity = order_data.get("filled_quantity", 0)
                        self.orders[oid].average_price = float(order_data.get("average_price", 0))
                
                if status == OrderStatus.FILLED:
                    fill_prices[oid] = self.orders[oid].average_price
                    filled_orders.add(oid)
                elif status in [OrderStatus.REJECTED, OrderStatus.CANCELLED]:
                    return False, fill_prices
            
            # If all orders filled, return success
            if len(filled_orders) == len(order_ids):
                return True, fill_prices
            
            await asyncio.sleep(1)
        
        logger.warning(f"Order verification timeout after {timeout}s")
        return False, fill_prices
    
    async def _cancel_order_batch(self, order_ids: List[str]):
        """Cancel multiple orders in parallel"""
        tasks = [self.api.cancel_order(oid) for oid in order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _rollback_partial_fills(self, fill_prices: Dict[str, float]):
        """Rollback any filled orders from partial execution"""
        rollback_tasks = []
        
        for oid, fill_price in fill_prices.items():
            if oid in self.orders:
                order = self.orders[oid]
                reverse_side = "BUY" if order.side == "SELL" else "SELL"
                
                # Place reverse order at slightly worse price to ensure fill
                reverse_price = fill_price * 1.01 if reverse_side == "BUY" else fill_price * 0.99
                
                rollback_task = self.place_order(
                    order.instrument_key,
                    order.quantity,
                    reverse_price,
                    reverse_side
                )
                rollback_tasks.append(rollback_task)
        
        if rollback_tasks:
            await asyncio.gather(*rollback_tasks, return_exceptions=True)
            logger.critical(f"Emergency rollback executed for {len(rollback_tasks)} orders")
    
    async def _emergency_rollback(self, order_ids: List[str], fill_prices: Dict[str, float]):
        """Emergency rollback in case of critical failure"""
        await self._cancel_order_batch(order_ids)
        await self._rollback_partial_fills(fill_prices)
