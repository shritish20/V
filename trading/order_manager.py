import asyncio
import time
import logging
from typing import List, Dict, Tuple, Optional
from threading import Lock
from core.models import Position, Order
from core.enums import OrderStatus
from core.config import IST, PAPER_TRADING, ROLLBACK_SLIPPAGE_PERCENT
from .api_client import HybridUpstoxAPI
from datetime import datetime
from api.dependencies import get_global_engine_ref # BUG #5 FIX: Import global engine getter

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
            "quantity": abs(quantity), "product": "I", "validity": "DAY", "price": round(price, 2), 
            "tag": "VOLGUARD_HYBRID", "instrument_key": instrument_key, "order_type": "LIMIT", 
            "transaction_type": side, "disclosed_quantity": 0, "trigger_price": 0
        }
        
        if PAPER_TRADING:
            response = await self.api.place_order(payload)
            if response.get("status") == "error": return None
            
            order_data = response.get("data", {})
            order_id = order_data.get("order_id", f"SIM_{int(time.time() * 1000)}")
            
            order = Order(
                order_id=order_id, instrument_key=instrument_key, quantity=quantity, price=price, side=side, 
                status=OrderStatus.PLACED, placed_time=datetime.now(IST)
            )
            with self._lock: self.orders[order_id] = order
            return order

        response = await self.api.place_order(payload)
        order_id = response.get("data", {}).get("order_id")

        if order_id:
            order = Order(order_id=order_id, instrument_key=instrument_key, quantity=quantity, price=price, side=side, status=OrderStatus.PLACED, placed_time=datetime.now(IST))
            with self._lock: self.orders[order_id] = order
            logger.info(f"Order placed: {order_id} at ₹{price:.2f}")
            return order
        logger.error(f"Order placement failed: {response}")
        return None


    async def execute_basket_order(self, legs: List[Position]) -> Tuple[bool, Dict[str, float]]:
        """Execute basket order with rollback on partial fills - CRITICAL SAFETY"""
        placed_orders = []
        fill_prices = {}
        try:
            for leg in legs:
                side = "SELL" if leg.quantity < 0 else "BUY"
                order = await self.place_order(leg.instrument_key, abs(leg.quantity), leg.entry_price, side)
                if order: placed_orders.append(order.order_id)
                else: await self._cancel_order_batch(placed_orders); return False, {}

            success, fill_prices = await self.verify_fills(placed_orders)
            if not success:
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

        if PAPER_TRADING:
            await asyncio.sleep(0.1) 
            for oid in order_ids:
                with self._lock:
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
                
                try: status = OrderStatus(status_str)
                except ValueError: status = OrderStatus.PENDING

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

            if len(filled_orders) == len(order_ids): return True, fill_prices
            await asyncio.sleep(1)

        logger.warning(f"Order verification timeout after {timeout}s")
        return False, fill_prices

    async def _cancel_order_batch(self, order_ids: List[str]):
        """Cancel multiple orders in parallel"""
        if PAPER_TRADING: logger.info(f"PAPER TRADING: Simulating cancel batch for {len(order_ids)} orders"); return
        tasks = [self.api.cancel_order(oid) for oid in order_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _rollback_partial_fills(self, fill_prices: Dict[str, float]):
        """CRITICAL FIX 4: Implements Full Kill Switch on Rollback Failure."""
        rollback_orders_to_verify = []

        for oid, fill_price in fill_prices.items():
            if oid in self.orders:
                order = self.orders[oid]
                reverse_side = "BUY" if order.side == "SELL" else "SELL"
                
                slippage_factor_buy = 1.0 + ROLLBACK_SLIPPAGE_PERCENT
                slippage_factor_sell = 1.0 - ROLLBACK_SLIPPAGE_PERCENT
                
                if reverse_side == "BUY": reverse_price = fill_price * slippage_factor_buy
                else: reverse_price = fill_price * slippage_factor_sell

                rollback_order = await self.place_order(order.instrument_key, order.quantity, reverse_price, reverse_side)
                if rollback_order:
                    rollback_orders_to_verify.append(rollback_order.order_id)

        if rollback_orders_to_verify:
            success, final_fill_prices = await self.verify_fills(rollback_orders_to_verify, timeout=10)
            
            if not success:
                logger.critical(f"EMERGENCY: Rollback orders FAILED to fill completely. {len(rollback_orders_to_verify) - len(final_fill_prices)} unhedged positions remain. KILL SWITCH ENGAGED.")
                
                # BUG #5 FIX: Use global engine reference to stop system
                engine = get_global_engine_ref()
                if engine:
                    engine.circuit_breaker = True
                    engine.running = False
                    await engine.alerts.send_alert("ROLLBACK_FAILURE", "Unhedged positions remain. Engine stopped.", urgent=True)
                
                await self.api.close() 
            else:
                logger.critical(f"Emergency rollback executed and VERIFIED for {len(final_fill_prices)} orders.")
        
    async def _emergency_rollback(self, order_ids: List[str], fill_prices: Dict[str, float]):
        """Emergency rollback in case of critical failure"""
        await self._cancel_order_batch(order_ids)
        await self._rollback_partial_fills(fill_prices)
