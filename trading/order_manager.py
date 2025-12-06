import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from core.config import settings
from core.models import MultiLegTrade, TradeStatus, ExitReason
from utils.logger import setup_logger

logger = setup_logger()

class EnhancedOrderManager:
    def __init__(self, api, alerts):
        self.api = api
        self.alerts = alerts
        self.pending_orders: Dict[str, Dict] = {}
        self.order_updates: asyncio.Queue = asyncio.Queue()
        self.running = False
        self.task: Optional[asyncio.Task] = None

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self._order_loop())
        logger.info("Order manager started")

    async def stop(self):
        self.running = False
        if self.task:
            await self.task
        logger.info("Order manager stopped")

    async def _order_loop(self):
        while self.running:
            try:
                order_update = await asyncio.wait_for(self.order_updates.get(), timeout=1.0)
                await self._handle_order_update(order_update)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in order loop: {e}")

    async def _handle_order_update(self, update: Dict[str, Any]):
        order_id = update.get("order_id")
        status = update.get("status")
        if order_id and status:
            logger.info(f"Order {order_id} status: {status}")
            if status == "FILLED":
                await self._handle_fill(order_id, update)
            elif status in ["CANCELLED", "REJECTED"]:
                await self._handle_cancel(order_id, update)

    async def _handle_fill(self, order_id: str, update: Dict[str, Any]):
        trade = self.pending_orders.pop(order_id, None)
        if trade:
            logger.info(f"Order {order_id} filled for trade {trade.id}")
            trade.status = TradeStatus.OPEN
            await self.alerts.send_alert("ORDER_FILLED", f"Trade {trade.id} entered successfully")

    async def _handle_cancel(self, order_id: str, update: Dict[str, Any]):
        trade = self.pending_orders.pop(order_id, None)
        if trade:
            logger.warning(f"Order {order_id} cancelled/rejected for trade {trade.id}")
            trade.status = TradeStatus.CLOSED
            await self.alerts.send_alert("ORDER_CANCELLED", f"Trade {trade.id} entry failed")

    async def place_basket_order(self, trade: MultiLegTrade) -> bool:
        try:
            order_ids = []
            for leg in trade.legs:
                order = {
                    "instrument_key": leg.instrument_key,
                    "quantity": abs(leg.quantity),
                    "price": 0.0,  # Market order
                    "order_type": "MARKET",
                    "product": "I",
                    "transaction_type": "BUY" if leg.quantity > 0 else "SELL",
                    "validity": "DAY"
                }
                order_id = await self.api.place_order(order)
                if order_id:
                    order_ids.append(order_id)
                    self.pending_orders[order_id] = trade
                else:
                    logger.error(f"Failed to place order for {leg.symbol}")
                    return False

            trade.basket_order_id = ",".join(order_ids)
            logger.info(f"Basket order placed for trade {trade.id} with IDs: {order_ids}")
            return True
        except Exception as e:
            logger.error(f"Failed to place basket order: {e}")
            return False

    async def place_gtt_exit_orders(self, trade: MultiLegTrade) -> bool:
        try:
            if not settings.ENABLE_GTT_ORDERS:
                return True

            for leg in trade.legs:
                if leg.quantity > 0:
                    gtt_order = {
                        "instrument_key": leg.instrument_key,
                        "quantity": abs(leg.quantity),
                        "product": "I",
                        "transaction_type": "SELL",
                        "order_type": "LIMIT",
                        "trigger_price": trade.breakeven_upper if leg.option_type == "CE" else trade.breakeven_lower,
                        "price": trade.breakeven_upper if leg.option_type == "CE" else trade.breakeven_lower,
                        "validity": "GTT"
                    }
                    order_id = await self.api.place_gtt_order(gtt_order)
                    if order_id:
                        trade.gtt_order_ids.append(order_id)
                        logger.info(f"GTT exit order placed for {leg.symbol}: {order_id}")
                    else:
                        logger.warning(f"Failed to place GTT order for {leg.symbol}")
            return True
        except Exception as e:
            logger.error(f"Failed to place GTT exit orders: {e}")
            return False

    async def cancel_all_orders(self, trade: MultiLegTrade) -> bool:
        try:
            if trade.basket_order_id:
                order_ids = trade.basket_order_id.split(",")
                for order_id in order_ids:
                    await self.api.cancel_order(order_id)
            for gtt_id in trade.gtt_order_ids:
                await self.api.cancel_order(gtt_id)
            logger.info(f"All orders cancelled for trade {trade.id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel orders for trade {trade.id}: {e}")
            return False
