from core.models import MultiLegTrade, Order, OrderStatus
from core.enums import TradeStatus, ExitReason
from core.config import settings
from utils.logger import setup_logger

logger = setup_logger("TradeMgr")

class EnhancedTradeManager:
    def __init__(self, api, db, om, pricing, risk, alerts, capital):
        self.api = api
        self.db = db
        self.om = om
        self.pricing = pricing
        self.risk = risk
        self.alerts = alerts
        self.capital = capital
        self.feed = None 

    async def execute_strategy(self, trade: MultiLegTrade) -> bool:
        if not self.risk.check_pre_trade(trade):
            return False
        
        executed_legs = []
        for leg in trade.legs:
            if leg.current_greeks.iv <= 0 or leg.current_greeks.iv > 500:
                logger.error(f"Sanity Check Failed: IV {leg.current_greeks.iv}")
                return False
                
            order = Order(
                instrument_key=leg.instrument_key,
                transaction_type="BUY" if leg.quantity > 0 else "SELL",
                quantity=abs(leg.quantity),
                price=leg.entry_price,
                order_type="LIMIT",
                product="I",
            )
            filled = await self.om.place_and_monitor(order)
            if filled.status == OrderStatus.FILLED:
                leg.entry_price = filled.average_price
                executed_legs.append(leg)
            else:
                logger.error(f"Leg failed: {leg.instrument_key}. Rolling back.")
                await self._rollback_trade(executed_legs)
                return False
        
        trade.status = TradeStatus.OPEN
        return True

    async def _rollback_trade(self, legs):
        for leg in legs:
            order = Order(
                instrument_key=leg.instrument_key,
                transaction_type="SELL" if leg.quantity > 0 else "BUY",
                quantity=abs(leg.quantity),
                price=0.0,
                order_type="MARKET",
                product="I",
            )
            await self.om.place_and_monitor(order)

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        logger.info(f"Closing Trade {trade.id} Reason: {reason}")
        for leg in trade.legs:
            order = Order(
                instrument_key=leg.instrument_key,
                transaction_type="SELL" if leg.quantity > 0 else "BUY",
                quantity=abs(leg.quantity),
                price=0.0,
                order_type="MARKET",
                product="I",
            )
            await self.om.place_and_monitor(order)
        
        trade.status = TradeStatus.CLOSED
        trade.exit_reason = reason
        await self.capital.release_capital(trade.capital_bucket.value, trade.id)

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: dict):
        updated = False
        for leg in trade.legs:
            if leg.instrument_key in quotes:
                leg.current_price = quotes[leg.instrument_key]
                updated = True
        
        if updated:
            trade.calculate_trade_greeks()

    async def monitor_active_trades(self, trades):
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue
            
            total_pnl = 0.0
            for leg in trade.legs:
                ltp = (
                    self.feed.rt_quotes.get(leg.instrument_key, leg.current_price)
                    if self.feed
                    else leg.current_price
                )
                total_pnl += (ltp - leg.entry_price) * leg.quantity
            
            stop_loss_amt = -(trade.entry_premium * settings.STOP_LOSS_PCT)
            if total_pnl <= stop_loss_amt:
                logger.warning(
                    f"🛑 STOP LOSS TRIGGERED: Trade {trade.id} PnL {total_pnl:.2f}"
                )
                await self.close_trade(trade, ExitReason.STOP_LOSS)
                continue
            
            target_profit = trade.entry_premium * settings.TAKE_PROFIT_PCT
            if total_pnl >= target_profit:
                logger.success(
                    f"💰 TARGET HIT: Trade {trade.id} PnL {total_pnl:.2f}"
                )
                await self.close_trade(trade, ExitReason.PROFIT_TARGET)
