import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from core.config import settings, IST
from core.models import MultiLegTrade, Position, GreeksSnapshot, TradeStatus, ExitReason
from core.enums import StrategyType, CapitalBucket, ExpiryType

logger = logging.getLogger("VolGuard18")

class EnhancedTradeManager:
    def __init__(self, api, db, om, pricing, risk_mgr, alerts, capital_allocator):
        self.api = api
        self.db = db
        self.om = om
        self.pricing = pricing
        self.risk_mgr = risk_mgr
        self.alerts = alerts
        self.capital_allocator = capital_allocator

    async def execute_strategy(self, strategy_name: str, legs_spec: List[Dict], lots: int,
                               spot: float, expiry_type: ExpiryType,
                               capital_bucket: CapitalBucket) -> Optional[MultiLegTrade]:
        try:
            entry_time = datetime.now(IST)
            expiry_date = self._get_expiry_date(expiry_type)
            legs = []
            total_premium = 0.0
            capital_required = 0.0

            for spec in legs_spec:
                strike = spec["strike"]
                option_type = spec["option_type"]
                quantity = spec["quantity"] * lots * settings.LOT_SIZE
                symbol = f"NIFTY{strike}{option_type}"

                ltp = await self.api.get_quotes([symbol])
                price = ltp.get(symbol, {}).get("last_price", 0.0)
                if price == 0.0:
                    logger.warning(f"Could not fetch price for {symbol}")
                    return None

                greeks = self.pricing.calculate_greeks(spot, strike, option_type, expiry_date.isoformat())
                position = Position(
                    symbol=symbol,
                    instrument_key=f"NSE_FO|{symbol}{expiry_date.isoformat().replace('-', '')}",
                    strike=strike,
                    option_type=option_type,
                    quantity=quantity,
                    entry_price=price,
                    entry_time=entry_time,
                    current_price=price,
                    current_greeks=greeks,
                    expiry_type=expiry_type,
                    capital_bucket=capital_bucket
                )
                legs.append(position)
                total_premium += price * abs(quantity)
                capital_required += price * abs(quantity)

            net_premium_per_share = total_premium / (lots * settings.LOT_SIZE)

            if not self.capital_allocator.allocate_capital(capital_bucket.value, capital_required):
                logger.warning(f"Failed to allocate capital for strategy {strategy_name}")
                return None

            trade = MultiLegTrade(
                legs=legs,
                strategy_type=StrategyType(strategy_name),
                net_premium_per_share=net_premium_per_share,
                entry_time=entry_time,
                lots=lots,
                status=TradeStatus.OPEN,
                expiry_date=expiry_date.isoformat(),
                expiry_type=expiry_type,
                capital_bucket=capital_bucket
            )

            success = await self.om.place_basket_order(trade)
            if success:
                await self.om.place_gtt_exit_orders(trade)
                await self.db.save_trade(trade)
                logger.info(f"✅ Trade executed: {strategy_name} for {capital_bucket.value}")
                return trade
            else:
                logger.error(f"Failed to place orders for trade {trade.id}")
                self.capital_allocator.release_capital(capital_bucket.value, capital_required)
                return None

        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
            return None

    async def update_trade_prices(self, trade: MultiLegTrade, spot: float, quotes: Dict[str, float]):
        try:
            for leg in trade.legs:
                leg.update_price(quotes.get(leg.symbol, leg.current_price))
                leg.update_greeks(self.pricing.calculate_greeks(spot, leg.strike, leg.option_type, trade.expiry_date))
            trade.calculate_trade_greeks()
        except Exception as e:
            logger.error(f"Failed to update trade prices: {e}")

    async def manage_trade_exits(self, trade: MultiLegTrade, metrics: AdvancedMetrics, spot: float) -> Optional[ExitReason]:
        return await self.risk_mgr.should_exit_trade(trade, metrics, spot)

    async def close_trade(self, trade: MultiLegTrade, reason: ExitReason):
        try:
            logger.info(f"Closing trade {trade.id} due to {reason.value}")
            trade.exit_reason = reason
            trade.exit_time = datetime.now(IST)
            trade.status = TradeStatus.CLOSED

            await self.om.cancel_all_orders(trade)

            for leg in trade.legs:
                exit_price = await self._get_exit_price(leg)
                leg.update_price(exit_price)

            pnl = trade.total_unrealized_pnl()
            self.capital_allocator.release_capital(trade.capital_bucket.value,
                                                   sum(abs(leg.entry_price * leg.quantity) for leg in trade.legs))
            self.capital_allocator.update_performance(trade.capital_bucket.value, pnl, pnl > 0)

            await self.db.save_trade(trade)
            await self.alerts.send_alert("TRADE_EXIT", f"Trade {trade.id} closed with PnL: ₹{pnl:,.2f}")
            logger.info(f"Trade {trade.id} closed. PnL: ₹{pnl:,.2f}")
        except Exception as e:
            logger.error(f"Failed to close trade {trade.id}: {e}")

    async def _get_exit_price(self, position: Position) -> float:
        try:
            quotes = await self.api.get_quotes([position.symbol])
            return quotes.get(position.symbol, {}).get("last_price", position.current_price)
        except:
            return position.current_price

    def _get_expiry_date(self, expiry_type: ExpiryType) -> datetime:
        from datetime import timedelta
        today = datetime.now(IST).date()
        if expiry_type == ExpiryType.WEEKLY:
            return today + timedelta(days=7 - today.weekday() + 3)  # Next Thursday
        elif expiry_type == ExpiryType.MONTHLY:
            from calendar import monthrange
            last_day = monthrange(today.year, today.month)[1]
            return today.replace(day=min(last_day, today.day + 30))
        else:
            return today
