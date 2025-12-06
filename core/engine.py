import asyncio
import time
from datetime import datetime
from typing import List
from sqlalchemy import select
from core.config import settings
from core.models import MultiLegTrade, Position, GreeksSnapshot
from core.enums import (
    TradeStatus,
    StrategyType,
    CapitalBucket,
    ExpiryType,
    ExitReason,
)
from database.manager import HybridDatabaseManager
from database.models import DbStrategy
from trading.api_client import EnhancedUpstoxAPI
from trading.live_data_feed import LiveDataFeed
from trading.order_manager import EnhancedOrderManager
from trading.risk_manager import AdvancedRiskManager
from trading.trade_manager import EnhancedTradeManager
from capital.allocator import SmartCapitalAllocator
from trading.hedge_manager import PortfolioHedgeManager
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from analytics.greek_validator import GreekValidator
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import AdvancedEventIntelligence
from trading.strategy_engine import IntelligentStrategyEngine
from utils.logger import setup_logger

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)

        self.greeks_cache = {}
        self.greek_validator = GreekValidator(
            self.greeks_cache, self.sabr, settings.TRADING_LOOP_INTERVAL
        )
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION
        )
        
        # Initialize V18 Intelligence Modules
        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()
        
        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)
        self.om = EnhancedOrderManager(self.api, self.db)
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        self.hedge_mgr = PortfolioHedgeManager(self.api, self.om, self.risk_mgr)
        
        # Initialize V18 Strategy Engine with V19 Capital Awareness
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics,
            self.event_intel,
            self.capital_allocator
        )
        
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing, self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed

        self.running = False
        self.trades: List[MultiLegTrade] = []
        self.health_task = None
        self.error_count = 0
        self.last_error_time = 0

    async def initialize(self):
        logger.info("🚀 Booting VolGuard 19.0 (Endgame)...")
        await self.db.init_db()
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        
        asyncio.create_task(self.data_feed.start())
        self.health_task = asyncio.create_task(self._system_heartbeat())
        
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
            
        logger.info("✅ Engine Initialized.")

    async def _system_heartbeat(self):
        while self.running:
            await asyncio.sleep(10)
            lag = asyncio.get_event_loop().time() - self.data_feed.last_tick_time
            if lag > 60:
                logger.critical(f"❤️ FEED STALLED ({lag:.0f}s).")
                try:
                    await self.api.get_short_term_positions()
                except Exception:
                    self.error_count += 1

    async def _reconcile_broker_positions(self):
        """Adopt 'zombie' positions that exist at broker but not in internal state."""
        try:
            broker_positions = await self.api.get_short_term_positions()
            broker_map = {
                p["instrument_token"]: int(p["quantity"])
                for p in broker_positions
                if int(p["quantity"]) != 0
            }
            internal_map = {}
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    for l in t.legs:
                        internal_map[l.instrument_key] = (
                            internal_map.get(l.instrument_key, 0) + l.quantity
                        )

            for token, qty in broker_map.items():
                if token not in internal_map:
                    logger.critical(f"🚨 ZOMBIE ADOPTED: {token} Qty: {qty}")
                    dummy_leg = Position(
                        symbol="UNKNOWN",
                        instrument_key=token,
                        strike=0,
                        option_type="CE",
                        quantity=qty,
                        entry_price=0.0,
                        entry_time=datetime.now(settings.IST),
                        current_price=0.0,
                        current_greeks=GreeksSnapshot(
                            timestamp=datetime.now(settings.IST)
                        ),
                        expiry_type=ExpiryType.INTRADAY,
                        capital_bucket=CapitalBucket.INTRADAY,
                    )
                    new_trade = MultiLegTrade(
                        legs=[dummy_leg],
                        strategy_type=StrategyType.WAIT,
                        net_premium_per_share=0.0,
                        entry_time=datetime.now(settings.IST),
                        expiry_date=datetime.now(settings.IST).strftime("%Y-%m-%d"),
                        expiry_type=ExpiryType.INTRADAY,
                        capital_bucket=CapitalBucket.INTRADAY,
                        status=TradeStatus.EXTERNAL,
                    )
                    new_trade.id = f"ZOMBIE-{int(time.time())}"
                    self.trades.append(new_trade)
                    
                    async with self.db.get_session() as session:
                        db_strat = DbStrategy(
                            id=new_trade.id,
                            type="EXTERNAL",
                            status="EXTERNAL",
                            entry_time=new_trade.entry_time,
                            capital_bucket="INTRADAY",
                            metadata_json={
                                "legs": [dummy_leg.dict()],
                                "note": "Auto-adopted zombie",
                            },
                        )
                        await session.merge(db_strat)
        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _restore_from_snapshot(self):
        logger.info("📥 Restoring open trades from DB...")
        async with self.db.get_session() as session:
            result = await session.execute(
                select(DbStrategy).where(DbStrategy.status.in_([TradeStatus.OPEN.value]))
            )
            for db_strat in result.scalars().all():
                if not db_strat.metadata_json:
                    continue
                try:
                    meta = db_strat.metadata_json
                    legs = []
                    for ld in meta.get("legs", []):
                        self.data_feed.subscribe_instrument(ld["instrument_key"])
                        legs.append(Position(**ld))
                    
                    trade = MultiLegTrade(
                        legs=legs,
                        strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time,
                        lots=meta.get("lots", 1),
                        status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date),
                        expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket),
                    )
                    trade.id = db_strat.id
                    trade.basket_order_id = db_strat.broker_ref_id
                    self.trades.append(trade)
                    
                    value = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(
                        trade.capital_bucket.value, value, trade_id=trade.id
                    )
                except Exception as e:
                    logger.error(f"Hydration Failed: {e}")

    async def _update_greeks_and_risk(self, spot: float):
        # 1. Price & Greeks
        tasks = [
            self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes)
            for t in self.trades
            if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

        # 2. Risk State
        total_pnl = 0.0
        for t in self.trades:
            if hasattr(t, "total_unrealized_pnl"):
                total_pnl += t.total_unrealized_pnl()
        
        self.risk_mgr.update_portfolio_state(self.trades, total_pnl)
        
        if self.risk_mgr.check_portfolio_limits():
            logger.critical("🚨 RISK LIMIT BREACHED. FLATTENING.")
            await self._emergency_flatten()

        # 3. Auto-Hedging
        portfolio_delta = self.risk_mgr.portfolio_delta
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        await self.hedge_mgr.check_and_hedge(portfolio_delta, vix)

    async def _emergency_flatten(self):
        logger.critical("🔥 EMERGENCY FLATTEN TRIGGERED 🔥")
        tasks = [
            self.trade_mgr.close_trade(t, ExitReason.RISK_BREACH)
            for t in self.trades
            if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def save_final_snapshot(self):
        async with self.db.get_session() as session:
            for trade in self.trades:
                if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                    legs_json = [l.dict() for l in trade.legs]
                    db_strat = DbStrategy(
                        id=str(trade.id),
                        type=trade.strategy_type.value,
                        status=trade.status.value,
                        entry_time=trade.entry_time,
                        capital_bucket=trade.capital_bucket.value,
                        pnl=trade.total_unrealized_pnl() if hasattr(trade, "total_unrealized_pnl") else 0.0,
                        metadata_json={
                            "legs": legs_json,
                            "order_ids": getattr(trade, "gtt_order_ids", []),
                        },
                        broker_ref_id=getattr(trade, "basket_order_id", None),
                        expiry_date=datetime.strptime(
                            trade.expiry_date, "%Y-%m-%d"
                        ).date(),
                    )
                    await session.merge(db_strat)
            await session.commit()

    async def run(self):
        await self.initialize()
        self.running = True
        while self.running:
            try:
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("💥 TOO MANY ERRORS. SUICIDE.")
                    await self.shutdown()
                    break
                
                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    # Logic to check metrics and potentially enter trades would go here
                    # triggering self.strategy_engine.select_strategy_with_capital(...)
                    await self.trade_mgr.monitor_active_trades(self.trades)
                
                if time.time() - self.last_error_time > 60:
                    self.error_count = 0
            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
            await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

    async def shutdown(self):
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
