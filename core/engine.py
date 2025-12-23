#!/usr/bin/env python3
"""
VolGuard 20.0 – Production-Hardened Engine (Fortress Architecture)
- INTEGRATED: OAuth Token Manager (Auto-Refresh)
- INTEGRATED: Sheriff PID Control (Graceful Kill)
- FIXED: Position Reconciliation Race Conditions (Includes PENDING status)
- FIXED: Async loop initialization for Fortress services
"""
from __future__ import annotations

import asyncio
import time
import hashlib
import logging
import os
from pathlib import Path
from datetime import datetime, time as dtime
from typing import Dict, List, Optional

from sqlalchemy import select, update
from core.config import settings, IST
from core.models import (
    MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics,
    TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason,
)
from database.manager import HybridDatabaseManager
from database.models import DbStrategy, DbRiskState, DbMarketContext
from trading.api_client import EnhancedUpstoxAPI, TokenExpiredError
from trading.token_manager import setup_token_manager
from trading.live_data_feed import LiveDataFeed
from trading.order_manager import EnhancedOrderManager
from trading.risk_manager import AdvancedRiskManager
from trading.trade_manager import EnhancedTradeManager
from capital.allocator import SmartCapitalAllocator
from trading.instruments_master import InstrumentMaster
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from analytics.greek_validator import GreekValidator
from analytics.volatility import HybridVolatilityAnalytics
from trading.strategy_engine import IntelligentStrategyEngine
from utils.data_fetcher import DashboardDataFetcher
from utils.logger import setup_logger
from core.safety_layer import MasterSafetyLayer
from trading.live_order_executor import LiveOrderExecutor
from trading.position_lifecycle import PositionLifecycleManager
from analytics.vrp_zscore import VRPZScoreAnalyzer

logger = setup_logger("Engine")

MAX_CACHE_SIZE      = 5_000
CACHE_TTL_SEC       = 3_600
SAFETY_CHECK_INT    = 5
RECONCILE_INT       = 60

class EngineCircuitBreaker(Exception): pass

class VolGuard20Engine:
    def __init__(self) -> None:
        """
        Initializes non-async components only.
        Async locks and loops are moved to 'setup_async_components'
        """
        logger.info("🛠️  VolGuard-20 FORTRESS ENGINE Initialising")
        self.rt_quotes: Dict[str, float] = {}
        self._greeks_cache: Dict[str, GreeksSnapshot] = {}
        
        # Async placeholders (Initialized in setup_async_components)
        self._cache_lock: asyncio.Lock = None
        self._trade_lock: asyncio.Lock = None
        self._calibration_semaphore: asyncio.Lock = None
        self._thread_pool = None

        # --- External Connectors ---
        self.db   = HybridDatabaseManager()
        self.api  = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        self.token_manager = None

        # --- Instrument Master ---
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)

        # --- Models ---
        self.sabr    = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        # CRITICAL FIX: Link Master to Pricing Engine
        self.pricing.instrument_master = self.instruments_master

        # --- Analytics ---
        self.data_fetcher = DashboardDataFetcher(self.api)
        self.vol_analytics = HybridVolatilityAnalytics(self.data_fetcher)
        self.vrp_zscore = VRPZScoreAnalyzer(self.data_fetcher)

        # --- Data Feed ---
        self.data_feed = LiveDataFeed(self.rt_quotes, self._greeks_cache, self.sabr)

        # --- Risk & Capital ---
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        self.risk_mgr = AdvancedRiskManager(self.db, None)

        # --- Execution Stack ---
        self.om = EnhancedOrderManager(self.api, self.db)
        self.executor = LiveOrderExecutor(self.api, self.om)
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, None, self.capital_allocator,
        )
        self.trade_mgr.feed = self.data_feed
        self.lifecycle_mgr = PositionLifecycleManager(self.trade_mgr)

        # --- Safety & Strategy ---
        self.greek_validator = GreekValidator(self._greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        self.greek_validator.set_instrument_master(self.instruments_master)
        self.strategy_engine = IntelligentStrategyEngine(self.vol_analytics, None, self.capital_allocator, self.pricing)
        self.strategy_engine.set_instruments_master(self.instruments_master)
        
        self.safety_layer = MasterSafetyLayer(
            self.risk_mgr,
            getattr(self.trade_mgr, "margin_guard", None),
            self.lifecycle_mgr,
            self.vrp_zscore,
        )

        # --- Runtime State ---
        self.running = False
        self.trades: List[MultiLegTrade] = []
        self.error_count = 0
        self.last_error_time = 0.0
        self.last_safety_check = 0.0
        self.last_reconcile = 0.0
        self.last_sabr_calib = 0.0
        self.last_known_spot = 0.0
        self.last_metrics: Optional[AdvancedMetrics] = None
        self.ai_verdict = "SAFE"
        self.ai_is_fresh = False

    async def setup_async_components(self):
        """Initializes components requiring a running event loop"""
        self._cache_lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()
        self._calibration_semaphore = asyncio.Lock()
        self._thread_pool = asyncio.get_running_loop().run_in_executor
        logger.info("✅ Async Locks & ThreadPool Bound")

    async def initialize(self) -> None:
        logger.info("🚀  Initialising VolGuard-20 Fortress Services …")
        try:
            await self.setup_async_components()
            
            # --- Write PID for Sheriff Control ---
            try:
                pid_file = Path("data/engine.pid")
                pid_file.parent.mkdir(parents=True, exist_ok=True)
                pid_file.write_text(str(os.getpid()))
                logger.info(f"📝 Engine PID Registered: {os.getpid()}")
            except Exception as e:
                logger.error(f"Failed to write PID file: {e}")

            # --- Setup Token Manager ---
            await self.db.init_db()
            self.token_manager = await setup_token_manager(self.db, self.api)
            asyncio.create_task(self.token_manager.start_refresh_loop())
            logger.info("🔐 Token Manager Active")

            await self.instruments_master.download_and_load()
            await self.data_fetcher.load_all_data()
            await self.om.start()
            await self._restore_from_snapshot()
            
            # Startup Cleanup: Check for open positions and sync immediately
            await self._reconcile_broker_positions()
            
            self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
            self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
            asyncio.create_task(self.data_feed.start())
            
            if settings.GREEK_VALIDATION:
                asyncio.create_task(self.greek_validator.start())
            logger.info("✅  Engine Fully Initialised")
        except Exception as exc:
            logger.exception("🔥  Init Failed")
            raise

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        logger.info("🔁  Engine Loop Started")
        last_reset_date: Optional[datetime] = None
        
        while self.running:
            try:
                now  = datetime.now(IST)
                tick = time.time()
                
                if now.time() >= settings.MARKET_OPEN_TIME and now.date() != last_reset_date:
                    self.safety_layer.reset_daily_counters()
                    last_reset_date = now.date()
                
                if tick - self.last_safety_check > SAFETY_CHECK_INT:
                    is_safe = await self._check_safety_heartbeat()
                    if not is_safe:
                        logger.critical("🛑 SAFETY CHECK FAILED. HALTING.")
                        self.running = False
                        await self.shutdown()
                        break
                    await self._sync_market_context()
                    self.last_safety_check = tick
                
                if tick - self.last_reconcile > RECONCILE_INT:
                    await self._reconcile_broker_positions()
                    self.last_reconcile = tick
                
                live_spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if live_spot > 0:
                    self.last_known_spot = live_spot
                
                spot = self.last_known_spot
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    await self.lifecycle_mgr.monitor_lifecycle(self.trades)
                    await self._calculate_metrics(spot)
                
                market_open = settings.MARKET_OPEN_TIME <= now.time() <= settings.MARKET_CLOSE_TIME
                if market_open and live_spot > 0:
                    if tick - self.last_sabr_calib > 900:
                        asyncio.create_task(self._sabr_calibrate())
                    
                    if self.ai_verdict == "DANGER" and self.ai_is_fresh:
                        if int(tick) % 60 == 0: 
                            logger.warning("⚠️ Trading Paused: AI Sensing Danger.")
                    else:
                        await self._trading_logic(live_spot)
                
                await self.trade_mgr.monitor_active_trades(self.trades)
                
                if tick - self.last_error_time > 60:
                    self.error_count = 0
                
                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)
            except TokenExpiredError:
                logger.error("🔑 Token Expired – Triggering Manager Refresh")
                if self.token_manager:
                    await self.token_manager.get_current_token()
                await asyncio.sleep(5)
            except EngineCircuitBreaker:
                logger.critical("❌ Circuit Breaker Open – Shutting Down")
                await self.shutdown()
                break
            except Exception as exc:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.exception("Cycle Error")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    raise EngineCircuitBreaker from exc
                await asyncio.sleep(1)

    async def shutdown(self) -> None:
        logger.info("🛑  Shutdown Started")
        self.running = False
        if self.token_manager:
            await self.token_manager.stop()
        
        try:
            Path("data/engine.pid").unlink(missing_ok=True)
        except Exception: pass
        
        await self._emergency_flatten()
        await self._save_snapshot()
        await self.api.close()
        logger.info("✅  Shutdown Complete")

    async def _check_safety_heartbeat(self) -> bool:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
                state = res.scalars().first()
                if not state: return True
                lag = (datetime.utcnow() - state.sheriff_heartbeat).total_seconds()
                return not (lag > 45 or state.kill_switch_active)
        except Exception: return True

    async def _sync_market_context(self) -> None:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(select(DbMarketContext).order_by(DbMarketContext.timestamp.desc()).limit(1))
                ctx = res.scalars().first()
                if ctx:
                    self.ai_verdict = ctx.regime
                    self.ai_is_fresh = ctx.is_fresh
        except Exception: pass

    async def _update_greeks_and_risk(self, spot: float) -> None:
        async with self._trade_lock:
            tasks = [self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

    async def _calculate_metrics(self, spot: float) -> None:
        try:
            live_vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 0.0)
            vix = max(live_vix, 10.0)
            rv7, rv28, garch, egarch, ivp, iv_rank = self.vol_analytics.get_volatility_metrics(vix)
            struct = await self.pricing.get_market_structure(spot)
            self.last_metrics = AdvancedMetrics(
                timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp, iv_rank=iv_rank,
                realized_vol_7d=rv7, realized_vol_28d=rv28, garch_vol_7d=garch, egarch_vol_1d=egarch,
                atm_iv=struct.get("atm_iv", 0.0), monthly_iv=struct.get("monthly_iv", 0.0),
                vrp_score=struct.get("atm_iv", 0.0) - rv7 - garch,
                vrp_zscore=self.vrp_zscore.calculate_vrp_zscore(struct.get("atm_iv", 0.0), vix)[0],
                term_structure_spread=struct.get("term_structure_spread", 0.0),
                straddle_price=struct.get("straddle_price", 0.0),
                volatility_skew=struct.get("skew_index", 0.0),
                regime=self.vol_analytics.calculate_volatility_regime(vix, iv_rank),
                trend_status=self.vol_analytics.get_trend_status(spot),
                days_to_expiry=struct.get("days_to_expiry", 0.0),
                sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
                sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu,
            )
        except Exception: pass

    async def _sabr_calibrate(self) -> None:
        if self._calibration_semaphore.locked(): return
        async with self._calibration_semaphore:
            try:
                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self.pricing.calibrate_sabr(spot)
                    self.last_sabr_calib = time.time()
            except Exception: self.sabr.reset()

    async def _trading_logic(self, spot: float) -> None:
        if not self.last_metrics: return
        ai_ctx = {"verdict": self.ai_verdict, "is_fresh": self.ai_is_fresh}
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(
            self.last_metrics, spot, await self.capital_allocator.get_status(), ai_ctx
        )
        if strat == "WAIT": return
        
        trade_id = f"T-{int(time.time() * 1_000)}"
        real_legs = []
        for leg in legs:
            expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
            token = self.instruments_master.get_option_token(settings.UNDERLYING_SYMBOL, leg["strike"], leg["type"], expiry_dt)
            if not token: return
            real_legs.append(Position(
                symbol=settings.UNDERLYING_SYMBOL, instrument_key=token, strike=leg["strike"], option_type=leg["type"],
                quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1), entry_price=0.0, entry_time=datetime.now(IST),
                current_price=0.0, current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                expiry_type=etype, capital_bucket=bucket,
            ))
        
        trade = MultiLegTrade(legs=real_legs, strategy_type=StrategyType(strat), entry_time=datetime.now(IST),
                             expiry_date=legs[0]["expiry"], expiry_type=etype, capital_bucket=bucket, status=TradeStatus.PENDING, id=trade_id)
        
        approved, reason = await self.safety_layer.pre_trade_gate(trade, {"greeks_cache": self._greeks_cache})
        if not approved: return
        
        ok, msg = await self.executor.execute_with_hedge_priority(trade)
        if ok:
            val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
            await self.capital_allocator.allocate_capital(bucket.value, val, trade.id)
            async with self._trade_lock:
                trade.status = TradeStatus.OPEN
                self.trades.append(trade)

    async def _save_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.EXTERNAL):
                        db_obj = DbStrategy(
                            id=trade.id, type=trade.strategy_type.value, status=trade.status.value, entry_time=trade.entry_time,
                            capital_bucket=trade.capital_bucket.value, pnl=trade.total_unrealized_pnl(),
                            expiry_date=datetime.strptime(trade.expiry_date, "%Y-%m-%d").date(),
                            broker_ref_id=trade.basket_order_id, metadata_json={"legs": [l.dict() for l in trade.legs], "lots": trade.lots}
                        )
                        await session.merge(db_obj)
                await self.db.safe_commit(session)
        except Exception: logger.exception("Snapshot Failed")

    async def _restore_from_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                rows = await session.execute(select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value))
                for db_strat in rows.scalars():
                    meta = db_strat.metadata_json or {}
                    legs = [Position(**ld) for ld in meta.get("legs", [])]
                    trade = MultiLegTrade(legs=legs, strategy_type=StrategyType(db_strat.type), entry_time=db_strat.entry_time, 
                                         status=TradeStatus(db_strat.status), expiry_date=str(db_strat.expiry_date),
                                         expiry_type=ExpiryType(legs[0].expiry_type), capital_bucket=CapitalBucket(db_strat.capital_bucket),
                                         id=db_strat.id, basket_order_id=db_strat.broker_ref_id)
                    self.trades.append(trade)
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
        except Exception: logger.exception("Restore Failed")

    async def _reconcile_broker_positions(self) -> None:
        """
        HARDENED v3.0: Checks PENDING status to prevent duplicate zombies.
        """
        try:
            # Lock BEFORE fetch to ensure state consistency
            async with self._trade_lock:
                broker_positions = await self.api.get_short_term_positions()
                if not broker_positions: return
                
                broker_map = {}
                for p in broker_positions:
                    qty = int(p.get("quantity", 0))
                    if qty != 0:
                        broker_map[p["instrument_token"]] = qty
                
                internal_map = {}
                for trade in self.trades:
                    # PATCH: Include PENDING trades so we don't adopt their positions as zombies
                    if trade.status in (TradeStatus.OPEN, TradeStatus.PENDING):
                        for leg in trade.legs:
                            internal_map[leg.instrument_key] = internal_map.get(leg.instrument_key, 0) + leg.quantity
                
                # Check for zombies
                for token, b_qty in broker_map.items():
                    internal_qty = internal_map.get(token, 0)
                    if b_qty != internal_qty:
                        if internal_qty == 0:
                            logger.critical(f"🧟 ZOMBIE FOUND: {token} (Qty: {b_qty}) - Adopting...")
                            await self._adopt_zombie(token, b_qty)
                        else:
                            # Just warn, don't auto-fix partial fills yet
                            logger.error(f"⚠️ Mismatch: {token} (Broker {b_qty} vs Internal {internal_qty})")

        except Exception: logger.exception("Reconciliation Failed")

    async def _adopt_zombie(self, token: str, qty: int) -> None:
        # No lock needed (already held by caller)
        price = self.rt_quotes.get(token, 1.0)
        dummy = Position(symbol=settings.UNDERLYING_SYMBOL, instrument_key=token, strike=0.0, option_type="CE",
                        quantity=qty, entry_price=price, entry_time=datetime.now(IST), current_price=price,
                        current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)), expiry_type=ExpiryType.INTRADAY,
                        capital_bucket=CapitalBucket.INTRADAY)
        trade = MultiLegTrade(legs=[dummy], strategy_type=StrategyType.WAIT, entry_time=datetime.now(IST),
                             expiry_date=datetime.now(IST).strftime("%Y-%m-%d"), status=TradeStatus.EXTERNAL,
                             id=f"ZOMBIE-{int(time.time()*1000)}", capital_bucket=CapitalBucket.INTRADAY, expiry_type=ExpiryType.INTRADAY)
        self.trades.append(trade)
        self.data_feed.subscribe_instrument(token)

    async def _emergency_flatten(self) -> None:
        async with self._trade_lock:
            tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    engine = VolGuard20Engine()
    asyncio.run(engine.run())
