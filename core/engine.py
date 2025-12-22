#!/usr/bin/env python3
"""
VolGuard 20.0 – Production-Hardened Engine (Fortress Architecture)
- FIXED: rt_quotes initialization order
"""
from __future__ import annotations

import asyncio
import time
import hashlib
import logging
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
        logger.info("🛠️  VolGuard-20 FORTRESS ENGINE Initialising")

        # --- FIX 1: Initialize Shared State FIRST ---
        self.rt_quotes: Dict[str, float] = {}
        self._greeks_cache: Dict[str, GreeksSnapshot] = {}
        self._cache_lock = asyncio.Lock()
        
        # --- External Connectors ---
        self.db   = HybridDatabaseManager()
        self.api  = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)

        # --- Instrument Universe ---
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)

        # --- Models ---
        self.sabr    = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master

        self.greek_validator = GreekValidator(
            self._greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC
        )
        self.greek_validator.set_instrument_master(self.instruments_master)

        # --- Capital & Risk ---
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        self.risk_mgr = AdvancedRiskManager(self.db, None)

        # --- Data Pipelines ---
        self.data_fetcher = DashboardDataFetcher(self.api)
        self.vol_analytics = HybridVolatilityAnalytics(self.data_fetcher)
        
        # Now this works because self.rt_quotes exists
        self.data_feed = LiveDataFeed(self.rt_quotes, self._greeks_cache, self.sabr)

        # --- Strategy Stack ---
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, None, self.capital_allocator, self.pricing
        )
        self.strategy_engine.set_instruments_master(self.instruments_master)

        # --- Trade / Order Execution ---
        self.om = EnhancedOrderManager(self.api, self.db)
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, None, self.capital_allocator,
        )
        self.trade_mgr.feed = self.data_feed
        self.executor = LiveOrderExecutor(self.api, self.om)

        # --- Lifecycle ---
        self.lifecycle_mgr = PositionLifecycleManager(self.trade_mgr)

        # --- Safety Layer ---
        self.safety_layer = MasterSafetyLayer(
            self.risk_mgr,
            getattr(self.trade_mgr, "margin_guard", None),
            self.lifecycle_mgr,
            VRPZScoreAnalyzer(self.data_fetcher),
        )

        # --- Runtime State ---
        self.running            = False
        self.trades: List[MultiLegTrade] = []
        self._trade_lock        = asyncio.Lock()
        self.error_count        = 0
        self.last_error_time    = 0.0
        self.last_safety_check  = 0.0
        self.last_reconcile     = 0.0
        self.last_sabr_calib    = 0.0
        self.last_known_spot    = 0.0
        self.last_metrics: Optional[AdvancedMetrics] = None
        self.ai_verdict = "SAFE"
        self.ai_is_fresh = False
        self._thread_pool = asyncio.get_running_loop().run_in_executor
        self._calibration_semaphore = asyncio.Lock()

        logger.info("✅ Engine Skeleton Ready")

    # ... (Rest of the class methods remain exactly the same) ...
    # Copy the 'initialize', 'run', 'shutdown', etc. from previous version
    
    async def initialize(self) -> None:
        logger.info("🚀  Initialising VolGuard-20 …")
        try:
            await self.instruments_master.download_and_load()
            await self.data_fetcher.load_all_data()
            await self.db.init_db()
            await self.om.start()
            await self._restore_from_snapshot()
            await self._reconcile_broker_positions()
            self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
            self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
            asyncio.create_task(self.data_feed.start())
            if settings.GREEK_VALIDATION:
                asyncio.create_task(self.greek_validator.start())
            logger.info("✅  Engine Initialised")
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
                        logger.critical("🛑 SAFETY CHECK FAILED (Dead Sheriff or Kill Switch). HALTING.")
                        self.running = False
                        await self.shutdown()
                        break
                    await self._sync_market_context()
                    try:
                        await self.api.check_token_validity()
                    except TokenExpiredError:
                         logger.critical("🔑 Token Expired in Loop - Halting")
                         self.running = False
                         break
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
                        if tick % 60 == 0: 
                            logger.warning("⚠️ Trading Paused: AI actively sensing danger.")
                    else:
                        await self._trading_logic(live_spot)
                    await self.trade_mgr.monitor_active_trades(self.trades)
                if tick - self.last_error_time > 60:
                    self.error_count = 0
                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)
            except TokenExpiredError:
                logger.error("🔑  Token Expired – Pausing 10s")
                await asyncio.sleep(10)
            except EngineCircuitBreaker:
                logger.critical("❌  Circuit Breaker Open – Shutting Down")
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
        await self._emergency_flatten()
        await self._save_snapshot()
        await self.api.close()
        logger.info("✅  Shutdown Complete")

    async def _check_safety_heartbeat(self) -> bool:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(
                    select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
                )
                state = res.scalars().first()
                if not state: return True
                lag = (datetime.utcnow() - state.sheriff_heartbeat).total_seconds()
                if lag > 45: return False
                if state.kill_switch_active: return False
                return True
        except Exception:
            return True

    async def _sync_market_context(self) -> None:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(
                    select(DbMarketContext).order_by(DbMarketContext.timestamp.desc()).limit(1)
                )
                ctx = res.scalars().first()
                if ctx:
                    self.ai_verdict = ctx.regime
                    self.ai_is_fresh = ctx.is_fresh
        except Exception:
            pass
    
    @staticmethod
    def _make_client_order_id(trade_id: str, leg_idx: int, side: str) -> str:
        payload = f"{trade_id}#{leg_idx}#{side}"
        return "VG" + hashlib.blake2b(payload.encode(), digest_size=8).hexdigest().upper()

    async def _get_greek(self, token: str) -> Optional[GreeksSnapshot]:
        async with self._cache_lock: return self._greeks_cache.get(token)

    async def _set_greek(self, token: str, greek: GreeksSnapshot) -> None:
        async with self._cache_lock:
            if len(self._greeks_cache) >= MAX_CACHE_SIZE:
                self._greeks_cache.pop(next(iter(self._greeks_cache)))
            self._greeks_cache[token] = greek

    async def _evict_stale_cache(self) -> None:
        async with self._cache_lock:
            cutoff = time.time() - CACHE_TTL_SEC
            to_pop = [k for k, v in self._greeks_cache.items() if v.timestamp.timestamp() < cutoff]
            for k in to_pop: self._greeks_cache.pop(k, None)

    async def _trading_logic(self, spot: float) -> None:
        if not self.last_metrics: return
        ai_ctx = {"verdict": self.ai_verdict, "is_fresh": self.ai_is_fresh}
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(
            self.last_metrics, spot, await self.capital_allocator.get_status(), ai_ctx
        )
        if strat == "WAIT": return
        trade_id = f"T-{int(time.time() * 1_000)}"
        real_legs: List[Position] = []
        try:
            for idx, leg in enumerate(legs):
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token(
                    settings.UNDERLYING_SYMBOL, leg["strike"], leg["type"], expiry_dt
                )
                if not token: return
                qty = settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1)
                real_legs.append(
                    Position(
                        symbol=settings.UNDERLYING_SYMBOL, instrument_key=token,
                        strike=leg["strike"], option_type=leg["type"], quantity=qty,
                        entry_price=0.0, entry_time=datetime.now(IST), current_price=0.0,
                        current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                        expiry_type=etype, capital_bucket=bucket,
                    )
                )
            trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat),
                net_premium_per_share=0.0, entry_time=datetime.now(IST),
                expiry_date=legs[0]["expiry"], expiry_type=etype,
                capital_bucket=bucket, status=TradeStatus.PENDING, id=trade_id,
            )
            for idx, leg in enumerate(trade.legs):
                leg.client_order_id = self._make_client_order_id(trade.id, idx, "BUY" if leg.quantity > 0 else "SELL")
            approved, reason = await self.safety_layer.pre_trade_gate(trade, {"greeks_cache": self._greeks_cache})
            if not approved:
                logger.info(f"Trade blocked by safety: {reason}")
                return
            ok, msg = await self.executor.execute_with_hedge_priority(trade)
            if ok:
                val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                await self.capital_allocator.allocate_capital(bucket.value, val, trade.id)
                async with self._trade_lock:
                    trade.status = TradeStatus.OPEN
                    self.trades.append(trade)
                logger.info(f"Trade Executed: {trade.id}")
            else:
                logger.error(f"Execution Failed: {msg}")
        except Exception:
            logger.exception("Trading Logic Error")

    async def _update_greeks_and_risk(self, spot: float) -> None:
        async with self._trade_lock:
            tasks = [
                self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes)
                for t in self.trades if t.status == TradeStatus.OPEN
            ]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

    async def _calculate_metrics(self, spot: float) -> None:
        try:
            live_vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 0.0)
            if live_vix == 0 and not self.data_fetcher.vix_data.empty:
                live_vix = self.data_fetcher.vix_data["close"].iloc[-1]
            vix = max(live_vix, 10.0)
            rv7, rv28, garch, egarch, ivp, iv_rank = self.vol_analytics.get_volatility_metrics(vix)
            struct = await self.pricing.get_market_structure(spot)
            self.last_metrics = AdvancedMetrics(
                timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp, iv_rank=iv_rank,
                realized_vol_7d=rv7, realized_vol_28d=rv28, garch_vol_7d=garch, egarch_vol_1d=egarch,
                atm_iv=struct.get("atm_iv", 0.0), monthly_iv=struct.get("monthly_iv", 0.0),
                vrp_score=struct.get("atm_iv", 0.0) - rv7 - garch,
                spread_rv=struct.get("atm_iv", 0.0) - rv7,
                vrp_zscore=self.vrp_zscore.calculate_vrp_zscore(struct.get("atm_iv", 0.0), vix)[0],
                term_structure_spread=struct.get("term_structure_spread", 0.0),
                straddle_price=struct.get("straddle_price", 0.0),
                straddle_price_monthly=struct.get("straddle_price_monthly", 0.0),
                atm_theta=struct.get("atm_theta", 0.0), atm_vega=struct.get("atm_vega", 0.0),
                atm_delta=struct.get("atm_delta", 0.0), atm_gamma=struct.get("atm_gamma", 0.0),
                atm_pop=struct.get("atm_pop", 0.0), volatility_skew=struct.get("skew_index", 0.0),
                structure_confidence=struct.get("confidence", 0.0),
                regime=self.vol_analytics.calculate_volatility_regime(vix, iv_rank),
                event_risk_score=0.0, top_event="",
                trend_status=self.vol_analytics.get_trend_status(spot),
                days_to_expiry=struct.get("days_to_expiry", 0.0),
                expiry_date=struct.get("near_expiry", ""),
                pcr=struct.get("pcr", 1.0), max_pain=struct.get("max_pain", spot),
                efficiency_table=struct.get("efficiency_table", []),
                sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
                sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu,
                term_structure_slope=0.0,
            )
        except Exception:
            pass

    async def _sabr_calibrate(self) -> None:
        if self._calibration_semaphore.locked(): return
        async with self._calibration_semaphore:
            await self._calibrate_sabr_internal()

    async def _calibrate_sabr_internal(self) -> None:
        try:
            spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
            if spot <= 0: return
            await self.pricing.calibrate_sabr(spot)
            self.last_sabr_calib = time.time()
        except Exception:
            self.sabr.reset()

    async def _save_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.EXTERNAL):
                        db_obj = DbStrategy(
                            id=trade.id, type=trade.strategy_type.value,
                            status=trade.status.value, entry_time=trade.entry_time,
                            capital_bucket=trade.capital_bucket.value,
                            pnl=trade.total_unrealized_pnl(),
                            expiry_date=datetime.strptime(trade.expiry_date, "%Y-%m-%d").date(),
                            broker_ref_id=trade.basket_order_id,
                            metadata_json={"legs": [l.dict() for l in trade.legs], "lots": trade.lots},
                        )
                        await session.merge(db_obj)
                await self.db.safe_commit(session)
        except Exception:
            logger.exception("Snapshot Failed")

    async def _restore_from_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                rows = await session.execute(
                    select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value)
                )
                for db_strat in rows.scalars():
                    meta = db_strat.metadata_json or {}
                    legs = [Position(**ld) for ld in meta.get("legs", [])]
                    trade = MultiLegTrade(
                        legs=legs, strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time, lots=meta.get("lots", 1),
                        status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date),
                        expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket),
                        id=db_strat.id, basket_order_id=db_strat.broker_ref_id,
                    )
                    self.trades.append(trade)
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
        except Exception:
            logger.exception("Restore Failed")

    async def _reconcile_broker_positions(self) -> None:
        try:
            broker_positions = await self.api.get_short_term_positions()
            if not broker_positions: return
            broker_map = {p["instrument_token"]: int(p["quantity"]) for p in broker_positions if int(p["quantity"]) != 0}
            internal_map = {}
            async with self._trade_lock:
                for trade in self.trades:
                    if trade.status == TradeStatus.OPEN:
                        for leg in trade.legs:
                            internal_map[leg.instrument_key] = internal_map.get(leg.instrument_key, 0) + leg.quantity
            for token, b_qty in broker_map.items():
                i_qty = internal_map.get(token, 0)
                if b_qty != i_qty:
                    logger.warning(f"Mismatch: {token} Broker={b_qty} Internal={i_qty}")
                    if i_qty == 0: await self._adopt_zombie(token, b_qty)
        except Exception:
            logger.exception("Reconciliation Failed")

    async def _adopt_zombie(self, token: str, qty: int) -> None:
        logger.critical(f"Adopting Zombie: {token} Qty: {qty}")
        price = self.rt_quotes.get(token, 1.0)
        dummy = Position(
            symbol=settings.UNDERLYING_SYMBOL, instrument_key=token, strike=0.0,
            option_type="CE", quantity=qty, entry_price=price, entry_time=datetime.now(IST),
            current_price=price, current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
            expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY,
        )
        trade = MultiLegTrade(
            legs=[dummy], strategy_type=StrategyType.WAIT, net_premium_per_share=0.0,
            entry_time=datetime.now(IST), expiry_date=datetime.now(IST).strftime("%Y-%m-%d"),
            expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY,
            status=TradeStatus.EXTERNAL, id=f"ZOMBIE-{int(time.time() * 1_000)}",
        )
        async with self._trade_lock:
            self.trades.append(trade)
        self.data_feed.subscribe_instrument(token)

    async def _emergency_flatten(self) -> None:
        logger.critical("🚨 ENGINE FLATTENING POSITIONS")
        async with self._trade_lock:
            tasks = [
                self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER)
                for t in self.trades if t.status == TradeStatus.OPEN
            ]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        self.safety_layer.is_halted = True

if __name__ == "__main__":
    engine = VolGuard20Engine()
    asyncio.run(engine.run())
