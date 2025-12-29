#!/usr/bin/env python3
"""
VolGuard 20.0 Fortress Engine (Intelligence Edition)
- Integrated AI Risk Officer
- Real-time Market Intelligence
- Pure Quant Execution
"""
from __future__ import annotations
import asyncio
import time
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import select
from core.config import settings, IST
from core.models import (
    MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics,
    TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason,
)
from database.manager import HybridDatabaseManager
from database.models import DbStrategy, DbRiskState, DbMarketSnapshot

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
from trading.live_order_executor import LiveOrderExecutor, RollbackFailure
from trading.position_lifecycle import PositionLifecycleManager
from analytics.vrp_zscore import VRPZScoreAnalyzer
from core.metrics import get_metrics
from core.market_session import MarketSessionManager

# NEW IMPORT
from analytics.ai_risk_officer import AIRiskOfficer

logger = setup_logger("Engine")

STALE_DATA_THRESHOLD = 5.0
SAFETY_CHECK_INT = 5
RECONCILE_INT = 60

class StaleDataError(RuntimeError): pass
class EngineCircuitBreaker(Exception): pass

class VolGuard20Engine:
    def __init__(self) -> None:
        logger.info("🧠 VolGuard-20 INTELLIGENCE ENGINE Initialising...")
        
        # Async State
        self.rt_quotes: Dict[str, Dict] = {}
        self._greeks_cache: Dict[str, GreeksSnapshot] = {}
        self._cache_lock = None
        self._trade_lock = None
        self._calibration_semaphore = None
        
        # Infrastructure
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        self.token_manager = None
        
        # Masters
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        
        # Quant Core
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master
        
        # Analytics
        self.data_fetcher = DashboardDataFetcher(self.api)
        self.vol_analytics = HybridVolatilityAnalytics(self.data_fetcher)
        self.vrp_zscore = VRPZScoreAnalyzer(self.data_fetcher)
        self.data_feed = LiveDataFeed(self.rt_quotes, self._greeks_cache, self.sabr)
        
        # Risk & Capital
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        
        # Execution
        self.om = EnhancedOrderManager(self.api, self.db)
        self.executor = LiveOrderExecutor(self.api, self.om)
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed
        self.lifecycle_mgr = PositionLifecycleManager(self.trade_mgr)
        
        # Safety & Validators
        self.greek_validator = GreekValidator(self._greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        self.greek_validator.set_instrument_master(self.instruments_master)
        
        # Intelligence (NEW)
        self.ai_officer = None
        if settings.GROQ_API_KEY:
            self.ai_officer = AIRiskOfficer(settings.GROQ_API_KEY, self.db)
        
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, None, self.capital_allocator, self.pricing
        )
        self.strategy_engine.set_instruments_master(self.instruments_master)
        
        # Safety Layer (Updated with AI)
        self.safety_layer = MasterSafetyLayer(
            self.risk_mgr, self.trade_mgr.margin_guard, self.lifecycle_mgr,
            self.vrp_zscore, self.ai_officer
        )
        
        # Runtime
        self.running = False
        self.trades: List[MultiLegTrade] = []
        self.error_count = 0
        self.last_metrics = None
        self.metrics = get_metrics()
        self.market_session = MarketSessionManager(self.api)
        
        # Timers
        self.last_error_time = 0.0
        self.last_safety_check = 0.0
        self.last_reconcile = 0.0
        self.last_sabr_calib = 0.0

    def _get_safe_price(self, token: str) -> float:
        data = self.rt_quotes.get(token)
        if not data:
            raise StaleDataError(f"No market data available for {token}")
        
        lag = time.time() - data.get('last_updated', 0)
        if lag > STALE_DATA_THRESHOLD:
            self.metrics.log_stale_data(token)
            if int(time.time()) % 10 == 0:
                logger.warning(f"⏱️ STALE DATA {token}: {lag:.1f}s lag")
            raise StaleDataError(f"Data lag {lag:.1f}s > {STALE_DATA_THRESHOLD}s")
            
        return data.get('ltp', 0.0)

    async def setup_async_components(self):
        self._cache_lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()
        self._calibration_semaphore = asyncio.Lock()

    async def initialize(self) -> None:
        logger.info("⚙️ Initialising Engine Services...")
        await self.setup_async_components()
        
        pid_file = Path("data/engine.pid")
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(os.getpid()))
        
        await self.db.init_db()
        
        self.token_manager = await setup_token_manager(self.db, self.api)
        asyncio.create_task(self.token_manager.start_refresh_loop())
        
        await self.instruments_master.download_and_load()
        
        # Initial AI Learning
        if self.ai_officer:
            logger.info("🧠 Loading Historical Patterns...")
            await self.ai_officer.learn_from_history()
        
        # Historical Data
        today = datetime.now(IST).date()
        from_dt = today.replace(year=today.year - 1)
        await self.market_session.refresh()
        
        if self.market_session.can_fetch_historical(from_dt, today):
            logger.info("📜 Fetching Historical Data...")
            await self.data_fetcher.load_all_data()
        else:
            logger.info("⏩ Skipping History Fetch (Intraday)")
            
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        
        if self.market_session.can_use_websocket():
            asyncio.create_task(self.data_feed.start())
            
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
            
        logger.info("✅ Engine Fully Initialised")

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        logger.info("🚦 Engine Loop Started")
        
        last_reset_date = None
        consecutive_stale_errors = 0
        
        while self.running:
            try:
                now = datetime.now(IST)
                tick = time.time()
                
                # Daily Reset
                if now.time() >= settings.MARKET_OPEN_TIME and now.date() != last_reset_date:
                    self.safety_layer.reset_daily_counters()
                    self.metrics.reset_daily_counters()
                    last_reset_date = now.date()
                
                # Safety Check
                if tick - self.last_safety_check > SAFETY_CHECK_INT:
                    if not await self._check_safety_heartbeat():
                        logger.critical("🛑 SAFETY KILL SWITCH ACTIVE. HALTING.")
                        self.running = False
                        await self.shutdown()
                        break
                    self.last_safety_check = tick
                
                # Market Session
                await self.market_session.refresh()
                if self.market_session.current_mode() != "LIVE_MARKET":
                    await asyncio.sleep(5)
                    continue
                
                # Data Pipeline
                try:
                    spot = self._get_safe_price(settings.MARKET_KEY_INDEX)
                    vix = self._get_safe_price(settings.MARKET_KEY_VIX)
                    consecutive_stale_errors = 0
                    self.data_fetcher.inject_live_candle(spot, vix)
                except StaleDataError:
                    consecutive_stale_errors += 1
                    if consecutive_stale_errors >= 10:
                        if self.data_feed: self.data_feed.disconnect()
                        consecutive_stale_errors = 0
                    await asyncio.sleep(1)
                    continue
                
                # Core Logic
                await self._update_greeks_and_risk(spot)
                await self.lifecycle_mgr.monitor_lifecycle(self.trades)
                
                if tick - self.last_reconcile > RECONCILE_INT:
                    await self._reconcile_broker_positions()
                    self.last_reconcile = tick
                
                await self._calculate_metrics(spot, vix)
                
                # Strategy Execution
                if settings.MARKET_OPEN_TIME <= now.time() <= settings.MARKET_CLOSE_TIME:
                    if tick - self.last_sabr_calib > 900:
                        asyncio.create_task(self._sabr_calibrate())
                    
                    # Pass context for AI
                    try:
                        await self._trading_logic(spot)
                    except StaleDataError:
                        pass
                
                await self.trade_mgr.monitor_active_trades(self.trades)
                
                # Error Decay
                if tick - self.last_error_time > 60:
                    self.error_count = 0
                
                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)
                
            except TokenExpiredError:
                if self.token_manager: await self.token_manager.get_current_token()
                await asyncio.sleep(5)
            except EngineCircuitBreaker:
                await self.shutdown()
                break
            except Exception as exc:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {exc}")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    raise EngineCircuitBreaker from exc
                await asyncio.sleep(1)

    async def _trading_logic(self, spot: float) -> None:
        if not self.market_session.can_trade(): return
        if not self.last_metrics: return
        
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(
            self.last_metrics, spot, await self.capital_allocator.get_status()
        )
        
        if strat == "WAIT": return
        
        trade_id = f"T-{int(time.time() * 1_000)}"
        real_legs = []
        
        for leg in legs:
            expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
            token = self.instruments_master.get_option_token(
                settings.UNDERLYING_SYMBOL, leg["type"], expiry_dt
            )
            if not token: return
            
            real_legs.append(Position(
                symbol=settings.UNDERLYING_SYMBOL, instrument_key=token,
                strike=leg["strike"], option_type=leg["type"],
                quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                entry_price=0.0, entry_time=datetime.now(IST),
                current_price=0.0, current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                expiry_type=etype, capital_bucket=bucket
            ))
            
        trade = MultiLegTrade(
            id=trade_id, legs=real_legs, strategy_type=StrategyType(strat),
            status=TradeStatus.PENDING, entry_time=datetime.now(IST),
            expiry_date=legs[0]["expiry"], expiry_type=etype, capital_bucket=bucket
        )
        
        # PASS Metrics to Safety Layer for AI Validation
        metrics_dict = {
            "vix": self.last_metrics.vix,
            "spot_price": spot,
            "greeks_cache": self._greeks_cache,
            "ivp": self.last_metrics.ivp
        }
        
        approved, reason = await self.safety_layer.pre_trade_gate(trade, metrics_dict)
        
        if not approved:
            self.metrics.log_trade(success=False, trade_id=trade.id, strategy=strat, reason=reason)
            return
            
        # Execution
        logger.info(f"🚀 Executing {strat} (AI Approved)")
        val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
        await self.capital_allocator.allocate_capital(bucket.value, val, trade.id)
        
        try:
            await self.executor.execute_with_hedge_priority(trade)
            async with self._trade_lock:
                trade.status = TradeStatus.OPEN
                self.trades.append(trade)
                self.metrics.log_trade(success=True, trade_id=trade.id, strategy=strat)
        except RollbackFailure as e:
            logger.critical(f"🔥 ROLLBACK FAILED: {e}")
            raise EngineCircuitBreaker from e

    # ... (Standard helpers _sabr_calibrate, _save_snapshot, _restore, etc. remain unchanged)
    
    async def shutdown(self) -> None:
        self.running = False
        logger.info("🛑 Engine Shutdown Sequence...")
        if self.token_manager: await self.token_manager.stop()
        Path("data/engine.pid").unlink(missing_ok=True)
        await self._emergency_flatten()
        await self._save_snapshot()
        await self.api.close()
        logger.info("👋 Engine Offline")

    async def _check_safety_heartbeat(self) -> bool:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(
                    select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
                )
                state = res.scalars().first()
                if not state: return True
                lag = (datetime.utcnow() - state.sheriff_heartbeat).total_seconds()
                return not (lag > 45 or state.kill_switch_active)
        except: return True

    # ... (Include other standard helpers from original file to ensure completeness)
    async def _update_greeks_and_risk(self, spot: float) -> None:
        async with self._trade_lock:
            flat_quotes = {}
            for token, data in self.rt_quotes.items():
                if time.time() - data.get('last_updated', 0) <= STALE_DATA_THRESHOLD:
                    flat_quotes[token] = data.get('ltp', 0.0)
            
            tasks = [self.trade_mgr.update_trade_prices(t, spot, flat_quotes) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

    async def _calculate_metrics(self, spot: float, vix: float) -> None:
        try:
            rv7, rv28, garch, egarch, ivp, iv_rank = self.vol_analytics.get_volatility_metrics(vix)
            struct = await self.pricing.get_market_structure(spot)
            
            self.last_metrics = AdvancedMetrics(
                timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp, iv_rank=iv_rank,
                realized_vol_7d=rv7, realized_vol_28d=rv28, garch_vol_7d=garch, egarch_vol_1d=egarch,
                atm_iv=struct.get("atm_iv", 0.0), monthly_iv=struct.get("monthly_iv", 0.0),
                vrp_score=struct.get("atm_iv", 0.0) - rv7,
                vrp_zscore=self.vrp_zscore.calculate_vrp_zscore(struct.get("atm_iv", 0.0), vix)[0],
                term_structure_spread=struct.get("term_structure_spread", 0.0),
                volatility_skew=struct.get("skew_index", 0.0),
                straddle_price=struct.get("straddle_price", 0.0),
                straddle_price_monthly=struct.get("straddle_price_monthly", 0.0),
                regime=self.vol_analytics.calculate_volatility_regime(vix, iv_rank),
                efficiency_table=struct.get("efficiency_table", [])
            )
            
            if self.last_metrics.atm_iv > 0:
                async with self.db.get_session() as session:
                    snapshot = DbMarketSnapshot(
                        timestamp=datetime.utcnow(), spot_price=spot, vix=vix,
                        atm_iv_weekly=self.last_metrics.atm_iv,
                        atm_iv_monthly=self.last_metrics.monthly_iv,
                        iv_spread=self.last_metrics.term_structure_spread,
                        term_structure_tag="Backwardation" if self.last_metrics.term_structure_spread > 0 else "Contango",
                        rv_7d=self.last_metrics.realized_vol_7d,
                        garch_vol_7d=self.last_metrics.garch_vol_7d,
                        egarch_vol_1d=self.last_metrics.egarch_vol_1d,
                        iv_percentile=self.last_metrics.ivp,
                        vrp_spread=self.last_metrics.vrp_score,
                        vrp_zscore=self.last_metrics.vrp_zscore,
                        vrp_verdict=self.last_metrics.regime,
                        straddle_cost_weekly=self.last_metrics.straddle_price,
                        straddle_cost_monthly=self.last_metrics.straddle_price_monthly,
                        breakeven_lower=spot - self.last_metrics.straddle_price,
                        breakeven_upper=spot + self.last_metrics.straddle_price,
                        chain_json=self.last_metrics.efficiency_table
                    )
                    session.add(snapshot)
                    await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"Metric Calc Error: {e}")

    async def _sabr_calibrate(self) -> None:
        if self._calibration_semaphore.locked(): return
        async with self._calibration_semaphore:
            try:
                spot = self._get_safe_price(settings.MARKET_KEY_INDEX)
                if spot > 0:
                    await self.pricing.calibrate_sabr(spot)
                    self.last_sabr_calib = time.time()
            except: self.sabr.use_cached_params()

    async def _save_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.EXTERNAL):
                        db_obj = DbStrategy(
                            id=trade.id, type=trade.strategy_type.value, status=trade.status.value,
                            entry_time=trade.entry_time, capital_bucket=trade.capital_bucket.value,
                            pnl=trade.total_unrealized_pnl(),
                            expiry_date=datetime.strptime(trade.expiry_date, "%Y-%m-%d").date(),
                            metadata_json={"legs": [l.dict() for l in trade.legs], "lots": trade.lots}
                        )
                        await session.merge(db_obj)
                await self.db.safe_commit(session)
        except: logger.exception("Snapshot Failed")

    async def _restore_from_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                rows = await session.execute(select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value))
                for db_strat in rows.scalars():
                    legs = [Position(**ld) for ld in (db_strat.metadata_json or {}).get("legs", [])]
                    trade = MultiLegTrade(
                        id=db_strat.id, strategy_type=StrategyType(db_strat.type), legs=legs,
                        entry_time=db_strat.entry_time, status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date), expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket)
                    )
                    self.trades.append(trade)
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
        except: logger.exception("Restore Failed")

    async def _reconcile_broker_positions(self) -> None:
        try:
            async with self._trade_lock:
                broker_pos = await self.api.get_short_term_positions()
                if not broker_pos: return
                
                broker_map = {p["instrument_token"]: int(p.get("quantity", 0)) for p in broker_pos if int(p.get("quantity", 0)) != 0}
                internal_map = {}
                
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.PENDING):
                        for leg in trade.legs:
                            internal_map[leg.instrument_key] = internal_map.get(leg.instrument_key, 0) + leg.quantity
                
                for token, qty in broker_map.items():
                    if qty != internal_map.get(token, 0) and internal_map.get(token, 0) == 0:
                        logger.critical(f"🧟 ZOMBIE FOUND: {token} Qty: {qty}")
                        await self._adopt_zombie(token, qty)
        except: logger.exception("Reconciliation Failed")

    async def _adopt_zombie(self, token: str, qty: int) -> None:
        if not self.instruments_master.df is None:
            try:
                row = self.instruments_master.df[self.instruments_master.df['instrument_key'] == token].iloc[0]
                price = self._get_safe_price(token)
                trade = MultiLegTrade(
                    id=f"ZOMBIE-{int(time.time())}",
                    legs=[Position(
                        symbol=row['underlying_symbol'], instrument_key=token, strike=float(row['strike_price']),
                        option_type=row['instrument_type'], quantity=qty, entry_price=price, current_price=price,
                        entry_time=datetime.now(IST), current_greeks=GreeksSnapshot(),
                        expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY
                    )],
                    strategy_type=StrategyType.WAIT, status=TradeStatus.EXTERNAL,
                    entry_time=datetime.now(IST), expiry_date=str(datetime.now(IST).date()),
                    expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY
                )
                self.trades.append(trade)
                self.data_feed.subscribe_instrument(token)
            except: pass

    async def _emergency_flatten(self) -> None:
        async with self._trade_lock:
            tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    engine = VolGuard20Engine()
    asyncio.run(engine.run())
