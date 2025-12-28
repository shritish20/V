#!/usr/bin/env python3
"""
VolGuard 20.0 Fortress Engine (V3 Hybrid) – 24×7 Market-Aware Edition
- DATA SAFETY: 5-second freshness check on every tick.
- SINGLETON DB: Prevents AWS pool exhaustion.
- STRATEGY: Pure Quant (No AI).
- HYBRID: V3 Execution / V2 Option Chain
- MARKET-AWARE: WebSocket only when NSE is open; historical fetch only after 7 PM.
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
from database.models import DbStrategy, DbRiskState, DbMarketContext, DbMarketSnapshot
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

logger = setup_logger("Engine")

STALE_DATA_THRESHOLD = 5.0
SAFETY_CHECK_INT = 5
RECONCILE_INT = 60


class StaleDataError(RuntimeError): pass
class EngineCircuitBreaker(Exception): pass


class VolGuard20Engine:
    def __init__(self) -> None:
        logger.info("🛡️ VolGuard-20 FORTRESS ENGINE Initialising (V3 Hybrid – 24×7)")
        self.rt_quotes: Dict[str, Dict] = {}
        self._greeks_cache: Dict[str, GreeksSnapshot] = {}
        self._cache_lock: asyncio.Lock | None = None
        self._trade_lock: asyncio.Lock | None = None
        self._calibration_semaphore: asyncio.Lock | None = None
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        self.token_manager = None
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master
        self.data_fetcher = DashboardDataFetcher(self.api)
        self.vol_analytics = HybridVolatilityAnalytics(self.data_fetcher)
        self.vrp_zscore = VRPZScoreAnalyzer(self.data_fetcher)
        self.data_feed = LiveDataFeed(self.rt_quotes, self._greeks_cache, self.sabr)
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        self.om = EnhancedOrderManager(self.api, self.db)
        self.executor = LiveOrderExecutor(self.api, self.om)
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed
        self.lifecycle_mgr = PositionLifecycleManager(self.trade_mgr)
        self.greek_validator = GreekValidator(self._greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        self.greek_validator.set_instrument_master(self.instruments_master)
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, None, self.capital_allocator, self.pricing
        )
        self.strategy_engine.set_instruments_master(self.instruments_master)
        self.safety_layer = MasterSafetyLayer(
            self.risk_mgr, self.trade_mgr.margin_guard, self.lifecycle_mgr, self.vrp_zscore
        )
        self.running = False
        self.trades: List[MultiLegTrade] = []
        self.error_count = 0
        self.last_error_time = 0.0
        self.last_safety_check = 0.0
        self.last_reconcile = 0.0
        self.last_sabr_calib = 0.0
        self.last_metrics: Optional[AdvancedMetrics] = None
        self.metrics = get_metrics()
        self.market_session = MarketSessionManager(self.api)

    # --------------- CRITICAL FIX #1: Stale Data Guard -------------
    def _get_safe_price(self, token: str) -> float:
        data = self.rt_quotes.get(token)
        if not data:
            raise StaleDataError(f"No market data available for {token}")
        lag = time.time() - data.get('last_updated', 0)
        if lag > STALE_DATA_THRESHOLD:
            self.metrics.log_stale_data(token)
            if int(time.time()) % 10 == 0:
                logger.warning(f"⚠️ STALE DATA for {token}: {lag:.1f}s lag")
            raise StaleDataError(f"Data for {token} is {lag:.1f}s old (threshold: {STALE_DATA_THRESHOLD}s)")
        return data.get('ltp', 0.0)

    async def setup_async_components(self):
        self._cache_lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()
        self._calibration_semaphore = asyncio.Lock()
        logger.info("✅ Async Locks Bound")

    async def initialize(self) -> None:
        logger.info("🚀 Initialising VolGuard-20 Services...")
        await self.setup_async_components()
        pid_file = Path("data/engine.pid")
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(str(os.getpid()))
        await self.db.init_db()
        self.token_manager = await setup_token_manager(self.db, self.api)
        asyncio.create_task(self.token_manager.start_refresh_loop())
        await self.instruments_master.download_and_load()
        # -------- market-aware historical fetch --------
        today = datetime.now(IST).date()
        from_dt = today.replace(year=today.year - 1)
        await self.market_session.refresh()
        if self.market_session.can_fetch_historical(from_dt, today):
            logger.info("📚 EOD data available — fetching historical")
            await self.data_fetcher.load_all_data()
        else:
            logger.info("📚 Skipping historical fetch (EOD not ready)")
        # ------------------------------------------------
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        # -------- market-aware websocket start --------
        if self.market_session.can_use_websocket():
            logger.info("📡 Market open — starting WebSocket")
            asyncio.create_task(self.data_feed.start())
        else:
            logger.info("🌙 Market closed — WebSocket disabled")
        # ------------------------------------------------
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
        logger.info("✅ Engine Fully Initialised")

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        logger.info("🟢 Engine Loop Started (Quant Mode – 24×7)")
        last_reset_date: Optional[datetime] = None
        consecutive_stale_errors = 0
        while self.running:
            try:
                now = datetime.now(IST)
                tick = time.time()

                # ---------- daily reset ----------
                if now.time() >= settings.MARKET_OPEN_TIME and now.date() != last_reset_date:
                    self.safety_layer.reset_daily_counters()
                    self.metrics.reset_daily_counters()
                    last_reset_date = now.date()

                # ---------- safety heartbeat ----------
                if tick - self.last_safety_check > SAFETY_CHECK_INT:
                    if not await self._check_safety_heartbeat():
                        logger.critical("🛑 SAFETY KILL SWITCH ACTIVE. HALTING.")
                        self.running = False
                        await self.shutdown()
                        break
                    self.last_safety_check = tick

                # ---------- market session controller ----------
                await self.market_session.refresh()
                mode = self.market_session.current_mode()
                if mode != "LIVE_MARKET":
                    await asyncio.sleep(5)
                    continue
                # ----------------------------------------------

                # ---------- safe price & LIVE INJECTION ----------
                try:
                    spot = self._get_safe_price(settings.MARKET_KEY_INDEX)
                    vix  = self._get_safe_price(settings.MARKET_KEY_VIX)
                    consecutive_stale_errors = 0
                    
                    # 🔥 NEW: INJECT LIVE CANDLE FOR REAL-TIME GARCH/IVP
                    self.data_fetcher.inject_live_candle(spot, vix)

                except StaleDataError as e:
                    consecutive_stale_errors += 1
                    logger.warning(f"⚠️ Stale data ({consecutive_stale_errors}/10): {e}")
                    if consecutive_stale_errors >= 10:
                        logger.critical("🚨 PERSISTENT STALE DATA – reconnecting feed")
                        if self.data_feed:
                            self.data_feed.disconnect()
                        consecutive_stale_errors = 0
                    await asyncio.sleep(1)
                    continue

                await self._update_greeks_and_risk(spot)
                await self.lifecycle_mgr.monitor_lifecycle(self.trades)

                if tick - self.last_reconcile > RECONCILE_INT:
                    await self._reconcile_broker_positions()
                    self.last_reconcile = tick

                await self._calculate_metrics(spot, vix)

                market_open = settings.MARKET_OPEN_TIME <= now.time() <= settings.MARKET_CLOSE_TIME
                if market_open:
                    if tick - self.last_sabr_calib > 900:
                        asyncio.create_task(self._sabr_calibrate())
                    try:
                        await self._trading_logic(spot)
                    except StaleDataError as e:
                        logger.warning(f"⚠️ Skipping trade cycle: {e}")
                        continue

                await self.trade_mgr.monitor_active_trades(self.trades)

                if tick - self.last_error_time > 60:
                    self.error_count = 0

                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

            except TokenExpiredError:
                if self.token_manager:
                    await self.token_manager.get_current_token()
                await asyncio.sleep(5)
            except EngineCircuitBreaker:
                await self.shutdown()
                break
            except StaleDataError:
                await asyncio.sleep(1)
                continue
            except Exception as exc:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.exception("Cycle Error")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    raise EngineCircuitBreaker from exc
                await asyncio.sleep(1)

    async def shutdown(self) -> None:
        logger.info("🛑 Shutdown Started")
        self.running = False
        if self.token_manager:
            await self.token_manager.stop()
        try:
            Path("data/engine.pid").unlink(missing_ok=True)
        except Exception:
            pass
        await self._emergency_flatten()
        await self._save_snapshot()
        await self.api.close()
        logger.info("✅ Shutdown Complete")

    # ---------- remainder of engine helpers (unchanged) ----------
    async def _check_safety_heartbeat(self) -> bool:
        try:
            async with self.db.get_session() as session:
                res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
                state = res.scalars().first()
                if not state:
                    return True
                lag = (datetime.utcnow() - state.sheriff_heartbeat).total_seconds()
                return not (lag > 45 or state.kill_switch_active)
        except Exception:
            return True

    async def _update_greeks_and_risk(self, spot: float) -> None:
        async with self._trade_lock:
            flat_quotes = {}
            for token, data in self.rt_quotes.items():
                lag = time.time() - data.get('last_updated', 0)
                if lag <= STALE_DATA_THRESHOLD:
                    flat_quotes[token] = data.get('ltp', 0.0)
            tasks = [self.trade_mgr.update_trade_prices(t, spot, flat_quotes) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

    async def _calculate_metrics(self, spot: float, vix: float) -> None:
        try:
            rv7, rv28, garch, egarch, ivp, iv_rank = self.vol_analytics.get_volatility_metrics(vix)
            struct = await self.pricing.get_market_structure(spot)
            self.last_metrics = AdvancedMetrics(
                timestamp=datetime.now(IST), spot_price=spot, vix=vix,
                ivp=ivp, iv_rank=iv_rank,
                realized_vol_7d=rv7, realized_vol_28d=rv28,
                garch_vol_7d=garch, egarch_vol_1d=egarch,
                atm_iv=struct.get("atm_iv", 0.0),
                monthly_iv=struct.get("monthly_iv", 0.0),
                vrp_score=struct.get("atm_iv", 0.0) - rv7 - garch,
                vrp_zscore=self.vrp_zscore.calculate_vrp_zscore(struct.get("atm_iv", 0.0), vix)[0],
                term_structure_spread=struct.get("term_structure_spread", 0.0),
                straddle_price=struct.get("straddle_price", 0.0),
                straddle_price_monthly=struct.get("straddle_price_monthly", 0.0),
                volatility_skew=struct.get("skew_index", 0.0),
                regime=self.vol_analytics.calculate_volatility_regime(vix, iv_rank),
                trend_status=self.vol_analytics.get_trend_status(spot),
                days_to_expiry=struct.get("days_to_expiry", 0.0),
                sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
                sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu,
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
                        vrp_spread=self.last_metrics.atm_iv - self.last_metrics.realized_vol_7d,
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
        if self._calibration_semaphore.locked():
            return
        async with self._calibration_semaphore:
            try:
                spot = self._get_safe_price(settings.MARKET_KEY_INDEX)
                if spot > 0:
                    await self.pricing.calibrate_sabr(spot)
                    self.last_sabr_calib = time.time()
            except Exception:
                self.sabr.use_cached_params()

    async def _trading_logic(self, spot: float) -> None:
        if not self.market_session.can_trade():
            return
        if not self.last_metrics:
            return
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(
            self.last_metrics, spot, await self.capital_allocator.get_status()
        )
        if strat == "WAIT":
            return
        trade_id = f"T-{int(time.time() * 1_000)}"
        real_legs = []
        for leg in legs:
            expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
            token = self.instruments_master.get_option_token(settings.UNDERLYING_SYMBOL,
                                                             leg["strike"], leg["type"], expiry_dt)
            if not token:
                return
            real_legs.append(Position(
                symbol=settings.UNDERLYING_SYMBOL, instrument_key=token,
                strike=leg["strike"], option_type=leg["type"],
                quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                entry_price=0.0, entry_time=datetime.now(IST),
                current_price=0.0,
                current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                expiry_type=etype, capital_bucket=bucket,
            ))
        trade = MultiLegTrade(
            legs=real_legs, strategy_type=StrategyType(strat),
            entry_time=datetime.now(IST), expiry_date=legs[0]["expiry"],
            expiry_type=etype, capital_bucket=bucket,
            status=TradeStatus.PENDING, id=trade_id
        )
        approved, reason = await self.safety_layer.pre_trade_gate(trade, {"greeks_cache": self._greeks_cache})
        if not approved:
            return
        try:
            ok, msg = await self.executor.execute_with_hedge_priority(trade)
            if ok:
                val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                await self.capital_allocator.allocate_capital(bucket.value, val, trade.id)
                async with self._trade_lock:
                    trade.status = TradeStatus.OPEN
                    self.trades.append(trade)
                    self.metrics.log_trade(success=True, trade_id=trade.id, strategy=strat)
                    logger.info(f"✅ Trade {trade.id} opened successfully")
            else:
                self.metrics.log_trade(success=False, trade_id=trade.id, strategy=strat, reason=msg)
                logger.warning(f"⚠️ Trade {trade.id} execution failed: {msg}")
        except RollbackFailure as e:
            logger.critical("🚨🚨🚨 ROLLBACK FAILURE DETECTED 🚨🚨🚨")
            logger.critical(f"Trade ID: {trade.id}")
            logger.critical(f"Error: {e}")
            self.running = False
            try:
                if hasattr(self, 'alerts') and self.alerts:
                    await self.alerts.send_critical_alert(
                        f"ROLLBACK FAILURE - Trade {trade.id}\n"
                        f"System halted. Manual position closure required.\n"
                        f"Check logs immediately."
                    )
            except:
                pass
            raise EngineCircuitBreaker(f"Rollback failure - manual intervention required") from e

    # ---------- rest of helpers (unchanged) ----------
    async def _save_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.EXTERNAL):
                        db_obj = DbStrategy(
                            id=trade.id, type=trade.strategy_type.value, status=trade.status.value,
                            entry_time=trade.entry_time,
                            capital_bucket=trade.capital_bucket.value,
                            pnl=trade.total_unrealized_pnl(),
                            expiry_date=datetime.strptime(trade.expiry_date, "%Y-%m-%d").date(),
                            broker_ref_id=trade.basket_order_id,
                            metadata_json={"legs": [l.dict() for l in trade.legs], "lots": trade.lots}
                        )
                        await session.merge(db_obj)
                await self.db.safe_commit(session)
        except Exception:
            logger.exception("Snapshot Failed")

    async def _restore_from_snapshot(self) -> None:
        try:
            async with self.db.get_session() as session:
                rows = await session.execute(select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value))
                for db_strat in rows.scalars():
                    meta = db_strat.metadata_json or {}
                    legs = [Position(**ld) for ld in meta.get("legs", [])]
                    trade = MultiLegTrade(
                        legs=legs, strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time, status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date),
                        expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket),
                        id=db_strat.id, basket_order_id=db_strat.broker_ref_id
                    )
                    self.trades.append(trade)
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
        except Exception:
            logger.exception("Restore Failed")

    async def _reconcile_broker_positions(self) -> None:
        try:
            async with self._trade_lock:
                broker_positions = await self.api.get_short_term_positions()
                if not broker_positions:
                    return
                broker_map = {p["instrument_token"]: int(p.get("quantity", 0)) for p in broker_positions if int(p.get("quantity", 0)) != 0}
                internal_map = {}
                for trade in self.trades:
                    if trade.status in (TradeStatus.OPEN, TradeStatus.PENDING):
                        for leg in trade.legs:
                            internal_map[leg.instrument_key] = internal_map.get(leg.instrument_key, 0) + leg.quantity
                for token, b_qty in broker_map.items():
                    if b_qty != internal_map.get(token, 0) and internal_map.get(token, 0) == 0:
                        logger.critical(f"🧟 ZOMBIE FOUND: {token} (Qty: {b_qty}) - Adopting...")
                        await self._adopt_zombie(token, b_qty)
        except Exception:
            logger.exception("Reconciliation Failed")

    async def _adopt_zombie(self, token: str, qty: int) -> None:
        logger.info(f"🧟 Attempting to adopt zombie: {token} (Qty: {qty})")
        if not self.instruments_master or self.instruments_master.df is None:
            logger.error("❌ Cannot adopt zombie - instrument master not loaded")
            return
        try:
            instrument_row = self.instruments_master.df[self.instruments_master.df['instrument_key'] == token]
            if instrument_row.empty:
                logger.error(f"❌ Zombie token {token} not found in instrument master")
                return
            instrument = instrument_row.iloc[0]
            strike = float(instrument['strike_price'])
            option_type = instrument['instrument_type']
            symbol = instrument.get('underlying_symbol', settings.UNDERLYING_SYMBOL)
        except Exception as e:
            logger.error(f"❌ Failed to parse instrument data for {token}: {e}")
            return
        try:
            price = self._get_safe_price(token)
        except StaleDataError:
            price = max(strike * 0.95, 50.0)
            logger.warning(f"⚠ Zombie {token} fallback price ₹{price:.2f}")
        dummy = Position(
            symbol=symbol, instrument_key=token, strike=strike, option_type=option_type,
            quantity=qty, entry_price=price, entry_time=datetime.now(IST),
            current_price=price,
            current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
            expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY
        )
        trade = MultiLegTrade(
            legs=[dummy], strategy_type=StrategyType.WAIT, entry_time=datetime.now(IST),
            expiry_date=datetime.now(IST).strftime("%Y-%m-%d"), status=TradeStatus.EXTERNAL,
            id=f"ZOMBIE-{int(time.time()*1000)}", capital_bucket=CapitalBucket.INTRADAY,
            expiry_type=ExpiryType.INTRADAY
        )
        self.trades.append(trade)
        self.data_feed.subscribe_instrument(token)
        logger.info(f"✅ Zombie adopted: {symbol} {strike} {option_type} | Qty: {qty}")

    async def _emergency_flatten(self) -> None:
        async with self._trade_lock:
            tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    engine = VolGuard20Engine()
    asyncio.run(engine.run())
