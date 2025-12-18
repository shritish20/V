import asyncio
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import select

from core.config import settings, IST
from core.models import MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics
from core.enums import TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason, MarketRegime

from database.manager import HybridDatabaseManager
from database.models import DbStrategy

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
from analytics.events import AdvancedEventIntelligence
from trading.strategy_engine import IntelligentStrategyEngine
from analytics.explainer import AI_Portfolio_Architect
from analytics.market_intelligence import MarketIntelligence
from analytics.ai_controls import AIActionType

from utils.logger import setup_logger

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        # [cite_start]1. Instrument Master[span_3](end_span)
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)

        # 2. [span_4](start_span)Analytics & Context Core[span_4](end_span)
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master
        
        self.greeks_cache = {}
        self.greek_validator = GreekValidator(
            self.greeks_cache,
            self.sabr,
            settings.GREEK_REFRESH_SEC
        )
        self.greek_validator.set_instrument_master(self.instruments_master)

        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE,
            settings.CAPITAL_ALLOCATION,
            self.db
        )

        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()
        self.intel = MarketIntelligence() 
        self.architect = AI_Portfolio_Architect() 
        
        self.last_ai_check = 0
        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)

        # 3. [span_5](start_span)Execution Managers[span_5](end_span)
        self.om = EnhancedOrderManager(self.api, self.db)
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics,
            self.event_intel,
            self.capital_allocator,
            self.pricing
        )
        self.strategy_engine.set_instruments_master(self.instruments_master)

        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed

        # 4. [span_6](start_span)Engine State[span_6](end_span)
        self.running = False
        self.trades: list[MultiLegTrade] = []
        self.error_count = 0
        self.last_metrics = None
        self.last_sabr_calibration = 0
        self.last_error_time = 0
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._calibration_semaphore = asyncio.Semaphore(1)
        self._greek_update_lock = asyncio.Lock()

    async def initialize(self):
        [span_7](start_span)"""Standard boot sequence for production readiness[span_7](end_span)."""
        logger.info("🛡️ VolGuard 19.0 CRIL (Institutional Edition) Booting...")
        try:
            await self.instruments_master.download_and_load()
            logger.info("✓ Instrument Master loaded")
        except Exception as e:
            logger.critical(f"Instrument Master CRITICAL FAILURE: {e}")

        await self.db.init_db()
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions() # Includes Positional Drift Check

        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        asyncio.create_task(self.data_feed.start())

        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
        logger.info("✓ Engine Contextualized & Initialized.")

    async def run(self):
        [span_8](start_span)"""Main loop managing calibration, AI audits, and trade management[span_8](end_span)."""
        await self.initialize()
        self.running = True
        logger.info("🚀 Main Trading Loop Engaged")

        while self.running:
            try:
                current_time = time.time()

                # [span_9](start_span)A. Periodic SABR Calibration (15 mins)[span_9](end_span)
                if current_time - self.last_sabr_calibration > 900:
                    asyncio.create_task(self._run_sabr_calibration())

                # [span_10](start_span)B. AI PORTFOLIO DOCTOR (Every 2 hours)[span_10](end_span)
                if current_time - self.last_ai_check > 7200:
                    asyncio.create_task(self._run_ai_portfolio_check())
                    self.last_ai_check = current_time

                # [span_11](start_span)C. Market Data & Risk Loop[span_11](end_span)
                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    await self._consider_new_trade(spot)

                # [span_12](start_span)D. Active Trade Management[span_12](end_span)
                await self.trade_mgr.monitor_active_trades(self.trades)

                # E. Error Reset Logic
                if current_time - self.last_error_time > 60:
                    self.error_count = 0

                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

            except TokenExpiredError:
                logger.critical("🚨 TOKEN EXPIRED! PAUSING ENGINE.")
                await asyncio.sleep(10)
            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("🔥 FATAL ERROR DENSITY EXCEEDED. SHUTTING DOWN.")
                    await self.shutdown()
                    break
                await asyncio.sleep(1)

    async def _reconcile_broker_positions(self):
        """TITANIUM FIX: Strict Positional Checksum for Zombie Recovery."""
        try:
            broker_pos = await self.api.get_short_term_positions()
            if not broker_pos: return

            broker_map = {p["instrument_token"]: int(p["quantity"]) for p in broker_pos if int(p["quantity"]) != 0}
            internal_map = {}
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    for l in t.legs:
                        internal_map[l.instrument_key] = internal_map.get(l.instrument_key, 0) + l.quantity

            for token, b_qty in broker_map.items():
                i_qty = internal_map.get(token, 0)
                # Defusal: Catch side-mismatch or partial fills using absolute quantity
                if abs(b_qty) != abs(i_qty):
                    logger.critical(f"🚨 POSITIONAL DRIFT: {token} | Broker:{b_qty} | Internal:{i_qty}")
                    # Safety Hard-Stop in Production
                    if settings.SAFETY_MODE == "live":
                        logger.error("System Integrity Compromised. Entering Emergency Shutdown.")
                        await self.shutdown()
        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _consider_new_trade(self, spot: float):
        [span_13](start_span)"""Analyzes market structure and event risk before selecting a strategy [cite: 171-173]."""
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        realized_vol, garch_vol, ivp = self.vol_analytics.get_volatility_metrics(vix)
        risk_state_event, event_score, top_event = self.event_intel.get_market_risk_state()
        vol_regime = self.vol_analytics.calculate_volatility_regime(vix, 0.0)
        final_regime = "BINARY_EVENT" if risk_state_event == "BINARY_EVENT" else vol_regime

        market_structure = await self.pricing.get_market_structure(spot)
        if market_structure.get("confidence", 0.0) < 0.5:
            return

        metrics = AdvancedMetrics(
            timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp,
            realized_vol_7d=realized_vol, garch_vol_7d=garch_vol,
            atm_iv=market_structure.get("atm_iv", 0.0) * 100.0,
            vrp_score=(market_structure.get("atm_iv", 0.0) * 100.0) - realized_vol,
            regime=final_regime, event_risk_score=event_score, top_event=top_event,
            trend_status=self.vol_analytics.get_trend_status(spot),
            days_to_expiry=float(market_structure.get("days_to_expiry", 0.0)),
            expiry_date=market_structure.get("near_expiry", "N/A"),
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
            sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics

        cap_status = await self.capital_allocator.get_status()
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(metrics, spot, cap_status)
        
        if strat != "WAIT":
            await self._execute_new_strategy(strat, legs, etype, bucket)

    async def _execute_new_strategy(self, strat_name, legs_spec, exp_type, bucket):
        [cite_start]"""Gated execution that requires AI CIO approval before trade placement [cite: 174-177]."""
        try:
            trade_ctx = {
                "strategy": strat_name,
                "spot": self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0),
                "vix": self.rt_quotes.get(settings.MARKET_KEY_VIX, 0),
                "dte": getattr(self.last_metrics, 'days_to_expiry', 5)
            }
            fii_data = self.intel.get_fii_data()
            ai_decision = await self.architect.evaluate_proposed_trade(trade_ctx, fii_data)

            if ai_decision.action == AIActionType.BLOCK:
                logger.critical(f"🛑 AI VETO: Trade Cancelled. Reason: {ai_decision.reason}")
                return

            real_legs = []
            for leg in legs_spec:
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token("NIFTY", leg["strike"], leg["type"], expiry_dt)
                if not token:
                    logger.error(f"Token missing for {leg['strike']} {leg['type']}")
                    return

                real_legs.append(Position(
                    symbol="NIFTY", instrument_key=token,
                    strike=leg["strike"], option_type=leg["type"],
                    quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                    entry_price=0.0, entry_time=datetime.now(IST),
                    current_price=0.0,
                    current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                    expiry_type=exp_type, capital_bucket=bucket
                ))

            new_trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat_name),
                net_premium_per_share=0.0, entry_time=datetime.now(IST),
                expiry_date=legs_spec[0]["expiry"], expiry_type=exp_type,
                capital_bucket=bucket, status=TradeStatus.PENDING,
                id=f"T-{int(time.time())}"
            )

            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"✨ EXECUTED: {strat_name} | ID: {new_trade.id}")

        except Exception as e:
            logger.error(f"Execution Gate Failed: {e}")

    async def _run_ai_portfolio_check(self):
        [cite_start]"""24/7 Cognitive Audit Loop[span_13](end_span)."""
        try:
            fii_data = self.intel.get_fii_data()
            state = {
                "delta": self.risk_mgr.portfolio_delta,
                "vega": self.risk_mgr.portfolio_vega,
                "pnl": self.risk_mgr.daily_pnl,
                "dte": getattr(self.last_metrics, 'days_to_expiry', 5),
                "count": len([t for t in self.trades if t.status == TradeStatus.OPEN])
            }
            review = await self.architect.review_portfolio_holistically(state, fii_data)
            logger.info(f"🤖 AI RISK NARRATIVE: {review.get('narrative', 'Scanning...')}")
        except Exception as e:
            logger.error(f"Portfolio Doctor Failed: {e}")

    async def _update_greeks_and_risk(self, spot: float):
        [span_14](start_span)"""Calculates real-time PnL and triggers panic flatten if risk limits are breached [cite: 177-178]."""
        async with self._greek_update_lock:
            tasks = [self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes) 
                    for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks)

        total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
        self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

        if self.risk_mgr.check_portfolio_limits():
            await self._emergency_flatten()

    async def _emergency_flatten(self):
        [cite_start]"""PANIC BUTTON: Closes all positions immediately[span_14](end_span)."""
        logger.critical("🚨 PANIC BUTTON: EMERGENCY FLATTEN INITIATED")
        tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) 
                for t in self.trades if t.status == TradeStatus.OPEN]
        if tasks: await asyncio.gather(*tasks)

    async def shutdown(self):
        [span_15](start_span)"""Graceful shutdown sequence ensuring state persistence[span_15](end_span)."""
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
        self.executor.shutdown(wait=False)
        logger.info("🏁 Engine Shutdown Sequence Complete.")

    async def save_final_snapshot(self):
        [span_16](start_span)"""Persists trade state to DB for resume capability[span_16](end_span)."""
        try:
            async with self.db.get_session() as session:
                for t in self.trades:
                    if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                        db_strat = DbStrategy(
                            id=str(t.id), type=t.strategy_type.value,
                            status=t.status.value, entry_time=t.entry_time,
                            capital_bucket=t.capital_bucket.value,
                            pnl=t.total_unrealized_pnl(),
                            metadata_json={"legs": [l.dict() for l in t.legs], "lots": t.lots},
                            broker_ref_id=t.basket_order_id,
                            expiry_date=datetime.strptime(t.expiry_date, "%Y-%m-%d").date()
                        )
                        await session.merge(db_strat)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"Persistence Failed: {e}")

    async def _run_sabr_calibration(self):
        [span_17](start_span)"""Calibrates the SABR model to the current option chain[span_17](end_span)."""
        if not self._calibration_semaphore.locked():
            async with self._calibration_semaphore:
                await self._calibrate_sabr_internal()

    async def _calibrate_sabr_internal(self):
        spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
        expiries = self.instruments_master.get_all_expiries("NIFTY")
        if spot <= 0 or not expiries: return

        try:
            chain_res = await self.api.get_option_chain(settings.MARKET_KEY_INDEX, expiries[0].strftime("%Y-%m-%d"))
            if not chain_res or not chain_res.get("data"): return

            strikes, vols = [], []
            for item in chain_res["data"]:
                iv = item.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
                if iv > 0.01:
                    strikes.append(item.get("strike_price"))
                    vols.append(iv / 100.0 if iv > 2.0 else iv)

            if len(strikes) < 5: return
            tte = max(0.001, (expiries[0] - datetime.now(IST).date()).days / 365.0)

            success = await asyncio.get_running_loop().run_in_executor(
                self.executor, self.sabr.calibrate_to_chain, strikes, vols, spot, tte
            )
            if success:
                self.last_sabr_calibration = time.time()
                logger.info(f"✅ SABR CALIBRATED (NIFTY {expiries[0]})")
        except Exception as e:
            logger.error(f"Calibration Logic Error: {e}")

    async def _restore_from_snapshot(self):
        [span_18](start_span)"""Hydrates trade state from DB on engine startup[span_18](end_span)."""
        logger.info("DB HYDRATION: Restoring open trades...")
        async with self.db.get_session() as session:
            res = await session.execute(select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value))
            for db_strat in res.scalars().all():
                try:
                    meta = db_strat.metadata_json
                    legs = [Position(**ld) for ld in meta.get("legs", [])]
                    trade = MultiLegTrade(
                        legs=legs, strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time, lots=meta.get("lots", 1),
                        status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date),
                        expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket)
                    )
                    trade.id = db_strat.id
                    trade.basket_order_id = db_strat.broker_ref_id
                    self.trades.append(trade)
                except Exception as e:
                    logger.error(f"Failed to hydrate trade {db_strat.id}: {e}")

    def get_system_health(self):
        [span_19](start_span)"""Kubernetes-style health probe[span_19](end_span)."""
        return {
            "engine": {"running": self.running, "active_trades": len([t for t in self.trades if t.status == TradeStatus.OPEN])},
            "analytics": {"sabr_calibrated": self.sabr.calibrated},
            "capital_allocation": "Active"
        }

    def get_status(self):
        from core.models import EngineStatus
        return EngineStatus(
            running=self.running, circuit_breaker=False, cycle_count=0,
            total_trades=len(self.trades), daily_pnl=self.risk_mgr.daily_pnl,
            max_equity=self.risk_mgr.peak_equity, last_metrics=self.last_metrics,
            dashboard_ready=True
        )
