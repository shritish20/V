import asyncio
import time
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
from utils.logger import setup_logger

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        # 1. Instrument Master (The Phonebook)
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        
        # 2. Analytics Core
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master # Explicit Link
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)

        self.greeks_cache = {}
        self.greek_validator = GreekValidator(self.greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        self.greek_validator.set_instrument_master(self.instruments_master)
        
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )

        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()
        self.architect = AI_Portfolio_Architect()
        
        self.last_ai_check = 0
        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)
        
        # 3. Execution Managers
        self.om = EnhancedOrderManager(self.api, self.db)
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, self.event_intel, self.capital_allocator, self.pricing
        )
        self.strategy_engine.set_instruments_master(self.instruments_master)

        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing, self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed

        # 4. Engine State
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
        logger.info("🚀 Booting VolGuard 20.0 (Institutional Edition)...")
        
        # A. Download Instruments (Real Data Only)
        try:
            await self.instruments_master.download_and_load()
            logger.info("✅ Instrument Master loaded")
        except Exception as e:
            logger.critical(f"Instrument Master CRITICAL FAILURE: {e}")
            # In production, we might want to exit here if no data
            
        # B. Database & Order Manager
        await self.db.init_db()
        await self.om.start()
        
        # C. Restore State
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        
        # D. Start Feeds
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        asyncio.create_task(self.data_feed.start())
        
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
            
        logger.info("✅ Engine Initialized.")

    async def run(self):
        await self.initialize()
        self.running = True
        logger.info("🟢 Engine Loop Started")
        
        while self.running:
            try:
                current_time = time.time()
                
                # 1. Periodic SABR Calibration (15 mins)
                if current_time - self.last_sabr_calibration > 900:
                    asyncio.create_task(self._run_sabr_calibration())
                
                # 2. Portfolio Doctor (Hourly)
                if current_time - self.last_ai_check > 3600:
                    asyncio.create_task(self._run_ai_portfolio_check())
                    self.last_ai_check = current_time

                # 3. Market Data & Risk Loop
                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    await self._consider_new_trade(spot)

                # 4. Monitor Trades
                await self.trade_mgr.monitor_active_trades(self.trades)

                # 5. Reset Error Counter
                if current_time - self.last_error_time > 60:
                    self.error_count = 0

                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

            except TokenExpiredError:
                logger.critical("⛔ TOKEN EXPIRED! Pausing...")
                await asyncio.sleep(10)
            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("💥 Too many errors. Shutdown.")
                    await self.shutdown()
                    break
                await asyncio.sleep(1)

    async def _consider_new_trade(self, spot: float):
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        
        # 1. ANALYTICS
        realized_vol, garch_vol, ivp = self.vol_analytics.get_volatility_metrics(vix)
        
        # 2. EVENT INTEL
        risk_state, event_score, top_event = self.event_intel.get_market_risk_state() 
        
        # 3. MARKET STRUCTURE (Deep Scan)
        market_structure = await self.pricing.get_market_structure(spot)
        
        # -- REAL DATA EXTRACTION --
        atm_iv_decimal = market_structure.get("atm_iv", 0.0) # e.g. 0.145
        confidence = market_structure.get("confidence", 0.0)
        dte = market_structure.get("days_to_expiry", 0.0)
        
        real_straddle_price = market_structure.get("straddle_price", 0.0)
        real_term_slope = market_structure.get("term_structure", 0.0)
        real_skew_idx = market_structure.get("skew_index", 0.0)
        
        # VRP Calculation (Percent based)
        # Convert decimal IV (0.145) to Percent (14.5) for proper comparison
        atm_iv_percent = atm_iv_decimal * 100.0
        vrp_score = atm_iv_percent - garch_vol
        
        # 4. METRICS PACKAGE
        metrics = AdvancedMetrics(
            timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp,
            
            # Pro Metrics
            realized_vol_7d=realized_vol, garch_vol_7d=garch_vol,
            atm_iv=atm_iv_percent, vrp_score=vrp_score,
            
            # Structure
            term_structure_slope=real_term_slope, 
            volatility_skew=real_skew_idx,
            straddle_price=real_straddle_price,
            structure_confidence=confidence,
            
            # Risk Context
            regime=risk_state, event_risk_score=event_score, top_event=top_event,
            trend_status=self.vol_analytics.get_trend_status(spot),
            
            # Execution
            days_to_expiry=float(dte),
            expiry_date=market_structure.get("near_expiry", "N/A"),
            
            pcr=1.0, max_pain=spot,
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
            sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics

        # 5. DATA INTEGRITY CHECK (Strict)
        if confidence < 0.5:
            # If Sunday or bad data, we STOP here. No trading.
            return

        # 6. STRATEGY SELECTION
        cap_status = await self.capital_allocator.get_status()
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(metrics, spot, cap_status)
        
        if strat != "WAIT":
            trade_ctx = {
                "strategy": strat, "spot": spot, "vix": vix, 
                "vrp": vrp_score, "event": top_event, "dte": dte
            }
            asyncio.create_task(self._log_ai_trade_opinion(trade_ctx))
            await self._execute_new_strategy(strat, legs, etype, bucket)

    async def _log_ai_trade_opinion(self, trade_ctx):
        try:
            analysis = await self.architect.analyze_trade_setup(trade_ctx)
            risk = analysis.get("risk_level", "UNKNOWN")
            logger.info(f"🤖 AI OBSERVER: {trade_ctx['strategy']} | Risk: {risk} | {analysis.get('narrative')}")
        except Exception as e:
            logger.error(f"AI Observer Error: {e}")

    async def _execute_new_strategy(self, strat_name, legs_spec, exp_type, bucket):
        real_legs = []
        try:
            for leg in legs_spec:
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token(
                    "NIFTY", leg["strike"], leg["type"], expiry_dt
                )
                if not token: return
                
                real_legs.append(Position(
                    symbol="NIFTY", instrument_key=token,
                    strike=leg["strike"], option_type=leg["type"],
                    quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                    entry_price=0.0, entry_time=datetime.now(settings.IST),
                    current_price=0.0, current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                    expiry_type=exp_type, capital_bucket=bucket
                ))
            
            new_trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat_name),
                net_premium_per_share=0.0, entry_time=datetime.now(settings.IST),
                expiry_date=legs_spec[0]["expiry"], expiry_type=exp_type,
                capital_bucket=bucket, status=TradeStatus.PENDING,
                id=f"T-{int(time.time())}"
            )
            
            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"✅ OPENED: {strat_name} (DTE: {self.last_metrics.days_to_expiry:.1f})")
        except Exception as e:
            logger.error(f"Execution logic failed: {e}")

    async def _update_greeks_and_risk(self, spot: float):
        async with self._greek_update_lock:
            tasks = []
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    tasks.append(self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes))
            if tasks: await asyncio.gather(*tasks)
                
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)
            
            if self.risk_mgr.check_portfolio_limits():
                await self._emergency_flatten()

    async def _emergency_flatten(self):
        logger.critical("🚨 EMERGENCY FLATTEN TRIGGERED")
        tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) 
                 for t in self.trades if t.status == TradeStatus.OPEN]
        if tasks: await asyncio.gather(*tasks)

    async def save_final_snapshot(self):
        try:
            async with self.db.get_session() as session:
                for t in self.trades:
                    if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                        legs_json = [l.dict() for l in t.legs]
                        db_strat = DbStrategy(
                            id=str(t.id), type=t.strategy_type.value, status=t.status.value,
                            capital_bucket=t.capital_bucket.value, entry_time=t.entry_time,
                            pnl=t.total_unrealized_pnl(), metadata_json={"legs": legs_json, "lots": t.lots},
                            broker_ref_id=t.basket_order_id,
                            expiry_date=datetime.strptime(t.expiry_date, "%Y-%m-%d").date()
                        )
                        await session.merge(db_strat)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"Snapshot save failed: {e}")

    async def shutdown(self):
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
        self.executor.shutdown(wait=False)
        logger.info("🛑 Engine Shutdown Complete")

    async def _run_sabr_calibration(self):
        if not self._calibration_semaphore.locked():
            async with self._calibration_semaphore:
                await self._calibrate_sabr_internal()

    async def _calibrate_sabr_internal(self):
        # Full Implementation of SABR Calibration
        spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
        if spot <= 0: return

        expiries = self.instruments_master.get_all_expiries("NIFTY")
        if not expiries: return
        expiry = expiries[0]

        try:
            chain_res = await self.api.get_option_chain(settings.MARKET_KEY_INDEX, expiry.strftime("%Y-%m-%d"))
            if not chain_res or not chain_res.get("data"): return
            
            strikes = []
            market_vols = []
            
            for item in chain_res["data"]:
                strike = item.get("strike_price")
                iv = item.get("call_options", {}).get("option_greeks", {}).get("iv", 0)
                # Normalize IV if needed
                if iv > 5.0: iv /= 100.0
                
                if strike and iv > 0.01:
                    strikes.append(strike)
                    market_vols.append(iv)
            
            if len(strikes) < 5: return
            
            time_to_expiry = max(0.001, (expiry - datetime.now(settings.IST).date()).days / 365.0)
            
            loop = asyncio.get_running_loop()
            success = await asyncio.wait_for(
                loop.run_in_executor(
                    self.executor, self.sabr.calibrate_to_chain,
                    strikes, market_vols, spot, time_to_expiry
                ), timeout=15.0
            )
            
            if success:
                self.last_sabr_calibration = time.time()
                logger.info(f"✅ SABR Calibrated (NIFTY {expiry})")
            else:
                self.sabr.reset()
        except Exception as e:
            logger.error(f"SABR Calibration Failed: {e}")
            self.sabr.reset()

    async def _restore_from_snapshot(self):
        logger.info("♻️ Restoring open trades from DB...")
        async with self.db.get_session() as session:
            result = await session.execute(
                select(DbStrategy).where(DbStrategy.status.in_([TradeStatus.OPEN.value]))
            )
            for db_strat in result.scalars().all():
                try:
                    meta = db_strat.metadata_json
                    legs = [Position(**ld) for ld in meta.get("legs",[])]
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
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
                except Exception as e:
                    logger.error(f"Hydration Failed for {db_strat.id}: {e}")

    async def _reconcile_broker_positions(self):
        try:
            broker_positions = await self.api.get_short_term_positions()
            if not broker_positions: return
            
            # Map Broker Positions
            broker_map = {p["instrument_token"]: int(p["quantity"]) for p in broker_positions if int(p["quantity"]) != 0}
            
            # Map Internal Positions
            internal_map = {}
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    for l in t.legs:
                        internal_map[l.instrument_key] = internal_map.get(l.instrument_key, 0) + l.quantity
            
            # Compare
            for token, b_qty in broker_map.items():
                i_qty = internal_map.get(token, 0)
                if b_qty != i_qty:
                    if i_qty == 0:
                        logger.critical(f"🧟 ZOMBIE ADOPTED: {token} Qty: {b_qty}")
                        await self._adopt_zombie_trade(token, b_qty)
                    else:
                        logger.warning(f"⚠️ POS MISMATCH: {token} Broker={b_qty}, Internal={i_qty}")
        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _adopt_zombie_trade(self, token, qty):
        current_price = 1.0 # Placeholder, will update on next tick
        greeks = GreeksSnapshot(timestamp=datetime.now(settings.IST))
        dummy_leg = Position(
            symbol="UNKNOWN", instrument_key=token, strike=0.0,
            option_type="CE", quantity=qty, entry_price=current_price,
            entry_time=datetime.now(settings.IST),
            current_price=current_price,
            current_greeks=greeks, expiry_type=ExpiryType.INTRADAY,
            capital_bucket=CapitalBucket.INTRADAY
        )
        new_trade = MultiLegTrade(
            legs=[dummy_leg], strategy_type=StrategyType.WAIT,
            net_premium_per_share=0.0,
            entry_time=datetime.now(settings.IST),
            expiry_date=datetime.now(settings.IST).strftime("%Y-%m-%d"),
            expiry_type=ExpiryType.INTRADAY,
            capital_bucket=CapitalBucket.INTRADAY,
            status=TradeStatus.EXTERNAL, id=f"ZOMBIE-{int(time.time())}"
        )
        self.trades.append(new_trade)
        self.data_feed.subscribe_instrument(token)

    async def _run_ai_portfolio_check(self):
        try:
            state = {
                "delta": self.risk_mgr.portfolio_delta,
                "vega": self.risk_mgr.portfolio_vega,
                "pnl": self.risk_mgr.daily_pnl,
                "count": len([t for t in self.trades if t.status == TradeStatus.OPEN])
            }
            review = await self.architect.review_portfolio_holistically(state)
            verdict = review.get("verdict", "UNKNOWN")
            logger.info(f"👨‍⚕️ PORTFOLIO DOCTOR: Verdict={verdict} | {review.get('narrative')}")
        except Exception as e:
            logger.error(f"Portfolio Doctor Error: {e}")

    async def get_dashboard_data(self):
        return await self._get_dashboard_data_async()

    async def _get_dashboard_data_async(self):
        cap_status = await self.capital_allocator.get_status()
        return {
            "spot_price": self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0),
            "vix": self.rt_quotes.get(settings.MARKET_KEY_VIX, 0),
            "pnl": self.risk_mgr.daily_pnl,
            "capital": cap_status,
            "metrics": self.last_metrics.dict() if self.last_metrics else {},
            "trades": [t.dict() for t in self.trades if t.status == TradeStatus.OPEN],
            "ai_insight": {
                "last_trade_analysis": self.architect.last_trade_analysis,
                "portfolio_review": self.architect.last_portfolio_review
            }
        }

    def get_system_health(self):
        return {
            "engine": {"running": self.running, "active_trades": len([t for t in self.trades if t.status == TradeStatus.OPEN])},
            "analytics": {"sabr_calibrated": self.sabr.calibrated},
            "capital_allocation": "Check /api/dashboard/data for async status"
        }

    def get_status(self):
        from core.models import EngineStatus
        return EngineStatus(
            running=self.running, circuit_breaker=False, cycle_count=0,
            total_trades=len(self.trades), daily_pnl=self.risk_mgr.daily_pnl,
            max_equity=self.risk_mgr.peak_equity, last_metrics=self.last_metrics,
            dashboard_ready=True
        )
