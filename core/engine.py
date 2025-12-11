import asyncio
import time
from datetime import datetime
from sqlalchemy import select
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from core.config import settings, IST
from core.models import MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics
from core.enums import TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason
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
from utils.logger import setup_logger

# NEW: The AI Architect
from analytics.explainer import AI_Portfolio_Architect

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        # Core Components
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)
            
        self.greeks_cache = {}
        self.greek_validator = GreekValidator(self.greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        
        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        
        # Analytics
        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()
        
        # NEW: AI Architect
        self.architect = AI_Portfolio_Architect()
        self.last_ai_check = 0
        
        # Live Data
        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)
        
        # Managers
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
        
        # State
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
        logger.info("⚡ Booting VolGuard 19.0 (Architect Edition)...")
        try:
            await self.instruments_master.download_and_load()
            logger.info("✓ Instrument Master loaded")
        except Exception as e:
            logger.critical(f"Instrument Master CRITICAL FAILURE: {e}")
            # Fallback logic handles simple file usage if download fails
            # (Keeping it simple as per original robust logic)
        
        await self.db.init_db()
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        
        asyncio.create_task(self.data_feed.start())
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
            
        logger.info("✓ Engine Initialized.")

    async def run(self):
        await self.initialize()
        self.running = True
        logger.info("🚀 Engine Loop Started")
        SABR_INTERVAL = 900
        
        while self.running:
            try:
                current_time = time.time()
                
                # 1. Periodic SABR Calibration (15 mins)
                if current_time - self.last_sabr_calibration > SABR_INTERVAL:
                    asyncio.create_task(self._run_sabr_calibration())
                
                # 2. NEW: Periodic Portfolio Doctor (Hourly)
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
                logger.critical("🔑 TOKEN EXPIRED! Pausing...")
                await asyncio.sleep(10)
            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("🔥 Too many errors. Shutdown.")
                    await self.shutdown()
                    break
                await asyncio.sleep(1)

    async def _consider_new_trade(self, spot: float):
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        
        # 1. Trend & Analytics
        prev_close = 0.0
        if not self.vol_analytics.data_fetcher.nifty_data.empty:
            prev_close = self.vol_analytics.data_fetcher.nifty_data['Close'].iloc[-1]
        
        realized_vol, garch, ivp = self.vol_analytics.get_volatility_metrics(vix)
        event_score = self.event_intel.get_event_risk_score()
        regime = self.vol_analytics.calculate_volatility_regime(
            vix=vix, ivp=ivp, realized_vol=realized_vol,
            daily_return=0.0, event_score=event_score
        )
        
        # 2. Real-Time Chain Metrics
        expiry = self.strategy_engine._get_expiry_date(ExpiryType.WEEKLY)
        pcr = 1.0
        skew = 0.0
        straddle_price = spot * 0.01

        if expiry:
            try:
                chain_res = await self.api.get_option_chain(settings.MARKET_KEY_INDEX, expiry)
                if chain_res and chain_res.get('data'):
                    chain_data = chain_res['data']
                    surface = self.vol_analytics.calculate_volatility_surface(chain_data, spot)
                    if surface:
                        atm_row = min(surface, key=lambda x: abs(x['moneyness']))
                        skew = atm_row.get('iv_skew', 0.0)
                    chain_metrics = self.vol_analytics.calculate_chain_metrics(chain_data)
                    pcr = chain_metrics.get('pcr', 1.0)
                    straddle_price = chain_metrics.get('straddle_price', spot * 0.01)
            except Exception as e:
                logger.warning(f"Failed to fetch chain metrics: {e}")

        # Build Metrics Object
        metrics = AdvancedMetrics(
            timestamp=datetime.now(IST), spot_price=spot, vix=vix, ivp=ivp,
            realized_vol_7d=realized_vol, garch_vol_7d=garch, iv_rv_spread=vix-realized_vol,
            volatility_skew=skew, straddle_price=straddle_price, event_risk_score=event_score,
            regime=regime, pcr=pcr, max_pain=spot, term_structure_slope=0.0,
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta, sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics

        # Strategy Selection
        cap_status = await self.capital_allocator.get_status()
        strat_name, legs_spec, exp_type, bucket = self.strategy_engine.select_strategy_with_capital(
            metrics, spot, cap_status
        )

        if strat_name != StrategyType.WAIT.value:
            # ----------------------------------------------------
            # NEW: AI PASSIVE ADVISOR HOOK
            # ----------------------------------------------------
            # We fire this task in the background. 
            # The Quant engine does NOT wait for it.
            trade_ctx = {
                "strategy": strat_name,
                "spot": spot,
                "vix": vix,
                "regime": regime,
                "reason": "Quant Signal Generated"
            }
            asyncio.create_task(self._log_ai_trade_opinion(trade_ctx))
            
            # Execute Immediately (Zero Latency)
            await self._execute_new_strategy(strat_name, legs_spec, exp_type, bucket)

    async def _log_ai_trade_opinion(self, trade_ctx):
        """Background task to get AI opinion without blocking execution"""
        try:
            analysis = await self.architect.analyze_trade_setup(trade_ctx)
            risk = analysis.get("risk_level", "UNKNOWN")
            narrative = analysis.get("narrative", "No narrative.")
            
            log_msg = f"🧠 AI OBSERVER: {trade_ctx['strategy']} | Risk: {risk} | {narrative}"
            
            if risk in ["HIGH", "EXTREME"]:
                logger.warning(log_msg)
            else:
                logger.info(log_msg)
        except Exception as e:
            logger.error(f"AI Observer Error: {e}")

    async def _run_ai_portfolio_check(self):
        """Hourly background task for holistic review"""
        try:
            state = {
                "delta": self.risk_mgr.portfolio_delta,
                "vega": self.risk_mgr.portfolio_vega,
                "pnl": self.risk_mgr.daily_pnl,
                "count": len([t for t in self.trades if t.status == TradeStatus.OPEN])
            }
            review = await self.architect.review_portfolio_holistically(state)
            
            verdict = review.get("verdict", "UNKNOWN")
            narrative = review.get("narrative", "")
            logger.info(f"🧐 PORTFOLIO DOCTOR: Verdict={verdict} | {narrative}")
            
        except Exception as e:
            logger.error(f"Portfolio Doctor Error: {e}")

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
                    current_price=0.0,
                    current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                    expiry_type=exp_type, capital_bucket=bucket
                ))

            new_trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat_name),
                net_premium_per_share=0.0,
                entry_time=datetime.now(settings.IST),
                expiry_date=legs_spec[0]["expiry"], expiry_type=exp_type,
                capital_bucket=bucket, status=TradeStatus.PENDING,
                id=f"T-{int(time.time())}"
            )

            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"🚀 OPENED: {strat_name} ({bucket.value})")

        except Exception as e:
            logger.error(f"Execution logic failed: {e}")

    async def _update_greeks_and_risk(self, spot: float):
        async with self._greek_update_lock:
            tasks = []
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    tasks.append(self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes))
            
            if tasks:
                await asyncio.gather(*tasks)

            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

            if self.risk_mgr.check_portfolio_limits():
                logger.critical("💥 RISK LIMIT BREACHED. FLATTENING.")
                await self._emergency_flatten()

    async def _emergency_flatten(self):
        logger.critical("🚨 EMERGENCY FLATTEN TRIGGERED")
        tasks = [
            self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER)
            for t in self.trades if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def save_final_snapshot(self):
        try:
            async with self.db.get_session() as session:
                for t in self.trades:
                    if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                        legs_json = [l.dict() for l in t.legs]
                        db_strat = DbStrategy(
                            id=str(t.id), type=t.strategy_type.value,
                            status=t.status.value,
                            capital_bucket=t.capital_bucket.value,
                            entry_time=t.entry_time,
                            pnl=t.total_unrealized_pnl(),
                            metadata_json={"legs": legs_json, "lots": t.lots},
                            broker_ref_id=t.basket_order_id,
                            expiry_date=datetime.strptime(t.expiry_date, "%Y-%m-%d").date()
                        )
                        await session.merge(db_strat)
                await self.db.safe_commit(session)
                logger.info(f"💾 Snapshot saved ({len(self.trades)} trades).")
        except Exception as e:
            logger.error(f"Snapshot save failed: {e}")

    async def shutdown(self):
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
        self.executor.shutdown(wait=False)
        logger.info("👋 Engine Shutdown Complete")

    async def _run_sabr_calibration(self):
        if not self._calibration_semaphore.locked():
            async with self._calibration_semaphore:
                await self._calibrate_sabr_internal()

    async def _calibrate_sabr_internal(self):
        spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
        if spot <= 0: return
        expiries = self.instruments_master.get_all_expiries("NIFTY")
        if not expiries: return
        expiry = expiries[0]
        
        try:
            chain_data = await self.api.get_option_chain(settings.MARKET_KEY_INDEX, expiry.strftime("%Y-%m-%d"))
            strikes = []
            market_vols = []
            data_list = chain_data.get("data", []) if chain_data else []
            for item in data_list:
                strike = item.get("strike_price")
                iv = item.get("call_options", {}).get("option_greeks", {}).get("iv")
                if strike and iv and 0.01 < iv < 2.0:
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
                logger.info(f"✨ SABR Calibrated (NIFTY {expiry})")
            else:
                self.sabr.reset()
        except Exception as e:
            logger.error(f"SABR Calibration Crashed: {e}")
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
                    
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(
                        trade.capital_bucket.value, val, trade_id=trade.id
                    )
                except Exception as e:
                    logger.error(f"Hydration Failed for {db_strat.id}: {e}")

    async def _reconcile_broker_positions(self):
        try:
            broker_positions = await self.api.get_short_term_positions()
            if not broker_positions: return
            broker_map = {
                p["instrument_token"]: int(p["quantity"])
                for p in broker_positions if int(p["quantity"]) != 0
            }
            internal_map = {}
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    for l in t.legs:
                        internal_map[l.instrument_key] = internal_map.get(l.instrument_key, 0) + l.quantity
            
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
        current_price = 1.0
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
        logger.info(f"✅ Adopted Zombie Trade {token} ({qty})")

    def get_dashboard_data(self):
        # Async check for capital allocator status
        # We need this to be synchronous for the API response usually, 
        # but in async API context it's fine.
        # Returning a coroutine if not awaited, but we'll use a wrapper in API.
        # Actually, let's make this async to be safe and clean.
        return self._get_dashboard_data_async()

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
    
    # Wrapper for sync calls if needed (though API is async now)
    async def get_dashboard_data(self):
        return await self._get_dashboard_data_async()

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
