import asyncio
import time
import functools
from datetime import datetime
from sqlalchemy import select
from concurrent.futures import ProcessPoolExecutor

from core.config import settings
from core.models import MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics
from core.enums import TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason

from database.manager import HybridDatabaseManager
from database.models import DbStrategy

from trading.api_client import EnhancedUpstoxAPI
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

logger = setup_logger("Engine")

class VolGuard17Engine:
    """
    FIXED: Offloaded SABR calibration to ProcessPoolExecutor to prevent event loop blocking.
    Addresses High Priority Issue #5 from Code Review.
    """
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        # Components
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)

        self.greeks_cache = {}
        self.greek_validator = GreekValidator(
            self.greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC
        )

        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION
        )

        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()

        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)

        self.om = EnhancedOrderManager(self.api, self.db)
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, 
            self.event_intel, 
            self.capital_allocator,
            self.pricing 
        )

        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing, self.risk_mgr, None, self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed

        # State
        self.running = False
        self.trades: list[MultiLegTrade] = []
        self.health_task = None
        self.error_count = 0
        self.last_error_time = 0
        self.last_metrics = None
        self.last_sabr_calibration = 0
        
        # Dedicated executor for CPU-heavy tasks (SABR Calibration)
        self.executor = ProcessPoolExecutor(max_workers=1)
        
        # Lock for Thread/Task Safety during Greek aggregation
        self._greek_update_lock = asyncio.Lock()

    async def initialize(self):
        logger.info("🛡️ Booting VolGuard 19.0 (Endgame)...")
        
        try:
            await self.instruments_master.download_and_load()
            logger.info("✅ Instrument Master Ready")
        except Exception as e:
            logger.critical(f"❌ FATAL: Instrument Master failed: {e}")
            raise RuntimeError("Cannot proceed without instrument master")

        await self.db.init_db()
        await self.om.start()
        
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()

        initial_tokens = set()
        initial_tokens.add(settings.MARKET_KEY_INDEX)
        initial_tokens.add(settings.MARKET_KEY_VIX)
        
        for trade in self.trades:
            if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                for leg in trade.legs:
                    initial_tokens.add(leg.instrument_key)
        
        logger.info(f"📡 Pre-subscribing to {len(initial_tokens)} instruments...")
        for token in initial_tokens:
            self.data_feed.subscribe_instrument(token)

        asyncio.create_task(self.data_feed.start())
        self.health_task = asyncio.create_task(self._system_heartbeat())
        
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())

        logger.info("✓ Engine Initialized.")

    async def _system_heartbeat(self):
        while self.running:
            await asyncio.sleep(10)
            try:
                lag = time.time() - self.data_feed.last_tick_time
                if lag > 60:
                    logger.critical(f"❤ FEED STALLED ({lag:.0f}s).")
            except Exception:
                self.error_count += 1

    async def _reconcile_broker_positions(self):
        try:
            broker_positions = await self.api.get_short_term_positions()
            if not broker_positions: return

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
                    logger.critical(f"🧟 ZOMBIE ADOPTED: {token} Qty: {qty}")
                    await self._adopt_zombie_trade(token, qty)

        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _adopt_zombie_trade(self, token, qty):
        current_price = 1.0
        greeks = GreeksSnapshot(timestamp=datetime.now(settings.IST))
        
        try:
            quotes = await self.api.get_quotes([token])
            if quotes.get("status") == "success":
                data = quotes["data"].get(token, {})
                current_price = data.get("last_price", 1.0)
            
            if settings.GREEK_VALIDATION:
                 broker_greeks = await self.api.get_option_greeks([token])
                 if broker_greeks:
                     bg = broker_greeks.get(token, {})
                     greeks = GreeksSnapshot(
                         timestamp=datetime.now(settings.IST),
                         delta=bg.get("delta", 0),
                         gamma=bg.get("gamma", 0),
                         theta=bg.get("theta", 0),
                         vega=bg.get("vega", 0),
                         iv=bg.get("iv", 0)
                     )
        except Exception:
            pass

        dummy_leg = Position(
            symbol="UNKNOWN",
            instrument_key=token,
            strike=0, 
            option_type="CE",
            quantity=qty,
            entry_price=current_price,
            entry_time=datetime.now(settings.IST),
            current_price=current_price,
            current_greeks=greeks,
            expiry_type=ExpiryType.INTRADAY,
            capital_bucket=CapitalBucket.INTRADAY
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
            id=f"ZOMBIE-{int(time.time())}"
        )
        self.trades.append(new_trade)
        self.data_feed.subscribe_instrument(token)

    async def _restore_from_snapshot(self):
        logger.info("📂 Restoring open trades from DB...")
        async with self.db.get_session() as session:
            result = await session.execute(
                select(DbStrategy).where(
                    DbStrategy.status.in_([TradeStatus.OPEN.value])
                )
            )
            for db_strat in result.scalars().all():
                if not db_strat.metadata_json: continue
                try:
                    meta = db_strat.metadata_json
                    legs = []
                    for ld in meta.get("legs", []):
                        legs.append(Position(**ld))
                    
                    trade = MultiLegTrade(
                        legs=legs,
                        strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time,
                        lots=meta.get("lots", 1),
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

    async def _run_sabr_calibration(self):
        """
        Runs SABR calibration in a separate process to avoid blocking the main event loop.
        """
        spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
        if spot <= 0: return

        expiries = self.instruments_master.get_all_expiries("NIFTY")
        if not expiries: return
        expiry = expiries[0]
        
        try:
            chain_data = await self.api.get_option_chain(
                settings.MARKET_KEY_INDEX, 
                expiry.strftime("%Y-%m-%d")
            )
            
            strikes = []
            market_vols = []
            
            data_list = chain_data.get("data", []) if chain_data else []
            for item in data_list:
                strike = item.get("strike_price")
                call_greek = item.get("call_options", {}).get("option_greeks", {})
                iv = call_greek.get("iv") if call_greek else 0
                
                if strike and iv and 0.01 < iv < 2.0:
                    strikes.append(strike)
                    market_vols.append(iv)
            
            if len(strikes) < 5: return

            time_to_expiry = max(0.001, (expiry - datetime.now(settings.IST).date()).days / 365.0)

            loop = asyncio.get_running_loop()
            func = functools.partial(
                self.sabr.calibrate_to_chain, 
                strikes, market_vols, spot, time_to_expiry
            )
            
            # CRITICAL FIX: Use self.executor (ProcessPool) instead of default ThreadPool
            # This ensures heavy math doesn't block the AsyncIO loop
            success = await loop.run_in_executor(self.executor, func)
            
            if success:
                self.last_sabr_calibration = time.time()
                logger.info(f"🧮 SABR Calibrated (NIFTY {expiry})")
            else:
                self.sabr.reset()
                self.sabr.calibrated = False
                
        except Exception as e:
            logger.error(f"SABR Calibration Crashed: {e}")
            self.sabr.reset()
            self.sabr.calibrated = False

    async def _update_greeks_and_risk(self, spot: float):
        # Async Lock to prevent race condition during Greek aggregation
        async with self._greek_update_lock:
            tasks = []
            for t in self.trades:
                if t.status == TradeStatus.OPEN:
                    tasks.append(self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes))
            
            if tasks:
                await asyncio.gather(*tasks)

            total_pnl = sum(
                t.total_unrealized_pnl() for t in self.trades 
                if t.status == TradeStatus.OPEN
            )
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)
            
            if self.risk_mgr.check_portfolio_limits():
                logger.critical("🚨 RISK LIMIT BREACHED. FLATTENING.")
                await self._emergency_flatten()

    async def _consider_new_trade(self, spot: float):
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        
        realized_vol, garch, ivp = self.vol_analytics.get_volatility_metrics(vix)
        event_score = self.event_intel.get_event_risk_score()
        regime = self.vol_analytics.calculate_volatility_regime(vix, ivp, realized_vol)

        metrics = AdvancedMetrics(
            timestamp=datetime.now(settings.IST),
            spot_price=spot, vix=vix, ivp=ivp,
            realized_vol_7d=realized_vol, garch_vol_7d=garch,
            iv_rv_spread=vix-realized_vol, event_risk_score=event_score,
            regime=regime, pcr=1.0, max_pain=spot,
            term_structure_slope=0, volatility_skew=0,
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
            sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics

        capital_status = self.capital_allocator.get_status()
        strat_name, legs_spec, exp_type, bucket = self.strategy_engine.select_strategy_with_capital(
            metrics, spot, capital_status
        )

        if strat_name == StrategyType.WAIT.value:
            return

        real_legs = []
        try:
            for leg in legs_spec:
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token(
                    "NIFTY", leg["strike"], leg["type"], expiry_dt
                )
                if not token: return

                real_legs.append(
                    Position(
                        symbol="NIFTY",
                        instrument_key=token,
                        strike=leg["strike"],
                        option_type=leg["type"],
                        quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                        entry_price=0.0,
                        entry_time=datetime.now(settings.IST),
                        current_price=0.0,
                        current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                        expiry_type=exp_type,
                        capital_bucket=bucket
                    )
                )

            new_trade = MultiLegTrade(
                legs=real_legs,
                strategy_type=StrategyType(strat_name),
                net_premium_per_share=0.0,
                entry_time=datetime.now(settings.IST),
                expiry_date=legs_spec[0]["expiry"],
                expiry_type=exp_type,
                capital_bucket=bucket,
                status=TradeStatus.PENDING,
                id=f"T-{int(time.time())}"
            )
            
            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"🚀 OPENED: {strat_name} ({bucket.value})")

        except Exception as e:
            logger.error(f"Execution Failed: {e}")

    async def _emergency_flatten(self):
        logger.critical("🔥 EMERGENCY FLATTEN TRIGGERED")
        tasks = [
            self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER)
            for t in self.trades if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def save_final_snapshot(self):
        async with self.db.get_session() as session:
            for t in self.trades:
                if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                    legs_json = [l.dict() for l in t.legs]
                    db_strat = DbStrategy(
                        id=str(t.id),
                        type=t.strategy_type.value,
                        status=t.status.value,
                        entry_time=t.entry_time,
                        capital_bucket=t.capital_bucket.value,
                        pnl=t.total_unrealized_pnl(),
                        metadata_json={"legs": legs_json, "lots": t.lots},
                        broker_ref_id=t.basket_order_id,
                        expiry_date=datetime.strptime(t.expiry_date, "%Y-%m-%d").date()
                    )
                    await session.merge(db_strat)
            await session.commit()
            logger.info(f"💾 Snapshot saved.")

    def get_status(self):
        from core.models import EngineStatus
        return EngineStatus(
            running=self.running,
            circuit_breaker=False,
            cycle_count=0,
            total_trades=len(self.trades),
            daily_pnl=self.risk_mgr.daily_pnl,
            max_equity=self.risk_mgr.peak_equity,
            last_metrics=self.last_metrics,
            dashboard_ready=True
        )
    
    def get_system_health(self):
        return {
            "engine": {
                "running": self.running,
                "active_trades": len([t for t in self.trades if t.status == TradeStatus.OPEN])
            },
            "analytics": {
                "sabr_calibrated": self.sabr.calibrated
            },
            "capital_allocation": self.capital_allocator.get_status()
        }

    def get_dashboard_data(self):
        return {
            "spot_price": self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0),
            "vix": self.rt_quotes.get(settings.MARKET_KEY_VIX, 0),
            "pnl": self.risk_mgr.daily_pnl,
            "capital": self.capital_allocator.get_status(),
            "trades": [t.dict() for t in self.trades if t.status == TradeStatus.OPEN],
            "metrics": self.last_metrics.dict() if self.last_metrics else {}
        }

    async def run(self):
        await self.initialize()
        self.running = True
        SABR_INTERVAL = 900 
        
        logger.info("🚀 Engine Loop Started")

        while self.running:
            try:
                if time.time() - self.last_sabr_calibration > SABR_INTERVAL:
                    asyncio.create_task(self._run_sabr_calibration())

                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    await self._consider_new_trade(spot)
                    await self.trade_mgr.monitor_active_trades(self.trades)

                if time.time() - self.last_error_time > 60:
                    self.error_count = 0

            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
                if self.error_count > settings.MAX_ERROR_COUNT:
                     logger.critical("☠️ Too many errors. Committing suicide.")
                     await self.shutdown()
                     break

            await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

    async def shutdown(self):
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
        # FIX: Ensure executor is properly shutdown
        self.executor.shutdown(wait=False)
        logger.info("👋 Engine Shutdown Complete")
