import asyncio
import time
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import select

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
from analytics.market_intelligence import MarketIntelligence
from trading.strategy_engine import IntelligentStrategyEngine
from analytics.explainer import AI_Portfolio_Architect
from analytics.journal import JournalManager
from utils.data_fetcher import DashboardDataFetcher
from utils.logger import setup_logger

from core.safety_layer import MasterSafetyLayer
# FIX: Import the correct LiveOrderExecutor
from trading.live_order_executor import LiveOrderExecutor 
from trading.position_lifecycle import PositionLifecycleManager
from analytics.vrp_zscore import VRPZScoreAnalyzer

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)

        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.pricing.set_api(self.api)
        self.pricing.instrument_master = self.instruments_master
        
        self.greeks_cache = {}
        self.greek_validator = GreekValidator(self.greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        self.greek_validator.set_instrument_master(self.instruments_master)

        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION, self.db
        )
        
        self.data_fetcher = DashboardDataFetcher(self.api)
        self.vol_analytics = HybridVolatilityAnalytics(self.data_fetcher)
        
        self.event_intel = AdvancedEventIntelligence()
        self.intel = MarketIntelligence()
        self.architect = AI_Portfolio_Architect()
        self.journal = JournalManager(self.db, self.api)

        self.last_ai_check = 0
        self.cycle_count = 0
        self.rt_quotes = {}
        
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)
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

        # Analytics & Hardening
        self.vrp_zscore = VRPZScoreAnalyzer(self.data_fetcher)
        self.lifecycle_mgr = PositionLifecycleManager(self.trade_mgr)
        
        # FIX: Instantiate LiveOrderExecutor with BOTH API and OrderManager
        self.hardened_executor = LiveOrderExecutor(self.api, self.om)
        
        self.safety_layer = MasterSafetyLayer(
            self.risk_mgr,
            getattr(self.trade_mgr, 'margin_guard', None),
            self.lifecycle_mgr,
            self.vrp_zscore
        )

        self.running = False
        self.trades: list[MultiLegTrade] = []
        self.error_count = 0
        self.last_metrics = None
        self.last_sabr_calibration = 0
        self.last_error_time = 0
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._calibration_semaphore = asyncio.Semaphore(1)
        self._greek_update_lock = asyncio.Lock()
        self.last_known_spot = 0.0

    async def initialize(self):
        logger.info("🚀 VolGuard 19.0 Booting...")
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

            await self._hydrate_offline_state()
            logger.info("✅ Engine Initialized")
        except Exception as e:
            logger.critical(f"Init Failed: {e}")

    async def _hydrate_offline_state(self):
        try:
            if not self.data_fetcher.nifty_data.empty:
                last_close = self.data_fetcher.nifty_data['close'].iloc[-1]
                self.last_known_spot = last_close
                await self._update_market_context(last_close)
        except: pass

    async def run(self):
        await self.initialize()
        self.running = True
        last_reset_date = None
        
        while self.running:
            try:
                current_time = time.time()
                self.cycle_count += 1
                now = datetime.now(IST)
                
                if now.time() >= settings.MARKET_OPEN_TIME and now.date() != last_reset_date:
                    self.safety_layer.reset_daily_counters()
                    last_reset_date = now.date()

                if current_time - self.last_ai_check > 3600:
                    asyncio.create_task(self._run_ai_portfolio_check())
                    self.last_ai_check = current_time
                
                # Spot determination
                live_spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if live_spot > 0: self.last_known_spot = live_spot
                spot_to_use = self.last_known_spot
                
                # Always Thinking (24/7)
                if spot_to_use > 0:
                    await self._update_greeks_and_risk(spot_to_use)
                    await self.lifecycle_mgr.monitor_lifecycle(self.trades)
                    await self._update_market_context(spot_to_use)
                
                # Trading Logic (Market Hours)
                is_market_live = settings.MARKET_OPEN_TIME <= now.time() <= settings.MARKET_CLOSE_TIME
                if is_market_live and live_spot > 0:
                    if current_time - self.last_sabr_calibration > 900:
                        asyncio.create_task(self._run_sabr_calibration())
                    await self._attempt_trading_logic(live_spot)
                    await self.trade_mgr.monitor_active_trades(self.trades)

                if current_time - self.last_error_time > 60: self.error_count = 0
                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

            except Exception as e:
                self.error_count += 1
                logger.error(f"Loop: {e}")
                await asyncio.sleep(1)

    async def _update_market_context(self, spot: float):
        try:
            live_vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 0.0)
            if live_vix == 0 and not self.data_fetcher.vix_data.empty:
                 live_vix = self.data_fetcher.vix_data['close'].iloc[-1]
            vix = max(live_vix, 10.0)
            
            # 1. Vol Metrics
            rv7, rv28, garch, egarch, ivp, iv_rank = self.vol_analytics.get_volatility_metrics(vix)
            
            # 2. Market Structure
            market_structure = await self.pricing.get_market_structure(spot)
            
            atm_iv = market_structure.get("atm_iv", 0.0)
            
            # 3. VRP & Spreads
            vrp_comp = atm_iv - rv7 - garch
            spread_rv = atm_iv - rv7
            
            z_score, _, _ = self.vrp_zscore.calculate_vrp_zscore(atm_iv, vix)
            
            # 4. Regime
            risk_state_event, event_score, top_event = self.event_intel.get_market_risk_state()
            vol_regime = self.vol_analytics.calculate_volatility_regime(vix, iv_rank)
            final_regime = "BINARY_EVENT" if risk_state_event == "BINARY_EVENT" else vol_regime

            self.last_metrics = AdvancedMetrics(
                timestamp=datetime.now(IST), spot_price=spot, vix=vix, 
                ivp=ivp, iv_rank=iv_rank,
                realized_vol_7d=rv7, realized_vol_28d=rv28,
                garch_vol_7d=garch, egarch_vol_1d=egarch,
                atm_iv=atm_iv,
                monthly_iv=market_structure.get("monthly_iv", 0.0),
                vrp_score=vrp_comp,
                spread_rv=spread_rv,
                vrp_zscore=z_score,
                term_structure_spread=market_structure.get("term_structure_spread", 0.0),
                straddle_price=market_structure.get("straddle_price", 0.0),
                straddle_price_monthly=market_structure.get("straddle_price_monthly", 0.0),
                atm_theta=market_structure.get("atm_theta", 0.0),
                atm_vega=market_structure.get("atm_vega", 0.0),
                atm_delta=market_structure.get("atm_delta", 0.0),
                atm_gamma=market_structure.get("atm_gamma", 0.0),
                atm_pop=market_structure.get("atm_pop", 0.0),
                volatility_skew=market_structure.get("skew_index", 0.0),
                structure_confidence=market_structure.get("confidence", 0.0),
                regime=final_regime, event_risk_score=event_score, top_event=top_event,
                trend_status=self.vol_analytics.get_trend_status(spot),
                days_to_expiry=float(market_structure.get("days_to_expiry", 0.0)),
                expiry_date=market_structure.get("near_expiry", "N/A"),
                pcr=market_structure.get("pcr", 1.0),
                max_pain=market_structure.get("max_pain", spot),
                efficiency_table=market_structure.get("efficiency_table", []),
                sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
                sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu,
                term_structure_slope=0.0
            )
        except Exception as e:
            logger.error(f"Context Update Failed: {e}")

    async def _attempt_trading_logic(self, spot: float):
        if not self.last_metrics: return
        metrics = self.last_metrics
        
        if metrics.structure_confidence < 0.5: return

        cap_status = await self.capital_allocator.get_status()
        ai_context = getattr(self.architect, 'last_trade_analysis', {})
        
        strat, legs, etype, bucket = self.strategy_engine.select_strategy_with_capital(
            metrics, spot, cap_status, ai_context
        )

        if strat != "WAIT":
            trade_ctx = {"strategy": strat, "spot": spot, "vix": metrics.vix, "event": metrics.top_event, "regime": metrics.regime}
            asyncio.create_task(self._log_ai_trade_opinion(trade_ctx))
            
            real_legs = []
            for leg in legs:
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token("NIFTY", leg["strike"], leg["type"], expiry_dt)
                if not token: return
                
                real_legs.append(Position(
                    symbol="NIFTY", instrument_key=token, strike=leg["strike"], 
                    option_type=leg["type"],
                    quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                    entry_price=0.0, entry_time=datetime.now(settings.IST),
                    current_price=0.0,
                    current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                    expiry_type=etype, capital_bucket=bucket
                ))

            new_trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat),
                net_premium_per_share=0.0, entry_time=datetime.now(settings.IST),
                expiry_date=legs[0]["expiry"], expiry_type=etype,
                capital_bucket=bucket, status=TradeStatus.PENDING,
                id=f"T-{int(time.time())}"
            )

            current_metrics_dict = {"vix": metrics.vix, "atm_iv": metrics.atm_iv, "greeks_cache": self.greeks_cache}
            approved, reason = await self.safety_layer.pre_trade_gate(new_trade, current_metrics_dict)
            if not approved:
                logger.warning(f"🚫 TRADE BLOCKED: {reason}")
                self.safety_layer.post_trade_update(False)
                return

            success, msg = await self.hardened_executor.execute_with_hedge_priority(new_trade)
            if success:
                val = sum(abs(l.entry_price * l.quantity) for l in new_trade.legs)
                await self.capital_allocator.allocate_capital(bucket.value, val, new_trade.id)
                new_trade.status = TradeStatus.OPEN
                self.trades.append(new_trade)
                self.safety_layer.post_trade_update(True)
                logger.info(f"✅ ORDER COMPLETED: {strat}")
            else:
                logger.error(f"❌ EXECUTION FAILED: {msg}")
                self.safety_layer.post_trade_update(False)

    async def _log_ai_trade_opinion(self, trade_ctx):
        try:
            analysis = await self.architect.analyze_trade_setup(trade_ctx)
            logger.info(f"🤖 AI OBSERVER: {trade_ctx['strategy']} | Risk: {analysis.get('risk_level', 'UNKNOWN')}")
        except: pass

    async def _run_ai_portfolio_check(self):
        try:
            state = {"delta": self.risk_mgr.portfolio_delta, "pnl": self.risk_mgr.daily_pnl}
            await self.architect.review_portfolio_holistically(state, self.intel.get_fii_data())
        except: pass

    async def _update_greeks_and_risk(self, spot: float):
        async with self._greek_update_lock:
            tasks = [self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes) for t in self.trades if t.status == TradeStatus.OPEN]
            if tasks: await asyncio.gather(*tasks)
            total_pnl = sum(t.total_unrealized_pnl() for t in self.trades if t.status == TradeStatus.OPEN)
            self.risk_mgr.update_portfolio_state(self.trades, total_pnl)
            if self.risk_mgr.check_portfolio_limits(): await self._emergency_flatten()

    async def _emergency_flatten(self):
        tasks = [self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER) for t in self.trades if t.status == TradeStatus.OPEN]
        if tasks: await asyncio.gather(*tasks)
        self.safety_layer.is_halted = True

    async def save_final_snapshot(self):
        try:
            async with self.db.get_session() as session:
                for t in self.trades:
                    if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                        db_strat = DbStrategy(
                            id=str(t.id), type=t.strategy_type.value,
                            status=t.status.value, entry_time=t.entry_time,
                            capital_bucket=t.capital_bucket.value,
                            pnl=t.total_unrealized_pnl(),
                            expiry_date=datetime.strptime(t.expiry_date, "%Y-%m-%d").date(),
                            broker_ref_id=t.basket_order_id,
                            metadata_json={"legs": [l.dict() for l in t.legs], "lots": t.lots},
                        )
                        await session.merge(db_strat)
                await self.db.safe_commit(session)
        except Exception as e: logger.error(f"Snapshot Failed: {e}")

    async def shutdown(self):
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
        self.executor.shutdown(wait=False)

    async def _run_sabr_calibration(self):
        if not self._calibration_semaphore.locked():
            async with self._calibration_semaphore:
                await self._calibrate_sabr_internal()

    async def _calibrate_sabr_internal(self):
        pass

    async def _restore_from_snapshot(self):
        logger.info("💾 Restoring Session...")
        async with self.db.get_session() as session:
            result = await session.execute(select(DbStrategy).where(DbStrategy.status == TradeStatus.OPEN.value))
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
                        capital_bucket=CapitalBucket(db_strat.capital_bucket),
                        id=db_strat.id, basket_order_id=db_strat.broker_ref_id
                    )
                    self.trades.append(trade)
                    val = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(trade.capital_bucket.value, val, trade.id)
                except Exception as e: logger.error(f"Recovery Error: {e}")

    async def _reconcile_broker_positions(self):
        pass

    async def _adopt_zombie_trade(self, token, qty):
        pass

    async def get_dashboard_data(self):
        m = self.last_metrics
        if not m: return {"status": "Initializing", "timestamp": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")}
        
        def tag(val, type_):
            if val is None: return "N/A"
            if type_ == 'pcr': return "Bullish (>1)" if val > 1 else "Bearish (<0.7)" if val < 0.7 else "Neutral"
            if type_ == 'ivp': return "Expensive (Sell)" if val > 80 else "Cheap (Buy)" if val < 20 else "Normal"
            if type_ == 'vrp': return "High Edge (Sell)" if val > 2.5 else "Neg Edge (Buy)" if val < -2.5 else "Fair"
            if type_ == 'term': return "Backwardation (Fear)" if val > 1.5 else "Contango (Normal)" if val < -1.0 else "Flat"
            if type_ == 'skew': return "Call Skew (Bullish)" if val > 0.5 else "Put Skew (Fear)" if val < -1.5 else "Normal"
            if type_ == 'zscore':
                if val > 2.0: return "Extremely Expensive"
                if val > 1.0: return "Expensive"
                if val < -2.0: return "Extremely Cheap"
                if val < -1.0: return "Cheap"
                return "Fair Value"
            return ""

        return {
            "timestamp": m.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "spot_price": round(m.spot_price, 2),
            "system_status": {
                "running": self.running,
                "safety_halt": self.safety_layer.is_halted,
                "trades_today": self.safety_layer.trades_today
            },
            "atm_metrics": {
                "straddle_cost_weekly": round(m.straddle_price, 2),
                "straddle_cost_monthly": round(m.straddle_price_monthly, 2),
                "breakeven_weekly": [
                    round(m.spot_price - m.straddle_price), 
                    round(m.spot_price + m.straddle_price)
                ]
            },
            "weekly_option_metrics": {
                "theta": round(m.atm_theta, 2),
                "vega": round(m.atm_vega, 2),
                "delta": round(m.atm_delta, 2),
                "gamma": round(m.atm_gamma, 4),
                "pop": round(m.atm_pop, 1),
                "skew": round(m.volatility_skew, 2),
                "skew_tag": tag(m.volatility_skew, 'skew')
            },
            "iv_term_structure": {
                "weekly_iv": round(m.atm_iv, 2),
                "monthly_iv": round(m.monthly_iv, 2),
                "spread": round(m.term_structure_spread, 2),
                "tag": tag(m.term_structure_spread, 'term')
            },
            "quant_models": {
                "rv_7d": round(m.realized_vol_7d, 2),
                "rv_28d": round(m.realized_vol_28d, 2),
                "garch": round(m.garch_vol_7d, 2),
                "egarch": round(m.egarch_vol_1d, 2)
            },
            "regime_signals": {
                "vix": round(m.vix, 2),
                "ivp": round(m.ivp, 0),
                "ivp_tag": tag(m.ivp, 'ivp'),
                "iv_rank": round(m.iv_rank, 2),
                "spread_rv": round(m.spread_rv, 2),
                "vrp_score": round(m.vrp_score, 2),
                "vrp_tag": tag(m.vrp_score, 'vrp'),
                "vrp_zscore": round(m.vrp_zscore, 2),
                "zscore_tag": tag(m.vrp_zscore, 'zscore')
            },
            "chain_metrics": {
                "max_pain": m.max_pain,
                "pcr": m.pcr,
                "pcr_tag": tag(m.pcr, 'pcr'),
                "efficiency_table": m.efficiency_table
            },
            "active_trades": [
                {
                    "id": t.id,
                    "strategy": t.strategy_type.value,
                    "pnl": round(t.total_unrealized_pnl(), 2),
                    "expiry": t.expiry_date
                } for t in self.trades if t.status in [TradeStatus.OPEN, TradeStatus.PENDING]
            ]
        }
