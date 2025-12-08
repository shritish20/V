import asyncio
import time
from datetime import datetime
from sqlalchemy import select
from concurrent.futures import ThreadPoolExecutor

from core.config import settings
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

logger = setup_logger("Engine")

class VolGuard17Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        # Initialize API with Token Safety
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        
        # Components
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)
        
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)

        self.greeks_cache = {}
        self.greek_validator = GreekValidator(self.greeks_cache, self.sabr, settings.GREEK_REFRESH_SEC)
        
        self.capital_allocator = SmartCapitalAllocator(settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION)
        
        # Analytics
        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()
        
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

        # State
        self.running = False
        self.trades: list[MultiLegTrade] = []
        self.health_task = None
        self.error_count = 0
        self.last_metrics = None
        self.last_sabr_calibration = 0
        self.last_error_time = 0
        self.executor = ThreadPoolExecutor(max_workers=2)
        self._calibration_semaphore = asyncio.Semaphore(1)
        self._greek_update_lock = asyncio.Lock()

    async def initialize(self):
        logger.info("⚡ Booting VolGuard 19.0 (Endgame)...")
        try:
            await self.instruments_master.download_and_load()
        except Exception as e:
            logger.critical(f"FATAL: Instrument Master failed: {e}")
            raise RuntimeError("Cannot proceed without instrument master")

        await self.db.init_db()
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()
        
        # Subscribe to Index and VIX
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_INDEX)
        self.data_feed.subscribe_instrument(settings.MARKET_KEY_VIX)
        
        # Start background services
        asyncio.create_task(self.data_feed.start())
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())
            
        logger.info("✅ Engine Initialized.")

    async def run(self):
        await self.initialize()
        self.running = True
        logger.info("🟢 Engine Loop Started")
        
        SABR_INTERVAL = 900
        
        while self.running:
            try:
                # 1. Periodic SABR Calibration
                if time.time() - self.last_sabr_calibration > SABR_INTERVAL:
                    asyncio.create_task(self._run_sabr_calibration())

                # 2. Market Data & Risk Loop
                spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
                if spot > 0:
                    await self._update_greeks_and_risk(spot)
                    await self._consider_new_trade(spot)
                
                # 3. Monitor Active Trades (PnL, Exits)
                await self.trade_mgr.monitor_active_trades(self.trades)
                
                # 4. Error Counter Reset
                if time.time() - self.last_error_time > 60:
                    self.error_count = 0
                
                await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

            # CRITICAL FIX: Handle 401 Unauthorized gracefully
            except TokenExpiredError:
                logger.critical("♻️ TOKEN EXPIRED! Pausing for 10s to allow external refresh...")
                # The API allows hot-swapping tokens via /api/token/refresh
                # We pause here to give the user (or auto-script) time to push the new token.
                await asyncio.sleep(10)

            except Exception as e:
                self.error_count += 1
                self.last_error_time = time.time()
                logger.error(f"Cycle Error: {e}")
                
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("💀 Too many errors. Committing suicide.")
                    await self.shutdown()
                    break
                await asyncio.sleep(1)

    async def _consider_new_trade(self, spot: float):
        """The Main Decision Brain"""
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)
        
        # 2. Trend & Analytics
        prev_close = 0.0
        if not self.vol_analytics.data_fetcher.nifty_data.empty:
             prev_close = self.vol_analytics.data_fetcher.nifty_data['Close'].iloc[-1]
        
        daily_return = (spot - prev_close) / prev_close if prev_close > 0 else 0.0
            
        realized_vol, garch, ivp = self.vol_analytics.get_volatility_metrics(vix)
        event_score = self.event_intel.get_event_risk_score()
        
        # 3. Regime Detection
        regime = self.vol_analytics.calculate_volatility_regime(
            vix=vix, ivp=ivp, realized_vol=realized_vol, 
            daily_return=daily_return, event_score=event_score
        )

        # 4. Real-Time Chain Metrics
        expiry = self.strategy_engine._get_expiry_date(ExpiryType.WEEKLY)
        pcr = 1.0
        skew = 0.0
        
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
             except Exception as e:
                 logger.warning(f"Failed to fetch chain metrics: {e}")

        # 5. Build Metrics
        metrics = AdvancedMetrics(
            timestamp=datetime.now(settings.IST),
            spot_price=spot, vix=vix, ivp=ivp,
            realized_vol_7d=realized_vol, garch_vol_7d=garch,
            iv_rv_spread=vix-realized_vol, event_risk_score=event_score,
            regime=regime, pcr=pcr, max_pain=spot, 
            term_structure_slope=0, volatility_skew=skew,
            sabr_alpha=self.sabr.alpha, sabr_beta=self.sabr.beta,
            sabr_rho=self.sabr.rho, sabr_nu=self.sabr.nu
        )
        self.last_metrics = metrics

        # 6. Strategy Selection
        capital_status = self.capital_allocator.get_status()
        strat_name, legs_spec, exp_type, bucket = self.strategy_engine.select_strategy_with_capital(
            metrics, spot, capital_status
        )

        if strat_name == StrategyType.WAIT.value:
            return

        # 7. Execute
        await self._execute_new_strategy(strat_name, legs_spec, exp_type, bucket)

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
                    symbol="NIFTY", instrument_key=token, strike=leg["strike"],
                    option_type=leg["type"], quantity=settings.LOT_SIZE * (1 if leg["side"] == "BUY" else -1),
                    entry_price=0.0, entry_time=datetime.now(settings.IST), current_price=0.0,
                    current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                    expiry_type=exp_type, capital_bucket=bucket
                 ))
            
            new_trade = MultiLegTrade(
                legs=real_legs, strategy_type=StrategyType(strat_name),
                net_premium_per_share=0.0, entry_time=datetime.now(settings.IST),
                expiry_date=legs_spec[0]["expiry"], expiry_type=exp_type,
                capital_bucket=bucket, status=TradeStatus.PENDING, id=f"T-{int(time.time())}"
            )
            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"🚀 OPENED: {strat_name} ({bucket.value})")
        except Exception as e:
            logger.error(f"Execution logic failed: {e}")

    async def _reconcile_broker_positions(self):
        """
        FIXED: Smarter Reconciliation that checks quantities, not just existence.
        """
        try:
            broker_positions = await self.api.get_short_term_positions()
            if not broker_positions: return
            
            # Map Broker: Token -> Net Qty
            broker_map = {
                p["instrument_token"]: int(p["quantity"])
                for p in broker_positions if int(p["quantity"]) != 0
            }
            
            # Map Internal: Token -> Net Qty
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
                        # Case: Zombie (Broker has it, We don't)
                        logger.critical(f"🧟 ZOMBIE ADOPTED: {token} Qty: {b_qty}")
                        await self._adopt_zombie_trade(token, b_qty)
                    else:
                        # Case: Mismatch (We have 75, Broker has 150)
                        logger.warning(f"⚠️ POS MISMATCH: {token} Broker={b_qty}, Internal={i_qty}")

        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _adopt_zombie_trade(self, token, qty):
        current_price = 1.0
        greeks = GreeksSnapshot(timestamp=datetime.now(settings.IST))
        
        # Parsed details default
        symbol_code = "UNKNOWN"
        strike_price = 0.0
        opt_type = "CE"

        try:
            quotes = await self.api.get_quotes([token])
            if quotes.get("status") == "success":
                data = quotes["data"].get(token, {})
                current_price = data.get("last_price", 1.0)
                
                # CRITICAL FIX: Parse Trading Symbol for details (e.g. "NIFTY 22000 CE")
                trading_symbol = data.get("symbol", "") or data.get("trading_symbol", "")
                if trading_symbol:
                    parts = trading_symbol.split()
                    for p in parts:
                        if p.isdigit() and float(p) > 1000: # Heuristic for Strike
                            strike_price = float(p)
                        if p in ["CE", "PE"]:
                            opt_type = p
                        # NIFTY ONLY STRICT CHECK
                        if "NIFTY" in p:
                            symbol_code = p

            if settings.GREEK_VALIDATION:
                broker_greeks = await self.api.get_option_greeks([token])
                if broker_greeks:
                    bg = broker_greeks.get(token, {})
                    greeks = GreeksSnapshot(
                        timestamp=datetime.now(settings.IST),
                        delta=bg.get("delta", 0), gamma=bg.get("gamma", 0),
                        theta=bg.get("theta", 0), vega=bg.get("vega", 0),
                        iv=bg.get("iv", 0)
                    )
        except Exception:
            pass

        # Adopt with best-guess details so Risk Manager isn't blind
        dummy_leg = Position(
            symbol=symbol_code, instrument_key=token, strike=strike_price,
            option_type=opt_type, quantity=qty, entry_price=current_price,
            entry_time=datetime.now(settings.IST), current_price=current_price,
            current_greeks=greeks, expiry_type=ExpiryType.INTRADAY,
            capital_bucket=CapitalBucket.INTRADAY
        )
        
        new_trade = MultiLegTrade(
            legs=[dummy_leg], strategy_type=StrategyType.WAIT,
            net_premium_per_share=0.0, entry_time=datetime.now(settings.IST),
            expiry_date=datetime.now(settings.IST).strftime("%Y-%m-%d"),
            expiry_type=ExpiryType.INTRADAY, capital_bucket=CapitalBucket.INTRADAY,
            status=TradeStatus.EXTERNAL, id=f"ZOMBIE-{int(time.time())}"
        )
        self.trades.append(new_trade)
        self.data_feed.subscribe_instrument(token)
        logger.info(f"🧟 Adopted Zombie: {symbol_code} {strike_price} {opt_type}")

    async def _restore_from_snapshot(self):
        logger.info("💾 Restoring open trades from DB...")
        async with self.db.get_session() as session:
            result = await session.execute(
                select(DbStrategy).where(DbStrategy.status.in_([TradeStatus.OPEN.value]))
            )
            for db_strat in result.scalars().all():
                if not db_strat.metadata_json: continue
                try:
                    meta = db_strat.metadata_json
                    legs = [Position(**ld) for ld in meta.get("legs", [])]
                    
                    trade = MultiLegTrade(
                        legs=legs, strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time, lots=meta.get("lots", 1),
                        status=TradeStatus(db_strat.status), expiry_date=str(db_strat.expiry_date),
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
            
            try:
                # CRITICAL FIX: Increased timeout to 15s for slower VPS
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
            except asyncio.TimeoutError:
                logger.error("SABR calibration timeout (>15s)")
                self.sabr.reset()
        except Exception as e:
            logger.error(f"SABR Calibration Crashed: {e}")
            self.sabr.reset()

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
                logger.critical("🚨 RISK LIMIT BREACHED. FLATTENING.")
                await self._emergency_flatten()

    async def _emergency_flatten(self):
        logger.critical("🛑 EMERGENCY FLATTEN TRIGGERED")
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
                            id=str(t.id), type=t.strategy_type.value, status=t.status.value,
                            entry_time=t.entry_time, capital_bucket=t.capital_bucket.value,
                            pnl=t.total_unrealized_pnl(), metadata_json={"legs": legs_json, "lots": t.lots},
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
        logger.info("🔴 Engine Shutdown Complete")

    def get_status(self):
        from core.models import EngineStatus
        return EngineStatus(
            running=self.running, circuit_breaker=False, cycle_count=0,
            total_trades=len(self.trades), daily_pnl=self.risk_mgr.daily_pnl,
            max_equity=self.risk_mgr.peak_equity, last_metrics=self.last_metrics,
            dashboard_ready=True
        )

    def get_system_health(self):
        return {
            "engine": {"running": self.running, "active_trades": len([t for t in self.trades if t.status == TradeStatus.OPEN])},
            "analytics": {"sabr_calibrated": self.sabr.calibrated},
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
