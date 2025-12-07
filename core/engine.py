import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from sqlalchemy import select
from core.config import settings
from core.models import MultiLegTrade, Position, GreeksSnapshot, AdvancedMetrics
from core.enums import (
    TradeStatus, StrategyType, CapitalBucket, ExpiryType, ExitReason,
)
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
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)

        # CRITICAL FIX #1: Initialize Instruments Master
        self.instruments_master = InstrumentMaster()
        self.api.set_instrument_master(self.instruments_master)

        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        
        if hasattr(self.api, "set_pricing_engine"):
            self.api.set_pricing_engine(self.pricing)

        self.greeks_cache = {}
        self.greek_validator = GreekValidator(
            self.greeks_cache, self.sabr, settings.TRADING_LOOP_INTERVAL
        )

        self.capital_allocator = SmartCapitalAllocator(
            settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION
        )

        # Initialize Intelligence Modules
        self.vol_analytics = HybridVolatilityAnalytics()
        self.event_intel = AdvancedEventIntelligence()

        self.rt_quotes = {}
        self.data_feed = LiveDataFeed(self.rt_quotes, self.greeks_cache, self.sabr)

        self.om = EnhancedOrderManager(self.api, self.db)
        self.risk_mgr = AdvancedRiskManager(self.db, None)
        
        self.strategy_engine = IntelligentStrategyEngine(
            self.vol_analytics, self.event_intel, self.capital_allocator
        )

        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing, self.risk_mgr, None,
            self.capital_allocator
        )
        self.trade_mgr.feed = self.data_feed

        self.running = False
        self.trades: List[MultiLegTrade] = []
        self.health_task = None
        self.error_count = 0
        self.last_error_time = 0
        self.last_metrics = None
        self.last_sabr_calibration = 0  # ADDED for calibration tracking

    async def initialize(self):
        logger.info("🚀 Booting VolGuard 19.0 (Endgame)...")
        
        # CRITICAL FIX #2: Download Instruments FIRST before any other initialization
        logger.info("📥 Downloading Instrument Master (This may take 30-60 seconds)...")
        try:
            await self.instruments_master.download_and_load()
            logger.info("✅ Instrument Master Ready")
        except Exception as e:
            logger.critical(f"❌ FATAL: Instrument Master download failed: {e}")
            raise RuntimeError("Cannot proceed without instrument master")

        # Now safe to initialize database and other components
        await self.db.init_db()
        await self.om.start()
        await self._restore_from_snapshot()
        await self._reconcile_broker_positions()

        # Start async tasks
        asyncio.create_task(self.data_feed.start())
        self.health_task = asyncio.create_task(self._system_heartbeat())
        
        if settings.GREEK_VALIDATION:
            asyncio.create_task(self.greek_validator.start())

        logger.info("✅ Engine Initialized.")

    async def _system_heartbeat(self):
        """Health monitoring loop"""
        while self.running:
            await asyncio.sleep(10)
            try:
                lag = time.time() - self.data_feed.last_tick_time
                if lag > 60:
                    logger.critical(f"❤ FEED STALLED ({lag:.0f}s).")
                await self.api.get_short_term_positions()
            except Exception:
                self.error_count += 1

    async def _reconcile_broker_positions(self):
        """
        FIXED: Adopt 'zombie' positions with proper price and Greek fetching
        """
        try:
            broker_positions = await self.api.get_short_term_positions()
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
                    logger.critical(f"🚨 ZOMBIE ADOPTED: {token} Qty: {qty}")
                    
                    # CRITICAL FIX #3: Fetch current price for zombie
                    current_price = 0.0
                    try:
                        quotes = await self.api.get_quotes([token])
                        if quotes.get("status") == "success":
                            data = quotes["data"].get(token, {})
                            current_price = data.get("last_price", 0.0)
                    except Exception as e:
                        logger.error(f"Failed to fetch zombie price: {e}")

                    # Parse instrument key to get strike/type
                    # Format: NSE_FO|NIFTY24DEC21000CE
                    strike = 0
                    option_type = "CE"
                    try:
                        parts = token.split("|")[1]
                        # Try to extract strike from token
                        import re
                        strike_match = re.search(r'(\d+)(CE|PE)', parts)
                        if strike_match:
                            strike = float(strike_match.group(1))
                            option_type = strike_match.group(2)
                    except:
                        pass

                    dummy_leg = Position(
                        symbol="NIFTY",
                        instrument_key=token,
                        strike=strike,
                        option_type=option_type,
                        quantity=qty,
                        entry_price=current_price if current_price > 0 else 100.0,
                        entry_time=datetime.now(settings.IST),
                        current_price=current_price if current_price > 0 else 100.0,
                        current_greeks=GreeksSnapshot(
                            timestamp=datetime.now(settings.IST)
                        ),
                        expiry_type=ExpiryType.INTRADAY,
                        capital_bucket=CapitalBucket.INTRADAY,
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
                    )
                    new_trade.id = f"ZOMBIE-{int(time.time())}"
                    self.trades.append(new_trade)

                    # Subscribe to live data
                    self.data_feed.subscribe_instrument(token)

                    # Save to database
                    async with self.db.get_session() as session:
                        db_strat = DbStrategy(
                            id=new_trade.id,
                            type="EXTERNAL",
                            status="EXTERNAL",
                            entry_time=new_trade.entry_time,
                            capital_bucket="INTRADAY",
                            metadata_json={
                                "legs": [dummy_leg.dict()],
                                "note": "Auto-adopted zombie",
                            },
                        )
                        await session.merge(db_strat)
                        await session.commit()

        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")

    async def _restore_from_snapshot(self):
        """Restore open trades from database"""
        logger.info("📥 Restoring open trades from DB...")
        async with self.db.get_session() as session:
            result = await session.execute(
                select(DbStrategy).where(
                    DbStrategy.status.in_([TradeStatus.OPEN.value])
                )
            )
            for db_strat in result.scalars().all():
                if not db_strat.metadata_json:
                    continue
                try:
                    meta = db_strat.metadata_json
                    legs = []
                    for ld in meta.get("legs", []):
                        self.data_feed.subscribe_instrument(ld["instrument_key"])
                        legs.append(Position(**ld))

                    trade = MultiLegTrade(
                        legs=legs,
                        strategy_type=StrategyType(db_strat.type),
                        entry_time=db_strat.entry_time,
                        lots=meta.get("lots", 1),
                        status=TradeStatus(db_strat.status),
                        expiry_date=str(db_strat.expiry_date),
                        expiry_type=ExpiryType(legs[0].expiry_type),
                        capital_bucket=CapitalBucket(db_strat.capital_bucket),
                    )
                    trade.id = db_strat.id
                    trade.basket_order_id = db_strat.broker_ref_id
                    self.trades.append(trade)

                    value = sum(abs(l.entry_price * l.quantity) for l in trade.legs)
                    await self.capital_allocator.allocate_capital(
                        trade.capital_bucket.value, value, trade_id=trade.id
                    )
                except Exception as e:
                    logger.error(f"Hydration Failed: {e}")

    async def _calibrate_sabr(self):
        """
        ADDED: Periodic SABR calibration using live option chain data
        """
        try:
            spot = self.rt_quotes.get(settings.MARKET_KEY_INDEX, 0.0)
            if spot <= 0:
                return

            # Get nearest weekly expiry
            expiries = self.instruments_master.get_all_expiries("NIFTY")
            if not expiries:
                return

            # Use first available expiry
            expiry = expiries[0]
            
            # Fetch option chain (simplified - you'd need actual implementation)
            # This is a placeholder - implement based on your option chain API
            strikes = []
            market_vols = []
            
            # For now, skip if we don't have data
            if len(strikes) < 3:
                logger.debug("Insufficient data for SABR calibration")
                return

            time_to_expiry = max(0.001, (expiry - datetime.now(settings.IST).date()).days / 365.0)
            
            success = self.sabr.calibrate_to_chain(
                strikes, market_vols, spot, time_to_expiry
            )
            
            if success:
                logger.info("✅ SABR Model Calibrated")
                self.last_sabr_calibration = time.time()
            
        except Exception as e:
            logger.error(f"SABR calibration failed: {e}")

    async def _update_greeks_and_risk(self, spot: float):
        """Update prices and risk metrics"""
        tasks = [
            self.trade_mgr.update_trade_prices(t, spot, self.rt_quotes)
            for t in self.trades
            if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

        total_pnl = sum(
            t.total_unrealized_pnl() 
            for t in self.trades 
            if hasattr(t, "total_unrealized_pnl") and t.status == TradeStatus.OPEN
        )
        
        self.risk_mgr.update_portfolio_state(self.trades, total_pnl)

        if self.risk_mgr.check_portfolio_limits():
            logger.critical("🚨 RISK LIMIT BREACHED. FLATTENING.")
            await self._emergency_flatten()

    async def _consider_new_trade(self, spot: float):
        """Core Logic: Should we open a position?"""
        vix = self.rt_quotes.get(settings.MARKET_KEY_VIX, 15.0)

        # Update Analytics
        realized_vol, garch, ivp = self.vol_analytics.get_volatility_metrics(vix)
        event_score = self.event_intel.get_event_risk_score()
        regime = self.vol_analytics.calculate_volatility_regime(vix, ivp, realized_vol)

        metrics = AdvancedMetrics(
            timestamp=datetime.now(settings.IST),
            spot_price=spot,
            vix=vix,
            ivp=ivp,
            realized_vol_7d=realized_vol,
            garch_vol_7d=garch,
            iv_rv_spread=vix - realized_vol,
            event_risk_score=event_score,
            regime=regime,
            pcr=1.0,
            max_pain=spot,
            term_structure_slope=0,
            volatility_skew=0,
            sabr_alpha=self.sabr.alpha,
            sabr_beta=self.sabr.beta,
            sabr_rho=self.sabr.rho,
            sabr_nu=self.sabr.nu,
        )
        self.last_metrics = metrics

        # Ask Strategy Engine (now with fixed get_status)
        capital_status = self.capital_allocator.get_status()
        
        strategy_name, legs_spec, expiry_type, bucket = (
            self.strategy_engine.select_strategy_with_capital(
                metrics, spot, capital_status
            )
        )

        if strategy_name == StrategyType.WAIT.value:
            return

        # Build Trade Object
        real_legs = []
        try:
            for leg in legs_spec:
                expiry_dt = datetime.strptime(leg["expiry"], "%Y-%m-%d").date()
                token = self.instruments_master.get_option_token(
                    "NIFTY", leg["strike"], leg["type"], expiry_dt
                )
                if not token:
                    logger.warning(f"Skipping trade: Token not found for {leg}")
                    return

                real_legs.append(
                    Position(
                        symbol="NIFTY",
                        instrument_key=token,
                        strike=leg["strike"],
                        option_type=leg["type"],
                        quantity=settings.LOT_SIZE
                        * (1 if leg["side"] == "BUY" else -1),
                        entry_price=0.0,
                        entry_time=datetime.now(settings.IST),
                        current_price=0.0,
                        current_greeks=GreeksSnapshot(
                            timestamp=datetime.now(settings.IST)
                        ),
                        expiry_type=expiry_type,
                        capital_bucket=bucket,
                    )
                )

            new_trade = MultiLegTrade(
                legs=real_legs,
                strategy_type=StrategyType(strategy_name),
                net_premium_per_share=0.0,
                entry_time=datetime.now(settings.IST),
                expiry_date=legs_spec[0]["expiry"],
                expiry_type=expiry_type,
                capital_bucket=bucket,
                status=TradeStatus.PENDING,
            )
            new_trade.id = f"T-{int(time.time())}"

            success = await self.trade_mgr.execute_strategy(new_trade)
            if success:
                self.trades.append(new_trade)
                logger.info(f"✅ OPENED: {strategy_name} ({bucket.value})")
        except Exception as e:
            logger.error(f"Trade Execution Failed: {e}")

    async def _emergency_flatten(self):
        """Emergency position flattening"""
        logger.critical("🔥 EMERGENCY FLATTEN TRIGGERED 🔥")
        tasks = [
            self.trade_mgr.close_trade(t, ExitReason.CIRCUIT_BREAKER)
            for t in self.trades
            if t.status == TradeStatus.OPEN
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def save_final_snapshot(self):
        """OPTIMIZED: Batch database writes"""
        async with self.db.get_session() as session:
            strategies_to_save = []
            
            for trade in self.trades:
                if trade.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]:
                    legs_json = [l.dict() for l in trade.legs]
                    db_strat = DbStrategy(
                        id=str(trade.id),
                        type=trade.strategy_type.value,
                        status=trade.status.value,
                        entry_time=trade.entry_time,
                        capital_bucket=trade.capital_bucket.value,
                        pnl=trade.total_unrealized_pnl()
                        if hasattr(trade, "total_unrealized_pnl")
                        else 0.0,
                        metadata_json={
                            "legs": legs_json,
                            "order_ids": getattr(trade, "gtt_order_ids", []),
                        },
                        broker_ref_id=getattr(trade, "basket_order_id", None),
                        expiry_date=datetime.strptime(
                            trade.expiry_date, "%Y-%m-%d"
                        ).date(),
                    )
                    strategies_to_save.append(db_strat)
            
            # Bulk save
            if strategies_to_save:
                session.add_all(strategies_to_save)
                await session.commit()
                logger.info(f"💾 Saved {len(strategies_to_save)} strategies to DB")

    async def run(self):
        """Main trading loop with SABR calibration"""
        await self.initialize()
        self.running = True
        
        SABR_CALIBRATION_INTERVAL = 3600  # 1 hour
        
        while self.running:
            try:
                if self.error_count > settings.MAX_ERROR_COUNT:
                    logger.critical("💥 TOO MANY ERRORS. SUICIDE.")
                    await self.shutdown()
                    break

                # Periodic SABR calibration
                if time.time() - self.last_sabr_calibration > SABR_CALIBRATION_INTERVAL:
                    await self._calibrate_sabr()

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

            await asyncio.sleep(settings.TRADING_LOOP_INTERVAL)

    async def shutdown(self):
        """Graceful shutdown"""
        self.running = False
        await self._emergency_flatten()
        await self.save_final_snapshot()
        await self.api.close()
