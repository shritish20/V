import asyncio
import time
import logging
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple, Any, Set
from concurrent.futures import ProcessPoolExecutor
import uuid
from core.config import settings, IST, get_full_url
from core.models import *
from core.enums import *
from database.manager import HybridDatabaseManager
from trading.api_client import EnhancedUpstoxAPI
from trading.order_manager import EnhancedOrderManager
from trading.risk_manager import AdvancedRiskManager
from trading.strategy_engine import IntelligentStrategyEngine
from trading.trade_manager import EnhancedTradeManager
from analytics.volatility import HybridVolatilityAnalytics
from analytics.events import AdvancedEventIntelligence
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from analytics.chain_metrics import ChainMetricsCalculator
from analytics.visualizer import DashboardVisualizer
from capital.allocator import SmartCapitalAllocator
from capital.portfolio import PortfolioManager
from alerts.system import CriticalAlertSystem
from utils.logger import setup_logger
from utils.data_fetcher import DashboardDataFetcher
import prometheus_client
from prometheus_client import Counter, Gauge, Histogram

logger = setup_logger()

ENGINE_CYCLES = Counter('volguard_engine_cycles_total', 'Total engine cycles completed')
MARKET_DATA_UPDATES = Counter('volguard_market_data_updates_total', 'Market data updates')
TRADES_EXECUTED = Counter('volguard_trades_executed_total', 'Total trades executed')
CAPITAL_ALLOCATION_USAGE = Gauge('volguard_capital_allocation_usage', 'Capital allocation usage', ['bucket'])
DASHBOARD_UPDATES = Counter('volguard_dashboard_updates_total', 'Dashboard updates completed')
ERROR_COUNTER = Counter('volguard_errors_total', 'Total errors', ['type'])

cpu_executor = ProcessPoolExecutor(max_workers=2)

class VolGuard18Engine:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        self.vol_analytics = HybridVolatilityAnalytics()
        self.sabr = EnhancedSABRModel()
        self.pricing = HybridPricingEngine(self.sabr)
        self.api.set_pricing_engine(self.pricing)
        self.chain_metrics = ChainMetricsCalculator()
        self.event_intel = AdvancedEventIntelligence()
        self.visualizer = DashboardVisualizer()
        self.data_fetcher = DashboardDataFetcher()
        self.capital_allocator = SmartCapitalAllocator(settings.ACCOUNT_SIZE, settings.CAPITAL_ALLOCATION)
        self.portfolio_manager = PortfolioManager()
        self.alerts = CriticalAlertSystem()
        self.om = EnhancedOrderManager(self.api, self.alerts)
        self.risk_mgr = AdvancedRiskManager(self.db, self.alerts)
        self.strategy_engine = IntelligentStrategyEngine(self.vol_analytics, self.event_intel, self.sabr)
        self.trade_mgr = EnhancedTradeManager(
            self.api, self.db, self.om, self.pricing,
            self.risk_mgr, self.alerts, self.capital_allocator
        )

        self.rt_quotes: Dict[str, float] = {}
        self.rt_quotes_lock = asyncio.Lock()
        self.ws_task: Optional[asyncio.Task] = None
        self.subscribed_instruments: Set[str] = set()

        self.dashboard_data: Optional[DashboardData] = None
        self.last_dashboard_update: datetime = datetime.now(IST)
        self.dashboard_update_task: Optional[asyncio.Task] = None

        self.daily_pnl: float = 0.0
        self.max_equity: float = settings.ACCOUNT_SIZE
        self.cycle_count: int = 0
        self.total_trades: int = 0
        self.equity: float = settings.ACCOUNT_SIZE

        self.running: bool = False
        self.circuit_breaker: bool = False
        self.last_metrics: Optional[AdvancedMetrics] = None
        self.dashboard_ready: bool = False

        logger.info("🚀 VolGuard 18.0 Engine Initialized")
        logger.info(f"💰 Capital Allocation: {settings.CAPITAL_ALLOCATION}")
        logger.info(f"📊 Account Size: ₹{settings.ACCOUNT_SIZE:,.0f}")
        logger.info(f"🔄 Paper Trading: {settings.PAPER_TRADING}")

    async def initialize(self):
        logger.info("Initializing VolGuard 18.0...")
        await self._initialize_dashboard()
        self.ws_task = asyncio.create_task(self.api.connect_ws(self.update_quote))
        await self.om.start()
        await self._startup_reconciliation()
        self.dashboard_ready = True
        logger.info("✅ VolGuard 18.0 Initialization Complete")

    async def _initialize_dashboard(self):
        try:
            spot, metrics = await self._get_market_metrics()
            if spot > 0:
                await self._update_dashboard_data(spot, metrics)
                logger.info("Dashboard initialized successfully")
            else:
                logger.warning("Dashboard initialization delayed - waiting for market data")
        except Exception as e:
            logger.error(f"Dashboard initialization failed: {e}")
            ERROR_COUNTER.labels(type='dashboard').inc()

    async def _startup_reconciliation(self):
        logger.info("Starting broker position reconciliation...")
        try:
            broker_positions = await self.api.get_short_term_positions()
            if broker_positions and len(broker_positions) > 0:
                logger.warning(f"Found {len(broker_positions)} open positions on broker")
                self.trades = [t for t in self.trades if t.status != TradeStatus.EXTERNAL]
                for pos_data in broker_positions:
                    instrument_key = pos_data.get('instrument_key', '')
                    symbol = pos_data.get('symbol', '')
                    expiry_date = pos_data.get('expiry_date', '2099-12-31')
                    expiry_type = self._classify_expiry(expiry_date)
                    capital_bucket = self._get_capital_bucket_for_expiry(expiry_type)

                    position = Position(
                        symbol=symbol,
                        instrument_key=instrument_key,
                        strike=pos_data.get('strike_price', 0.0),
                        option_type=pos_data.get('option_type', 'CE'),
                        quantity=pos_data.get('net_quantity', 0),
                        entry_price=pos_data.get('average_price', 0.0),
                        current_price=pos_data.get('last_price', 0.0),
                        entry_time=datetime.now(IST),
                        current_greeks=GreeksSnapshot(timestamp=datetime.now(IST)),
                        expiry_type=expiry_type,
                        capital_bucket=capital_bucket
                    )

                    position_value = abs(position.quantity * position.entry_price)
                    self.capital_allocator.allocate_capital(capital_bucket.value, position_value)

                    external_trade = MultiLegTrade(
                        legs=[position],
                        strategy_type=StrategyType.WAIT,
                        net_premium_per_share=0.0,
                        entry_time=position.entry_time,
                        lots=abs(position.quantity) // settings.LOT_SIZE,
                        status=TradeStatus.EXTERNAL,
                        expiry_date=expiry_date,
                        expiry_type=expiry_type,
                        capital_bucket=capital_bucket
                    )

                    self.trades.append(external_trade)

                self.circuit_breaker = True
                await self.alerts.send_alert(
                    "RECONCILIATION_WARNING",
                    f"Broker shows {len(broker_positions)} unmanaged positions. Circuit Breaker engaged.",
                    urgent=True
                )
            else:
                logger.info("Reconciliation complete. No external positions found.")
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}")
            ERROR_COUNTER.labels(type='reconciliation').inc()
