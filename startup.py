import time
import threading
from core.settings import settings
from auth.token_manager import OAuthTokenManager 
from database.manager import HybridDatabaseManager
from websocket.ws_state import WebSocketState
from websocket.market_ws import MarketWebSocket
from sheriff.sheriff import Sheriff
from execution.rest_client import UpstoxRESTClient
from execution.execution_orchestrator import ExecutionOrchestrator
from workers.recovery_worker import RecoveryWorker
from workers.analytics_worker import AnalyticsWorker
from workers.monitoring_worker import MonitoringWorker
from infra.fetcher import MarketFetcher
from capital.capital_manager import CapitalManager

class VolGuardStartup:
    def start(self):
        print("\n🚀 VolGuard 20.0 Starting...\n")
        
        # 1. State & Clients
        ws_state = WebSocketState()
        rest_client = UpstoxRESTClient(settings.UPSTOX_ACCESS_TOKEN)
        sheriff = Sheriff({"RISK_LIMITS": {"MAX_DELTA": 100}})
        capital = CapitalManager(settings)
        fetcher = MarketFetcher(settings, rest_client)
        
        # 2. Execution Orchestrator
        orchestrator = ExecutionOrchestrator(rest_client, sheriff, settings.ALGO_TAG)

        # 3. WebSockets
        market_ws = MarketWebSocket(settings.UPSTOX_ACCESS_TOKEN, settings.MARKET_KEYS, ws_state)
        market_ws.start()

        # 4. Workers
        analytics = AnalyticsWorker(fetcher, orchestrator, ws_state, capital, sheriff)
        monitor = MonitoringWorker(ws_state, orchestrator, capital)
        
        # 5. Launch Threads
        threading.Thread(target=analytics.run, daemon=True).start()
        threading.Thread(target=monitor.run, daemon=True).start()

        print("\n✅ System LIVE. Press Ctrl+C to exit.\n")
        while True: time.sleep(1)
