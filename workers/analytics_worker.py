import time
from logic_core.analytics import AnalyticsEngine
from logic_core.regime import RegimeClassifier

class AnalyticsWorker:
    def __init__(self, fetcher, execution_orchestrator, ws_state, capital_manager, sheriff, poll_interval=60):
        self.fetcher = fetcher
        self.exec = execution_orchestrator
        self.ws_state = ws_state
        self.capital = capital_manager
        self.sheriff = sheriff
        self.poll_interval = poll_interval
        self.analytics = AnalyticsEngine()

    def run(self):
        while True:
            try:
                # 1. Fetch & Build State
                price_df = self.fetcher.get_spot_history()
                vix_df = self.fetcher.get_vix_history()
                live = self.ws_state.snapshot()["market"]
                
                market_state = self.analytics.build_market_state(
                    spot=live.get("NSE_INDEX|Nifty 50", 0),
                    vix=live.get("NSE_INDEX|India VIX", 0),
                    price_history=price_df,
                    vix_history=vix_df,
                    chain_metrics={}
                )

                # 2. Regime & Capital
                regime = RegimeClassifier.classify(market_state)
                capital_ok = self.capital.can_allocate(regime.allowed_exposure_pct)
                strategy_orders = self.capital.build_strategy_orders(market_state, regime)

                # 3. Execute
                self.exec.try_execute(
                    market_state, regime, self.capital.current_trade(),
                    self.capital.system_health(), capital_ok, strategy_orders
                )

            except Exception as e:
                print(f"[AnalyticsWorker] Error: {e}")
            time.sleep(self.poll_interval)
