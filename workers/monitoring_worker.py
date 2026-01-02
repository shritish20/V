import time
from datetime import datetime
from logic_core.pnl import pnl_attribution
from logic_core.risk import evaluate_trade_risk

class MonitoringWorker:
    def __init__(
        self,
        ws_state,
        execution_orchestrator,
        capital_manager,
        poll_interval: int = 5
    ):
        self.ws_state = ws_state
        self.exec = execution_orchestrator
        self.capital = capital_manager
        self.poll_interval = poll_interval

    def run(self):
        print("👁️ Monitoring Worker: STARTED")
        while True:
            try:
                # ==========================================
                # 1. SYSTEM HEALTH HEARTBEAT (CRITICAL)
                # ==========================================
                # We update this every cycle so Sheriff knows if data is stale
                snapshot = self.ws_state.snapshot()
                now = datetime.utcnow()
                
                # Check Market Data Latency
                last_market = snapshot.get("last_market_tick", datetime.min)
                market_lag = (now - last_market).total_seconds()
                
                # Check Portfolio Data Latency
                last_portfolio = snapshot.get("last_portfolio_tick", datetime.min)
                
                # Update Capital/Sheriff State
                # Considered "Healthy" if data is less than 5 seconds old
                self.capital.update_health("ws_market", market_lag < 5)
                self.capital.update_health("latency_ms", market_lag * 1000)
                self.capital.update_health("last_tick_time", now.isoformat())

                # ==========================================
                # 2. CHECK ACTIVE TRADE
                # ==========================================
                trade = self.capital.current_trade()
                
                if not trade:
                    # Even if no trade, we keep looping to update health stats
                    time.sleep(self.poll_interval)
                    continue

                # ==========================================
                # 3. LIVE POSITION UPDATE
                # ==========================================
                positions = snapshot.get("positions", {})
                self.capital.update_trade_from_positions(positions)

                # ==========================================
                # 4. CONTINUOUS RISK EVALUATION
                # ==========================================
                # Logic Core evaluates the updated trade state
                risk_violation = evaluate_trade_risk(self.capital.trade_state())
                
                if risk_violation:
                    print(f"🚨 RISK BREACH DETECTED: {risk_violation}")
                    
                    # 1. Force Exit via Execution Layer
                    self.exec.exit_engine.force_exit(
                        self.exec.algo_tag,
                        reason=risk_violation
                    )
                    
                    # 2. Close Internal State
                    self.capital.close_trade(f"RISK_STOP: {risk_violation}")
                    continue

                # ==========================================
                # 5. PNL SNAPSHOT & ATTRIBUTION
                # ==========================================
                # Capture metrics for post-trade analysis
                try:
                    prev, curr, greeks = self.capital.pnl_inputs()
                    pnl_breakdown = pnl_attribution(prev, curr, greeks)
                    self.capital.record_pnl(pnl_breakdown)
                except Exception as pnl_err:
                    # Don't crash worker on PnL calc error, just log it
                    print(f"⚠️ PnL Calculation Warning: {pnl_err}")

            except Exception as e:
                print(f"❌ [MonitoringWorker] CRITICAL LOOP ERROR: {e}")
                # Prevent CPU spin loop if error is persistent
                time.sleep(1)

            time.sleep(self.poll_interval)
