import time

class RecoveryWorker:
    def __init__(self, rest_client, capital_manager, execution_orchestrator):
        self.rest = rest_client
        self.capital = capital_manager
        self.exec = execution_orchestrator

    def run(self):
        """
        Fix 4: Active Reconciliation
        Run once at startup or triggered manually.
        """
        print("🚑 Recovery Worker: Starting Reconciliation...")
        
        try:
            # 1. Get Broker State
            broker_positions = self.rest.get_positions()
            # Normalize broker response to list if dict returned
            if isinstance(broker_positions, dict):
                 # Handle Upstox 'data' wrapper if needed
                 broker_positions = broker_positions.get('data', [])

            has_broker_positions = len(broker_positions) > 0
            
            # 2. Get Internal State
            internal_trade = self.capital.current_trade()
            
            # 3. Detect Ghost Trade (Internal thinks we are in, Broker says we are flat)
            if internal_trade and not has_broker_positions:
                print("🚨 RECOVERY MISMATCH: Ghost Trade Detected.")
                # We are recording a trade that doesn't exist.
                # Action: Clean up internal state.
                self.capital.active_trade = None
                self.capital.deployed_capital = 0.0
                print("✅ Internal state reset to FLAT.")

            # 4. Detect Zombie Position (Internal says flat, Broker says we have positions)
            elif not internal_trade and has_broker_positions:
                print("🚨 RECOVERY MISMATCH: Zombie Positions Detected.")
                # We have positions we are not tracking.
                # Action: FORCE EXIT.
                self.exec.exit_engine.force_exit("RECOVERY_ZOMBIE_KILL")
                print("✅ Zombie positions liquidated.")
            
            else:
                print("✅ State Reconciled: Sync OK.")

        except Exception as e:
            print(f"❌ Recovery Worker Failed: {e}")
