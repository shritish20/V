class RecoveryWorker:
    def __init__(self, rest_client, capital_manager, execution_orchestrator):
        self.rest = rest_client
        self.capital = capital_manager
        self.exec = execution_orchestrator

    def run(self):
        # Reconcile DB vs Broker here
        pass
