class OrderExecutor:
    def __init__(self, rest_client):
        self.client = rest_client

    def execute_batch(self, orders: list, algo_tag: str = "VOLGUARD") -> dict:
        responses = []
        for order in orders:
            try:
                res = self.client.place_order(order, algo_tag)
                responses.append(res)
            except Exception as e:
                return {"success": False, "error": str(e), "partial_fills": responses}
        return {"success": True, "orders": responses}
