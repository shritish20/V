import requests

class UpstoxRESTClient:
    BASE_V3 = "https://api.upstox.com/v3"
    BASE_V2 = "https://api.upstox.com/v2"

    def __init__(self, access_token: str):
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def get_ltp(self, instrument_keys: list) -> dict:
        r = requests.get(f"{self.BASE_V3}/market-quote/ltp", headers=self.headers,
                        params={"instrument_key": ",".join(instrument_keys)}, timeout=3)
        return r.json().get("data", {})

    def place_order(self, payload: dict, algo_tag: str) -> dict:
        headers = dict(self.headers)
        headers["X-Algo-Name"] = algo_tag
        r = requests.post(f"{self.BASE_V3}/order/place", headers=headers, json=payload, timeout=5)
        return r.json()

    def get_positions(self) -> dict:
        r = requests.get(f"{self.BASE_V2}/portfolio/short-term-positions", headers=self.headers, timeout=3)
        return r.json().get("data", [])
