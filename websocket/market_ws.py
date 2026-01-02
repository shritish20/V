import upstox_client
import threading

class MarketWebSocket:
    def __init__(self, access_token: str, instrument_keys: list, ws_state):
        self.state = ws_state
        config = upstox_client.Configuration()
        config.access_token = access_token
        self.client = upstox_client.ApiClient(config)
        self.streamer = upstox_client.MarketDataStreamerV3(self.client, instrument_keys, "full")
        self.streamer.on("message", self._on_message)
        self.streamer.auto_reconnect(True, interval=5, retryCount=10)

    def _on_message(self, message):
        if "ltp" in message:
            self.state.update_market(message["ltp"])

    def start(self):
        t = threading.Thread(target=self.streamer.connect, daemon=True)
        t.start()
