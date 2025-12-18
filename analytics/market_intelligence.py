import yfinance as yf
import feedparser
import logging
import time
import requests
import io
import pandas as pd
from datetime import datetime, timedelta
from core.config import settings

logger = logging.getLogger("MarketIntel")

class MarketIntelligence:
    def __init__(self):
        # Global Risk Proxies
        self.tickers = {
            "S&P500_FUT": "ES=F",
            "BITCOIN": "BTC-USD",
            "USDINR": "INR=X",
            "INDIA_VIX": "^INDIAVIX"
        }
        # News Feeds
        self.news_feeds = [
            "https://news.google.com/rss/search?q=Nifty+50+NSE+India&hl=en-IN&gl=IN&ceid=IN:en",
            "https://news.google.com/rss/search?q=Global+Market+Crash+Risk&hl=en-US&gl=US&ceid=US:en"
        ]
        # Your Raw FII Data Link
        self.fii_url = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/fii_data.csv"

    def _parse_numeric(self, val):
        """Helper to convert '-1.65L' to float -165000.0"""
        if not isinstance(val, str): return float(val)
        try:
            clean = val.replace(',', '').upper()
            if 'L' in clean:
                return float(clean.replace('L', '')) * 100000
            if 'K' in clean:
                return float(clean.replace('K', '')) * 1000
            return float(clean)
        except: return 0.0

    def get_fii_data(self):
        """Ingests institutional participant data from GitHub CSV."""
        try:
            res = requests.get(self.fii_url, timeout=10)
            if res.status_code == 200:
                df = pd.read_csv(io.StringIO(res.text))
                # Get last 4 rows for the latest date's state (Futures/Options/Calls/Puts)
                data = df.tail(4).to_dict('records')
                for row in data:
                    row['Net_Val'] = self._parse_numeric(row.get('Net', 0))
                    row['Chg_Val'] = self._parse_numeric(row.get('Chg', 0))
                return data
            return []
        except Exception as e:
            logger.error(f"FII Data Fetch Failed: {e}")
            return []

    def get_macro_sentiment(self):
        """Fetches live % change of global assets for gap prediction."""
        data = {}
        try:
            tickers_list = " ".join(self.tickers.values())
            t_obj = yf.Tickers(tickers_list)
            for name, sym in self.tickers.items():
                try:
                    price = t_obj.tickers[sym].fast_info.last_price
                    prev = t_obj.tickers[sym].fast_info.previous_close
                    change = ((price - prev) / prev) * 100
                    data[name] = f"{change:+.2f}%"
                except: data[name] = "N/A"
            return data
        except Exception as e:
            logger.error(f"Macro Fetch Error: {e}")
            return {}

    def get_latest_headlines(self, limit=5, max_age_hours=24):
        """Fetches recent headlines filtered by time."""
        headlines = []
        now = datetime.now()
        cutoff = now - timedelta(hours=max_age_hours)
        for url in self.news_feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    try:
                        pub_time = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                        if pub_time < cutoff: continue
                        age = (now - pub_time).total_seconds() / 3600
                        headlines.append(f"[{age:.1f}h ago] {entry.title}")
                    except: continue
            except: continue
        return list(set(headlines))[:limit]
