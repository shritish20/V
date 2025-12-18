import yfinance as yf
import feedparser
import logging
import requests
import io
import pandas as pd
from datetime import datetime
from core.config import settings

logger = logging.getLogger("MarketIntel")

class MarketIntelligence:
    def __init__(self):
        self.tickers = {
            "S&P500_FUT": "ES=F",
            "BITCOIN": "BTC-USD",
            "USDINR": "INR=X",
            "INDIA_VIX": "^INDIAVIX"
        }
        self.news_feeds = [
            "https://news.google.com/rss/search?q=Nifty+50+NSE+India&hl=en-IN&gl=IN&ceid=IN:en",
            "https://news.google.com/rss/search?q=Global+Market+Risk&hl=en-US&gl=US&ceid=US:en"
        ]
        self.fii_url = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/fii_data.csv"

    def _parse_numeric_lakhs(self, val):
        """Converts '-1.65L' to -165000.0"""
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
        """Ingests and normalizes your FII data from GitHub."""
        try:
            res = requests.get(self.fii_url, timeout=10)
            if res.status_code == 200:
                df = pd.read_csv(io.StringIO(res.text))
                # Get the last 4 rows for the most recent date state
                data = df.tail(4).to_dict('records')
                for row in data:
                    row['Net_Value'] = self._parse_numeric_lakhs(row.get('Net', 0))
                return data
            return []
        except Exception as e:
            logger.error(f"FII GitHub Fetch Failed: {e}")
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
        except: return {}

    def get_latest_headlines(self, limit=5):
        """Scans RSS feeds for context."""
        headlines = []
        for url in self.news_feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:3]:
                    headlines.append(entry.title)
            except: continue
        return list(set(headlines))[:limit]
