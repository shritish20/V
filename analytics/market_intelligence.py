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
        # 1. Global Risk Proxies for Gap Prediction & Sentiment
        self.tickers = {
            "S&P500_FUT": "ES=F",       # US Markets (Leading indicator for Nifty Open)
            "BITCOIN": "BTC-USD",       # Risk-On/Off Sentiment Proxy
            "USDINR": "INR=X",          # Currency Risk (Critical for FII flows)
            "INDIA_VIX": "^INDIAVIX"    # Fear Gauge
        }
        
        # 2. Institutional (FII) Data Source
        # This points to your specific GitHub repository for daily derivative flow data.
        self.fii_url = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/fii_data.csv"
        
        # 3. Expanded News Sources (RSS)
        # Targeted search queries to feed the AI Architect better context.
        self.news_feeds = [
            "https://news.google.com/rss/search?q=Nifty+50+Indian+Stock+Market&hl=en-IN&gl=IN&ceid=IN:en",
            "https://news.google.com/rss/search?q=Global+Macro+News+Economy&hl=en-US&gl=US&ceid=US:en",
            "https://news.google.com/rss/search?q=RBI+Policy+Fed+Meeting+Impact&hl=en-IN&gl=IN&ceid=IN:en"
        ]

    def get_fii_data(self):
        """
        Ingests institutional participant data from GitHub.
        Captures Futures, Options, Calls, and Puts positioning.
        """
        try:
            res = requests.get(self.fii_url, timeout=10)
            if res.status_code == 200:
                df = pd.read_csv(io.StringIO(res.text))
                # Returns the latest state (tail 4 rows typically cover the participant categories)
                return df.tail(4).to_dict('records')
            else:
                logger.warning(f"FII Data HTTP Error: {res.status_code}")
                return []
        except Exception as e:
            logger.error(f"FII Data Fetch Failed: {e}")
            return []

    def get_macro_sentiment(self):
        """
        Fetches live % change of global assets for gap prediction.
        Refactored for higher performance using the fast_info property.
        """
        data = {}
        try:
            # Fetch all tickers in one batch call
            tickers_list = " ".join(self.tickers.values())
            tickers = yf.Tickers(tickers_list)
            
            for name, symbol in self.tickers.items():
                try:
                    info = tickers.tickers[symbol].fast_info
                    last_price = info.last_price
                    prev_close = info.previous_close
                    
                    if last_price and prev_close:
                        change_pct = ((last_price - prev_close) / prev_close) * 100
                        data[name] = f"{change_pct:+.2f}%"
                    else:
                        data[name] = "N/A"
                except Exception:
                    data[name] = "N/A" 
            return data
        except Exception as e:
            logger.error(f"Macro Fetch Error: {e}")
            return {}

    def get_latest_headlines(self, limit=5, max_age_hours=24):
        """
        Fetches headlines filtered by time to ensure AI context is fresh.
        """
        headlines = []
        try:
            now = datetime.now()
            cutoff = now - timedelta(hours=max_age_hours)

            for url in self.news_feeds:
                try:
                    feed = feedparser.parse(url)
                    if not feed.entries:
                        continue
                        
                    for entry in feed.entries:
                        try:
                            pub_time = None
                            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                pub_time = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                            
                            # Filter outdated context
                            if pub_time and pub_time < cutoff:
                                continue
                                
                            age_str = "FRESH"
                            if pub_time:
                                age_hours = (now - pub_time).total_seconds() / 3600
                                age_str = f"{age_hours:.1f}h ago"
                            
                            # Clean headline titles
                            clean_title = entry.title.split(' - ')[0]
                            headlines.append(f"[{age_str}] {clean_title}")
                            
                        except Exception:
                            continue
                except Exception:
                    continue
                        
            return list(set(headlines))[:limit]
            
        except Exception as e:
            logger.error(f"News Fetch Error: {e}")
            return []
