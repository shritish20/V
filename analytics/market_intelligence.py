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
            [span_0](start_span)"S&P500_FUT": "ES=F",       # US Markets (Leading indicator for Nifty Open)[span_0](end_span)
            [span_1](start_span)"BITCOIN": "BTC-USD",       # Risk-On/Off Sentiment Proxy[span_1](end_span)
            [span_2](start_span)"USDINR": "INR=X",          # Currency Risk (Critical for FII flows)[span_2](end_span)
            [span_3](start_span)"INDIA_VIX": "^INDIAVIX"    # Fear Gauge[span_3](end_span)
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
        [span_4](start_span)Refactored for higher performance using the fast_info property.[span_4](end_span)
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
                        [span_5](start_span)change_pct = ((last_price - prev_close) / prev_close) * 100[span_5](end_span)
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
        [span_6](start_span)Fetches headlines filtered by time to ensure AI context is fresh.[span_6](end_span)
        """
        headlines = []
        try:
            now = datetime.now()
            [span_7](start_span)cutoff = now - timedelta(hours=max_age_hours)[span_7](end_span)

            for url in self.news_feeds:
                try:
                    feed = feedparser.parse(url)
                    if not feed.entries:
                        continue
                        
                    for entry in feed.entries:
                        try:
                            pub_time = None
                            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                [span_8](start_span)pub_time = datetime.fromtimestamp(time.mktime(entry.published_parsed))[span_8](end_span)
                            
                            # Filter outdated context
                            if pub_time and pub_time < cutoff:
                                continue
                                
                            age_str = "FRESH"
                            if pub_time:
                                age_hours = (now - pub_time).total_seconds() / 3600
                                [span_9](start_span)age_str = f"{age_hours:.1f}h ago"[span_9](end_span)
                            
                            # Clean headline titles
                            clean_title = entry.title.split(' - ')[0]
                            [span_10](start_span)headlines.append(f"[{age_str}] {clean_title}")[span_10](end_span)
                            
                        except Exception:
                            continue
                except Exception:
                    continue
                        
            return list(set(headlines))[:limit]
            
        except Exception as e:
            logger.error(f"News Fetch Error: {e}")
            return []
