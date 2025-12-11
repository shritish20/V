import yfinance as yf
import feedparser
import logging
import time
from datetime import datetime, timedelta
from core.config import settings

logger = logging.getLogger("MarketIntel")

class MarketIntelligence:
    def __init__(self):
        # 1. Global Risk Proxies
        self.tickers = {
            "S&P500_FUT": "ES=F",       # US Markets (Leading indicator)
            "BITCOIN": "BTC-USD",       # Risk-On/Off Sentiment
            "USDINR": "INR=X",          # Currency Risk
            "INDIA_VIX": "^INDIAVIX"    # Validation check
        }
        
        # 2. News Sources (RSS)
        self.news_feeds = [
            "https://news.google.com/rss/search?q=Indian+Stock+Market+falling&hl=en-IN&gl=IN&ceid=IN:en",
            "https://news.google.com/rss/search?q=Global+Markets+Crash&hl=en-US&gl=US&ceid=US:en",
            "https://news.google.com/rss/search?q=Nifty+50+Prediction&hl=en-IN&gl=IN&ceid=IN:en"
        ]

    def get_macro_sentiment(self):
        """Fetches live % change of global assets."""
        data = {}
        try:
            # yfinance allows fetching multiple tickers in one call (faster)
            tickers_list = " ".join(self.tickers.values())
            tickers = yf.Tickers(tickers_list)
            
            for name, symbol in self.tickers.items():
                try:
                    # Fast info avoids downloading full history
                    info = tickers.tickers[symbol].fast_info
                    last_price = info.last_price
                    prev_close = info.previous_close
                    
                    if last_price and prev_close:
                        change_pct = ((last_price - prev_close) / prev_close) * 100
                        data[name] = f"{change_pct:+.2f}%"
                    else:
                        data[name] = "N/A"
                except Exception:
                    data[name] = "N/A" # Default to neutral on error
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
            # Calculate cutoff time (current time - max_age_hours)
            now = datetime.now()
            cutoff = now - timedelta(hours=max_age_hours)

            for url in self.news_feeds:
                try:
                    feed = feedparser.parse(url)
                    if not feed.entries:
                        continue
                        
                    for entry in feed.entries:
                        try:
                            # 1. Parse Timestamp
                            pub_time = None
                            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                # Convert struct_time to datetime
                                pub_time = datetime.fromtimestamp(time.mktime(entry.published_parsed))
                            
                            # 2. Filter Old News
                            if pub_time and pub_time < cutoff:
                                continue
                                
                            # 3. Format Output
                            age_str = "FRESH"
                            if pub_time:
                                age_hours = (now - pub_time).total_seconds() / 3600
                                age_str = f"{age_hours:.1f}h ago"
                            
                            clean_title = entry.title.split(' - ')[0] # Remove source name often at end
                            headlines.append(f"[{age_str}] {clean_title}")
                            
                        except Exception:
                            continue
                except Exception:
                    continue
                        
            # Return top N unique headlines
            return list(set(headlines))[:limit]
            
        except Exception as e:
            logger.error(f"News Fetch Error: {e}")
            return []
