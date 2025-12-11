import yfinance as yf
import feedparser
import logging
from datetime import datetime

logger = logging.getLogger("MarketIntel")

class MarketIntelligence:
    def __init__(self):
        # 1. Global Risk Proxies
        self.tickers = {
            "S&P500_FUT": "ES=F",    # US Markets (Leading indicator)
            "BITCOIN": "BTC-USD",    # Risk-On/Off Sentiment
            "USDINR": "INR=X",       # Currency Risk
            "INDIA_VIX": "^INDIAVIX" # Validation check
        }
        
        # 2. News Sources (RSS)
        self.news_feeds = [
            "https://news.google.com/rss/search?q=Indian+Stock+Market+falling&hl=en-IN&gl=IN&ceid=IN:en",
            "https://news.google.com/rss/search?q=Global+Markets+Crash&hl=en-US&gl=US&ceid=US:en",
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
                        data[name] = round(change_pct, 2)
                    else:
                        data[name] = 0.0
                except Exception:
                    data[name] = 0.0 # Default to neutral on error
                    
        except Exception as e:
            logger.error(f"Macro Fetch Error: {e}")
            
        return data

    def get_latest_headlines(self, limit=5):
        """Fetches top critical headlines with timestamps."""
        headlines = []
        try:
            for url in self.news_feeds:
                feed = feedparser.parse(url)
                # Take top 3 from each feed
                for entry in feed.entries[:3]:
                    try:
                        # Parse timestamp if available
                        if hasattr(entry, 'published_parsed'):
                            dt = datetime(*entry.published_parsed[:6])
                            time_str = dt.strftime("%d-%b %H:%M")
                        else:
                            time_str = "Recent"
                    except:
                        time_str = "Unknown"

                    # Clean up title
                    clean_title = entry.title.split(' - ')[0]
                    headlines.append(f"[{time_str}] {clean_title}")
            
            return list(set(headlines))[:limit]
            
        except Exception as e:
            logger.error(f"News Fetch Error: {e}")
            return []
