import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Optional
from core.config import settings

logger = logging.getLogger("DataFetcher")

class DashboardDataFetcher:
    def __init__(self, api_client):
        self.api = api_client
        # FIX: Define columns explicitly to prevent KeyError 'close' on empty data
        self.cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
        
        # Initialize with specific columns immediately
        self.nifty_data: pd.DataFrame = pd.DataFrame(columns=self.cols) 
        self.vix_data: pd.DataFrame = pd.DataFrame(columns=self.cols)
        self.events_calendar = None

    async def load_all_data(self):
        logger.info("🔄 Fetching Volatility Data (Live Stitch)...")
        await self._fetch_nifty_history()
        await self._fetch_vix_history()
        logger.info(f"✅ Data Hydrated. Nifty Rows: {len(self.nifty_data)} | VIX Rows: {len(self.vix_data)}")

    async def _fetch_instrument_data(self, instrument_key: str, days_back=365) -> pd.DataFrame:
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        # Initialize EMPTY but VALID DataFrame (Prevents KeyErrors)
        df = pd.DataFrame(columns=self.cols)

        # 1. Historical (V2/V3)
        try:
            res = await self.api.get_historical_candles(instrument_key, "day", to_date, from_date)
            if res.get("status") == "success":
                data = res.get("data", {}).get("candles", [])
                if data:
                    hist_df = pd.DataFrame(data, columns=self.cols)
                    hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp']).dt.normalize()
                    hist_df.set_index('timestamp', inplace=True)
                    hist_df.sort_index(inplace=True)
                    df = hist_df
                else:
                    logger.warning(f"⚠️ Upstox returned 0 history candles for {instrument_key}")
        except Exception as e:
            logger.error(f"History Fetch Failed {instrument_key}: {e}")

        # 2. Live (Today)
        try:
            res = await self.api.get_market_quote_ohlc(instrument_key, "1d")
            if res.get("status") == "success":
                quote_wrapper = res.get("data", {}).get(instrument_key, {})
                quote = quote_wrapper.get("ohlc", quote_wrapper)
                
                if quote and 'close' in quote:
                    today_ts = pd.to_datetime(datetime.now().date()).normalize()
                    
                    # Ensure all fields exist, default to 0 if missing
                    new_row = [
                        quote.get('open', 0), 
                        quote.get('high', 0), 
                        quote.get('low', 0), 
                        quote.get('close', 0), 
                        quote.get('volume', 0), 
                        quote.get('oi', 0)
                    ]
                    
                    # Create row DF with explicit columns to match main DF
                    row_df = pd.DataFrame([new_row], columns=['open', 'high', 'low', 'close', 'volume', 'oi'], index=[today_ts])
                    
                    if today_ts in df.index:
                        df.loc[today_ts] = new_row
                    else:
                        df = pd.concat([df, row_df])
        except Exception:
            pass

        return df

    async def _fetch_nifty_history(self):
        df = await self._fetch_instrument_data(settings.MARKET_KEY_INDEX)
        # Check if 'close' exists before doing math
        if not df.empty and 'close' in df.columns:
            df['Log_Returns'] = np.log(df['close'] / df['close'].shift(1)).fillna(0)
            self.nifty_data = df
        else:
            # Fallback to empty structure with correct columns + Log_Returns
            self.nifty_data = pd.DataFrame(columns=self.cols + ['Log_Returns'])

    async def _fetch_vix_history(self):
        df = await self._fetch_instrument_data(settings.MARKET_KEY_VIX)
        if not df.empty and 'close' in df.columns:
            self.vix_data = df
        else:
            # Fallback to empty structure
            self.vix_data = pd.DataFrame(columns=self.cols)
