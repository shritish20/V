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
        self.nifty_data: pd.DataFrame = pd.DataFrame() 
        self.vix_data: pd.DataFrame = pd.DataFrame()
        self.events_calendar = None

    async def load_all_data(self):
        logger.info("🔄 Fetching Volatility Data (Live Stitch)...")
        await self._fetch_nifty_history()
        await self._fetch_vix_history()
        logger.info("✅ Data Layer Hydrated.")

    async def _fetch_instrument_data(self, instrument_key: str, days_back=365) -> pd.DataFrame:
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        df = pd.DataFrame()

        # 1. Historical (V2)
        try:
            res = await self.api.get_historical_candles(instrument_key, "day", to_date, from_date)
            if res.get("status") == "success":
                data = res.get("data", {}).get("candles", [])
                if data:
                    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
                    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.normalize()
                    df.set_index('timestamp', inplace=True)
                    df.sort_index(inplace=True)
        except Exception as e:
            logger.error(f"History Fetch Failed {instrument_key}: {e}")

        # 2. Live (Today) - Using Market Quote OHLC V3
        try:
            res = await self.api.get_market_quote_ohlc(instrument_key, "1d")
            if res.get("status") == "success":
                quote = res.get("data", {}).get(instrument_key, {}).get("ohlc", {})
                if quote:
                    today_ts = pd.to_datetime(datetime.now().date()).normalize()
                    new_row = [quote['open'], quote['high'], quote['low'], quote['close'], 0, 0] # Vol/OI might be missing
                    
                    if today_ts in df.index:
                        df.loc[today_ts] = new_row
                    else:
                        row_df = pd.DataFrame([new_row], 
                                            columns=['open', 'high', 'low', 'close', 'volume', 'oi'], 
                                            index=[today_ts])
                        df = pd.concat([df, row_df])
        except Exception:
            pass

        return df

    async def _fetch_nifty_history(self):
        df = await self._fetch_instrument_data(settings.MARKET_KEY_INDEX)
        if not df.empty:
            df['Log_Returns'] = np.log(df['close'] / df['close'].shift(1)).fillna(0)
            self.nifty_data = df

    async def _fetch_vix_history(self):
        df = await self._fetch_instrument_data(settings.MARKET_KEY_VIX)
        if not df.empty:
            df = df.rename(columns={'close': 'Close'})
            self.vix_data = df
