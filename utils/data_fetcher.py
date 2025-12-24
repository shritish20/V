import pandas as pd
import numpy as np
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from core.config import settings
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger("DataFetcher")

class DashboardDataFetcher:
    def __init__(self, api_client):
        self.api = api_client
        self.cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
        # Initialize with specific columns immediately to prevent KeyErrors in analytics
        self.nifty_data: pd.DataFrame = pd.DataFrame(columns=self.cols)
        self.vix_data: pd.DataFrame = pd.DataFrame(columns=self.cols)
        self.events_calendar = None

    async def load_all_data(self):
        """Main entry point called by Engine startup and periodic refresh."""
        logger.info("🔄 Hydrating Volatility Data (Historical + Live Stitch)...")
        
        # We use the safe wrapper to ensure the engine doesn't crash if Upstox is down
        self.nifty_data = await self._fetch_nifty_with_returns()
        self.vix_data = await self.fetch_instrument_data_safe(settings.MARKET_KEY_VIX)
        
        logger.info(f"✅ Data Hydrated. Nifty Rows: {len(self.nifty_data)} | VIX Rows: {len(self.vix_data)}")

    async def _fetch_nifty_with_returns(self) -> pd.DataFrame:
        """Fetches Nifty and pre-calculates Log Returns for GARCH/RV models."""
        df = await self.fetch_instrument_data_safe(settings.MARKET_KEY_INDEX)
        if not df.empty and 'close' in df.columns:
            df['Log_Returns'] = np.log(df['close'] / df['close'].shift(1)).fillna(0)
            return df
        return pd.DataFrame(columns=self.cols + ['Log_Returns'])

    # -----------------------------------------
    # 🔁 RETRY-ENABLED HISTORICAL DATA FETCHER
    # -----------------------------------------
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _fetch_instrument_data(self, instrument_key: str, days_back: int = 365) -> pd.DataFrame:
        """
        Fetches historical daily candles AND stitches today's live OHLC.
        Retries up to 5 times on network issues or empty data.
        """
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # 1. Fetch Historical Data
        res = await self.api.get_historical_candles(instrument_key, "day", to_date, from_date)

        # Hard validation: Upstox sometimes returns 'success' but no data
        if res.get("status") != "success" or not res.get("data") or not res["data"].get("candles"):
            raise ValueError(f"Empty API response for {instrument_key}")

        candles = res["data"]["candles"]
        df = pd.DataFrame(candles, columns=self.cols)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.normalize()
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        # 2. Live Stitching (Today's Data)
        # Required because historical-candles usually only goes up to YESTERDAY
        try:
            live_res = await self.api.get_market_quote_ohlc(instrument_key, "1d")
            if live_res.get("status") == "success":
                quote_wrapper = live_res.get("data", {}).get(instrument_key, {})
                quote = quote_wrapper.get("ohlc", quote_wrapper)
                
                if quote and 'close' in quote:
                    today_ts = pd.to_datetime(datetime.now().date()).normalize()
                    new_row = [
                        quote.get('open', 0), quote.get('high', 0),
                        quote.get('low', 0), quote.get('close', 0),
                        quote.get('volume', 0), quote.get('oi', 0)
                    ]
                    # Update today's candle or append it
                    df.loc[today_ts] = new_row
        except Exception as stitch_err:
            logger.warning(f"⚠️ Live stitch skipped for {instrument_key}: {stitch_err}")

        logger.info(f"✅ Loaded {len(df)} candles for {instrument_key}")
        return df

    # -----------------------------------------
    # 🛑 SAFE WRAPPER
    # -----------------------------------------
    async def fetch_instrument_data_safe(self, instrument_key: str, days_back: int = 365) -> pd.DataFrame:
        """Guarantees no exception propagation to the main Engine loop."""
        try:
            return await self._fetch_instrument_data(instrument_key, days_back)
        except Exception as e:
            logger.error(f"❌ FINAL FAILURE: Could not fetch history for {instrument_key}: {e}")
            return pd.DataFrame(columns=self.cols)
