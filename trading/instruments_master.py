import gzip
import json
import io
import logging
import aiohttp
import pandas as pd
from datetime import datetime, date
from typing import Optional
from core.config import settings

logger = logging.getLogger("InstrumentMaster")
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

class InstrumentMaster:
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated = None
        self._cache_index_fut = {}
        self._cache_options = {}

    async def download_and_load(self):
        logger.info("📥 Downloading Instrument Master from Upstox...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(INSTRUMENT_URL) as resp:
                    if resp.status != 200:
                        raise ValueError(f"Failed to download: {resp.status}")
                    data = await resp.read()
                    with gzip.open(io.BytesIO(data), 'rt') as f:
                        json_data = json.load(f)
            
            self.df = pd.DataFrame(json_data)
            # Filter for NIFTY/BANKNIFTY Futures & Options
            self.df = self.df[
                (self.df['segment'] == 'NSE_FO') & 
                (self.df['name'].isin(['NIFTY', 'BANKNIFTY']))
            ]
            
            # Robust expiry parsing
            # Upstox sometimes sends milliseconds epoch or YYYY-MM-DD
            if not self.df.empty:
                sample = self.df.iloc[0]['expiry']
                if isinstance(sample, int):
                    self.df['expiry'] = pd.to_datetime(self.df['expiry'], unit='ms').dt.date
                else:
                    self.df['expiry'] = pd.to_datetime(self.df['expiry']).dt.date

            self.last_updated = datetime.now()
            self._cache_index_fut.clear()
            self._cache_options.clear()
            logger.info(f"✅ Instrument Master Ready. {len(self.df)} contracts.")
            
        except Exception as e:
            logger.critical(f"❌ Failed to load Instrument Master: {e}")
            raise

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        if self.df is None or self.df.empty: return None
        today = date.today()
        cache_key = f"{symbol}_FUT_{today}"
        if cache_key in self._cache_index_fut: return self._cache_index_fut[cache_key]

        try:
            futs = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'FUT') &
                (self.df['expiry'] >= today)
            ].sort_values('expiry')
            
            if futs.empty: return None
            token = futs.iloc[0]['instrument_key']
            self._cache_index_fut[cache_key] = token
            return token
        except Exception:
            return None

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: return self._cache_options[cache_key]

        try:
            opt = self.df[
                (self.df['name'] == symbol) &
                (self.df['strike'] == float(strike)) &
                (self.df['instrument_type'] == 'OPTIDX') &
                (self.df['option_type'] == option_type) &
                (self.df['expiry'] == expiry_date)
            ]
            if opt.empty: return None
            token = opt.iloc[0]['instrument_key']
            self._cache_options[cache_key] = token
            return token
        except Exception:
            return None
