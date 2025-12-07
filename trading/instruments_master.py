import gzip
import json
import io
import logging
import aiohttp
import pandas as pd
from datetime import datetime, date
from typing import Optional, List
from core.config import settings

logger = logging.getLogger("InstrumentMaster")

# Official Upstox Master URL
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

class InstrumentMaster:
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated = None
        self._cache_index_fut = {}
        self._cache_options = {}

    async def download_and_load(self):
        """
        Downloads the massive NSE master file, unzips it,
        and filters strictly for NIFTY/BANKNIFTY to save RAM.
        """
        logger.info("📥 Downloading Instrument Master from Upstox...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(INSTRUMENT_URL) as resp:
                    if resp.status != 200:
                        raise ValueError(f"Failed to download: {resp.status}")
                    data = await resp.read()
                    
                    # Unzip and Load JSON
                    with gzip.open(io.BytesIO(data), 'rt', encoding='utf-8') as f:
                        json_data = json.load(f)
            
            # Convert to DataFrame
            full_df = pd.DataFrame(json_data)
            
            # --- CRITICAL: Optimization ---
            # Filter strictly for NSE_FO segment and Indices we care about
            # This reduces memory usage from ~400MB to <50MB
            self.df = full_df[
                (full_df['segment'] == 'NSE_FO') & 
                (full_df['name'].isin(['NIFTY', 'BANKNIFTY', 'FINNIFTY']))
            ].copy()
            
            # Clean up huge DF to free memory
            del full_df 
            
            # Parse Expiry Dates
            # Upstox usually sends 'expiry' as "YYYY-MM-DD" in this file
            if not self.df.empty:
                self.df['expiry'] = pd.to_datetime(self.df['expiry'], errors='coerce').dt.date
                self.df = self.df.dropna(subset=['expiry'])

            self.last_updated = datetime.now()
            self._cache_index_fut.clear()
            self._cache_options.clear()
            
            count = len(self.df)
            logger.info(f"✅ Instrument Master Ready. Loaded {count} contracts.")
            
        except Exception as e:
            logger.critical(f"❌ Failed to load Instrument Master: {e}")
            # Do not raise here if you want the bot to retry later, 
            # but for startup, raising is safer.
            raise

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        """Returns the instrument_key for the current month's future"""
        if self.df is None or self.df.empty: 
            return None
            
        today = date.today()
        cache_key = f"{symbol}_FUT_{today}"
        
        if cache_key in self._cache_index_fut: 
            return self._cache_index_fut[cache_key]

        try:
            # Filter for Futures that haven't expired
            futs = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'FUTIDX') & # Upstox uses FUTIDX for index futures
                (self.df['expiry'] >= today)
            ].sort_values('expiry')
            
            if futs.empty: 
                return None
                
            # Grab the nearest expiry
            token = futs.iloc[0]['instrument_key']
            self._cache_index_fut[cache_key] = token
            return token
        except Exception as e:
            logger.error(f"Error resolving future for {symbol}: {e}")
            return None

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        """
        Resolves 'NIFTY 21000 CE 28DEC' -> 'NSE_FO|12345'
        """
        if self.df is None: 
            return None
            
        # Composite cache key
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: 
            return self._cache_options[cache_key]

        try:
            # OPTIDX is standard for Index Options
            opt = self.df[
                (self.df['name'] == symbol) &
                (self.df['strike'] == float(strike)) &
                (self.df['instrument_type'] == 'OPTIDX') & 
                (self.df['option_type'] == option_type) &
                (self.df['expiry'] == expiry_date)
            ]
            
            if opt.empty: 
                logger.warning(f"Token not found for {symbol} {strike} {option_type} {expiry_date}")
                return None
                
            token = opt.iloc[0]['instrument_key']
            self._cache_options[cache_key] = token
            return token
        except Exception as e:
            logger.error(f"Error resolving option token: {e}")
            return None

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        """Helper to get sorted list of available expiries"""
        if self.df is None: return []
        try:
            dates = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'OPTIDX')
            ]['expiry'].unique()
            return sorted(dates)
        except:
            return []
