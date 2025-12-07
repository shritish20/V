import gzip
import json
import io
import logging
import aiohttp
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict

from core.config import settings

logger = logging.getLogger("InstrumentMaster")

# Official Upstox Master URL
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
# Local Cache Path
DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "instruments_lite.csv"

class InstrumentMaster:
    """
    Optimized Instrument Master for VolGuard 19.0.
    - Caches filtered data to disk to fix startup latency.
    - Loads strictly NIFTY/BANKNIFTY/FINNIFTY options to save RAM.
    - Auto-refreshes cache if file is older than today.
    """
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None
        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}
        
        # Ensure data directory exists
        if not DATA_DIR.exists():
            DATA_DIR.mkdir(parents=True, exist_ok=True)

    async def download_and_load(self):
        """
        Smart Load Logic:
        1. Check if local cache exists and is from today.
        2. If yes, load from disk (Fast).
        3. If no, download from Upstox, filter, save to disk, then load (Slow first time).
        """
        if self._load_from_cache():
            logger.info("🚀 Instrument Master loaded from local cache (Instant Startup)")
            return

        logger.info("🌐 Local cache stale/missing. Downloading Instrument Master from Upstox...")
        try:
            await self._download_and_process()
            logger.info(f"✅ Download complete. Saved optimized master to {CACHE_FILE}")
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            # Emergency Fallback: Try to load stale cache if download fails
            if CACHE_FILE.exists():
                logger.warning("⚠️ ATTENTION: Using STALE local cache due to download failure.")
                self.df = pd.read_csv(CACHE_FILE)
                self._post_load_processing()
            else:
                # If no cache and no download, we must crash or the bot will trade blind
                raise RuntimeError("Critical: Cannot load instruments. No cache and download failed.") from e

    def _load_from_cache(self) -> bool:
        """Returns True if valid cache was loaded"""
        if not CACHE_FILE.exists():
            return False

        try:
            # Check modification time
            mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
            if mtime < date.today():
                logger.info(f"Cache expired (Date: {mtime}). Refreshing...")
                return False

            # Load CSV
            self.df = pd.read_csv(CACHE_FILE)
            self._post_load_processing()
            return True
        except Exception as e:
            logger.warning(f"Cache load failed (corrupt?): {e}")
            return False

    async def _download_and_process(self):
        """Downloads full master, filters it, and saves only what we need"""
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENT_URL) as resp:
                if resp.status != 200:
                    raise ValueError(f"Upstox API Error: {resp.status}")
                data = await resp.read()

        # Unzip and Load JSON (CPU Intensive)
        with gzip.open(io.BytesIO(data), 'rt', encoding='utf-8') as f:
            json_data = json.load(f)

        full_df = pd.DataFrame(json_data)

        # ---------------------------------------------------------
        # CRITICAL OPTIMIZATION: Filter strictly for NSE_FO & Indices
        # This reduces file size from ~500MB to ~10MB
        # ---------------------------------------------------------
        filtered_df = full_df[
            (full_df['segment'] == 'NSE_FO') & 
            (full_df['name'].isin(['NIFTY', 'BANKNIFTY', 'FINNIFTY']))
        ].copy()

        # Clean up huge DF immediately to free RAM
        del full_df, json_data

        # Pre-process dates before saving
        filtered_df['expiry'] = pd.to_datetime(filtered_df['expiry'], errors='coerce').dt.date
        filtered_df = filtered_df.dropna(subset=['expiry'])

        # Save to CSV for next time
        filtered_df.to_csv(CACHE_FILE, index=False)
        
        self.df = filtered_df
        self._post_load_processing()

    def _post_load_processing(self):
        """Standard processing after loading data source"""
        if self.df is None or self.df.empty:
            raise ValueError("Loaded instrument data is empty!")

        # Ensure expiry is date object (CSV loads it as string)
        if self.df['expiry'].dtype == 'object' or self.df['expiry'].dtype == 'string':
             self.df['expiry'] = pd.to_datetime(self.df['expiry']).dt.date

        # Clear internal lookups
        self._cache_index_fut.clear()
        self._cache_options.clear()
        
        self.last_updated = datetime.now()
        count = len(self.df)
        logger.info(f"Instrument Master Ready. Active Contracts: {count}")

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
                (self.df['instrument_type'] == 'FUTIDX') & 
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
        High-speed lookup using memory cache first.
        """
        if self.df is None:
            return None

        # Composite cache key
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        
        if cache_key in self._cache_options:
            return self._cache_options[cache_key]

        try:
            # OPTIDX is standard for Index Options
            # Ensure strike comparisons are float-safe
            opt = self.df[
                (self.df['name'] == symbol) & 
                (abs(self.df['strike'] - float(strike)) < 0.1) & 
                (self.df['instrument_type'] == 'OPTIDX') & 
                (self.df['option_type'] == option_type) & 
                (self.df['expiry'] == expiry_date)
            ]

            if opt.empty:
                # Debug log only if strictly needed to avoid spam
                # logger.warning(f"Token not found: {symbol} {strike} {option_type} {expiry_date}")
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
