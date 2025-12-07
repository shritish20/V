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

INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "instruments_lite.csv"

class InstrumentMaster:
    """
    Robust Instrument Master with Cache Integrity Checks.
    """
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None
        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}
        
        if not DATA_DIR.exists():
            DATA_DIR.mkdir(parents=True, exist_ok=True)

    async def download_and_load(self):
        if self._load_from_cache():
            logger.info("🚀 Instrument Master loaded from local cache")
            return

        logger.info("🌐 Downloading Instrument Master from Upstox...")
        try:
            await self._download_and_process()
            logger.info(f"✅ Download complete. Saved to {CACHE_FILE}")
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            if CACHE_FILE.exists():
                logger.warning("⚠️ Using STALE local cache due to download failure.")
                self.df = pd.read_csv(CACHE_FILE)
                self._post_load_processing()
            else:
                raise RuntimeError("Critical: Cannot load instruments. No cache and download failed.") from e

    def _load_from_cache(self) -> bool:
        if not CACHE_FILE.exists():
            return False

        try:
            mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
            if mtime < date.today():
                logger.info(f"Cache expired (Date: {mtime}). Refreshing...")
                return False

            # Load and Validate
            df = pd.read_csv(CACHE_FILE)
            
            # INTEGRITY CHECK 1: Required Columns
            required_cols = {'instrument_key', 'name', 'strike', 'option_type', 'expiry', 'instrument_type'}
            if not required_cols.issubset(df.columns):
                logger.error("❌ Cache corrupted: Missing columns")
                CACHE_FILE.unlink()
                return False
                
            # INTEGRITY CHECK 2: Empty Data
            if len(df) < 100:
                logger.error("❌ Cache corrupted: File too small")
                CACHE_FILE.unlink()
                return False

            self.df = df
            self._post_load_processing()
            return True
            
        except Exception as e:
            logger.warning(f"Cache load failed: {e}")
            if CACHE_FILE.exists():
                CACHE_FILE.unlink() # Nuke corrupt file
            return False

    async def _download_and_process(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENT_URL) as resp:
                if resp.status != 200:
                    raise ValueError(f"Upstox API Error: {resp.status}")
                data = await resp.read()

        with gzip.open(io.BytesIO(data), 'rt', encoding='utf-8') as f:
            json_data = json.load(f)

        full_df = pd.DataFrame(json_data)

        # Optimize: Filter strictly for NSE_FO & Indices
        filtered_df = full_df[
            (full_df['segment'] == 'NSE_FO') & 
            (full_df['name'].isin(['NIFTY', 'BANKNIFTY', 'FINNIFTY']))
        ].copy()

        del full_df, json_data

        filtered_df['expiry'] = pd.to_datetime(filtered_df['expiry'], errors='coerce').dt.date
        filtered_df = filtered_df.dropna(subset=['expiry'])

        filtered_df.to_csv(CACHE_FILE, index=False)
        self.df = filtered_df
        self._post_load_processing()

    def _post_load_processing(self):
        if self.df is None or self.df.empty:
            raise ValueError("Loaded instrument data is empty!")

        if self.df['expiry'].dtype == 'object' or self.df['expiry'].dtype == 'string':
             self.df['expiry'] = pd.to_datetime(self.df['expiry']).dt.date

        self._cache_index_fut.clear()
        self._cache_options.clear()
        self.last_updated = datetime.now()

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        if self.df is None or self.df.empty: return None
        today = date.today()
        cache_key = f"{symbol}_FUT_{today}"
        
        if cache_key in self._cache_index_fut:
            return self._cache_index_fut[cache_key]

        try:
            futs = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'FUTIDX') & 
                (self.df['expiry'] >= today)
            ].sort_values('expiry')

            if futs.empty: return None
            token = futs.iloc[0]['instrument_key']
            self._cache_index_fut[cache_key] = token
            return token
        except Exception:
            return None

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        if self.df is None: return None
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: return self._cache_options[cache_key]

        try:
            opt = self.df[
                (self.df['name'] == symbol) & 
                (abs(self.df['strike'] - float(strike)) < 0.1) & 
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

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        if self.df is None: return []
        try:
            dates = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'OPTIDX')
            ]['expiry'].unique()
            return sorted(dates)
        except:
            return []
