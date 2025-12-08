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
    PRODUCTION FIXED v2.0:
    - Prevents infinite loop when cache is stale
    - Properly deletes expired cache files
    - Better validation logic
    - Cleaner error handling
    """
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None
        
        # Memory backup for stale data in case download fails
        self._stale_cache: Optional[pd.DataFrame] = None
        
        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}
        
        if not DATA_DIR.exists():
            DATA_DIR.mkdir(parents=True, exist_ok=True)

    async def download_and_load(self):
        """
        PRODUCTION FIX v2.0: Prevents infinite loop and handles stale cache properly.
        """
        # 1. Try loading local cache
        cache_result = self._load_from_cache()
        
        if cache_result == "FRESH":
            logger.info("🚀 Instrument Master loaded from local cache (Fresh)")
            return
        elif cache_result == "STALE":
            logger.info("📦 Cache exists but stale. Keeping memory backup before download.")
            # Stale cache is now in self._stale_cache as backup
        # elif cache_result == "MISSING": pass through to download

        # 2. Attempt download
        logger.info("🌐 Downloading Instrument Master from Upstox...")
        try:
            await self._download_and_process()
            logger.info(f"✅ Download complete. Saved to {CACHE_FILE}")
            
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            
            # 3. CRITICAL FIX: Restore from in-memory stale cache if available
            if self._stale_cache is not None:
                logger.warning("⚠️ NETWORK ERROR: Restoring STALE cache as emergency backup.")
                self.df = self._stale_cache
                self._post_load_processing()
                return

            # 4. Last Resort: Check if file exists on disk (might have been created between steps)
            if CACHE_FILE.exists():
                logger.warning("⚠️ Using STALE local file due to download failure.")
                try:
                    self.df = pd.read_csv(CACHE_FILE)
                    self._post_load_processing()
                except Exception as read_err:
                    logger.critical(f"❌ Stale file read failed: {read_err}")
                    raise RuntimeError("Critical: Cannot load instruments. Download failed and Cache corrupt.") from e
            else:
                raise RuntimeError("Critical: No instruments available (No Cache + Download Failed).") from e

    def _load_from_cache(self) -> str:
        """
        PRODUCTION FIX v2.0: Returns status string to prevent infinite loops.
        
        Returns:
            "FRESH": Cache is valid and loaded
            "STALE": Cache exists but old (saved to memory backup)
            "MISSING": No cache exists
        """
        if not CACHE_FILE.exists():
            return "MISSING"

        try:
            mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
            df = pd.read_csv(CACHE_FILE)
            
            # Validation: Check structure
            required_cols = {'instrument_key', 'name', 'strike', 'option_type', 'expiry', 'instrument_type'}
            if not required_cols.issubset(df.columns):
                logger.error("❌ Cache corrupted: Missing columns")
                self._safe_delete_cache()
                return "MISSING"
                
            if len(df) < 100:
                logger.error("❌ Cache corrupted: File too small")
                self._safe_delete_cache()
                return "MISSING"

            if df['strike'].min() < 100 or df['strike'].max() > 200000:
                logger.error("❌ Cache corrupted: Strike prices out of bounds")
                self._safe_delete_cache()
                return "MISSING"

            # CRITICAL FIX: Check for Future Expiries (validity check)
            temp_expiry = pd.to_datetime(df['expiry'], errors='coerce').dt.date
            today = date.today()
            
            if not temp_expiry[temp_expiry >= today].any():
                logger.error("❌ Cache Useless: All instruments expired. Forcing re-download.")
                self._safe_delete_cache()
                return "MISSING"

            # Cache is structurally valid
            # Check if it's fresh (today) or stale (older)
            if mtime < today:
                logger.info(f"📦 Cache dated ({mtime}). Saving to memory backup before refresh.")
                self._stale_cache = df.copy()  # Save for emergency
                return "STALE"  # ← CRITICAL FIX: Return string instead of False

            # Cache is fresh (today)
            self.df = df
            self._post_load_processing()
            return "FRESH"
            
        except Exception as e:
            logger.warning(f"Cache load failed: {e}")
            self._safe_delete_cache()
            return "MISSING"

    def _safe_delete_cache(self):
        """Helper to safely delete cache file"""
        try:
            if CACHE_FILE.exists():
                CACHE_FILE.unlink()
                logger.debug("🗑️ Deleted stale/corrupt cache file")
        except Exception as e:
            logger.warning(f"Could not delete cache: {e}")

    async def _download_and_process(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENT_URL) as resp:
                if resp.status != 200:
                    raise ValueError(f"Upstox API Error: {resp.status}")
                data = await resp.read()

        with gzip.open(io.BytesIO(data), 'rt', encoding='utf-8') as f:
            json_data = json.load(f)

        full_df = pd.DataFrame(json_data)

        # Filter to NIFTY/BANKNIFTY/FINNIFTY Options and Futures
        filtered_df = full_df[
            (full_df['segment'] == 'NSE_FO') & 
            (full_df['name'].isin(['NIFTY', 'BANKNIFTY', 'FINNIFTY']))
        ].copy()

        del full_df, json_data

        filtered_df['expiry'] = pd.to_datetime(filtered_df['expiry'], errors='coerce').dt.date
        filtered_df = filtered_df.dropna(subset=['expiry'])

        # Save to disk
        filtered_df.to_csv(CACHE_FILE, index=False)
        self.df = filtered_df
        self._post_load_processing()

    def _post_load_processing(self):
        if self.df is None or self.df.empty:
            raise ValueError("Loaded instrument data is empty!")

        # Ensure expiry is date object, not string/timestamp
        if self.df['expiry'].dtype == 'object' or self.df['expiry'].dtype == 'string':
            self.df['expiry'] = pd.to_datetime(self.df['expiry']).dt.date
        elif pd.api.types.is_datetime64_any_dtype(self.df['expiry']):
            self.df['expiry'] = self.df['expiry'].dt.date

        self._cache_index_fut.clear()
        self._cache_options.clear()
        self.last_updated = datetime.now()
        
        logger.debug(f"📊 Loaded {len(self.df)} NIFTY/BANKNIFTY/FINNIFTY contracts")

    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        if self.df is None or self.df.empty: 
            return None
        
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

            if futs.empty: 
                return None
            
            token = futs.iloc[0]['instrument_key']
            self._cache_index_fut[cache_key] = token
            return token
        except Exception:
            return None

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        if self.df is None: 
            return None
        
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: 
            return self._cache_options[cache_key]

        try:
            opt = self.df[
                (self.df['name'] == symbol) & 
                (abs(self.df['strike'] - float(strike)) < 0.1) & 
                (self.df['instrument_type'] == 'OPTIDX') & 
                (self.df['option_type'] == option_type) & 
                (self.df['expiry'] == expiry_date)
            ]
            if opt.empty: 
                return None
            
            token = opt.iloc[0]['instrument_key']
            self._cache_options[cache_key] = token
            return token
        except Exception:
            return None

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        if self.df is None: 
            return []
        try:
            today = date.today()
            dates = self.df[
                (self.df['name'] == symbol) & 
                (self.df['instrument_type'] == 'OPTIDX') & 
                (self.df['expiry'] >= today)  # Only future expiries
            ]['expiry'].unique()
            return sorted(dates)
        except:
            return []
