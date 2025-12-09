import gzip
import json
import logging
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict
import pytz

# Configure Logging
logger = logging.getLogger("InstrumentMaster")

# --- CONFIGURATION ---
DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "instruments_lite.csv"
JSON_FILE = DATA_DIR / "complete.json.gz"

# List of Upstox URLs to try (Primary -> Backup -> Legacy)
DOWNLOAD_URLS = [
    "https://assets.upstox.com/feed/instruments/NSE_FO/complete.json.gz",
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
    "https://api.upstox.com/v2/feed/instruments/NSE_FO/complete.json.gz"
]

class InstrumentMaster:
    """
    Robust Instrument Master for Render Deployment.
    - Auto-downloads from Upstox (tries multiple mirrors).
    - Filters for NIFTY 50 and INDIA VIX.
    - Fixes Timezone issues (UTC -> IST).
    """

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None
        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}

        # Ensure data dir exists
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    async def download_and_load(self):
        """
        Main Entry Point.
        1. Checks if we have a fresh cache (created today).
        2. If not, downloads fresh data from Upstox.
        3. Processes and filters for NIFTY.
        """
        today = date.today()
        
        # 1. Check Local Cache Validity
        if CACHE_FILE.exists():
            try:
                # Check file modification time
                mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
                if mtime == today:
                    logger.info(f"✅ Found fresh local cache from {mtime}")
                    self.df = pd.read_csv(CACHE_FILE)
                    self._post_load_processing()
                    return
                else:
                    logger.info(f"⚠️ Cache is stale ({mtime}). Downloading fresh data...")
            except Exception:
                logger.warning("⚠️ Cache file corrupt. Redownloading.")

        # 2. Download and Build
        await self._download_and_build()

    async def _download_and_build(self):
        """Try downloading from multiple URLs until one succeeds."""
        success = False
        
        # FIX: Add User-Agent to prevent 403 Forbidden errors
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in DOWNLOAD_URLS:
                try:
                    logger.info(f"🌐 Attempting download from: {url}")
                    async with session.get(url, timeout=30) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            with open(JSON_FILE, "wb") as f:
                                f.write(data)
                            logger.info("✅ Download successful!")
                            success = True
                            break
                        else:
                            logger.warning(f"❌ Failed {url} [Status: {resp.status}]")
                except Exception as e:
                    logger.warning(f"❌ Connection error for {url}: {e}")

        if not success:
            raise RuntimeError("🔥 CRITICAL: Could not download instruments from ANY source. Check internet/DNS.")

        # 3. Process the JSON
        self._process_json_to_csv()

    def _process_json_to_csv(self):
        """Reads JSON, filters NIFTY/VIX, Fixes Timezones, Saves CSV."""
        logger.info("⚙️ Processing instrument file...")
        
        try:
            with gzip.open(JSON_FILE, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read downloaded JSON: {e}")

        df = pd.DataFrame(data)

        # --- FILTERING LOGIC ---
        # 1. Filter for NSE Futures & Options OR NSE Indices
        # VIX is often in "NSE_INDEX" segment or just "NSE_FO" depending on the file
        if 'segment' in df.columns:
            df = df[df["segment"].isin(["NSE_FO", "NSE_INDEX"])]
        elif 'exchange' in df.columns:
             df = df[df["exchange"].isin(["NSE_FO", "NSE_INDEX"])]

        # 2. Filter strictly for NIFTY Index AND INDIA VIX
        # CRITICAL FIX: Explicitly include "INDIA VIX"
        mask = (
            (df["underlying_symbol"] == "NIFTY") | 
            (df["name"] == "NIFTY") |
            (df["name"] == "INDIA VIX") | 
            (df["name"] == "Nifty 50")
        )
        df = df[mask]

        if df.empty:
            raise RuntimeError("⚠️ Filtered NIFTY/VIX instruments are empty! structure changed?")

        # --- TIMEZONE FIX (Critical for Render/Server) ---
        try:
            # Convert milliseconds to datetime (UTC)
            df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", utc=True)
            
            # Convert to IST
            ist = pytz.timezone("Asia/Kolkata")
            df["expiry"] = df["expiry"].dt.tz_convert(ist).dt.date
        except Exception as e:
            # Fallback for simple date strings
            df["expiry"] = pd.to_datetime(df["expiry"]).dt.date

        # Keep only useful columns to save RAM
        cols_to_keep = [
            "instrument_key", "trading_symbol", "expiry", 
            "strike_price", "instrument_type", "lot_size", "exchange_token", "name"
        ]
        existing_cols = [c for c in cols_to_keep if c in df.columns]
        df = df[existing_cols]

        # Save Cache
        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()
        logger.info(f"💾 Saved {len(df)} instruments to cache.")

    def _post_load_processing(self):
        """Final cleanup after loading data."""
        if self.df is None or self.df.empty:
            raise ValueError("InstrumentMaster loaded empty dataset.")

        # Ensure expiry is valid date object where applicable
        if "expiry" in self.df.columns and not self.df["expiry"].isnull().all():
             self.df["expiry"] = pd.to_datetime(self.df["expiry"]).dt.date

        self.last_updated = datetime.now()
        self._cache_index_fut.clear()
        self._cache_options.clear()
        logger.info("🚀 Instrument Master Ready.")

    # ==========================
    #  Lookup Methods
    # ==========================

    def get_current_future(self) -> Optional[str]:
        """Get NIFTY Futures token for nearest expiry."""
        if self.df is None: return None
        
        today = date.today()
        cache_key = f"NIFTY_FUT_{today}"
        
        if cache_key in self._cache_index_fut:
            return self._cache_index_fut[cache_key]

        # Filter for Futures
        futs = self.df[
            (self.df["instrument_type"].isin(["FUT", "FUTIDX"])) & 
            (self.df["name"] == "NIFTY") &
            (self.df["expiry"] >= today)
        ].sort_values("expiry")

        if futs.empty:
            return None
            
        token = futs.iloc[0]["instrument_key"]
        self._cache_index_fut[cache_key] = token
        return token

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        """Find Option Token (CE/PE)."""
        if self.df is None: return None

        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options:
            return self._cache_options[cache_key]

        # Filter: Expiry match + CE/PE match + Strike close enough
        opt = self.df[
            (self.df["expiry"] == expiry_date) &
            (self.df["instrument_type"] == option_type.upper()) &
            (abs(self.df["strike_price"] - float(strike)) < 2.0) 
        ]

        if opt.empty:
            return None
        
        token = opt.iloc[0]["instrument_key"]
        self._cache_options[cache_key] = token
        return token

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        """Get sorted list of future expiries."""
        if self.df is None: return []
        
        today = date.today()
        # Filter for options only to get expiries
        opts = self.df[
            (self.df["instrument_type"].isin(["CE", "PE", "OPTIDX"])) &
            (self.df["expiry"] >= today)
        ]
        
        expiries = sorted(opts["expiry"].unique())
        return expiries
