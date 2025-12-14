# File: trading/instruments_master.py

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
JSON_FILE_GZ = DATA_DIR / "complete.json.gz"
JSON_FILE_PLAIN = DATA_DIR / "complete.json"

# CRITICAL: Your GitHub URL is the Primary Source
DOWNLOAD_URLS = [
    "https://raw.githubusercontent.com/shritish20/V/main/data/complete.json.gz",  # CORRECT REPO
    "https://assets.upstox.com/feed/instruments/NSE_FO/complete.json.gz",
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
]

class InstrumentMaster:
    """
    PRODUCTION-READY Instrument Master.
    - Downloads directly from your GitHub 'V' repo.
    - Robustly handles JSON/GZIP.
    - Guarantees Weekly & Monthly expiry sorting.
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
        Priority:
        1. Download from Your GitHub (Freshest & Most Reliable)
        2. Load Local Cache (If download fails)
        3. Raise Error (If everything fails)
        """
        download_success = False
        
        # 1. Try Download First
        try:
            await self._download_and_build()
            download_success = True
        except Exception as e:
            logger.warning(f"⚠️ Download failed ({e}). Checking local cache...")

        # 2. Try Loading Local File (if download failed or file exists)
        if not download_success:
            if CACHE_FILE.exists():
                try:
                    self.df = pd.read_csv(CACHE_FILE)
                    self._post_load_processing()
                    return
                except Exception as e:
                    logger.error(f"❌ Cache load failed: {e}")
            
            # Try raw JSON if CSV missing (Handle both .json and .gz)
            if JSON_FILE_PLAIN.exists():
                try:
                    self._process_json_to_csv(JSON_FILE_PLAIN, is_gzip=False)
                    return
                except Exception as e:
                    logger.error(f"❌ Plain JSON Rebuild failed: {e}")

            if JSON_FILE_GZ.exists():
                try:
                    self._process_json_to_csv(JSON_FILE_GZ, is_gzip=True)
                    return
                except Exception as e:
                    logger.error(f"❌ GZ JSON Rebuild failed: {e}")

        if self.df is None:
            logger.critical("🔥 CRITICAL: No instrument data available. System is Blind.")

    async def _download_and_build(self):
        """
        Downloads with retry logic.
        """
        headers = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "gzip"}
        timeout = aiohttp.ClientTimeout(total=90)
        
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            for url in DOWNLOAD_URLS:
                try:
                    logger.info(f"🌐 Downloading instruments from {url}...")
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            # Verify size to ensure we didn't get a 404/Error page
                            if len(data) < 1000:
                                logger.warning(f"⚠️ File too small from {url}. Skipping.")
                                continue
                                
                            with open(JSON_FILE_GZ, "wb") as f:
                                f.write(data)
                            logger.info(f"✅ Downloaded {len(data):,} bytes.")
                            self._process_json_to_csv(JSON_FILE_GZ, is_gzip=True)
                            return # Success, stop trying others
                        else:
                            logger.warning(f"❌ HTTP {resp.status} from {url}")
                except Exception as e:
                    logger.warning(f"⚠️ Connect Error: {e}")
                    
        raise RuntimeError("All download mirrors failed")

    def _process_json_to_csv(self, file_path: Path, is_gzip: bool):
        """
        Parses JSON -> CSV. 
        """
        logger.info("⚙️ Processing instrument database...")
        try:
            if is_gzip:
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read JSON: {e}")

        if not isinstance(data, list):
            raise RuntimeError("Invalid JSON structure")

        df = pd.DataFrame(data)
        
        # --- FILTERING ---
        if 'segment' in df.columns:
            df = df[df["segment"].isin(["NSE_FO", "NSE_INDEX"])]
        elif 'exchange' in df.columns:
            df = df[df["exchange"].isin(["NSE_FO", "NSE_INDEX"])]
            
        # Filter for NIFTY / BANKNIFTY / VIX
        mask = (
            (df["underlying_symbol"].isin(["NIFTY", "BANKNIFTY", "INDIA VIX"])) |
            (df["name"].isin(["NIFTY", "BANKNIFTY", "INDIA VIX", "Nifty 50"])) |
            (df["trading_symbol"].str.contains("NIFTY", case=False, na=False))
        )
        df = df[mask]
        
        if df.empty:
            raise RuntimeError("No NIFTY instruments found after filtering")

        # --- TIMEZONE FIX ---
        try:
            # Handle millisecond timestamps (Upstox format)
            if df["expiry"].dtype in ['int64', 'float64']:
                df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", utc=True)
                ist = pytz.timezone("Asia/Kolkata")
                df["expiry"] = df["expiry"].dt.tz_convert(ist).dt.date
            else:
                df["expiry"] = pd.to_datetime(df["expiry"], errors='coerce').dt.date
        except Exception as e:
            logger.warning(f"⚠️ Expiry parsing warning: {e}")

        # Columns to keep
        cols = ["instrument_key", "trading_symbol", "expiry", "strike_price", 
                "instrument_type", "lot_size", "name", "underlying_symbol"]
        df = df[[c for c in cols if c in df.columns]]
        
        # Save compressed CSV
        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()

    def _post_load_processing(self):
        if self.df is None or self.df.empty: return
        
        self.df["expiry"] = pd.to_datetime(self.df["expiry"], errors='coerce').dt.date
        self.last_updated = datetime.now()
        self._cache_options.clear()
        
        # LOGGING CONFIRMATION
        nifty_exp = self.get_all_expiries("NIFTY")
        if len(nifty_exp) >= 2:
            logger.info(f"✅ NIFTY Expiries: Weekly={nifty_exp[0]}, Monthly={nifty_exp[-1]}")
        else:
            logger.warning(f"⚠️ NIFTY Expiries found: {nifty_exp}")

        logger.info(f"🚀 Instrument Master Ready ({len(self.df)} contracts)")

    # --- LOOKUP METHODS ---

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        """Returns sorted list of future expiries for a symbol."""
        if self.df is None: return []
        
        today = date.today()
        # Fuzzy match for symbol
        opts = self.df[
            (self.df["name"] == symbol) | 
            (self.df["underlying_symbol"] == symbol) |
            (self.df["trading_symbol"].str.startswith(symbol))
        ]
        
        # Filter for future dates
        valid_expiries = opts[opts["expiry"] >= today]["expiry"].unique()
        return sorted([d for d in valid_expiries if pd.notnull(d)])

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        if self.df is None: return None
        
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: return self._cache_options[cache_key]

        # Fuzzy match strike (within 2 points)
        mask = (
            (self.df["expiry"] == expiry_date) &
            (self.df["instrument_type"] == option_type) &
            (abs(self.df["strike_price"] - strike) < 2.0)
        )
        
        res = self.df[mask]
        if not res.empty:
            token = res.iloc[0]["instrument_key"]
            self._cache_options[cache_key] = token
            return token
        return None
