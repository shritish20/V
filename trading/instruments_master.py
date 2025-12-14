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

# PRIMARY SOURCE: Your GitHub Repo (Raw Content URL)
# We use the 'raw.githubusercontent.com' domain which gives the file content directly
DOWNLOAD_URLS = [
    "https://raw.githubusercontent.com/shritish20/V/main/data/complete.json.gz",
    "https://assets.upstox.com/feed/instruments/NSE_FO/complete.json.gz",
]

class InstrumentMaster:
    """
    PRODUCTION INSTRUMENT MASTER.
    - Sources: Local File > Your GitHub > Upstox.
    - No Simulation. Real Data Only.
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
        Main Loading Sequence.
        """
        # 1. Try Local Files First (Fastest)
        if await self._load_local_files():
            return

        # 2. Force Download from GitHub
        logger.info("⬇️ Local files missing/stale. downloading from GitHub...")
        try:
            await self._download_and_build()
        except Exception as e:
            logger.critical(f"🔥 CRITICAL: Failed to download Instrument Data: {e}")
            # NO FALLBACK. System must know data is missing.

    async def _load_local_files(self) -> bool:
        # Check for .json (Uncompressed) - Highest Priority (User Upload)
        if JSON_FILE_PLAIN.exists():
            try:
                self._process_json_to_csv(JSON_FILE_PLAIN, is_gzip=False)
                return True
            except Exception as e:
                logger.error(f"❌ Local JSON corrupted: {e}")

        # Check for .json.gz (Compressed)
        if JSON_FILE_GZ.exists():
            try:
                self._process_json_to_csv(JSON_FILE_GZ, is_gzip=True)
                return True
            except Exception as e:
                logger.error(f"❌ Local GZ corrupted: {e}")

        # Check for .csv (Processed Cache)
        if CACHE_FILE.exists():
            try:
                self.df = pd.read_csv(CACHE_FILE)
                self._post_load_processing()
                return True
            except Exception as e:
                logger.error(f"❌ Cache corrupted: {e}")
        
        return False

    async def _download_and_build(self):
        # GitHub often blocks python-requests/aiohttp, so we use a Browser User-Agent
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Encoding": "gzip"
        }
        timeout = aiohttp.ClientTimeout(total=120) # 2 minutes timeout for large files
        
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            for url in DOWNLOAD_URLS:
                try:
                    logger.info(f"🌐 Fetching: {url}")
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) < 1000:
                                logger.warning(f"⚠️ File too small ({len(data)} bytes). Skipping.")
                                continue
                                
                            # Save to disk
                            with open(JSON_FILE_GZ, "wb") as f:
                                f.write(data)
                            
                            logger.info(f"✅ Downloaded {len(data)/1024/1024:.2f} MB")
                            self._process_json_to_csv(JSON_FILE_GZ, is_gzip=True)
                            return
                        else:
                            logger.warning(f"❌ HTTP {resp.status} error from {url}")
                except Exception as e:
                    logger.warning(f"⚠️ Connect Error ({url}): {e}")
        
        raise RuntimeError("All download sources failed")

    def _process_json_to_csv(self, file_path: Path, is_gzip: bool):
        logger.info("⚙️ Parsing Instrument Database...")
        try:
            if is_gzip:
                with gzip.open(file_path, "rt", encoding="utf-8") as f: data = json.load(f)
            else:
                with open(file_path, "r", encoding="utf-8") as f: data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"JSON Parse Error: {e}")

        if not isinstance(data, list):
            raise RuntimeError("Invalid JSON structure")

        df = pd.DataFrame(data)
        
        # --- STRICT FILTERING ---
        # We strictly need NSE_FO and NSE_INDEX
        mask = (
            (df["segment"] == "NSE_FO") | 
            (df["segment"] == "NSE_INDEX")
        )
        df = df[mask]
        
        # Filter for NIFTY, BANKNIFTY, INDIA VIX
        symbols = ["NIFTY", "BANKNIFTY", "INDIA VIX", "Nifty 50", "Nifty Bank"]
        mask_sym = (
            (df["name"].isin(symbols)) |
            (df["underlying_symbol"].isin(["NIFTY", "BANKNIFTY", "INDIA VIX"])) |
            (df["trading_symbol"].str.contains("NIFTY", case=False, na=False))
        )
        df = df[mask_sym]
        
        if df.empty:
            raise RuntimeError("Filtered dataframe is empty. No NIFTY/BANKNIFTY found.")

        # --- TIMEZONE FIX ---
        # Upstox provides expiry in milliseconds (int)
        if "expiry" in df.columns:
            try:
                # Convert milliseconds to datetime
                if df["expiry"].dtype in ['int64', 'float64']:
                    df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", utc=True)
                    # Convert to IST
                    ist = pytz.timezone("Asia/Kolkata")
                    df["expiry"] = df["expiry"].dt.tz_convert(ist).dt.date
                else:
                    df["expiry"] = pd.to_datetime(df["expiry"], errors='coerce').dt.date
            except Exception as e:
                logger.warning(f"⚠️ Expiry parsing warning: {e}")

        # Select Columns
        cols = ["instrument_key", "trading_symbol", "expiry", "strike_price", 
                "instrument_type", "lot_size", "name", "underlying_symbol", "exchange_token"]
        df = df[[c for c in cols if c in df.columns]]
        
        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()

    def _post_load_processing(self):
        if self.df is None or self.df.empty: return
        self.df["expiry"] = pd.to_datetime(self.df["expiry"], errors='coerce').dt.date
        self.last_updated = datetime.now()
        self._cache_options.clear()
        
        exps = self.get_all_expiries("NIFTY")
        if exps:
            logger.info(f"✅ Data Loaded. NIFTY Expiries: {exps[0]} (Near) to {exps[-1]} (Far)")
        else:
            logger.warning("⚠️ Data Loaded but NO NIFTY Expiries found. Check filtering logic.")

    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        if self.df is None: return []
        today = date.today()
        # Precise Matching
        opts = self.df[
            (self.df["name"] == symbol) | 
            (self.df["underlying_symbol"] == symbol) |
            (self.df["trading_symbol"].str.startswith(symbol))
        ]
        valid_expiries = opts[opts["expiry"] >= today]["expiry"].unique()
        return sorted([d for d in valid_expiries if pd.notnull(d)])

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        if self.df is None: return None
        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options: return self._cache_options[cache_key]

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
