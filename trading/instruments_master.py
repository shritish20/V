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

# CRITICAL FIX: Prioritize LOCAL file first, then fallback to downloads
DOWNLOAD_URLS = [
    # PRIMARY: Your GitHub mirror (if you upload it)
    "https://raw.githubusercontent.com/shritish20/VolGuard/main/complete.json.gz",
    # FALLBACK: Upstox official (may be blocked on cloud)
    "https://assets.upstox.com/feed/instruments/NSE_FO/complete.json.gz",
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
]

class InstrumentMaster:
    """
    PRODUCTION-READY Instrument Master for Render Deployment.
    - Prioritizes LOCAL cache (you already have the file)
    - Auto-downloads only if cache is stale
    - Fixes Timezone issues (UTC -> IST)
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
        PRODUCTION FIX v2.0:
        1. Check if LOCAL complete.json.gz exists (you downloaded it)
        2. If exists and fresh -> use it
        3. If missing/stale -> try download with retries
        4. If download fails -> use stale cache (degraded mode)
        """
        today = date.today()
        
        # --- STEP 1: Check if RAW JSON exists locally ---
        if JSON_FILE.exists():
            try:
                # Check file age
                mtime = datetime.fromtimestamp(JSON_FILE.stat().st_mtime).date()
                age_days = (today - mtime).days
                
                if age_days <= 7:  # Fresh within 1 week
                    logger.info(f"✅ Using LOCAL instruments file (age: {age_days} days)")
                    self._process_json_to_csv()
                    return
                else:
                    logger.info(f"⚠️ Local file is {age_days} days old. Attempting refresh...")
            except Exception as e:
                logger.warning(f"⚠️ Local file corrupt: {e}. Downloading fresh...")
        
        # --- STEP 2: Check if processed CSV exists ---
        if CACHE_FILE.exists():
            try:
                mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
                age_days = (today - mtime).days
                
                if age_days <= 7:
                    logger.info(f"✅ Using LOCAL CSV cache (age: {age_days} days)")
                    self.df = pd.read_csv(CACHE_FILE)
                    self._post_load_processing()
                    return
            except Exception:
                logger.warning("⚠️ CSV cache corrupt. Rebuilding...")
        
        # --- STEP 3: Download fresh data ---
        try:
            await self._download_and_build()
        except Exception as e:
            logger.error(f"❌ Download failed: {e}")
            
            # --- STEP 4: Graceful Degradation ---
            # Use ANY available cache, even if stale
            if CACHE_FILE.exists():
                logger.warning("⚠️ USING STALE CACHE (degraded mode)")
                self.df = pd.read_csv(CACHE_FILE)
                self._post_load_processing()
                return
            elif JSON_FILE.exists():
                logger.warning("⚠️ USING STALE JSON (degraded mode)")
                self._process_json_to_csv()
                return
            else:
                raise RuntimeError("🔥 CRITICAL: No instrument data available (local or remote)")

    async def _download_and_build(self):
        """
        PRODUCTION FIX: Robust download with anti-blocking measures
        """
        success = False
        
        # CRITICAL FIX: Enhanced headers to bypass cloud IP blocks
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://upstox.com/",
            "Connection": "keep-alive",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        timeout = aiohttp.ClientTimeout(total=90)  # Generous timeout for slow networks
        
        async with aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
            connector=aiohttp.TCPConnector(
                ssl=False,  # CRITICAL: Bypass SSL verification in Docker
                limit=5,
                force_close=True
            )
        ) as session:
            for i, url in enumerate(DOWNLOAD_URLS):
                try:
                    logger.info(f"🌐 Attempt {i+1}/{len(DOWNLOAD_URLS)}: {url}")
                    
                    # CRITICAL FIX: Add delay between attempts (avoid rate limits)
                    if i > 0:
                        await asyncio.sleep(3)
                    
                    async with session.get(url, timeout=90) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            
                            # Validate data is not empty
                            if len(data) < 1000:
                                logger.warning(f"❌ Downloaded data too small ({len(data)} bytes). Skipping.")
                                continue
                            
                            with open(JSON_FILE, "wb") as f:
                                f.write(data)
                            logger.info(f"✅ Downloaded {len(data):,} bytes successfully!")
                            success = True
                            break
                        else:
                            logger.warning(f"❌ HTTP {resp.status} from {url}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Timeout for {url}")
                except Exception as e:
                    logger.warning(f"❌ Error for {url}: {e}")

        if not success:
            raise RuntimeError("🔥 All download sources failed. Check internet/firewall.")

        # Process downloaded file
        self._process_json_to_csv()

    def _process_json_to_csv(self):
        """
        PRODUCTION FIX: Robust JSON processing with validation
        """
        logger.info("⚙️ Processing instrument file...")
        
        try:
            with gzip.open(JSON_FILE, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read JSON: {e}")

        if not isinstance(data, list) or len(data) == 0:
            raise RuntimeError(f"Invalid JSON structure (expected list, got {type(data)})")

        df = pd.DataFrame(data)
        logger.info(f"📊 Loaded {len(df)} raw instruments")

        # --- FILTERING LOGIC ---
        # 1. Filter for NSE Futures & Options OR NSE Indices
        if 'segment' in df.columns:
            df = df[df["segment"].isin(["NSE_FO", "NSE_INDEX"])]
        elif 'exchange' in df.columns:
            df = df[df["exchange"].isin(["NSE_FO", "NSE_INDEX"])]
        
        logger.info(f"📊 After exchange filter: {len(df)} instruments")

        # 2. Filter for NIFTY and INDIA VIX
        mask = (
            (df["underlying_symbol"] == "NIFTY") | 
            (df["name"] == "NIFTY") |
            (df["name"] == "INDIA VIX") | 
            (df["name"] == "Nifty 50") |
            (df["trading_symbol"].str.contains("NIFTY", case=False, na=False))
        )
        df = df[mask]
        
        logger.info(f"📊 After NIFTY/VIX filter: {len(df)} instruments")

        if df.empty:
            raise RuntimeError("⚠️ No NIFTY/VIX instruments found after filtering!")

        # --- TIMEZONE FIX ---
        try:
            # Handle both millisecond timestamps and date strings
            if df["expiry"].dtype in ['int64', 'float64']:
                df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", utc=True)
                ist = pytz.timezone("Asia/Kolkata")
                df["expiry"] = df["expiry"].dt.tz_convert(ist).dt.date
            else:
                df["expiry"] = pd.to_datetime(df["expiry"], errors='coerce').dt.date
        except Exception as e:
            logger.warning(f"⚠️ Expiry parsing issue: {e}. Using fallback.")
            df["expiry"] = pd.to_datetime(df["expiry"], errors='coerce').dt.date

        # Keep only useful columns
        cols_to_keep = [
            "instrument_key", "trading_symbol", "expiry", 
            "strike_price", "instrument_type", "lot_size", 
            "exchange_token", "name", "underlying_symbol"
        ]
        existing_cols = [c for c in cols_to_keep if c in df.columns]
        df = df[existing_cols]

        # Drop rows with missing critical data
        df = df.dropna(subset=["instrument_key", "trading_symbol"])

        # Save cache
        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()
        logger.info(f"💾 Saved {len(df)} instruments to cache")

    def _post_load_processing(self):
        """Final cleanup after loading data."""
        if self.df is None or self.df.empty:
            raise ValueError("InstrumentMaster loaded empty dataset.")

        # Ensure expiry is valid date
        if "expiry" in self.df.columns:
            self.df["expiry"] = pd.to_datetime(self.df["expiry"], errors='coerce').dt.date

        self.last_updated = datetime.now()
        self._cache_index_fut.clear()
        self._cache_options.clear()
        logger.info(f"🚀 Instrument Master Ready ({len(self.df)} contracts)")

    # ==========================
    #  Lookup Methods
    # ==========================

    def get_current_future(self) -> Optional[str]:
        """Get NIFTY Futures token for nearest expiry."""
        if self.df is None or self.df.empty:
            return None
        
        today = date.today()
        cache_key = f"NIFTY_FUT_{today}"
        
        if cache_key in self._cache_index_fut:
            return self._cache_index_fut[cache_key]

        # Filter for Futures
        futs = self.df[
            (self.df["instrument_type"].isin(["FUT", "FUTIDX"])) & 
            (
                (self.df["name"] == "NIFTY") | 
                (self.df["underlying_symbol"] == "NIFTY")
            ) &
            (self.df["expiry"] >= today)
        ].sort_values("expiry")

        if futs.empty:
            logger.warning("⚠️ No NIFTY futures found")
            return None
            
        token = futs.iloc[0]["instrument_key"]
        self._cache_index_fut[cache_key] = token
        return token

    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        """Find Option Token (CE/PE)."""
        if self.df is None or self.df.empty:
            return None

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
        if self.df is None or self.df.empty:
            return []
        
        today = date.today()
        # Filter for options to get expiries
        opts = self.df[
            (self.df["instrument_type"].isin(["CE", "PE", "OPTIDX"])) &
            (
                (self.df["underlying_symbol"] == symbol) |
                (self.df["name"] == symbol)
            ) &
            (self.df["expiry"] >= today)
        ]
        
        expiries = sorted(opts["expiry"].dropna().unique())
        return expiries
