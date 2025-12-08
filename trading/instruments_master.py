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

# Upstox BOD instruments (JSON, NOT CSV)
INSTRUMENT_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "instruments_lite.csv"


class InstrumentMaster:
    """
    PRODUCTION FIX — Upstox V3 Instrument Master (NIFTY ONLY)

    ✔ Uses underlying_symbol instead of name
    ✔ Parses expiry as epoch milliseconds (unit='ms')
    ✔ Loads only NIFTY index F&O (OPTIDX / FUTIDX)
    ✔ Safe cache handling
    ✔ Prevents infinite loops & stale data problems
    """

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None

        # Backup memory cache on stale data
        self._stale_cache: Optional[pd.DataFrame] = None

        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}

        if not DATA_DIR.exists():
            DATA_DIR.mkdir(parents=True, exist_ok=True)

    # =====================================================================
    # PUBLIC ENTRY
    # =====================================================================
    async def download_and_load(self):
        """Loads cache → else download → else restore stale"""
        cache_status = self._load_from_cache()

        if cache_status == "FRESH":
            logger.info("🚀 Instrument Master loaded from local cache (Fresh)")
            return

        if cache_status == "STALE":
            logger.info("📦 Cache exists but stale. Keeping memory backup.")

        # Try downloading
        try:
            logger.info("🌐 Downloading Instrument Master from Upstox...")
            await self._download_and_process()
            logger.info(f"✅ Download complete. Saved to {CACHE_FILE}")

        except Exception as e:
            logger.error(f"❌ Download failed: {e}")

            if self._stale_cache is not None:
                logger.warning("⚠️ NETWORK ERROR: Restoring STALE cache.")
                self.df = self._stale_cache
                self._post_load_processing()
                return

            if CACHE_FILE.exists():
                try:
                    logger.warning("⚠️ Using STALE file due to download failure.")
                    self.df = pd.read_csv(CACHE_FILE)
                    self._post_load_processing()
                except Exception as read_err:
                    logger.critical(f"❌ Cache read failed: {read_err}")
                    raise RuntimeError("Critical: No instruments available.") from e
            else:
                raise RuntimeError("Critical: No instruments available.") from e

    # =====================================================================
    # CACHE HANDLING
    # =====================================================================
    def _load_from_cache(self) -> str:
        """Returns: FRESH | STALE | MISSING"""
        if not CACHE_FILE.exists():
            return "MISSING"

        try:
            mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
            df = pd.read_csv(CACHE_FILE)

            required_cols = {
                'instrument_key', 'underlying_symbol',
                'strike', 'option_type', 'expiry', 'instrument_type'
            }
            if not required_cols.issubset(df.columns):
                logger.error("❌ Cache corrupted: Missing columns")
                self._safe_delete_cache()
                return "MISSING"

            if len(df) < 50:
                logger.error("❌ Cache corrupted: Too few rows")
                self._safe_delete_cache()
                return "MISSING"

            temp_expiry = pd.to_datetime(df['expiry'], errors='coerce').dt.date
            today = date.today()

            if not temp_expiry[temp_expiry >= today].any():
                logger.error("❌ Cache expired — forcing refresh.")
                self._safe_delete_cache()
                return "MISSING"

            if mtime < today:
                logger.info(f"📦 Cache is stale (mtime={mtime}). Saving backup.")
                self._stale_cache = df.copy()
                return "STALE"

            self.df = df
            self._post_load_processing()
            return "FRESH"

        except Exception:
            self._safe_delete_cache()
            return "MISSING"

    def _safe_delete_cache(self):
        try:
            if CACHE_FILE.exists():
                CACHE_FILE.unlink()
        except Exception:
            pass

    # =====================================================================
    # DOWNLOAD & PROCESS — **MAIN LOGIC**
    # =====================================================================
    async def _download_and_process(self):
        """Downloads NSE full instruments JSON and filters only NIFTY index options & futures."""
        async with aiohttp.ClientSession() as session:
            async with session.get(INSTRUMENT_URL) as resp:
                if resp.status != 200:
                    raise ValueError(f"Upstox API Error: {resp.status}")
                data = await resp.read()

        with gzip.open(io.BytesIO(data), 'rt', encoding='utf-8') as f:
            json_data = json.load(f)

        full_df = pd.DataFrame(json_data)

        # ---------------------------------------
        # ✔ Upstox JSON fields:
        # underlying_symbol: "NIFTY"
        # segment: "NSE_FO"
        # expiry: epoch ms
        # instrument_type: FUTIDX / OPTIDX
        # ---------------------------------------

        filtered_df = full_df[
            (full_df["segment"] == "NSE_FO") &
            (full_df["underlying_symbol"] == "NIFTY") &
            (full_df["instrument_type"].isin(["FUTIDX", "OPTIDX"]))
        ].copy()

        # Convert expiry (epoch ms → date)
        filtered_df["expiry"] = pd.to_datetime(
            filtered_df["expiry"], unit="ms", errors="coerce"
        ).dt.date

        filtered_df = filtered_df.dropna(subset=["expiry"])

        # Save cache
        filtered_df.to_csv(CACHE_FILE, index=False)
        self.df = filtered_df
        self._post_load_processing()

    # =====================================================================
    # POST PROCESSING
    # =====================================================================
    def _post_load_processing(self):
        if self.df is None or self.df.empty:
            raise ValueError("InstrumentMaster loaded empty dataset.")

        # Ensure expiry is date
        if pd.api.types.is_datetime64_any_dtype(self.df['expiry']):
            self.df['expiry'] = self.df['expiry'].dt.date
        else:
            self.df['expiry'] = pd.to_datetime(self.df['expiry']).dt.date

        self._cache_index_fut.clear()
        self._cache_options.clear()
        self.last_updated = datetime.now()

        logger.info(f"📊 Loaded {len(self.df)} NIFTY contracts")

    # =====================================================================
    # FUTURES
    # =====================================================================
    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        if self.df is None or self.df.empty:
            return None

        today = date.today()
        cache_key = f"{symbol}_FUT_{today}"

        if cache_key in self._cache_index_fut:
            return self._cache_index_fut[cache_key]

        futs = self.df[
            (self.df["underlying_symbol"] == symbol) &
            (self.df["instrument_type"] == "FUTIDX") &
            (self.df["expiry"] >= today)
        ].sort_values("expiry")

        if futs.empty:
            return None

        token = futs.iloc[0]["instrument_key"]
        self._cache_index_fut[cache_key] = token
        return token

    # =====================================================================
    # OPTIONS
    # =====================================================================
    def get_option_token(self, symbol: str, strike: float, option_type: str, expiry_date: date) -> Optional[str]:
        if self.df is None:
            return None

        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options:
            return self._cache_options[cache_key]

        opt = self.df[
            (self.df["underlying_symbol"] == symbol) &
            (self.df["instrument_type"] == "OPTIDX") &
            (self.df["option_type"] == option_type) &
            (abs(self.df["strike"] - float(strike)) < 0.1) &
            (self.df["expiry"] == expiry_date)
        ]

        if opt.empty:
            return None

        token = opt.iloc[0]["instrument_key"]
        self._cache_options[cache_key] = token
        return token

    # =====================================================================
    # EXPIRIES
    # =====================================================================
    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        if self.df is None:
            return []

        today = date.today()

        expiries = self.df[
            (self.df["underlying_symbol"] == symbol) &
            (self.df["instrument_type"] == "OPTIDX") &
            (self.df["expiry"] >= today)
        ]["expiry"].unique()

        return sorted(expiries)
