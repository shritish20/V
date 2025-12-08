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

PRIMARY_URL = "https://assets.upstox.com/market-quote/instruments/NSE/instruments.json.gz"
BACKUP_URL_1 = "https://assets-cdn.upstox.com/market-quote/instruments/NSE/instruments.json.gz"
BACKUP_URL_2 = "https://upstox.com/api/instruments/NSE.json.gz"

DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "instruments_lite.csv"


class InstrumentMaster:
    """
    FINAL PRODUCTION VERSION — Render Safe, Upstox Safe

    ✔ Loads ONLY NIFTY INDEX (OPTIDX + FUTIDX)
    ✔ Auto fallback URLs if Upstox CDN fails
    ✔ Handles gzip OR plain JSON automatically
    ✔ No infinite loops
    ✔ No empty dataset failures
    """

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None

        self._stale_cache: Optional[pd.DataFrame] = None

        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}

        DATA_DIR.mkdir(exist_ok=True)

    # ---------------------------------------------------------------------
    async def download_and_load(self):
        """Loads cached instruments or downloads fresh."""
        cache_status = self._load_from_cache()

        if cache_status == "FRESH":
            logger.info("🚀 Using fresh instrument cache")
            return

        if cache_status == "STALE":
            logger.info("📦 Using stale cache backup if needed")

        # Try sequential download attempts
        for url in [PRIMARY_URL, BACKUP_URL_1, BACKUP_URL_2]:
            try:
                logger.info(f"🌐 Trying instrument URL: {url}")
                await self._download_and_process(url)
                logger.info("✅ Instruments downloaded successfully")
                return
            except Exception as e:
                logger.error(f"❌ Failed: {e}")

        # All URLs failed — restore stale cache
        if self._stale_cache is not None:
            logger.warning("⚠️ Restored stale instruments (download failed).")
            self.df = self._stale_cache
            self._post_load_processing()
            return

        raise RuntimeError("Critical: No instruments available.")

    # ---------------------------------------------------------------------
    def _load_from_cache(self) -> str:
        if not CACHE_FILE.exists():
            return "MISSING"

        try:
            df = pd.read_csv(CACHE_FILE)
            if df.empty:
                raise ValueError("Cache empty")

            self.df = df
            self._post_load_processing()

            mtime = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime).date()
            today = date.today()

            if mtime == today:
                return "FRESH"

            self._stale_cache = df.copy()
            return "STALE"

        except Exception:
            return "MISSING"

    # ---------------------------------------------------------------------
    async def _download_and_process(self, url: str):
        """Downloads and parses URL → filters NIFTY ONLY."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status}")
                raw = await resp.read()

        # Try parsing as gzip first
        try:
            with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception:
            # Maybe Upstox sent plain JSON
            logger.warning("⚠️ Gzip failed → trying plain JSON")
            json_data = json.loads(raw.decode())

        full_df = pd.DataFrame(json_data)

        # FILTER — ONLY NIFTY
        df = full_df[
            (full_df["segment"] == "NSE_FO") &
            (full_df["underlying_symbol"] == "NIFTY") &
            (full_df["instrument_type"].isin(["OPTIDX", "FUTIDX"]))
        ].copy()

        # expiry is epoch ms
        df["expiry"] = pd.to_datetime(df["expiry"], unit="ms").dt.date
        df = df.dropna(subset=["expiry"])

        if df.empty:
            raise RuntimeError("Upstox returned empty instrument list.")

        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()

    # ---------------------------------------------------------------------
    def _post_load_processing(self):
        self.df["expiry"] = pd.to_datetime(self.df["expiry"]).dt.date
        self.last_updated = datetime.now()
        self._cache_index_fut.clear()
        self._cache_options.clear()
        logger.info(f"📊 Loaded {len(self.df)} NIFTY contracts")

    # ---------------------------------------------------------------------
    def get_current_future(self, symbol="NIFTY") -> Optional[str]:
        today = date.today()
        key = f"{symbol}_FUT_{today}"

        if key in self._cache_index_fut:
            return self._cache_index_fut[key]

        df = self.df[
            (self.df["underlying_symbol"] == symbol) &
            (self.df["instrument_type"] == "FUTIDX") &
            (self.df["expiry"] >= today)
        ].sort_values("expiry")

        if df.empty:
            return None

        token = df.iloc[0]["instrument_key"]
        self._cache_index_fut[key] = token
        return token

    # ---------------------------------------------------------------------
    def get_option_token(self, symbol, strike, option_type, expiry):
        key = f"{symbol}_{strike}_{option_type}_{expiry}"
        if key in self._cache_options:
            return self._cache_options[key]

        df = self.df[
            (self.df["underlying_symbol"] == symbol) &
            (self.df["instrument_type"] == "OPTIDX") &
            (self.df["option_type"] == option_type) &
            (abs(self.df["strike"] - float(strike)) < 0.1) &
            (self.df["expiry"] == expiry)
        ]

        if df.empty:
            return None

        token = df.iloc[0]["instrument_key"]
        self._cache_options[key] = token
        return token

    # ---------------------------------------------------------------------
    def get_all_expiries(self, symbol="NIFTY") -> List[date]:
        today = date.today()
        return sorted(
            self.df[
                (self.df["underlying_symbol"] == symbol) &
                (self.df["instrument_type"] == "OPTIDX") &
                (self.df["expiry"] >= today)
            ]["expiry"].unique()
        )
