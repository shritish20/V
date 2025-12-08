import gzip
import json
import io
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd

from core.config import settings

logger = logging.getLogger("InstrumentMaster")

DATA_DIR = Path("data")
LOCAL_JSON_GZ = DATA_DIR / "complete.json.gz"        # <-- your offline file
CACHE_FILE = DATA_DIR / "instruments_lite.csv"      # pre-filtered NIFTY-only


class InstrumentMaster:
    """
    OFFLINE INSTRUMENT MASTER (NIFTY ONLY)

    - Never calls Upstox over the network.
    - Reads from data/complete.json.gz (full BOD JSON you downloaded).
    - Filters only NIFTY F&O (index options + futures).
    - Caches to instruments_lite.csv for faster subsequent boots.
    """

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.last_updated: Optional[datetime] = None

        self._cache_index_fut: Dict[str, str] = {}
        self._cache_options: Dict[str, str] = {}

        DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ======================================================================
    # PUBLIC ENTRY
    # ======================================================================
    async def download_and_load(self):
        """
        Kept async for compatibility with existing engine.
        But this is now 100% offline:
        - First tries CSV cache.
        - If missing → builds from local JSON.GZ.
        """
        # 1) Try cached CSV
        if CACHE_FILE.exists():
            try:
                df = pd.read_csv(CACHE_FILE)
                if not df.empty:
                    self.df = df
                    self._post_load_processing()
                    logger.info("🚀 Instrument Master: loaded from cached CSV")
                    return
                else:
                    logger.warning("Cached instruments_lite.csv is empty. Rebuilding from JSON.")
            except Exception as e:
                logger.warning(f"Failed to load cache CSV: {e}. Rebuilding from JSON.")

        # 2) Build from local JSON.GZ
        self._build_from_local_json()
        logger.info(f"✅ Instrument Master initialized from {LOCAL_JSON_GZ}")

    # ======================================================================
    # CORE LOAD LOGIC
    # ======================================================================
    def _build_from_local_json(self):
        if not LOCAL_JSON_GZ.exists():
            raise RuntimeError(
                f"Critical: Local instruments file not found: {LOCAL_JSON_GZ}. "
                f"Place your Upstox BOD JSON.GZ there."
            )

        # Read and parse local JSON.GZ
        try:
            with gzip.open(LOCAL_JSON_GZ, "rt", encoding="utf-8") as f:
                json_data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to read local instruments JSON: {e}")

        full_df = pd.DataFrame(json_data)
        if full_df.empty:
            raise RuntimeError("Local instruments JSON produced empty DataFrame.")

        # ------------------------------------------------------------------
        # Upstox JSON fields (from their docs):
        #   - segment: "NSE_FO"
        #   - underlying_symbol: "NIFTY"
        #   - underlying_type: "INDEX"
        #   - instrument_type: "CE"/"PE" for options, something else for futures
        #   - expiry: epoch milliseconds
        #   - strike_price: for options
        #   - instrument_key: unique key
        # ------------------------------------------------------------------

        # Filter to NSE_FO derivatives on NIFTY (index)
        df = full_df[
            (full_df["segment"] == "NSE_FO") &
            (full_df["underlying_symbol"] == "NIFTY")
        ].copy()

        # If underlying_type exists, also enforce INDEX
        if "underlying_type" in df.columns:
            df = df[df["underlying_type"] == "INDEX"]

        # Convert expiry from epoch milliseconds to date
        df["expiry"] = pd.to_datetime(df["expiry"], unit="ms", errors="coerce").dt.date
        df = df.dropna(subset=["expiry"])

        if df.empty:
            raise RuntimeError("Filtered NIFTY instruments are empty after processing.")

        # Save a slim CSV for future boots
        df.to_csv(CACHE_FILE, index=False)
        self.df = df
        self._post_load_processing()

    # ======================================================================
    # POST PROCESSING
    # ======================================================================
    def _post_load_processing(self):
        if self.df is None or self.df.empty:
            raise ValueError("InstrumentMaster loaded empty dataset.")

        # Ensure expiry is date
        if pd.api.types.is_datetime64_any_dtype(self.df["expiry"]):
            self.df["expiry"] = self.df["expiry"].dt.date
        else:
            self.df["expiry"] = pd.to_datetime(self.df["expiry"]).dt.date

        self.last_updated = datetime.now()
        self._cache_index_fut.clear()
        self._cache_options.clear()

        logger.info(f"📊 InstrumentMaster: {len(self.df)} NIFTY rows loaded")

    # ======================================================================
    # FUTURES
    # ======================================================================
    def get_current_future(self, symbol: str = "NIFTY") -> Optional[str]:
        """
        Best-effort NIFTY index future:
        - Filter NIFTY rows, non-option types (instrument_type != CE/PE),
        - Pick nearest expiry >= today.
        """
        if self.df is None or self.df.empty:
            return None

        today = date.today()
        cache_key = f"{symbol}_FUT_{today}"
        if cache_key in self._cache_index_fut:
            return self._cache_index_fut[cache_key]

        df = self.df[self.df["underlying_symbol"] == symbol].copy()
        if "instrument_type" in df.columns:
            # exclude CE/PE (options) → remainder assumed futures
            df = df[~df["instrument_type"].isin(["CE", "PE"])]

        df = df[df["expiry"] >= today].sort_values("expiry")

        if df.empty:
            return None

        token = df.iloc[0]["instrument_key"]
        self._cache_index_fut[cache_key] = token
        return token

    # ======================================================================
    # OPTIONS
    # ======================================================================
    def get_option_token(
        self,
        symbol: str,
        strike: float,
        option_type: str,
        expiry_date: date
    ) -> Optional[str]:
        """
        Map NIFTY + strike + CE/PE + expiry → instrument_key
        Using JSON fields:
          - underlying_symbol == symbol
          - instrument_type == "CE"/"PE"
          - strike_price (not 'strike')
          - expiry
        """
        if self.df is None or self.df.empty:
            return None

        cache_key = f"{symbol}_{strike}_{option_type}_{expiry_date}"
        if cache_key in self._cache_options:
            return self._cache_options[cache_key]

        df = self.df[self.df["underlying_symbol"] == symbol].copy()

        # Options only: CE/PE
        if "instrument_type" in df.columns:
            df = df[df["instrument_type"].isin(["CE", "PE"])]

        # Match expiry
        df = df[df["expiry"] == expiry_date]

        # Match CE/PE
        df = df[df["instrument_type"] == option_type.upper()]

        # Match strike using strike_price column
        strike_col = "strike_price" if "strike_price" in df.columns else "strike"
        df = df[abs(df[strike_col] - float(strike)) < 0.1]

        if df.empty:
            return None

        token = df.iloc[0]["instrument_key"]
        self._cache_options[cache_key] = token
        return token

    # ======================================================================
    # EXPIRIES
    # ======================================================================
    def get_all_expiries(self, symbol: str = "NIFTY") -> List[date]:
        """
        Returns all future expiries for NIFTY options.
        We identify options as instrument_type in ["CE", "PE"].
        """
        if self.df is None or self.df.empty:
            return []

        today = date.today()
        df = self.df[self.df["underlying_symbol"] == symbol].copy()

        if "instrument_type" in df.columns:
            df = df[df["instrument_type"].isin(["CE", "PE"])]

        expiries = df[df["expiry"] >= today]["expiry"].unique()
        expiries = [e for e in expiries if isinstance(e, date)]
        return sorted(expiries)
