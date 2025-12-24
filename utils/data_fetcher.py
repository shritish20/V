import asyncio
import logging
from datetime import datetime, timedelta
import pandas as pd

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class DashboardDataFetcher:
    def __init__(self, api):
        self.api = api

    # -----------------------------------------
    # 🔁 RETRY-ENABLED HISTORICAL DATA FETCHER
    # -----------------------------------------
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _fetch_instrument_data(
        self,
        instrument_key: str,
        days_back: int = 365
    ) -> pd.DataFrame:
        """
        Fetch historical daily candles with automatic retry on:
        - API errors
        - Empty / invalid data responses
        """

        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        res = await self.api.get_historical_candles(
            instrument_key,
            "day",
            to_date,
            from_date
        )

        # ---------------------------
        # 🔴 HARD VALIDATION CHECK
        # ---------------------------
        if (
            res.get("status") != "success"
            or not res.get("data")
            or not res["data"].get("candles")
        ):
            raise ValueError(
                f"Retry triggered: Empty or invalid data for {instrument_key}"
            )

        candles = res["data"]["candles"]

        df = pd.DataFrame(
            candles,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
            ],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.normalize()
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        logger.info(f"✅ Loaded {len(df)} candles for {instrument_key}")
        return df

    # -----------------------------------------
    # 🛑 SAFE WRAPPER (NO EXCEPTION LEAK)
    # -----------------------------------------
    async def fetch_instrument_data_safe(
        self,
        instrument_key: str,
        days_back: int = 365
    ) -> pd.DataFrame:
        """
        External-safe wrapper.
        Guarantees no exception propagation to callers.
        """

        try:
            return await self._fetch_instrument_data(instrument_key, days_back)
        except Exception as e:
            logger.error(
                f"❌ FINAL FAILURE: Could not fetch history for {instrument_key}: {e}"
            )
            return pd.DataFrame()
