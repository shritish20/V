# utils/data_fetcher.py
import pandas as pd
import numpy as np
import logging
import asyncio
from datetime import datetime, timedelta, date as date_type
from zoneinfo import ZoneInfo
from sqlalchemy import select, and_
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbHistoricalCandle

logger = logging.getLogger("DataFetcher")
IST = ZoneInfo("Asia/Kolkata")

class DashboardDataFetcher:
    def __init__(self, api_client):
        self.api = api_client
        self.db = HybridDatabaseManager() # Singleton
        self.cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
        self.nifty_data: pd.DataFrame = pd.DataFrame(columns=self.cols)
        self.vix_data: pd.DataFrame = pd.DataFrame(columns=self.cols)
        self.events_calendar = None

    async def load_all_data(self):
        """
        NEW LOGIC:
        1. Attempt to load 365 days of history from Database (Fast).
        2. Identify missing date ranges.
        3. Fetch missing data from Upstox & Persist to Database.
        """
        logger.info("🔄 Synchronizing Persistent Volatility History...")
        
        # Load NIFTY
        self.nifty_data = await self._sync_instrument_history(settings.MARKET_KEY_INDEX)
        
        # Calculate Log Returns for GARCH/RV
        if not self.nifty_data.empty and 'close' in self.nifty_data.columns:
            self.nifty_data['Log_Returns'] = np.log(
                self.nifty_data['close'] / self.nifty_data['close'].shift(1)
            ).fillna(0)

        # Load VIX
        self.vix_data = await self._sync_instrument_history(settings.MARKET_KEY_VIX)
        
        logger.info(
            f"✅ History Ready: NIFTY({len(self.nifty_data)} rows) | VIX({len(self.vix_data)} rows)"
        )

    async def _sync_instrument_history(self, instrument_key: str, days_back: int = 365) -> pd.DataFrame:
        """
        Main Sync Engine: Loads from DB, fetches missing from API, saves to DB.
        """
        try:
            # 1. Load existing from DB
            db_df = await self._load_from_db(instrument_key, days_back)
            
            now_ist = datetime.now(IST)
            today = now_ist.date()
            cutoff_date = today - timedelta(days=days_back)
            
            # 2. Determine Missing Range
            if db_df.empty:
                from_date, to_date = cutoff_date, today
                logger.info(f"📥 No cache for {instrument_key}. Fetching full 365 days.")
            else:
                latest_date = db_df.index.max().date()
                if latest_date >= today:
                    return db_df # Already up to date
                
                from_date = latest_date + timedelta(days=1)
                to_date = today
                logger.info(f"📥 Syncing {instrument_key} gaps from {from_date} to {to_date}")

            # 3. Fetch Missing from Upstox
            new_data = await self._fetch_upstox_range(instrument_key, from_date, to_date)
            
            if not new_data.empty:
                # 4. Persist new data to DB
                await self._save_to_db(instrument_key, new_data)
                
                # 5. Merge and return
                if not db_df.empty:
                    combined = pd.concat([db_df, new_data]).sort_index()
                    return combined[~combined.index.duplicated(keep='last')]
                return new_data
                
            return db_df
        except Exception as e:
            logger.error(f"Sync error for {instrument_key}: {e}")
            return pd.DataFrame(columns=self.cols)

    async def _load_from_db(self, instrument_key: str, days_back: int) -> pd.DataFrame:
        """Loads historical candles from PostgreSQL."""
        cutoff = datetime.now(IST).date() - timedelta(days=days_back)
        try:
            async with self.db.get_session() as session:
                stmt = select(DbHistoricalCandle).where(
                    and_(
                        DbHistoricalCandle.instrument_key == instrument_key,
                        DbHistoricalCandle.date >= cutoff
                    )
                ).order_by(DbHistoricalCandle.date)
                res = await session.execute(stmt)
                rows = res.scalars().all()
            
            if not rows: return pd.DataFrame(columns=self.cols)
            
            df = pd.DataFrame([{
                'timestamp': r.date, 'open': r.open, 'high': r.high, 
                'low': r.low, 'close': r.close, 'volume': r.volume, 'oi': r.oi
            } for r in rows])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df.set_index('timestamp')
        except Exception as e:
            logger.error(f"DB Load error: {e}")
            return pd.DataFrame(columns=self.cols)

    async def _fetch_upstox_range(self, key: str, start: date_type, end: date_type) -> pd.DataFrame:
        """Fetches a specific historical range from Upstox."""
        try:
            res = await self.api.get_historical_candles(key, "day", end.strftime("%Y-%m-%d"), start.strftime("%Y-%m-%d"))
            if res.get("status") != "success" or not res.get("data", {}).get("candles"):
                return pd.DataFrame(columns=self.cols)
            
            df = pd.DataFrame(res["data"]["candles"], columns=self.cols)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(IST).dt.normalize()
            df.set_index("timestamp", inplace=True)
            return df.sort_index()
        except Exception as e:
            logger.error(f"Upstox fetch error: {e}")
            return pd.DataFrame(columns=self.cols)

    async def _save_to_db(self, instrument_key: str, df: pd.DataFrame):
        """Persists candles to database with UPSERT (merge) logic."""
        try:
            async with self.db.get_session() as session:
                for ts, row in df.iterrows():
                    candle = DbHistoricalCandle(
                        instrument_key=instrument_key,
                        date=ts.date() if hasattr(ts, 'date') else ts,
                        open=float(row['open']),
                        high=float(row['high']),
                        low=float(row['low']),
                        close=float(row['close']),
                        volume=float(row['volume']),
                        oi=float(row['oi'])
                    )
                    await session.merge(candle)
                await self.db.safe_commit(session)
                logger.info(f"💾 Persisted {len(df)} candles for {instrument_key}")
        except Exception as e:
            logger.error(f"DB Save error: {e}")

    # Legacy compatibility wrapper
    async def fetch_instrument_data_safe(self, key: str, days: int = 365):
        return await self._sync_instrument_history(key, days)
