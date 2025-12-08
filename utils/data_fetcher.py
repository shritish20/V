import pandas as pd
import numpy as np
import logging
import requests
from io import StringIO
from scipy.stats import percentileofscore
from typing import Optional

logger = logging.getLogger("DataFetcher")

class DashboardDataFetcher:
    """
    Production Data Fetcher.
    Connects to User's 3 GitHub Datasets to power:
    1. GARCH Volatility Model (Nifty 50 Log Returns)
    2. IV Percentile Rank (India VIX History)
    3. Event Risk Scoring (Macro Calendar)
    """
    
    # Dataset URLs
    URL_NIFTY = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/nifty_50.csv"
    URL_VIX = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/vix_ivp.csv"
    URL_EVENTS = "https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/events_calendar.csv"

    def __init__(self):
        self.nifty_data: pd.DataFrame = pd.DataFrame()     # For GARCH
        self.vix_data: pd.DataFrame = pd.DataFrame()       # For IVP
        self.events_calendar: Optional[pd.DataFrame] = None # For Event Risk
        
        # Load all datasets on initialization
        self._load_all_data()

    def _load_all_data(self):
        """Orchestrates the loading of all three datasets."""
        logger.info("🔄 Initializing Data Layer...")
        self._load_nifty_returns()
        self._load_vix_history()
        self._load_event_calendar()
        logger.info("✅ Data Layer Ready.")

    def _fetch_csv(self, url: str) -> Optional[pd.DataFrame]:
        """Helper to download and parse CSV safely."""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                logger.error(f"❌ HTTP {response.status_code} for {url}")
                return None
            return pd.read_csv(StringIO(response.text))
        except Exception as e:
            logger.error(f"❌ Download Failed for {url}: {e}")
            return None

    def _load_nifty_returns(self):
        """
        Loads Nifty 50 Data and calculates 'Log_Returns' for GARCH model.
        Target: analytics/volatility.py
        """
        df = self._fetch_csv(self.URL_NIFTY)
        if df is not None:
            try:
                df.columns = df.columns.str.strip()
                # Parse DD-MMM-YYYY format (e.g., 05-DEC-2025)
                df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y', errors='coerce')
                
                # Sort ascending (Oldest -> Newest) is CRITICAL for return calc
                df = df.sort_values('Date').reset_index(drop=True)
                
                # Calculate Log Returns: ln(Current / Previous)
                if 'Close' in df.columns:
                    df['Log_Returns'] = np.log(df['Close'] / df['Close'].shift(1))
                    df['Log_Returns'] = df['Log_Returns'].fillna(0)
                    self.nifty_data = df
                    logger.info(f"📊 Nifty Data Loaded: {len(df)} rows (Log Returns Ready)")
                else:
                    logger.error("❌ Nifty CSV missing 'Close' column")
            except Exception as e:
                logger.error(f"❌ Nifty Processing Error: {e}")

    def _load_vix_history(self):
        """
        Loads VIX Data for IV Percentile calculations.
        Target: calculate_iv_percentile()
        """
        df = self._fetch_csv(self.URL_VIX)
        if df is not None:
            try:
                df.columns = df.columns.str.strip()
                # Parse DD-MMM-YYYY format (e.g., 09-DEC-2024)
                df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%Y', errors='coerce')
                self.vix_data = df.sort_values('Date')
                logger.info(f"📉 VIX Data Loaded: {len(df)} rows")
            except Exception as e:
                logger.error(f"❌ VIX Processing Error: {e}")

    def _load_event_calendar(self):
        """
        Loads Macro Events and normalizes columns for Risk Engine.
        Target: analytics/events.py
        """
        df = self._fetch_csv(self.URL_EVENTS)
        if df is not None:
            try:
                df.columns = df.columns.str.strip()
                
                # 1. Parse YYYY-MM-DD format (e.g., 2025-11-25)
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                
                # 2. Normalize Columns for events.py compatibility
                # events.py looks for: 'Event' and 'Impact'
                # Your CSV has: 'Event' and 'Importance'
                if 'Importance' in df.columns:
                    df = df.rename(columns={'Importance': 'Impact'})
                
                self.events_calendar = df
                logger.info(f"📅 Event Calendar Loaded: {len(df)} events")
                
            except Exception as e:
                logger.error(f"❌ Event Calendar Processing Error: {e}")

    def calculate_iv_percentile(self, current_vix: float, lookback_days: int = 252) -> float:
        """
        Calculates strict IV Percentile from loaded VIX history.
        """
        if self.vix_data.empty or 'Close' not in self.vix_data.columns:
            logger.warning("⚠️ VIX Data missing. Returning neutral 50% IVP.")
            return 50.0

        try:
            # Use last 'lookback_days' of VIX closes
            history = self.vix_data['Close'].tail(lookback_days).values
            
            if len(history) < 10:
                return 50.0

            # Calculate Percentile Rank (Kind='weak' matches standard finance definition)
            ivp = percentileofscore(history, current_vix, kind='weak')
            return float(ivp)

        except Exception as e:
            logger.error(f"❌ IVP Calculation Failed: {e}")
            return 50.0

    def get_market_status(self) -> str:
        return "OPEN"
