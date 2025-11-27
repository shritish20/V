import pandas as pd 
from datetime import datetime, timedelta
from typing import Dict
import logging

logger = logging.getLogger("VolGuardHybrid")

class EventIntelligence:
    """Advanced event risk scoring"""
    def __init__(self):
        self.calendar = pd.DataFrame()
        self.risk_cache: Dict[str, float] = {}
        self._load_calendar()

    def _load_calendar(self):
        """Load event calendar"""
        try:
            df = pd.read_csv("https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/events_calendar.csv")
            df['Date'] = pd.to_datetime(df['Date'])
            self.calendar = df
            logger.info(f"Loaded {len(df)} macro events")
        except Exception as e: 
            logger.warning(f"Event calendar load failed: {e}")
            self._create_default_calendar()

    def _create_default_calendar(self):
        """Create default event calendar"""
        dates = pd.date_range(start='2024-01-01', end='2025-12-31', freq='D')
        events = []
        for date in dates:
            if date.weekday() == 3 and 1 <= date.day <= 7:
                events.append({'Date': date, 'Event': 'RBI Policy', 'Importance': 'High'})
            elif date.weekday() == 2 and 10 <= date.day <= 16 and date.month in [1, 3, 5, 7, 9, 11]:
                events.append({'Date': date, 'Event': 'FOMC Meeting', 'Importance': 'Very High'})
            elif date.month == 2 and date.day == 1:
                events.append({'Date': date, 'Event': 'Union Budget', 'Importance': 'Very High'})
        self.calendar = pd.DataFrame(events)

    def get_event_risk_score(self, hours_lookahead: int = 48) -> float:
        """Calculate event risk score"""
        cache_key = f"{datetime.now().strftime('%Y-%m-%d %H')}"
        if cache_key in self.risk_cache:
            return self.risk_cache[cache_key]

        now = datetime.now()
        future = now + timedelta(hours=hours_lookahead)
        upcoming = self.calendar[
            (self.calendar['Date'] >= now) & (self.calendar['Date'] <= future)
        ]
        
        score = 0.0
        for _, row in upcoming.iterrows():
            importance = row.get('Importance', 'Medium')
            event_type = row.get('Event', '')
            
            base_score = 0.5
            if importance == 'Very High': base_score = 2.0
            elif importance == 'High': base_score = 1.0

            if any(critical in event_type for critical in ['Fed', 'CPI', 'GDP', 'NFP', 'RBI', 'Budget']):
                base_score *= 1.5
            
            score += base_score
        
        final_score = min(score, 3.0)
        self.risk_cache[cache_key] = final_score
        return final_score

    def get_event_aware_multiplier(self, risk_score: float) -> float:
        """Convert risk score to position size multiplier"""
        if risk_score >= 2.5: return 0.3
        elif risk_score >= 2.0: return 0.5
        elif risk_score >= 1.0: return 0.7
        else: return 1.0
