import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
import logging
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher

logger = logging.getLogger("VolGuard18")

class AdvancedEventIntelligence:
    def __init__(self):
        self.data_fetcher = DashboardDataFetcher()
        self.events_cache: Dict[str, List[Dict]] = {}
        self.event_scores: Dict[str, float] = {}
        self.last_update: datetime = None
        self.event_weights = {
            'FED_MEETING': 2.5,
            'RBI_POLICY': 2.0,
            'BUDGET': 3.0,
            'QUARTERLY_RESULTS': 1.5,
            'GLOBAL_EVENT': 2.0,
            'ECONOMIC_DATA': 1.0,
            'EXPIRY_DAY': 1.2,
            'WEEKEND_GAP': 1.3,
        }

    def get_event_risk_score(self) -> float:
        try:
            events = self._load_upcoming_events()
            base_score = self._calculate_events_score(events)
            regime_adjustment = self._get_regime_adjustment()
            time_adjustment = self._get_time_adjustment()
            final_score = base_score + regime_adjustment + time_adjustment
            final_score = min(5.0, max(0.0, final_score))
            self.event_scores[datetime.now(IST).strftime("%Y-%m-%d")] = final_score
            logger.debug(f"Event risk score: {final_score:.2f}")
            return final_score
        except Exception as e:
            logger.error(f"Event risk score calculation failed: {e}")
            return 1.0

    def _load_upcoming_events(self) -> List[Dict]:
        cache_key = "upcoming_events"
        if cache_key in self.events_cache and self.last_update and (datetime.now(IST) - self.last_update).total_seconds() < 3600:
            return self.events_cache[cache_key]

        events = []
        try:
            if self.data_fetcher.events_calendar is not None:
                df = self.data_fetcher.events_calendar
                today = datetime.now(IST).date()
                next_week = today + timedelta(days=7)
                for _, row in df.iterrows():
                    event_date = row.get('Date')
                    if isinstance(event_date, pd.Timestamp):
                        event_date = event_date.date()
                    if event_date and today <= event_date <= next_week:
                        events.append({
                            'date': event_date.isoformat(),
                            'name': row.get('Event', 'Unknown'),
                            'impact': row.get('Impact', 'Medium'),
                            'category': self._categorize_event(row.get('Event', ''))
                        })
            events.extend(self._get_market_events())
            self.events_cache[cache_key] = events
            self.last_update = datetime.now(IST)
            return events
        except Exception as e:
            logger.error(f"Failed to load events: {e}")
            return []

    def _get_market_events(self) -> List[Dict]:
        today = datetime.now(IST)
        events = []
        if today.weekday() == 3:  # Thursday
            events.append({
                'date': today.date().isoformat(),
                'name': 'Weekly Options Expiry',
                'impact': 'High',
                'category': 'EXPIRY_DAY'
            })
        if today.weekday() == 4:  # Friday
            events.append({
                'date': (today + timedelta(days=1)).date().isoformat(),
                'name': 'Weekend Gap Risk',
                'impact': 'Medium',
                'category': 'WEEKEND_GAP'
            })
        return events

    def _categorize_event(self, event_name: str) -> str:
        event_name_lower = event_name.lower()
        if any(k in event_name_lower for k in ['fed', 'federal reserve', 'jerome powell']):
            return 'FED_MEETING'
        elif any(k in event_name_lower for k in ['rbi', 'monetary policy', 'repo rate']):
            return 'RBI_POLICY'
        elif any(k in event_name_lower for k in ['budget', 'union budget']):
            return 'BUDGET'
        elif any(k in event_name_lower for k in ['results', 'earnings', 'quarterly']):
            return 'QUARTERLY_RESULTS'
        elif any(k in event_name_lower for k in ['gdp', 'inflation', 'cpi', 'wpi']):
            return 'ECONOMIC_DATA'
        else:
            return 'GLOBAL_EVENT'

    def _calculate_events_score(self, events: List[Dict]) -> float:
        if not events:
            return 0.0
        total_score = 0.0
        for event in events:
            category = event.get('category', 'GLOBAL_EVENT')
            impact = event.get('impact', 'Medium')
            base_weight = self.event_weights.get(category, 1.0)
            impact_multiplier = {'Low': 0.5, 'Medium': 1.0, 'High': 1.5, 'Very High': 2.0}.get(impact, 1.0)
            try:
                event_date = datetime.strptime(event['date'], "%Y-%m-%d").date()
                days_to_event = (event_date - datetime.now(IST).date()).days
                time_decay = max(0.1, 1.0 - (days_to_event / 7))
                event_score = base_weight * impact_multiplier * time_decay
                total_score += event_score
            except:
                continue
        return min(3.0, total_score)

    def _get_regime_adjustment(self) -> float:
        return 0.0

    def _get_time_adjustment(self) -> float:
        now = datetime.now(IST)
        if now.day >= 25:
            return 0.2
        if 1 <= now.day <= 7:
            return 0.1
        return 0.0

    def get_event_aware_multiplier(self, event_score: float) -> float:
        if event_score < 1.0:
            return 1.2
        elif event_score < 2.0:
            return 1.0
        elif event_score < 3.0:
            return 0.7
        elif event_score < 4.0:
            return 0.5
        else:
            return 0.3

    def get_upcoming_events(self, days_ahead: int = 7) -> List[Dict]:
        events = self._load_upcoming_events()
        today = datetime.now(IST).date()
        target_date = today + timedelta(days=days_ahead)
        filtered = []
        for event in events:
            try:
                event_date = datetime.strptime(event['date'], "%Y-%m-%d").date()
                if today <= event_date <= target_date:
                    filtered.append(event)
            except:
                continue
        return filtered

    def clear_cache(self):
        self.events_cache.clear()
        self.event_scores.clear()
        logger.debug("Events cache cleared")

