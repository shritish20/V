# File: analytics/events.py

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging
from core.config import settings, IST
from utils.data_fetcher import DashboardDataFetcher

logger = logging.getLogger("EventIntel")

class AdvancedEventIntelligence:
    def __init__(self):
        self.data_fetcher = DashboardDataFetcher()
        self.event_weights = {
            'BUDGET': 5.0, 'ELECTION': 5.0, 'WAR': 5.0,
            'RBI_POLICY': 3.0, 'FED_MEETING': 3.0,
            'GDP': 2.0, 'INFLATION': 2.0, 'EARNINGS': 1.5,
            'OTHER': 1.0
        }

    def get_market_risk_state(self) -> Tuple[str, float, str]:
        events = self._get_upcoming_events()
        if not events: return "SAFE", 0.0, "None"

        max_score = 0.0
        top_event = "None"

        for e in events:
            base_score = self.event_weights.get(e.get('category', 'OTHER'), 1.0)
            days_out = e.get('days_to_event')
            
            if days_out <= 1: current_score = base_score * 1.5 # Immediate
            elif days_out <= 3: current_score = base_score
            else: current_score = base_score * 0.5

            if current_score > max_score:
                max_score = current_score
                top_event = e.get('name')

        if max_score >= 4.0: return "BINARY_EVENT", round(max_score, 2), top_event
        elif max_score >= 2.5: return "MACRO_RISK", round(max_score, 2), top_event
        else: return "SAFE", round(max_score, 2), top_event

    def _get_upcoming_events(self) -> List[Dict]:
        try:
            if self.data_fetcher.events_calendar is None:
                return []
            
            df = self.data_fetcher.events_calendar
            today = datetime.now(IST).date()
            
            # Robust Date Parsing (Safe Access)
            if not pd.api.types.is_datetime64_any_dtype(df['Date']):
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            df = df.dropna(subset=['Date'])
            
            # Filter Next 7 Days
            limit = today + timedelta(days=7)
            mask = (df['Date'].dt.date >= today) & (df['Date'].dt.date <= limit)
            active = df[mask].copy()
            
            events = []
            for _, row in active.iterrows():
                name = str(row.get('Event', 'Unknown'))
                category = self._categorize(name)
                
                # Correct safe date access
                event_dt = row['Date'].date()
                days_out = (event_dt - today).days
                
                events.append({
                    'date': event_dt,
                    'name': name,
                    'category': category,
                    'impact': row.get('Impact', 'Medium'),
                    'days_to_event': days_out
                })
            return events
        except Exception as e:
            logger.error(f"Event Fetch Failed: {e}")
            return []

    def _categorize(self, name: str) -> str:
        name = name.upper()
        if 'BUDGET' in name or 'ELECTION' in name: return 'BUDGET'
        if 'RBI' in name or 'REPO' in name: return 'RBI_POLICY'
        if 'FED' in name or 'FOMC' in name: return 'FED_MEETING'
        if 'GDP' in name: return 'GDP'
        if 'CPI' in name or 'INFLATION' in name: return 'INFLATION'
        if 'EARNINGS' in name or 'RESULTS' in name: return 'EARNINGS'
        return 'OTHER'
