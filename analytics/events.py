import pandas as pd 
from datetime import datetime, timedelta
from typing import Dict
import logging
import math

logger = logging.getLogger("VolGuard14")

class AdvancedEventIntelligence:
    """Advanced event risk scoring with exponential decay and clustering - FIXED"""
    
    def __init__(self):
        self.calendar = pd.DataFrame()
        self.risk_cache: Dict[str, float] = {}
        self._load_calendar()
        self.cluster_radius_hours = 6  # Events within 6 hours cluster together

    def _load_calendar(self):
        """Load event calendar with robust error handling"""
        try:
            df = pd.read_csv("https://raw.githubusercontent.com/shritish20/VolGuard/refs/heads/main/events_calendar.csv")
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date'])
            self.calendar = df
            logger.info(f"Loaded {len(df)} macro events")
        except Exception as e: 
            logger.warning(f"Event calendar load failed: {e}")
            self._create_default_calendar()

    def _create_default_calendar(self):
        """Create realistic default event calendar"""
        dates = pd.date_range(start='2024-01-01', end='2025-12-31', freq='D')
        events = []
        for date in dates:
            if date.weekday() == 3 and 1 <= date.day <= 7:  # First Thursday
                events.append({'Date': date, 'Event': 'RBI Policy', 'Importance': 'High'})
            elif date.weekday() == 2 and 10 <= date.day <= 16 and date.month in [1, 3, 5, 7, 9, 11]:
                events.append({'Date': date, 'Event': 'FOMC Meeting', 'Importance': 'Very High'})
            elif date.month == 2 and date.day == 1:
                events.append({'Date': date, 'Event': 'Union Budget', 'Importance': 'Very High'})
            elif date.weekday() == 0 and 25 <= date.day <= 31:  # Last Monday
                events.append({'Date': date, 'Event': 'Monthly Expiry', 'Importance': 'Medium'})
        self.calendar = pd.DataFrame(events)

    def get_event_risk_score(self, hours_lookahead: int = 48) -> float:
        """Enhanced event risk scoring with clustering and FIXED decay logic"""
        cache_key = f"{datetime.now().strftime('%Y-%m-%d %H')}_{hours_lookahead}"
        if cache_key in self.risk_cache:
            return self.risk_cache[cache_key]

        now = datetime.now()
        future = now + timedelta(hours=hours_lookahead)
        
        # Get upcoming events
        upcoming = self.calendar[
            (self.calendar['Date'] >= now) & (self.calendar['Date'] <= future)
        ].copy()
        
        if upcoming.empty:
            self.risk_cache[cache_key] = 0.0
            return 0.0

        # Cluster events by time proximity
        upcoming['TimeDelta'] = (upcoming['Date'] - now).dt.total_seconds() / 3600
        upcoming = upcoming.sort_values('TimeDelta')
        
        clusters = []
        current_cluster = []
        
        for _, event in upcoming.iterrows():
            if not current_cluster:
                current_cluster.append(event)
            else:
                # Check if event belongs to current cluster
                last_event_time = current_cluster[-1]['TimeDelta']
                if event['TimeDelta'] - last_event_time <= self.cluster_radius_hours:
                    current_cluster.append(event)
                else:
                    clusters.append(current_cluster)
                    current_cluster = [event]
        
        if current_cluster:
            clusters.append(current_cluster)

        # FIXED: Better decay logic matching 48-hour lookahead
        total_score = 0.0
        for cluster in clusters:
            cluster_score = 0.0
            for event in cluster:
                base_score = self._get_base_score(event['Importance'])
                time_to_event = event['TimeDelta']
                
                # FIXED: Better decay logic
                if time_to_event < 6:
                    event_score = base_score  # 100% impact within 6 hours
                elif time_to_event < 24:
                    # Linear decay from 100% to 50% between 6-24 hours
                    event_score = base_score * (1 - 0.5 * (time_to_event - 6) / 18)
                else:
                    # Linear decay from 50% to 25% between 24-48 hours
                    event_score = base_score * (0.5 - 0.25 * (time_to_event - 24) / 24)
                
                # Critical event multiplier
                if any(critical in str(event.get('Event', '')) for critical in 
                      ['Fed', 'CPI', 'GDP', 'NFP', 'RBI', 'Budget', 'Election']):
                    event_score *= 1.5
                
                cluster_score += event_score
            
            # Non-linear clustering: multiple events close together amplify risk
            if len(cluster) > 1:
                cluster_score *= (1 + 0.2 * (len(cluster) - 1))  # 20% amplification per additional event
            
            total_score += cluster_score

        final_score = min(total_score, 5.0)  # Cap at 5.0
        self.risk_cache[cache_key] = final_score
        
        # Clean old cache entries
        self._clean_risk_cache()
        
        return final_score

    def _get_base_score(self, importance: str) -> float:
        """Get base score for event importance"""
        scores = {
            'Very High': 2.0,
            'High': 1.0,
            'Medium': 0.5,
            'Low': 0.2
        }
        return scores.get(importance, 0.5)

    def get_event_aware_multiplier(self, risk_score: float) -> float:
        """Convert risk score to position size multiplier with smooth scaling"""
        if risk_score >= 4.0: return 0.2
        elif risk_score >= 3.0: return 0.4
        elif risk_score >= 2.0: return 0.6
        elif risk_score >= 1.0: return 0.8
        else: return 1.0

    def _clean_risk_cache(self):
        """Clean old risk cache entries"""
        current_time = datetime.now()
        self.risk_cache = {
            k: v for k, v in self.risk_cache.items()
            if (current_time - datetime.strptime(k.split('_')[0], '%Y-%m-%d %H')).total_seconds() < 3600
        }
