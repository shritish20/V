import asyncio
import json
import logging
import pytz
import calendar
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

# External Libraries (IO Bound - Run in Threads)
import yfinance as yf
import nselib
from nselib import derivatives
import feedparser
from groq import Groq

# Database & Core
from sqlalchemy import select
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import (
    DbTradeJournal, DbLearnedPattern, DbPatternWarning,
    DbTradePostmortem, DbRiskBriefing, DbMarketSnapshot
)
from core.models import MultiLegTrade

logger = logging.getLogger("AIRiskOfficer")

class AIRiskOfficer:
    """
    VolGuard Intelligence Core (v3.0 Final)
    The Central Nervous System that merges:
    1. Live Market Intelligence (FII, Macro, News, Events)
    2. Historical Wisdom (Pattern Learning from past trades)
    3. Pre-Trade Validation (Prevents repeating mistakes)
    4. Post-Trade Coaching (Grades every trade A-F)
    """
    
    def __init__(self, groq_api_key: str, db_manager: HybridDatabaseManager):
        if not groq_api_key:
            raise ValueError("GROQ_API_KEY is required for AI Risk Officer")
            
        self.groq = Groq(api_key=groq_api_key)
        self.db = db_manager
        self.ist = pytz.timezone('Asia/Kolkata')
        
        # In-Memory Cache
        self.patterns = []
        self.last_pattern_refresh = datetime.min
        self.current_risk_score = 0.0
        self.active_verdicts = []
        
        logger.info("🤖 AI Risk Officer (Intelligence Core) initialized")

    # ================================================================
    # PART A: MARKET INTELLIGENCE (The Eyes & Ears)
    # ================================================================

    async def fetch_fii_sentiment(self) -> Dict:
        """Async wrapper for NseLib (Blocking I/O)"""
        return await asyncio.to_thread(self._sync_fetch_fii)

    def _sync_fetch_fii(self) -> Dict:
        try:
            # Smart Logic: If before 7:30 PM IST, check yesterday first
            target_date = datetime.now(self.ist)
            if target_date.hour < 19 or (target_date.hour == 19 and target_date.minute < 30):
                target_date -= timedelta(days=1)
            
            raw_data = None
            days_checked = 0
            
            # Backtrack loop (Handle Holidays/Weekends)
            while days_checked < 7:
                date_str = target_date.strftime("%d-%m-%Y")
                if target_date.weekday() < 5: # Skip weekends
                    try:
                        raw_data = derivatives.participant_wise_open_interest(trade_date=date_str)
                        break
                    except:
                        pass
                target_date -= timedelta(days=1)
                days_checked += 1
            
            if raw_data is None:
                return {"status": "error", "msg": "FII Data Unavailable (NSE API Down or Holiday)"}

            # Parse Data
            fii = raw_data[raw_data['Client Type'] == 'FII'].iloc[0]
            c = lambda x: int(str(x).replace(',', ''))
            
            longs = c(fii['Future Index Long'])
            shorts = c(fii['Future Index Short'])
            calls_l = c(fii['Option Index Call Long'])
            puts_l = c(fii['Option Index Put Long'])
            
            net_fut = longs - shorts
            ls_ratio = longs / (longs + shorts) if (longs+shorts) > 0 else 0
            
            sentiment = "NEUTRAL"
            risk_impact = 0
            
            if ls_ratio > 0.60: 
                sentiment = "BULLISH"
                risk_impact = -1 # Reduces risk for long trades
            elif ls_ratio < 0.35: 
                sentiment = "BEARISH"
                risk_impact = 1.5 # Increases risk of crash
            
            return {
                "status": "success",
                "date": target_date.strftime("%d-%b"),
                "ls_ratio": round(ls_ratio, 2),
                "net_futures": net_fut,
                "pcr_index": round(puts_l/calls_l, 2) if calls_l > 0 else 0,
                "sentiment": sentiment,
                "risk_impact": risk_impact
            }
        except Exception as e:
            logger.error(f"FII Fetch Error: {e}")
            return {"status": "error", "msg": str(e)}

    async def fetch_global_macro(self) -> List[Dict]:
        """Async wrapper for yFinance"""
        return await asyncio.to_thread(self._sync_global_macro)

    def _sync_global_macro(self) -> List[Dict]:
        tickers = {
            "India VIX": "^INDIAVIX",
            "Brent Crude": "BZ=F",
            "USD/INR": "INR=X",
            "US 10Y Yield": "^TNX",
            "Dollar Index": "DX-Y.NYB"
        }
        results = []
        try:
            # Download 5 days to calculate change
            data = yf.download(list(tickers.values()), period="5d", progress=False)['Close']
            
            for name, ticker in tickers.items():
                if ticker not in data.columns: continue
                series = data[ticker].dropna()
                if len(series) < 2: continue
                
                price = series.iloc[-1]
                change = ((price - series.iloc[-2]) / series.iloc[-2]) * 100
                
                risk_impact = 0
                impact_msg = "Neutral"
                
                # Rule Engine
                if name == "India VIX":
                    if price > 18: risk_impact = 2; impact_msg = "CRITICAL HIGH"
                    elif change > 5: risk_impact = 1; impact_msg = "Spiking"
                elif name == "Brent Crude" and change > 3:
                     risk_impact = 0.5; impact_msg = "Inflationary"
                elif name == "US 10Y Yield" and price > 4.6:
                     risk_impact = 1; impact_msg = "Outflow Risk"
                elif name == "Dollar Index" and price > 105:
                     risk_impact = 0.5; impact_msg = "EM Pressure"

                results.append({
                    "asset": name,
                    "price": float(price),
                    "change": float(change),
                    "impact": impact_msg,
                    "risk_score": risk_impact
                })
            return results
        except Exception as e:
            logger.error(f"Macro Fetch Error: {e}")
            return []

    def calculate_upcoming_events(self) -> List[Dict]:
        """Pure Math Calculation (No API calls)"""
        events = []
        today = datetime.now(self.ist).date()
        
        # 1. India CPI (12th of Month)
        cpi_in = date(today.year, today.month, 12)
        if today.day > 12:
            cpi_in = (cpi_in.replace(day=1) + timedelta(days=32)).replace(day=12)
            
        # 2. US CPI (2nd Wednesday)
        def get_us_cpi(year, month):
            c = calendar.monthcalendar(year, month)
            first_wed = [week[calendar.WEDNESDAY] for week in c if week[calendar.WEDNESDAY] != 0][0]
            # US CPI is typically 2nd Wed (approx +7 days from 1st Wed if early)
            return date(year, month, first_wed + 7)

        cpi_us = get_us_cpi(today.year, today.month)
        if today > cpi_us:
            if today.month == 12: cpi_us = get_us_cpi(today.year + 1, 1)
            else: cpi_us = get_us_cpi(today.year, today.month + 1)

        # 3. FOMC (2025 Schedule Hardcoded)
        fomc_dates = [
            date(2025, 1, 29), date(2025, 3, 19), date(2025, 5, 7),
            date(2025, 6, 18), date(2025, 7, 30), date(2025, 9, 17),
            date(2025, 10, 29), date(2025, 12, 10)
        ]
        next_fomc = next((d for d in fomc_dates if d >= today), None)

        # Analyze Events
        check_list = [("🇮🇳 India CPI", cpi_in), ("🇺🇸 US CPI", cpi_us), ("🇺🇸 FOMC", next_fomc)]
        
        for name, dt in check_list:
            if not dt: continue
            days = (dt - today).days
            if days > 7: continue
            
            risk = 0
            msg = f"In {days} days"
            
            if days <= 1 and "CPI" in name: risk = 1.5; msg += " (VOLATILITY WARNING)"
            if days <= 2 and "FOMC" in name: risk = 3.0; msg += " (BLACKOUT PERIOD)"
            
            events.append({
                "event": name,
                "date": dt.strftime('%d-%b'),
                "status": msg,
                "risk_score": risk
            })
            
        return events

    async def fetch_smart_news(self) -> List[Dict]:
        """Async wrapper for FeedParser"""
        return await asyncio.to_thread(self._sync_smart_news)

    def _sync_smart_news(self) -> List[Dict]:
        queries = ["RBI Governor", "Jerome Powell", "India Inflation", "US Recession", "Nifty Outlook"]
        news_items = []
        seen_titles = set()
        
        for q in queries:
            try:
                url = f"https://news.google.com/rss/search?q={q.replace(' ','%20')}&hl=en-IN&gl=IN&ceid=IN:en"
                feed = feedparser.parse(url)
                
                for entry in feed.entries[:2]:
                    if entry.title in seen_titles: continue
                    
                    # Parse Time
                    pub_dt = datetime.now() # Default
                    if hasattr(entry, 'published_parsed'):
                        pub_dt = datetime.fromtimestamp(calendar.timegm(entry.published_parsed))
                    
                    # Only fresh news (24h)
                    if (datetime.now() - pub_dt).days < 1:
                        seen_titles.add(entry.title)
                        news_items.append({
                            "title": entry.title,
                            "time": pub_dt.strftime('%H:%M'),
                            "link": entry.link
                        })
            except:
                continue
        return news_items[:5]

    async def generate_comprehensive_briefing(self) -> Dict:
        """Synthesizes ALL intelligence into a single briefing"""
        logger.info("🧠 Synthesizing VolGuard Intelligence Briefing...")
        
        # 1. Parallel Fetching
        fii_task = self.fetch_fii_sentiment()
        macro_task = self.fetch_global_macro()
        news_task = self.fetch_smart_news()
        
        fii, macro, news = await asyncio.gather(fii_task, macro_task, news_task)
        events = self.calculate_upcoming_events()
        
        # 2. Calculate Aggregate Risk Score
        base_score = 0.0
        warnings = []
        
        if fii.get("status") == "success":
            base_score += fii.get("risk_impact", 0)
            if fii.get("sentiment") == "BEARISH": warnings.append("FIIs are Net Short")
        
        for m in macro:
            base_score += m.get("risk_score", 0)
            if m.get("risk_score") > 0: warnings.append(f"{m['asset']} is {m['impact']}")
            
        for e in events:
            base_score += e.get("risk_score", 0)
            if e.get("risk_score") > 1: warnings.append(f"{e['event']} imminent")

        # Clamp Score 0-10
        final_score = max(0.0, min(10.0, round(base_score, 1)))
        
        # 3. Generate LLM Narrative
        prompt = f"""
        Act as a Hedge Fund Risk Officer. Summarize this data for an Indian Nifty Trader:

        RISK SCORE: {final_score}/10
        
        DATA:
        - FII Sentiment: {fii.get('sentiment', 'N/A')} (L/S: {fii.get('ls_ratio', 'N/A')})
        - Macro Risks: {', '.join([m['asset']+': '+m['impact'] for m in macro if m['risk_score']>0] or ['None'])}
        - Upcoming Events: {', '.join([e['event'] for e in events])}
        - Recent News Headlines: {json.dumps([n['title'] for n in news])}

        OUTPUT FORMAT (JSON):
        {{
            "narrative": "3-4 concise sentences summarizing the market stance.",
            "action_plan": "Specific advice (e.g., Reduce Size, Buy Hedges, Stay Flat).",
            "confidence": "High/Medium/Low"
        }}
        """
        
        try:
            response = self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            ai_analysis = json.loads(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"LLM Error: {e}")
            ai_analysis = {"narrative": "AI Unavailable", "action_plan": "Trade with caution."}

        # 4. Save to Database
        try:
            async with self.db.get_session() as session:
                briefing = DbRiskBriefing(
                    timestamp=datetime.utcnow(),
                    briefing_text=ai_analysis["narrative"],
                    risk_score=final_score,
                    alert_level="RED" if final_score > 7 else "AMBER" if final_score > 4 else "GREEN",
                    market_context={
                        "fii": fii,
                        "macro": macro,
                        "events": events
                    },
                    active_risks=warnings,
                    system_health={"ai_status": "online"}
                )
                session.add(briefing)
                await self.db.safe_commit(session)
        except Exception as e:
            logger.error(f"DB Save Error: {e}")

        self.current_risk_score = final_score
        self.active_verdicts = warnings
        
        return {
            "score": final_score,
            "analysis": ai_analysis,
            "warnings": warnings,
            "data": {"fii": fii, "macro": macro, "events": events}
        }

    # ================================================================
    # PART B: HISTORICAL PATTERN LEARNING (The Wisdom)
    # ================================================================

    async def learn_from_history(self, force_refresh: bool = False):
        """Analyze trade journal to discover patterns (Weekly)"""
        now = datetime.utcnow()
        if not force_refresh and (now - self.last_pattern_refresh).days < 7:
            return

        logger.info("🔍 Analyzing trade history for patterns...")
        try:
            async with self.db.get_session() as session:
                # Get all closed trades
                stmt = select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0).order_by(DbTradeJournal.date)
                result = await session.execute(stmt)
                trades = result.scalars().all()
                
                if len(trades) < 10: return

                # Convert to dict format
                trade_dicts = []
                for t in trades:
                    trade_dicts.append({
                        "id": t.id,
                        "date": t.date,
                        "strategy": t.strategy_name or "UNKNOWN",
                        "entry_conditions": {
                            "vix": t.vix_at_entry or 0,
                        },
                        "outcome": {"pnl": t.net_pnl},
                    })
                
                patterns = await self._extract_patterns(trade_dicts)
                
                # Update DB
                for pattern in patterns:
                    await self._store_pattern(session, pattern)
                await self.db.safe_commit(session)
                
                self.patterns = patterns
                self.last_pattern_refresh = now
                logger.info(f"✅ Discovered {len(patterns)} patterns")
        
        except Exception as e:
            logger.error(f"Pattern learning failed: {e}")

    async def _extract_patterns(self, trades: List[Dict]) -> List[Dict]:
        """Logic to extract win/loss patterns"""
        by_strategy = defaultdict(list)
        for trade in trades:
            by_strategy[trade["strategy"]].append(trade)
        
        patterns = []
        for strategy, strat_trades in by_strategy.items():
            if len(strat_trades) < 5: continue
            
            losses = [t for t in strat_trades if t["outcome"]["pnl"] < 0]
            wins = [t for t in strat_trades if t["outcome"]["pnl"] > 0]
            
            # 1. FAILURE PATTERN: Low VIX Losses
            low_vix_losses = [t for t in losses if t["entry_conditions"]["vix"] < 13]
            if len(low_vix_losses) >= 3:
                patterns.append({
                    "type": "FAILURE",
                    "name": f"{strategy} in Low VIX",
                    "conditions": {"strategy": strategy, "vix_max": 13.0},
                    "occurrences": len(low_vix_losses),
                    "win_rate": 0.0,
                    "avg_pnl": sum(t["outcome"]["pnl"] for t in low_vix_losses) / len(low_vix_losses),
                    "severity": "HIGH",
                    "lesson": f"Avoid {strategy} when VIX < 13. Gamma risk is too high.",
                    "evidence": [t["id"] for t in low_vix_losses],
                })
                
            # 2. SUCCESS PATTERN: High Win Rate Setup
            if len(wins) >= 5:
                win_rate = len(wins) / len(strat_trades)
                if win_rate > 0.75:
                    patterns.append({
                        "type": "SUCCESS",
                        "name": f"{strategy} Golden Setup",
                        "conditions": {"strategy": strategy},
                        "occurrences": len(wins),
                        "win_rate": win_rate,
                        "avg_pnl": sum(t["outcome"]["pnl"] for t in wins) / len(wins),
                        "severity": "LOW",
                        "lesson": f"You are very strong at {strategy}. Double down on this.",
                        "evidence": [t["id"] for t in wins],
                    })
        return patterns

    async def _store_pattern(self, session, pattern: Dict):
        """Upsert pattern to DB"""
        stmt = select(DbLearnedPattern).where(DbLearnedPattern.pattern_name == pattern["name"])
        result = await session.execute(stmt)
        existing = result.scalars().first()
        
        if existing:
            existing.occurrence_count = pattern["occurrences"]
            existing.last_occurrence = datetime.utcnow()
        else:
            new_pattern = DbLearnedPattern(
                pattern_type=pattern["type"],
                pattern_name=pattern["name"],
                conditions_json=pattern["conditions"],
                occurrence_count=pattern["occurrences"],
                win_rate=pattern["win_rate"],
                avg_pnl=pattern["avg_pnl"],
                matching_trade_ids=pattern["evidence"],
                lesson_text=pattern["lesson"],
                severity=pattern["severity"],
                last_occurrence=datetime.utcnow()
            )
            session.add(new_pattern)

    async def _load_patterns_from_db(self):
        """Refreshes in-memory pattern cache"""
        try:
            async with self.db.get_session() as session:
                stmt = select(DbLearnedPattern).where(DbLearnedPattern.occurrence_count >= 3)
                result = await session.execute(stmt)
                db_patterns = result.scalars().all()
                self.patterns = [{"name": p.pattern_name, "conditions": p.conditions_json, "lesson": p.lesson_text, "type": p.pattern_type} for p in db_patterns]
        except Exception as e:
            logger.error(f"Failed to load patterns: {e}")

    # ================================================================
    # PART C: PRE-TRADE VALIDATION (The Gatekeeper)
    # ================================================================

    async def validate_trade(self, trade: MultiLegTrade, market_conditions: Dict) -> Tuple[bool, List[Dict], str]:
        """Checks trade against learned patterns"""
        if not self.patterns:
            await self._load_patterns_from_db()
        
        trade_features = {
            "strategy": trade.strategy_type.value,
            "vix": market_conditions.get("vix", 0),
        }
        
        matches = []
        for pattern in self.patterns:
            if pattern["type"] != "FAILURE": continue
            
            # Simple Matching Logic
            cond = pattern["conditions"]
            if cond.get("strategy") == trade_features["strategy"]:
                if "vix_max" in cond and trade_features["vix"] < cond["vix_max"]:
                    matches.append(pattern)
        
        if not matches:
            return True, [], ""
        
        # Generate Warning
        ai_warning = f"Wait! You historically lose money on {trade_features['strategy']} when VIX is low."
        return False, matches, ai_warning

    # ================================================================
    # PART D: POST-TRADE COACHING (The Review)
    # ================================================================

    async def generate_postmortem(self, trade: MultiLegTrade, final_pnl: float):
        """Generates an A-F grade for a closed trade"""
        logger.info(f"📊 Generatng Post-Mortem for Trade {trade.id}")
        
        prompt = f"""
        Grade this completed trade (A-F) based on execution quality.
        
        TRADE: {trade.strategy_type.value}
        PNL: {final_pnl}
        DURATION: {(trade.exit_time - trade.entry_time).total_seconds()/3600:.1f} hours
        
        OUTPUT JSON:
        {{
            "grade": "A/B/C/D/F",
            "mistakes": ["List of errors"],
            "good_points": ["List of good execution"],
            "lesson": "One sentence lesson"
        }}
        """
        
        try:
            response = self.groq.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3
            )
            analysis = json.loads(response.choices[0].message.content)
            
            # Save
            async with self.db.get_session() as session:
                pm = DbTradePostmortem(
                    trade_id=trade.id,
                    grade=analysis["grade"],
                    what_went_right=analysis["good_points"],
                    what_went_wrong=analysis["mistakes"],
                    lessons_learned=analysis["lesson"],
                    ai_analysis=json.dumps(analysis)
                )
                session.add(pm)
                await self.db.safe_commit(session)
                
            return analysis
        except Exception as e:
            logger.error(f"Post-Mortem Error: {e}")
