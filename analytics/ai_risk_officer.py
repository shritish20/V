import asyncio
import json
import logging
import pytz
import calendar
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple
from collections import defaultdict
import yfinance as yf
from nselib import derivatives
import feedparser
from groq import Groq
from sqlalchemy import select
from database.manager import HybridDatabaseManager
from database.models import DbTradeJournal
from database.models_risk import DbLearnedPattern, DbTradePostmortem, DbRiskBriefing
from core.models import MultiLegTrade

logger = logging.getLogger("AIRiskOfficer")

class AIRiskOfficer:
    def __init__(self, groq_api_key: str, db_manager: HybridDatabaseManager):
        if not groq_api_key: raise ValueError("GROQ_API_KEY required")
        self.groq = Groq(api_key=groq_api_key)
        self.db = db_manager
        self.ist = pytz.timezone('Asia/Kolkata')
        self.patterns = []
        
    async def fetch_fii_sentiment(self) -> Dict:
        return await asyncio.to_thread(self._sync_fetch_fii)

    def _sync_fetch_fii(self) -> Dict:
        try:
            target_date = datetime.now(self.ist)
            if target_date.hour < 19 or (target_date.hour == 19 and target_date.minute < 30):
                target_date -= timedelta(days=1)
            days_checked = 0
            raw_data = None
            while days_checked < 7:
                if target_date.weekday() < 5:
                    try:
                        raw_data = derivatives.participant_wise_open_interest(trade_date=target_date.strftime("%d-%m-%Y"))
                        break
                    except: pass
                target_date -= timedelta(days=1)
                days_checked += 1
            
            if raw_data is None: return {"status": "error", "msg": "FII Data Unavailable"}
            
            fii = raw_data[raw_data['Client Type'] == 'FII'].iloc[0]
            c = lambda x: int(str(x).replace(',', ''))
            longs, shorts = c(fii['Future Index Long']), c(fii['Future Index Short'])
            ls_ratio = longs / (longs + shorts) if (longs+shorts) > 0 else 0
            
            sentiment = "BULLISH" if ls_ratio > 0.60 else "BEARISH" if ls_ratio < 0.35 else "NEUTRAL"
            risk_impact = -1 if sentiment == "BULLISH" else 1.5 if sentiment == "BEARISH" else 0
            
            return {"status": "success", "ls_ratio": round(ls_ratio, 2), "sentiment": sentiment, "risk_impact": risk_impact}
        except Exception as e:
            return {"status": "error", "msg": str(e)}

    async def fetch_global_macro(self) -> List[Dict]:
        return await asyncio.to_thread(self._sync_global_macro)

    def _sync_global_macro(self) -> List[Dict]:
        tickers = {"India VIX": "^INDIAVIX", "Brent Crude": "BZ=F", "USD/INR": "INR=X"}
        results = []
        try:
            data = yf.download(list(tickers.values()), period="5d", progress=False)['Close']
            for name, ticker in tickers.items():
                if ticker not in data.columns: continue
                series = data[ticker].dropna()
                if len(series) < 2: continue
                price = series.iloc[-1]
                change = ((price - series.iloc[-2]) / series.iloc[-2]) * 100
                risk = 2 if name == "India VIX" and price > 18 else 0
                results.append({"asset": name, "price": float(price), "change": float(change), "risk_score": risk})
            return results
        except: return []

    async def fetch_smart_news(self) -> List[Dict]:
        return await asyncio.to_thread(self._sync_smart_news)

    def _sync_smart_news(self) -> List[Dict]:
        queries = ["RBI Governor", "Jerome Powell", "India Inflation"]
        news_items = []
        for q in queries:
            try:
                feed = feedparser.parse(f"https://news.google.com/rss/search?q={q.replace(' ','%20')}&hl=en-IN&gl=IN&ceid=IN:en")
                for entry in feed.entries[:1]:
                    news_items.append({"title": entry.title, "link": entry.link})
            except: pass
        return news_items[:3]

    async def generate_comprehensive_briefing(self) -> Dict:
        fii, macro, news = await asyncio.gather(self.fetch_fii_sentiment(), self.fetch_global_macro(), self.fetch_smart_news())
        base_score = fii.get("risk_impact", 0) + sum(m['risk_score'] for m in macro)
        final_score = max(0.0, min(10.0, round(base_score, 1)))
        
        prompt = f"Summarize for trader. Risk Score: {final_score}/10. FII: {fii}. Macro: {macro}. News: {news}. Output JSON: {{'narrative': '', 'action_plan': ''}}"
        try:
            resp = self.groq.chat.completions.create(model="llama-3.3-70b-versatile", messages=[{"role": "user", "content": prompt}], response_format={"type": "json_object"})
            analysis = json.loads(resp.choices[0].message.content)
        except: analysis = {"narrative": "AI Offline", "action_plan": "Caution"}

        async with self.db.get_session() as session:
            br = DbRiskBriefing(timestamp=datetime.utcnow(), briefing_text=analysis["narrative"], risk_score=final_score, alert_level="RED" if final_score > 7 else "GREEN", market_context={"fii": fii, "macro": macro}, active_risks=[], system_health={})
            session.add(br)
            await self.db.safe_commit(session)
        return {"score": final_score, "analysis": analysis}

    async def learn_from_history(self, force_refresh: bool = False):
        async with self.db.get_session() as session:
            trades = (await session.execute(select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0))).scalars().all()
            if len(trades) < 5: return
            
            # Simple Pattern Logic (Example)
            losses = [t for t in trades if t.net_pnl < 0 and (t.vix_at_entry or 0) < 13]
            if len(losses) >= 3:
                self.patterns = [{"type": "FAILURE", "conditions": {"vix_max": 13.0}, "name": "Low VIX Loss", "severity": "HIGH", "lesson": "Avoid Low VIX"}]
                # In prod, save to DB here

    async def validate_trade(self, trade: MultiLegTrade, market: Dict) -> Tuple[bool, List[Dict], str]:
        if not self.patterns: await self.learn_from_history()
        for p in self.patterns:
            if p["type"] == "FAILURE" and market.get("vix", 0) < p["conditions"]["vix_max"]:
                return False, [p], f"AI VETO: Matches {p['name']}"
        return True, [], ""

    async def generate_postmortem(self, trade: MultiLegTrade, pnl: float):
        prompt = f"Grade trade (A-F). Strategy: {trade.strategy_type.value}. PnL: {pnl}. Output JSON: {{'grade': '', 'lesson': ''}}"
        try:
            resp = self.groq.chat.completions.create(model="llama-3.3-70b-versatile", messages=[{"role": "user", "content": prompt}], response_format={"type": "json_object"})
            res = json.loads(resp.choices[0].message.content)
            async with self.db.get_session() as session:
                pm = DbTradePostmortem(trade_id=trade.id, grade=res["grade"], lessons_learned=res["lesson"], ai_analysis=json.dumps(res))
                session.add(pm)
                await self.db.safe_commit(session)
        except: pass
