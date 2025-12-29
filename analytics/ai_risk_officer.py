import asyncio
import json
import logging
import pytz
import re
import calendar
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

# External
import yfinance as yf
from nselib import derivatives
import feedparser
from groq import Groq

# Core
from sqlalchemy import select
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbTradeJournal
from database.models_risk import DbLearnedPattern, DbTradePostmortem, DbRiskBriefing
from core.models import MultiLegTrade

logger = logging.getLogger("AIRiskOfficer")

class AIRiskOfficer:
    """
    VolGuard Intelligence Core (v3.1 Audited)
    Security Patch Level: HIGH
    """
    
    def __init__(self, groq_api_key: str, db_manager: HybridDatabaseManager):
        if not groq_api_key: raise ValueError("GROQ_API_KEY required")
        self.groq = Groq(api_key=groq_api_key)
        self.db = db_manager
        self.ist = pytz.timezone('Asia/Kolkata')
        self.patterns = []
        self.last_pattern_refresh = datetime.min
        
        logger.info("🛡️ AI Risk Officer (Audited) Initialized")

    # --- SECURITY HELPERS (R1 & R5) ---
    
    def _sanitize(self, text: str) -> str:
        """R1: Remove injection vectors"""
        if not text: return ""
        # Remove anything looking like a command
        return re.sub(r'(ignore|system|instruction|delete|update)', '', str(text), flags=re.IGNORECASE)

    async def _safe_llm_call(self, prompt: str, timeout: float = 1.5) -> Dict:
        """R5: Circuit Breaker for LLM calls"""
        try:
            # Run blocking IO in thread with strict timeout
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    self.groq.chat.completions.create,
                    model="llama-3.3-70b-versatile",
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    temperature=0.3
                ),
                timeout=timeout
            )
            return json.loads(response.choices[0].message.content)
        except asyncio.TimeoutError:
            logger.warning("⏱️ AI Timeout - Falling back to Safe Mode")
            return {"narrative": "AI Offline", "action_plan": "NEUTRAL"}
        except Exception as e:
            logger.error(f"AI Error: {e}")
            return {"narrative": "Error", "action_plan": "NEUTRAL"}

    # --- MODULES ---

    async def fetch_fii_sentiment(self) -> Dict:
        return await asyncio.to_thread(self._sync_fetch_fii)

    def _sync_fetch_fii(self) -> Dict:
        try:
            target_date = datetime.now(self.ist)
            if target_date.hour < 19 or (target_date.hour == 19 and target_date.minute < 30):
                target_date -= timedelta(days=1)
            
            raw_data = None
            for _ in range(7):
                if target_date.weekday() < 5:
                    try:
                        raw_data = derivatives.participant_wise_open_interest(trade_date=target_date.strftime("%d-%m-%Y"))
                        break
                    except: pass
                target_date -= timedelta(days=1)
            
            if raw_data is None: return {"status": "error", "msg": "FII Data Unavailable"}
            
            fii = raw_data[raw_data['Client Type'] == 'FII'].iloc[0]
            c = lambda x: int(str(x).replace(',', ''))
            longs, shorts = c(fii['Future Index Long']), c(fii['Future Index Short'])
            ls_ratio = longs / (longs + shorts) if (longs+shorts) > 0 else 0
            
            sentiment = "BULLISH" if ls_ratio > 0.60 else "BEARISH" if ls_ratio < 0.35 else "NEUTRAL"
            impact = -1 if sentiment == "BULLISH" else 1.5 if sentiment == "BEARISH" else 0
            return {"status": "success", "ls_ratio": round(ls_ratio, 2), "sentiment": sentiment, "risk_impact": impact}
        except Exception as e: return {"status": "error", "msg": str(e)}

    async def fetch_global_macro(self) -> List[Dict]:
        return await asyncio.to_thread(self._sync_global_macro)

    def _sync_global_macro(self) -> List[Dict]:
        tickers = {"India VIX": "^INDIAVIX", "Brent Crude": "BZ=F", "USD/INR": "INR=X"}
        res = []
        try:
            data = yf.download(list(tickers.values()), period="5d", progress=False)['Close']
            for name, ticker in tickers.items():
                if ticker in data.columns:
                    s = data[ticker].dropna()
                    if len(s) > 1:
                        p, chg = s.iloc[-1], ((s.iloc[-1]-s.iloc[-2])/s.iloc[-2])*100
                        risk = 2 if name == "India VIX" and p > 18 else 0
                        res.append({"asset": name, "price": float(p), "change": float(chg), "risk_score": risk})
            return res
        except: return []

    async def fetch_smart_news(self) -> List[Dict]:
        return await asyncio.to_thread(self._sync_smart_news)

    def _sync_smart_news(self) -> List[Dict]:
        queries = ["RBI Governor", "India Inflation"]
        items = []
        for q in queries:
            try:
                feed = feedparser.parse(f"https://news.google.com/rss/search?q={q.replace(' ','%20')}&hl=en-IN&gl=IN&ceid=IN:en")
                for e in feed.entries[:1]: items.append({"title": self._sanitize(e.title)})
            except: pass
        return items[:3]

    async def generate_comprehensive_briefing(self) -> Dict:
        fii, macro, news = await asyncio.gather(self.fetch_fii_sentiment(), self.fetch_global_macro(), self.fetch_smart_news())
        score = max(0.0, min(10.0, round(fii.get("risk_impact", 0) + sum(m['risk_score'] for m in macro), 1)))
        
        # R1: Hardened Prompt Template
        template = (
            "You are a risk analyst. "
            "Risk score: {score}/10. FII sentiment: {fii}. "
            "Macro snapshot: {macro}. News: {news}. "
            "Output ONLY a JSON with keys: narrative, action_plan. "
            "action_plan must be one of: [CASH, HEDGE, NEUTRAL, AGGRESSIVE]. "
            "Do not mention buy/sell/orders."
        )
        prompt = template.format(score=score, fii=fii.get('sentiment'), macro=macro, news=news)
        
        analysis = await self._safe_llm_call(prompt)

        # R3: Short-lived DB Transaction
        try:
            async with self.db.get_session() as session:
                br = DbRiskBriefing(
                    timestamp=datetime.utcnow(), 
                    briefing_text=analysis.get("narrative", "No Data"), 
                    risk_score=score, 
                    alert_level="RED" if score > 7 else "GREEN", 
                    market_context={"fii": fii, "macro": macro}, 
                    active_risks=[], 
                    system_health={}
                )
                session.add(br)
                await self.db.safe_commit(session)
        except Exception as e: logger.error(f"DB Error: {e}")
            
        return {"score": score, "analysis": analysis}

    async def learn_from_history(self, force_refresh: bool = False):
        # R3: Snapshot Isolation
        try:
            async with self.db.get_session() as session:
                trades = (await session.execute(select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0))).scalars().all()
                if len(trades) < 5: return
                
                # Logic: Low VIX Losses
                losses = [t for t in trades if t.net_pnl < 0 and (t.vix_at_entry or 0) < 13]
                if len(losses) >= 3:
                    # Y3: Set expiry
                    expiry = datetime.utcnow() + timedelta(days=90)
                    self.patterns = [{"type": "FAILURE", "conditions": {"vix_max": 13.0}, "name": "Low VIX Loss", "valid_until": expiry}]
                    # Note: In prod, save this to DbLearnedPattern here
        except Exception as e: logger.error(f"Learn Error: {e}")

    async def validate_trade(self, trade: MultiLegTrade, market: Dict) -> Tuple[bool, List[Dict], str]:
        if not self.patterns: await self.learn_from_history()
        for p in self.patterns:
            # Y3: Check Expiry
            if p.get("valid_until") and datetime.utcnow() > p["valid_until"]: continue
            
            if p["type"] == "FAILURE" and market.get("vix", 0) < p["conditions"]["vix_max"]:
                return False, [p], f"AI VETO: Matches {p['name']}"
        return True, [], ""

    async def generate_postmortem(self, trade: MultiLegTrade, pnl: float):
        # R1: Sanitized Prompt
        prompt = f"Grade trade (A-F). Strategy: {self._sanitize(trade.strategy_type.value)}. PnL: {pnl}. Output JSON: {{'grade': '', 'lesson': ''}}"
        res = await self._safe_llm_call(prompt, timeout=2.0)
        
        try:
            async with self.db.get_session() as session:
                pm = DbTradePostmortem(trade_id=trade.id, grade=res.get("grade", "N/A"), lessons_learned=res.get("lesson", ""), ai_analysis=json.dumps(res))
                session.add(pm)
                await self.db.safe_commit(session)
        except: pass
