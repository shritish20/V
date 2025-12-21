import logging
import json
import time
from typing import Dict, Any, List, Optional
from google import genai
from google.genai import types
from core.config import settings
from analytics.market_intelligence import MarketIntelligence

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    """
    The 'Macro Filter'. 
    Provides a Veto on trades if narrative risk is high.
    """
    def __init__(self):
        self.last_trade_analysis = {}
        self.last_portfolio_review = {}
        self.api_key = settings.GEMINI_API_KEY
        
        if self.api_key:
            try:
                self.client = genai.Client(api_key=self.api_key)
                self.model_id = "gemini-2.0-flash"
                logger.info("🧠 AI Architect Initialized")
            except Exception as e:
                logger.error(f"AI Init Failed: {e}")
                self.client = None
        else:
            self.client = None
            logger.warning("⚠️ GEMINI_API_KEY missing. AI features disabled.")
            
        self.intel = MarketIntelligence()

    def _clean_json(self, text: str) -> Dict:
        try:
            clean_text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except Exception:
            return {}

    async def analyze_trade_setup(self, trade_ctx: Dict) -> Dict:
        """
        Micro-Level: Checks specific trade logic against news.
        """
        if not self.client: return {}
        try:
            news = self.intel.get_latest_headlines(limit=5)
            macro = self.intel.get_macro_sentiment()
            
            prompt = f"""
            ROLE: Institutional Risk Manager.
            PROPOSED TRADE: {json.dumps(trade_ctx)}
            MACRO: {json.dumps(macro)} | NEWS: {news}
            
            TASK: Is this trade dangerous given the news?
            OUTPUT JSON ONLY:
            {{
                "risk_level": "LOW" | "MEDIUM" | "HIGH" | "DANGER",
                "market_sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",
                "narrative": "One sentence reason"
            }}
            """
            
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            
            result = self._clean_json(response.text)
            if result:
                self.last_trade_analysis = {**result, "timestamp": time.time()}
            return result
        except Exception as e:
            logger.error(f"AI Analysis Failed: {e}")
            return {}

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: Optional[List] = None) -> Dict:
        """
        Macro-Level: Periodic portfolio health check.
        """
        if not self.client: return {"verdict": "OFFLINE"}
        try:
            news = self.intel.get_latest_headlines(limit=8)
            fii = fii_data if fii_data else self.intel.get_fii_data()
            
            prompt = f"""
            ROLE: Portfolio Manager.
            PORTFOLIO: {json.dumps(portfolio_state)}
            FII DATA: {json.dumps(fii)}
            NEWS: {news}
            
            TASK: Assess Portfolio Health.
            OUTPUT JSON ONLY:
            {{
                "verdict": "SAFE" | "CAUTION" | "DANGER",
                "action_command": "Maintain or Reduce Size",
                "narrative": "Summary of risks"
            }}
            """
            
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            
            result = self._clean_json(response.text)
            self.last_portfolio_review = {**result, "timestamp": time.time()}
            return result
        except Exception as e:
            logger.warning(f"AI Review Paused: {e}")
            return {}
