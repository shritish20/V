import logging
import json
import httpx
import time
from typing import Dict, Any
from core.config import settings
from analytics.market_intelligence import MarketIntelligence

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    """
    The 'Passive Advisor' for VolGuard.
    Capabilities:
    1. Trade Analysis: Narrates risks of a specific trade setup.
    2. Portfolio Doctor: Holistic review of Greeks vs. Macro.
    3. JSON Output: Structured data for dashboards/logs.
    4. Non-Blocking: Uses async HTTP to avoid freezing the trading loop.
    """
    def __init__(self):
        self.api_key = getattr(settings, 'GEMINI_API_KEY', '')
        self.model_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
        self.intel = MarketIntelligence()
        self.client = httpx.AsyncClient(timeout=15.0)
        
        # State Storage for Dashboard
        self.last_trade_analysis = {}
        self.last_portfolio_review = {}

    async def _call_gemini_json(self, prompt: str) -> Dict[str, Any]:
        """Helper to force Gemini to return strict JSON."""
        if not self.api_key:
            return {}

        full_prompt = f"""
        {prompt}
        
        SYSTEM INSTRUCTION:
        Reply ONLY with valid raw JSON. 
        Do not use markdown blocks (```json). 
        Do not add conversational text.
        """
        
        payload = {"contents": [{"parts": [{"text": full_prompt}]}]}
        
        try:
            response = await self.client.post(
                f"{self.model_url}?key={self.api_key}",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                if "candidates" in data and data["candidates"]:
                    raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
                    # Cleaning to ensure JSON parse works
                    clean_text = raw_text.replace("```json", "").replace("```", "").strip()
                    try:
                        return json.loads(clean_text)
                    except json.JSONDecodeError:
                        return {}
            
            logger.warning(f"AI Call Failed or Empty. Status: {response.status_code}")
            return {}
            
        except Exception as e:
            logger.error(f"AI Architect Connection Error: {e}")
            return {}

    async def analyze_trade_setup(self, trade_ctx: Dict) -> Dict:
        """
        Micro-Level: Observes a specific trade before/during execution.
        """
        # 1. Gather Fresh Context (Async)
        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()
        except Exception:
            news = []
            macro = {}

        prompt = f"""
        ACT AS: Institutional Risk Manager.
        
        PROPOSED TRADE:
        {json.dumps(trade_ctx)}
        
        MACRO CONTEXT:
        Global Markets: {json.dumps(macro)}
        News Wire (24h): {json.dumps(news)}
        
        TASK:
        Provide a risk commentary. Do NOT decide yes/no.
        Analyze if the news flow supports or contradicts the trade.
        
        OUTPUT JSON KEYS:
        - risk_level: "LOW" | "MEDIUM" | "HIGH" | "EXTREME"
        - primary_risk: <Short string identifying the main threat>
        - narrative: <2 sentence explanation for the trader>
        - macro_alignment: "SUPPORTIVE" | "NEUTRAL" | "CONTRADICTORY"
        """
        
        result = await self._call_gemini_json(prompt)
        if result:
            self.last_trade_analysis = result
            self.last_trade_analysis['timestamp'] = time.time()
        return result

    async def review_portfolio_holistically(self, portfolio_state: Dict) -> Dict:
        """
        Macro-Level: Checks total Greeks and PnL against market reality.
        """
        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()
        except Exception:
            news = []
            macro = {}
        
        prompt = f"""
        ACT AS: Senior Portfolio Manager.
        
        PORTFOLIO HEALTH CHECK:
        - Total Delta: {portfolio_state.get('delta', 0):.2f} (Directional Risk)
        - Total Vega: {portfolio_state.get('vega', 0):.2f} (Volatility Risk)
        - Daily PnL: {portfolio_state.get('pnl', 0):.2f}
        - Open Positions: {portfolio_state.get('count', 0)}
        
        MARKET REALITY:
        - Macro: {json.dumps(macro)}
        - News: {json.dumps(news)}
        
        TASK:
        Diagnose the portfolio. Are we positioned correctly for this news cycle?
        
        OUTPUT JSON KEYS:
        - health_score: <0 to 100>
        - verdict: "SAFE" | "CAUTION" | "DANGER"
        - narrative: <3 sentences explaining the portfolio's vulnerability or strength>
        - suggested_hedge: <Optional suggestion, e.g. "Buy NIFTY Puts">
        """
        
        result = await self._call_gemini_json(prompt)
        if result:
            self.last_portfolio_review = result
            self.last_portfolio_review['timestamp'] = time.time()
        return result
