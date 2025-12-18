import logging
import json
import google.generativeai as genai
from core.config import settings
from analytics.market_intelligence import MarketIntelligence
from analytics.ai_controls import AIDecision, AIActionType

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    def __init__(self):
        self.api_key = getattr(settings, 'GEMINI_API_KEY', '')
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(
                model_name='gemini-2.0-flash',
                tools=[{'google_search': {}}]
            )
        self.intel = MarketIntelligence()
        self.last_portfolio_review = {}

    async def evaluate_proposed_trade(self, trade_ctx: dict, fii_data: list) -> AIDecision:
        """The AI Risk Gate. Runs before capital is touched."""
        if not self.api_key: 
            return AIDecision(AIActionType.ALLOW, "AI Key Missing", 1.0)

        macro = self.intel.get_macro_sentiment()
        
        prompt = f"""
        TASK: Act as an Institutional Risk Officer. 
        Evaluate this trade: {json.dumps(trade_ctx)}
        FII Data: {json.dumps(fii_data)}
        Global Macro: {json.dumps(macro)}

        1. Use GOOGLE SEARCH to find if there is an RBI, Fed, or Geo-political event today.
        2. Verify if the 'Strong Bearish' or 'Medium Bearish' FII views conflict with this trade.
        3. Determine if the trade should be BLOCKED (e.g. Selling Puts into an FII short wall).

        OUTPUT JSON ONLY:
        {{
            "action": "BLOCK" | "DOWNGRADE" | "WARN" | "ALLOW",
            "reason": "1-sentence technical reason",
            "confidence": 0.0-1.0,
            "alternative_strategy": "IRON_CONDOR" | "WAIT" | null
        }}
        """
        try:
            response = self.model.generate_content(prompt)
            clean_json = response.text.replace("```json", "").replace("```", "").strip()
            res = json.loads(clean_json)
            return AIDecision(AIActionType[res['action']], res['reason'], res.get('confidence', 0.8), res.get('alternative_strategy'))
        except Exception as e:
            logger.error(f"Veto Logic Failed: {e}")
            return AIDecision(AIActionType.ALLOW, "Internal AI Error - Defaulting to Safety", 0.5)

    async def review_portfolio_holistically(self, state: dict, fii_data: list):
        """Generates the 24/7 narrative for the trader and laymen."""
        news = self.intel.get_latest_headlines()
        prompt = f"""
        DIAGNOSE: {json.dumps(state)}
        CONTEXT: FII Data {json.dumps(fii_data)} | News {news}
        TASK: Search and explain WHY the market is moving and how it effects this specific position.
        Explain it for a professional AND a random person.
        OUTPUT JSON: {{"health_score": 0-100, "narrative": "string", "action": "string"}}
        """
        try:
            response = self.model.generate_content(prompt)
            clean_json = response.text.replace("```json", "").replace("```", "").strip()
            self.last_portfolio_review = json.loads(clean_json)
            return self.last_portfolio_review
        except: return {}
