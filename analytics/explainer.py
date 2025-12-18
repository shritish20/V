import logging
import json
import httpx
import time
import google.generativeai as genai
from typing import Dict, Any
from core.config import settings
from analytics.market_intelligence import MarketIntelligence
from analytics.ai_controls import AIDecision, AIActionType

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    def __init__(self):
        self.api_key = getattr(settings, 'GEMINI_API_KEY', '')
        if self.api_key:
            genai.configure(api_key=self.api_key)
            # INITIALIZE WITH GOOGLE SEARCH
            self.model = genai.GenerativeModel(
                model_name='gemini-2.0-flash',
                tools=[{'google_search': {}}]
            )
        self.intel = MarketIntelligence()
        self.last_portfolio_review = {}
        self.last_trade_analysis = {}

    async def evaluate_proposed_trade(self, trade_ctx: Dict, fii_data: list) -> AIDecision:
        """The AI Risk Veto Gate. Runs before execution."""
        if not self.api_key:
            return AIDecision(AIActionType.ALLOW, "AI Key Missing", 1.0)

        macro = self.intel.get_macro_sentiment()
        
        prompt = f"""
        ACT AS: Institutional Risk Officer. 
        EVALUATE TRADE: {json.dumps(trade_ctx)}
        FII DATA: {json.dumps(fii_data)}
        MACRO: {json.dumps(macro)}

        1. Use GOOGLE SEARCH to see if there is an RBI Policy, Fed meet, or War news today.
        2. Identify if FII 'Strong Bearish' views conflict with this specific trade.
        3. Decide: BLOCK (Risk extreme), DOWNGRADE (Defined risk only), or ALLOW.

        OUTPUT VALID JSON ONLY:
        {{
            "action": "BLOCK" | "DOWNGRADE" | "WARN" | "ALLOW",
            "reason": "1-sentence technical justification",
            "confidence": 0.0-1.0,
            "alternative_strategy": "IRON_CONDOR" | "WAIT" | null
        }}
        """
        try:
            response = self.model.generate_content(prompt)
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            res = json.loads(clean_text)
            return AIDecision(
                action=AIActionType[res['action']],
                reason=res['reason'],
                confidence=res.get('confidence', 0.8),
                alternative_strategy=res.get('alternative_strategy')
            )
        except Exception as e:
            logger.error(f"AI Veto Failure: {e}")
            return AIDecision(AIActionType.ALLOW, "AI Context Error - Defaulting to Safety", 0.5)

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: list) -> Dict:
        """24/7 Contextual Narrative for the dashboard."""
        news = self.intel.get_latest_headlines()
        prompt = f"""
        PORTFOLIO: {json.dumps(portfolio_state)}
        CONTEXT: FII {json.dumps(fii_data)} | News {news}
        
        TASK: Search and explain WHY the market is moving and how it effects this specific position.
        Provide an institutional view for a pro AND a simple narrative for a random person.
        
        OUTPUT VALID JSON ONLY:
        {{
            "health_score": 0-100,
            "verdict": "SAFE" | "CAUTION" | "DANGER",
            "narrative": "3-sentence story explaining the 'Why' and the risk",
            "action": "Immediate command"
        }}
        """
        try:
            response = self.model.generate_content(prompt)
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            self.last_portfolio_review = json.loads(clean_text)
            return self.last_portfolio_review
        except: return {}
