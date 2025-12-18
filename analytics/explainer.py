import logging
import json
import time
import google.generativeai as genai
from typing import Dict, Any
from core.config import settings
from analytics.market_intelligence import MarketIntelligence

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    """
    The 'Passive Advisor' for VolGuard (Contextual Risk Intelligence Layer).
    
    Upgraded Capabilities:
    1. SDK Integration: Uses official google-generativeai for stability.
    2. Search Grounding: Uses real-time Google Search to verify Nifty drivers.
    3. Institutional Awareness: Cross-references Greeks with FII derivative data.
    4. Non-Blocking: Asynchronous design ensures trading execution remains fast.
    """
    def __init__(self):
        self.api_key = getattr(settings, 'GEMINI_API_KEY', '')
        if self.api_key:
            genai.configure(api_key=self.api_key)
            # Initialize Gemini 2.0 Flash with Google Search Tool enabled
            self.model = genai.GenerativeModel(
                model_name='gemini-2.0-flash',
                tools=[{'google_search': {}}]
            )
        else:
            self.model = None
            logger.warning("GEMINI_API_KEY not found. AI Architect will be disabled.")

        self.intel = MarketIntelligence()
        
        # State Storage for Dashboard/React UI
        self.last_trade_analysis = {}
        self.last_portfolio_review = {}

    async def analyze_trade_setup(self, trade_ctx: Dict) -> Dict:
        """
        Micro-Level: Observes a specific trade before/during execution.
        Evaluates setup against live macro sentiment.
        """
        if not self.model: return {}

        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()
        except Exception:
            news, macro = [], {}

        prompt = f"""
        ROLE: Institutional Risk Manager.
        
        PROPOSED TRADE:
        {json.dumps(trade_ctx, indent=2)}
        
        MACRO CONTEXT:
        - Global Markets: {json.dumps(macro)}
        - News Wire: {news}
        
        TASK:
        Provide a risk commentary. Do NOT decide yes/no.
        Analyze if the news flow supports or contradicts the trade.
        Use Google Search if necessary to check for immediate market-moving events today.
        
        OUTPUT ONLY VALID JSON:
        {{
            "risk_level": "LOW" | "MEDIUM" | "HIGH" | "EXTREME",
            "primary_risk": "Short string",
            "narrative": "2 sentence explanation",
            "macro_alignment": "SUPPORTIVE" | "NEUTRAL" | "CONTRADICTORY"
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            # Ensure markdown blocks are stripped for JSON parsing
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_text)
            
            self.last_trade_analysis = result
            self.last_trade_analysis['timestamp'] = time.time()
            return result
        except Exception as e:
            logger.error(f"AI Trade Analysis Failed: {e}")
            return {}

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: list = None) -> Dict:
        """
        Macro-Level: Professional Risk Advisory Loop with Search Grounding.
        Analyzes total Greeks vs Macro and Institutional Positioning.
        """
        if not self.model: return {}

        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()
            fii_context = fii_data if fii_data else self.intel.get_fii_data()
        except Exception:
            news, macro, fii_context = [], {}, []
        
        prompt = f"""
        ROLE: Senior Portfolio Manager (VolGuard CRIL).
        
        PORTFOLIO GREEKS & STATE:
        {json.dumps(portfolio_state, indent=2)}
        
        INSTITUTIONAL (FII) POSITIONING:
        {json.dumps(fii_context, indent=2)}
        
        GLOBAL REALITY:
        - Macro Shifts: {json.dumps(macro)}
        - Headlines: {news}
        
        TASK:
        1. Use GOOGLE SEARCH to identify specific Nifty/India drivers (RBI, Fed, Geopolitics).
        2. Synthesize WHY FIIs are positioned this way relative to our Greeks.
        3. Identify the 'Failure Mode' (e.g., "Gamma risk on gap-down" or "IV crush after event").
        4. Provide a dashboard-ready narrative.
        
        OUTPUT ONLY VALID JSON:
        {{
            "health_score": 0-100,
            "verdict": "SAFE" | "CAUTION" | "DANGER",
            "failure_mode": "string description",
            "narrative": "3 sentence summary for status dashboard",
            "suggested_hedge": "Optional tactical suggestion",
            "action_command": "1 sentence command"
        }}
        """
        
        try:
            response = self.model.generate_content(prompt)
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            result = json.loads(clean_text)
            
            self.last_portfolio_review = result
            self.last_portfolio_review['timestamp'] = time.time()
            return result
        except Exception as e:
            logger.error(f"AI Portfolio Doctor Failed: {e}")
            return {}
