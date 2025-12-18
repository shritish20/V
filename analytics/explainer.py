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
    The 'Passive Advisor' for VolGuard CRIL (Contextual Risk Intelligence Layer).
    
    UPGRADED v2.0:
    - Unified SDK: Uses google-genai for future-proof stability.
    - Search Grounding: Native Google Search integration for live Nifty context.
    - Institutional Bridge: Merges internal Greeks with external FII positioning.
    - Failure Mode Analysis: Specifically identifies 'Black Swan' triggers.
    """
    def __init__(self):
        self.api_key = getattr(settings, 'GEMINI_API_KEY', '')
        if self.api_key:
            # Initialize the modern Unified Client
            self.client = genai.Client(api_key=self.api_key)
            self.model_id = "gemini-2.0-flash"
            logger.info("✅ AI Architect Initialized with Google-GenAI SDK")
        else:
            self.client = None
            logger.warning("⚠️ GEMINI_API_KEY not found. AI Architect will be disabled.")

        self.intel = MarketIntelligence()
        
        # State Storage for Dashboard/React UI
        self.last_trade_analysis = {}
        self.last_portfolio_review = {}

    def _clean_json_response(self, text: str) -> Dict:
        """Strips markdown and ensures valid JSON parsing."""
        try:
            clean_text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except Exception as e:
            logger.error(f"AI JSON Parsing Error: {e} | Raw: {text[:100]}")
            return {}

    async def analyze_trade_setup(self, trade_ctx: Dict) -> Dict:
        """
        Micro-Level Review: Analyzes a specific trade signal before execution.
        """
        if not self.client: return {}

        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()

            prompt = f"""
            ROLE: Institutional Risk Manager.
            PROPOSED TRADE: {json.dumps(trade_ctx, indent=2)}
            MACRO: {json.dumps(macro)} | NEWS: {news}
            
            TASK: 
            1. Use GOOGLE SEARCH to check for breaking news affecting this specific strategy.
            2. Evaluate if the trade aligns with current institutional flows.
            
            OUTPUT ONLY VALID JSON:
            {{
                "risk_level": "LOW" | "MEDIUM" | "HIGH" | "EXTREME",
                "primary_risk": "Short string",
                "narrative": "2 sentence explanation",
                "macro_alignment": "SUPPORTIVE" | "NEUTRAL" | "CONTRADICTORY"
            }}
            """

            # Use new SDK syntax for Search Tool
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.1 # Low temp for deterministic risk analysis
                )
            )

            result = self._clean_json_response(response.text)
            if result:
                self.last_trade_analysis = {**result, "timestamp": time.time()}
            return result
        except Exception as e:
            logger.error(f"AI Trade Analysis Failed: {e}")
            return {}

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: Optional[List] = None) -> Dict:
        """
        Macro-Level Review: Professional Risk Advisory Loop with Search Grounding.
        Analyzes Portfolio Greeks against live Global Macro and FII positioning.
        """
        if not self.client: return {}

        try:
            news = self.intel.get_latest_headlines(max_age_hours=24)
            macro = self.intel.get_macro_sentiment()
            fii_context = fii_data if fii_data else self.intel.get_fii_data()

            prompt = f"""
            ROLE: Senior Portfolio Manager (VolGuard CRIL).
            PORTFOLIO STATE: {json.dumps(portfolio_state, indent=2)}
            FII POSITIONING: {json.dumps(fii_context, indent=2)}
            CONTEXT: {json.dumps(macro)} | {news}

            TASK:
            1. SEARCH GOOGLE for today's specific Nifty drivers (RBI, Fed, Geopolitics).
            2. Contrast our Greeks (Delta/Vega/Gamma) against institutional (FII) bias.
            3. Explicitly define the 'Failure Mode' (What event blows up this position?).
            
            OUTPUT ONLY VALID JSON:
            {{
                "health_score": 0-100,
                "verdict": "SAFE" | "CAUTION" | "DANGER",
                "failure_mode": "string",
                "narrative": "3 sentence dashboard summary",
                "suggested_hedge": "Optional tactic",
                "action_command": "1 sentence command"
            }}
            """

            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())]
                )
            )

            result = self._clean_json_response(response.text)
            if result:
                self.last_portfolio_review = {**result, "timestamp": time.time()}
            return result
        except Exception as e:
            logger.error(f"AI Portfolio Doctor Failed: {e}")
            return {}
