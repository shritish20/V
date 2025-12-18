import logging
import json
import asyncio
import time
from typing import Dict, Any, List, Optional
from google import genai
from google.genai import types
from core.config import settings
from analytics.market_intelligence import MarketIntelligence

logger = logging.getLogger("AI_Architect")

class AI_Portfolio_Architect:
    """
    The 'Passive Advisor' for VolGuard CRIL.
    Optimized for Zero-Cost (Free Tier) and Non-Blocking execution.
    """
    def __init__(self):
        # 1. ATTR INITIALIZATION (Fixes Dashboard AttributeError)
        self.last_trade_analysis = {}
        self.last_portfolio_review = {}
        
        self.api_key = settings.GEMINI_API_KEY
        if self.api_key:
            try:
                # Standard client - No search tools enabled to stay in FREE tier
                self.client = genai.Client(api_key=self.api_key)
                self.model_id = "gemini-2.0-flash"
                logger.info("✅ AI Architect Initialized (Zero-Cost Mode)")
            except Exception as e:
                self.client = None
                logger.error(f"❌ AI Initialization Failed: {e}")
        else:
            self.client = None
            logger.warning("⚠️ GEMINI_API_KEY missing. AI features disabled.")

        self.intel = MarketIntelligence()

    def _clean_json(self, text: str) -> Dict:
        """Helper to ensure AI text is converted to a dictionary safely."""
        try:
            clean_text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except Exception:
            return {}

    async def analyze_trade_setup(self, trade_ctx: Dict) -> Dict:
        """Micro-Level: Observes a specific trade before execution."""
        if not self.client: return {}

        try:
            news = self.intel.get_latest_headlines(limit=5)
            macro = self.intel.get_macro_sentiment()

            prompt = f"""
            ROLE: Institutional Risk Manager.
            PROPOSED TRADE: {json.dumps(trade_ctx)}
            MACRO: {json.dumps(macro)} | NEWS: {news}
            
            TASK: 
            Analyze if this trade is supported by current macro context.
            
            OUTPUT ONLY VALID JSON:
            {{
                "risk_level": "LOW" | "MEDIUM" | "HIGH",
                "primary_risk": "string",
                "narrative": "2 sentence explanation",
                "macro_alignment": "SUPPORTIVE" | "NEUTRAL" | "CONTRADICTORY"
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
            logger.error(f"AI Trade analysis failed: {str(e)[:50]}")
            return {}

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: Optional[List] = None) -> Dict:
        """Macro-Level: 24/7 Contextual Review (Non-Blocking)."""
        if not self.client: 
            return {"verdict": "OFFLINE", "narrative": "AI not configured."}

        try:
            macro = self.intel.get_macro_sentiment()
            news = self.intel.get_latest_headlines(limit=8)
            fii = fii_data if fii_data else self.intel.get_fii_data()

            prompt = f"""
            ROLE: Senior Portfolio Manager (VolGuard CRIL).
            DATA: Portfolio={json.dumps(portfolio_state)}, FII={json.dumps(fii)}, News={news}

            TASK:
            Synthesize FII bias with News. Define the 'Failure Mode' for tomorrow.
            
            OUTPUT ONLY VALID JSON:
            {{
                "health_score": 0-100,
                "verdict": "SAFE" | "CAUTION" | "DANGER",
                "failure_mode": "string",
                "narrative": "3 sentence dashboard summary",
                "action_command": "1 sentence command"
            }}
            """
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            
            result = self._clean_json(response.text)
            # This line updates the state the Dashboard reads
            self.last_portfolio_review = {**result, "timestamp": time.time()}
            return result

        except Exception as e:
            logger.warning(f"🛡️ AI Advisory paused (Free Tier): {str(e)[:50]}")
            return {"verdict": "REFRESHING", "narrative": "AI context is being updated."}
