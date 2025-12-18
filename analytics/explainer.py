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
    def __init__(self):
        self.api_key = settings.GEMINI_API_KEY
        if self.api_key:
            try:
                # Standard client - No special tools enabled to stay in FREE tier
                self.client = genai.Client(api_key=self.api_key)
                self.model_id = "gemini-2.0-flash"
                logger.info("✅ AI Architect Initialized (Free Tier Mode)")
            except Exception as e:
                self.client = None
                logger.error(f"❌ AI Initialization Failed: {e}")
        else:
            self.client = None

        self.intel = MarketIntelligence()
        self.last_portfolio_review = {}

    def _clean_json(self, text: str) -> Dict:
        try:
            clean_text = text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except:
            return {}

    async def review_portfolio_holistically(self, portfolio_state: Dict, fii_data: Optional[List] = None) -> Dict:
        """Advisory Loop using existing RSS news context (Zero Cost)."""
        if not self.client: return {"verdict": "OFFLINE", "narrative": "AI Architect not configured."}

        try:
            # 1. Gather context from your FREE sensors
            macro = self.intel.get_macro_sentiment()
            news = self.intel.get_latest_headlines(limit=8) # Get more headlines to compensate for no search
            fii = fii_data if fii_data else self.intel.get_fii_data()

            # 2. Build a high-fidelity prompt using that data
            prompt = f"""
            ROLE: Senior Portfolio Manager (VolGuard CRIL).
            
            CURRENT DATA:
            - Portfolio: {json.dumps(portfolio_state)}
            - FII Positions: {json.dumps(fii)}
            - Global Macro: {json.dumps(macro)}
            - Live News Wire: {news}

            TASK:
            Analyze the relationship between the FII positioning and the News Wire. 
            Identify if our Delta/Vega/Gamma exposure is dangerous given the 'Live News Wire'.
            Explicitly define the 'Failure Mode' for tomorrow's open.
            
            OUTPUT ONLY VALID JSON:
            {{
                "health_score": 0-100,
                "verdict": "SAFE" | "CAUTION" | "DANGER",
                "failure_mode": "string",
                "narrative": "3 sentence dashboard summary",
                "action_command": "1 sentence command"
            }}
            """

            # 3. Standard Generate Call (Free & Fast)
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1)
            )
            
            result = self._clean_json(response.text)
            self.last_portfolio_review = {**result, "timestamp": time.time()}
            return result

        except Exception as e:
            logger.error(f"🛡️ AI Advisor Paused: {str(e)[:50]}")
            return {"verdict": "OFFLINE", "narrative": "AI Service encountered an error."}
