import logging
import google.generativeai as genai
from core.config import settings
from analytics.market_intelligence import MarketIntelligence

logger = logging.getLogger("AI_CIO")

# Configure API Key if available
if hasattr(settings, 'GEMINI_API_KEY') and settings.GEMINI_API_KEY:
    genai.configure(api_key=settings.GEMINI_API_KEY)

class AICIO:
    def __init__(self):
        self.model_name = 'gemini-2.0-flash'
        self.intel = MarketIntelligence()

    def generate_adversarial_review(self, trade_context):
        """
        Fetches external data and argues WITH the internal logic.
        """
        try:
            # 1. Gather The "Outside View"
            macro = self.intel.get_macro_sentiment()
            news = self.intel.get_latest_headlines()
            
            # Format data for the prompt
            macro_str = ", ".join([f"{k}: {v}%" for k, v in macro.items()])
            news_str = "\n".join(news)

            # 2. The "Adversarial" Prompt
            prompt = f"""
            You are the cynical Chief Risk Officer (CRO) of a Hedge Fund.
            
            **Internal Quant Signal:**
            Strategy: {trade_context.get('strategy')}
            Rationale: {trade_context.get('rationale')}
            Data: VIX={trade_context.get('vix')}, Bias={trade_context.get('bias')}

            **External Reality Check (Live Data):**
            Global Markets: {macro_str}
            News Wire:
            {news_str}

            **Your Task:**
            Compare the Internal Signal vs. External Reality.
            PAY ATTENTION TO TIMESTAMPS in the news.
            
            1. If news is OLD (>24h), ignore it.
            2. If news is FRESH and contradicts the trade, WARN the trader.
            
            **Output Format:**
            VERDICT: [APPROVE] or [WARNING]
            REASON: One blunt sentence explaining why.
            """

            model = genai.GenerativeModel(self.model_name)
            response = model.generate_content(prompt)
            
            return response.text.strip() if response.text else "CIO Offline."

        except Exception as e:
            logger.error(f"AI CIO Failed: {e}")
            return "CIO Assessment Unavailable."
