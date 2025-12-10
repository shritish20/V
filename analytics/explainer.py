import logging
import google.generativeai as genai
from core.config import settings

logger = logging.getLogger("AI_CIO")

# Configure Free Gemini API
# Ensure GEMINI_API_KEY is in your .env file
if hasattr(settings, 'GEMINI_API_KEY') and settings.GEMINI_API_KEY:
    genai.configure(api_key=settings.GEMINI_API_KEY)

class AICIO:
    """
    The Artificial Chief Investment Officer.
    Translates complex Quant metrics into plain English for the user.
    """
    def __init__(self):
        # Use the free, fast model
        self.model_name = 'gemini-2.0-flash' 
        
    def generate_trade_journal(self, context: dict) -> str:
        """
        Sends market context to Gemini and gets a 'CIO Journal Entry'.
        """
        try:
            if not hasattr(settings, 'GEMINI_API_KEY') or not settings.GEMINI_API_KEY:
                return "AI Explanation Unavailable (No GEMINI_API_KEY found in .env)"

            model = genai.GenerativeModel(self.model_name)
            
            # Construct the persona and task
            prompt = f"""
            You are VolGuard, the Chief Investment Officer of an institutional Quantitative Hedge Fund.
            Your job is to explain the rationale behind the latest algorithmic trading decision to an investor.
            
            **Current Market Telemetry:**
            - Selected Strategy: {context.get('strategy')}
            - Market Regime: {context.get('regime')}
            - Directional Bias: {context.get('bias')}
            - Volatility Status: {context.get('vol_status')} (This compares Implied Volatility vs GARCH Forecast)
            - Skew: {context.get('skew')}
            - Event Risk Score: {context.get('event_score', 'N/A')}
            
            **Your Task:**
            Write a short, professional "CIO Journal Entry" (max 3 sentences).
            
            **Guidelines:**
            - Explain WHY this strategy was chosen based on the data above.
            - If Strategy is JADE LIZARD, mention we are exploiting "High Put Skew" or "Expensive Puts".
            - If Strategy is IRON CONDOR, mention "Volatility is cheap" (Trap) or "Neutral Bias".
            - If Strategy is SHORT STRANGLE, mention "Volatility is expensive" (Edge).
            - If Strategy is BULL PUT / BEAR CALL, mention we are "Following the Trend".
            - If WAITING, explain if it's due to Event Risk or dangerous Regime.
            
            **Tone:** Professional, Confident, Insightful.
            """
            
            # Call the API
            response = model.generate_content(prompt)
            
            if response.text:
                return response.text.strip()
            else:
                return "CIO is analyzing the market..."
            
        except Exception as e:
            logger.error(f"AI CIO Generation Failed: {e}")
            # Fallback simple text if AI fails
            return f"Executed {context.get('strategy')} based on {context.get('bias')} bias and {context.get('regime')} regime."
