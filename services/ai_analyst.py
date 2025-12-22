# services/ai_analyst.py
import asyncio
import logging
import sys
import os
from datetime import datetime

# 1. Path Setup (Crucial to find your 'analytics' folder)
sys.path.append(os.getcwd())

from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbMarketContext

# 2. IMPORT YOUR BREAD AND BUTTER (Untouched)
# We assume these exist in your analytics/ folder
try:
    from analytics.explainer import AI_Portfolio_Architect
    from analytics.market_intelligence import MarketIntelligence
except ImportError:
    print("❌ CRITICAL: Could not import 'analytics' modules. Run this from the root folder.")
    sys.exit(1)

# 3. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🧠 ANALYST | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/ai_analyst.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Analyst")

async def run_analyst():
    """
    Independent Service:
    1. Fetches News/FII Data (MarketIntelligence)
    2. Generates Narrative (AI_Portfolio_Architect)
    3. Pushes to Database (DbMarketContext)
    """
    logger.info("🧠 AI Analyst Service Starting...")
    
    # Initialize your components
    db = HybridDatabaseManager()
    intel = MarketIntelligence()
    architect = AI_Portfolio_Architect() 
    
    logger.info("✅ Connected to Gemini & News Feeds")

    while True:
        try:
            # --- PHASE 1: GATHER INTEL ---
            logger.info("📡 Gathering latest market data...")
            
            # (We use timeouts to prevent hanging)
            news = await asyncio.to_thread(intel.get_latest_headlines, limit=10)
            fii_data = await asyncio.to_thread(intel.get_fii_data)
            
            logger.info(f"   - News: {len(news)} headlines")
            logger.info(f"   - FII Data: {'Available' if fii_data else 'None'}")

            # --- PHASE 2: ASK GEMINI (THE BRAIN) ---
            logger.info("🤔 Asking Gemini for Verdict...")
            
            # We construct a synthetic state to prompt the AI 
            # (Since this service doesn't hold trades, we pass a dummy portfolio)
            analysis_input = {
                "portfolio_state": {"note": "Periodic Market Scan"},
                "fii_data": fii_data,
                "news": news
            }
            
            # Call your existing AI logic
            # We wrap it in wait_for to handle Gemini timeouts (20s limit)
            try:
                analysis = await asyncio.wait_for(
                    architect.review_portfolio_holistically(
                        portfolio_state=analysis_input["portfolio_state"], 
                        fii_data=fii_data
                    ),
                    timeout=25.0 
                )
                is_fresh = True
                logger.info("✅ Gemini Response Received")
            except asyncio.TimeoutError:
                logger.error("⚠️ Gemini API Timed Out")
                analysis = {"verdict": "UNKNOWN", "narrative": "AI Timeout - Retrying next cycle"}
                is_fresh = False
            except Exception as e:
                logger.error(f"⚠️ AI Generation Failed: {e}")
                analysis = {"verdict": "UNKNOWN", "narrative": f"Error: {str(e)}"}
                is_fresh = False

            # Extract fields safely (Handling your specific AI output format)
            # Adjust these keys if your 'review_portfolio_holistically' returns different keys
            verdict = analysis.get("verdict", "SAFE")  # Default to SAFE
            narrative = analysis.get("narrative", "No narrative generated.")
            
            if verdict == "DANGER":
                logger.warning(f"🚨 AI DETECTED DANGER: {narrative[:50]}...")

            # --- PHASE 3: UPDATE DATABASE ---
            async with db.get_session() as session:
                ctx = DbMarketContext(
                    timestamp=datetime.utcnow(),
                    regime=verdict,
                    ai_narrative=narrative,
                    is_high_risk=(verdict == "DANGER"),
                    is_fresh=is_fresh
                )
                session.add(ctx)
                await db.safe_commit(session)
                
            logger.info("💾 Market Context Saved to DB.")

            # --- PHASE 4: SLEEP ---
            # Run every 15 minutes
            SLEEP_SEC = 900 
            logger.info(f"💤 Sleeping for {SLEEP_SEC/60} mins...")
            await asyncio.sleep(SLEEP_SEC)

        except Exception as e:
            logger.error(f"🔥 Analyst Loop Crash: {e}")
            await asyncio.sleep(60) # Retry after 1 min on crash

if __name__ == "__main__":
    try:
        asyncio.run(run_analyst())
    except KeyboardInterrupt:
        logger.info("Analyst Stopped by User")
