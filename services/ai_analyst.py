#!/usr/bin/env python3
"""
VolGuard 20.0 – AI Analyst Service (Process 2)
- Independent Microservice
- Fetches Market Intel (News/FII)
- Queries Gemini for Strategy Verdicts
- Writes to DB (Engine reads from DB, not direct call)
"""
import asyncio
import logging
import sys
import os
from datetime import datetime

# 1. Path Setup
sys.path.append(os.getcwd())

from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbMarketContext

# --- FIX: Create logs directory before configuring logger ---
os.makedirs("logs", exist_ok=True)

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

# 2. Conditional Import for Robustness
try:
    from analytics.explainer import AI_Portfolio_Architect
    from analytics.market_intelligence import MarketIntelligence
    MODULES_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ Analytics modules missing ({e}). Running in Placeholder Mode.")
    MODULES_AVAILABLE = False

async def run_analyst():
    """
    Independent Service:
    1. Fetches News/FII Data (MarketIntelligence)
    2. Generates Narrative (AI_Portfolio_Architect)
    3. Pushes to Database (DbMarketContext)
    """
    logger.info("🧠 AI Analyst Service Starting...")
    
    # Check for API Key
    if not settings.GEMINI_API_KEY:
        logger.warning("⚠️ GEMINI_API_KEY not found in env. AI will be dormant.")
    
    # Initialize DB
    db = HybridDatabaseManager()
    
    # Initialize Components (if available)
    intel = None
    architect = None
    if MODULES_AVAILABLE and settings.GEMINI_API_KEY:
        try:
            intel = MarketIntelligence()
            architect = AI_Portfolio_Architect() 
            logger.info("✅ Connected to Gemini & News Feeds")
        except Exception as e:
            logger.error(f"❌ Failed to init AI components: {e}")

    while True:
        try:
            verdict = "SAFE"
            narrative = "AI Service Online - Waiting for Data..."
            is_fresh = False
            is_high_risk = False

            # --- PHASE 1: REAL ANALYSIS (If Configured) ---
            if architect and intel:
                logger.info("📡 Gathering latest market data...")
                
                # Fetch Data
                news = await asyncio.to_thread(intel.get_latest_headlines, limit=5)
                fii_data = await asyncio.to_thread(intel.get_fii_data)
                
                logger.info(f"   - News: {len(news)} headlines")

                # Ask Gemini
                logger.info("🤔 Asking Gemini for Verdict...")
                analysis_input = {
                    "portfolio_state": {"note": "Periodic Market Scan"},
                    "fii_data": fii_data,
                    "news": news
                }
                
                try:
                    analysis = await asyncio.wait_for(
                        architect.review_portfolio_holistically(
                            portfolio_state=analysis_input["portfolio_state"], 
                            fii_data=fii_data
                        ),
                        timeout=25.0 
                    )
                    
                    verdict = analysis.get("verdict", "SAFE")
                    narrative = analysis.get("narrative", "Analysis complete.")
                    is_fresh = True
                    is_high_risk = (verdict == "DANGER")
                    
                    logger.info(f"✅ Gemini Verdict: {verdict}")
                    
                except asyncio.TimeoutError:
                    logger.error("⚠️ Gemini API Timed Out")
                    narrative = "Gemini Timed Out - Using Previous Context"
                except Exception as e:
                    logger.error(f"⚠️ AI Gen Error: {e}")
                    narrative = f"AI Error: {str(e)}"

            # --- PHASE 2: FALLBACK (If No Key/Modules) ---
            elif not settings.GEMINI_API_KEY:
                narrative = "No API Key - AI in Passive Mode"
                is_fresh = True # We mark it fresh so Dashboard shows connection is alive

            # --- PHASE 3: WRITE TO DB ---
            # This is critical: The Dashboard relies on this heartbeat
            try:
                async with db.get_session() as session:
                    ctx = DbMarketContext(
                        timestamp=datetime.utcnow(),
                        regime=verdict,
                        ai_narrative=narrative,
                        is_high_risk=is_high_risk,
                        is_fresh=is_fresh
                    )
                    session.add(ctx)
                    await db.safe_commit(session)
                logger.info("💾 Market Context Updated in DB")
            except Exception as db_err:
                logger.error(f"Failed to write to DB: {db_err}")

            # --- PHASE 4: SLEEP ---
            # Run every 15 minutes (900s), or 1 min if dummy mode
            SLEEP_SEC = 900 if (architect and intel) else 60
            logger.info(f"💤 Sleeping for {SLEEP_SEC}s...")
            await asyncio.sleep(SLEEP_SEC)

        except Exception as e:
            logger.error(f"🔥 Analyst Loop Crash: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(run_analyst())
    except KeyboardInterrupt:
        logger.info("Analyst Stopped by User")
