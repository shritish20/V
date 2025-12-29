import asyncio
import logging
import sys
import os
from datetime import datetime, time as dtime

sys.path.append(os.getcwd())

from core.config import settings
from database.manager import HybridDatabaseManager
from analytics.ai_risk_officer import AIRiskOfficer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🧠 INTELLIGENCE | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/risk_officer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("RiskOfficerDaemon")

async def main():
    """Background service for Intelligence & Learning"""
    
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        logger.critical("❌ GROQ_API_KEY missing. Intelligence Core disabled.")
        return
    
    db = HybridDatabaseManager()
    await db.init_db()
    
    officer = AIRiskOfficer(groq_key, db)
    logger.info("🚀 VolGuard Intelligence Core Started")
    
    # Run once on startup
    logger.info("Initializing Intelligence...")
    await officer.learn_from_history(force_refresh=True)
    await officer.generate_comprehensive_briefing()
    
    last_briefing_date = None
    
    while True:
        try:
            now = datetime.now(settings.IST)
            
            # 1. Full Intelligence Cycle (Every hour at minute 0)
            if now.minute == 0: 
                logger.info("🔄 Hourly Intelligence Refresh...")
                result = await officer.generate_comprehensive_briefing()
                
                if result['score'] > 7:
                    logger.warning(f"🚨 CRITICAL RISK DETECTED: Score {result['score']}/10")
            
            # 2. Official Morning Briefing (8:00 AM IST)
            if now.time() >= dtime(8, 0) and last_briefing_date != now.date():
                logger.info("🌅 Generating Official Morning Briefing...")
                await officer.generate_comprehensive_briefing()
                last_briefing_date = now.date()

            # 3. Weekly Pattern Learning (Sundays at 10 PM)
            if now.weekday() == 6 and now.hour == 22 and now.minute == 0:
                logger.info("🎓 Running Weekly Pattern Analysis...")
                await officer.learn_from_history(force_refresh=True)

            # Sleep 1 minute
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Daemon Loop Error: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Intelligence Core Stopped.")
