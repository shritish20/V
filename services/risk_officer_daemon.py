import asyncio
import logging
import sys
import os
import json
from datetime import datetime, time as dtime
from sqlalchemy import select

sys.path.append(os.getcwd())

from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbTradeJournal
from database.models_risk import DbTradePostmortem
from analytics.ai_risk_officer import AIRiskOfficer
from core.models import MultiLegTrade, StrategyType # Helper needed to mock trade obj

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🧠 INTELLIGENCE | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("RiskOfficerDaemon")

async def main():
    """
    VolGuard Intelligence Daemon
    - Hourly: Market Briefings
    - Minutely: Post-Mortem Analysis of Closed Trades
    - Weekly: Pattern Learning
    """
    
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        logger.critical("❌ GROQ_API_KEY missing. Intelligence Core disabled.")
        return
    
    db = HybridDatabaseManager()
    await db.init_db()
    officer = AIRiskOfficer(groq_key, db)
    
    logger.info("🚀 VolGuard Intelligence Core Started")
    await officer.learn_from_history(force_refresh=True)
    await officer.generate_comprehensive_briefing()
    
    last_briefing_date = None
    
    while True:
        try:
            now = datetime.now(settings.IST)
            
            # 1. Hourly Briefings
            if now.minute == 0: 
                logger.info("🔄 Hourly Intelligence Refresh...")
                result = await officer.generate_comprehensive_briefing()
                if result['score'] > 7:
                    logger.warning(f"🚨 CRITICAL RISK: Score {result['score']}/10")
            
            # 2. Morning Briefing
            if now.time() >= dtime(8, 0) and last_briefing_date != now.date():
                logger.info("🌅 Official Morning Briefing...")
                await officer.generate_comprehensive_briefing()
                last_briefing_date = now.date()

            # 3. Weekly Learning (Sundays)
            if now.weekday() == 6 and now.hour == 22 and now.minute == 0:
                logger.info("🎓 Weekly Pattern Analysis...")
                await officer.learn_from_history(force_refresh=True)

            # 4. POST-MORTEM GENERATOR (Check for closed trades without analysis)
            # This makes the "Super Smart" feature autonomous
            async with db.get_session() as session:
                # Find closed trades in Journal
                stmt = select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0)
                journals = (await session.execute(stmt)).scalars().all()
                
                for j in journals:
                    # Check if PostMortem exists
                    pm_stmt = select(DbTradePostmortem).where(DbTradePostmortem.trade_id == j.id)
                    existing = (await session.execute(pm_stmt)).scalars().first()
                    
                    if not existing:
                        logger.info(f"📝 Generatng Post-Mortem for {j.id}...")
                        
                        # Mock a MultiLegTrade object for the officer
                        # (Since we just need ID/Strategy/Time/PnL)
                        mock_trade = MultiLegTrade(
                            id=j.id,
                            legs=[], # Not needed for PM logic
                            strategy_type=StrategyType(j.strategy_name or "IRON_CONDOR"),
                            status="CLOSED",
                            entry_time=j.date,
                            exit_time=datetime.utcnow(), # Approximation if not logged
                            expiry_date="2025-01-01",
                            expiry_type="WEEKLY",
                            capital_bucket="WEEKLY"
                        )
                        
                        await officer.generate_postmortem(mock_trade, j.net_pnl)

            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"Daemon Loop Error: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Intelligence Core Stopped.")
