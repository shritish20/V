import asyncio
import logging
import sys
import os
from datetime import datetime
from sqlalchemy import select
sys.path.append(os.getcwd())
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbTradeJournal
from database.models_risk import DbTradePostmortem
from analytics.ai_risk_officer import AIRiskOfficer
from core.models import MultiLegTrade, StrategyType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RiskOfficerDaemon")

async def main():
    if not settings.GROQ_API_KEY: 
        logger.critical("No API Key - Intelligence Disabled")
        return
        
    db = HybridDatabaseManager()
    await db.init_db()
    officer = AIRiskOfficer(settings.GROQ_API_KEY, db)
    
    await officer.learn_from_history()
    await officer.generate_comprehensive_briefing()
    
    while True:
        try:
            now = datetime.now(settings.IST)
            if now.minute == 0: await officer.generate_comprehensive_briefing()
            
            # R3: Short-lived session for Post-Mortems
            async with db.get_session() as session:
                stmt = select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0).order_by(DbTradeJournal.date.desc()).limit(20)
                closed_trades = (await session.execute(stmt)).scalars().all()
                
                for t in closed_trades:
                    # check existence
                    exists = (await session.execute(select(DbTradePostmortem).where(DbTradePostmortem.trade_id == t.id))).scalars().first()
                    if not exists:
                        # process outside of DB lock if possible, or keep fast
                        mock = MultiLegTrade(id=t.id, legs=[], strategy_type=StrategyType(t.strategy_name or "IC"), status="CLOSED", entry_time=t.date, expiry_date="2025", expiry_type="W", capital_bucket="W")
                        # Calls its own internal session
                        await officer.generate_postmortem(mock, t.net_pnl)

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Daemon Error: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__": asyncio.run(main())
