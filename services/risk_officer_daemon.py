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

    # initial load
    await officer.learn_from_history()
    await officer.generate_comprehensive_briefing()

    while True:
        try:
            now = datetime.now(settings.IST)
            # ---------- FII once per day after 19:00 ----------
            if officer._last_fii_date != now.date() and now.hour >= 19:
                logger.info("🔄 Fetching fresh FII data (daily)")
                await officer.fetch_fii_sentiment()
                # regenerate briefing with new FII block
                await officer.generate_comprehensive_briefing()

            # ---------- Post-mortems every minute ----------
            async with db.get_session() as session:
                closed_trades = (await session.execute(select(DbTradeJournal).where(DbTradeJournal.net_pnl != 0).order_by(DbTradeJournal.date.desc()).limit(20))).scalars().all()
                for t in closed_trades:
                    exists = (await session.execute(select(DbTradePostmortem).where(DbTradePostmortem.trade_id == t.id))).scalars().first()
                    if not exists:
                        mock_trade = MultiLegTrade(id=t.id, legs=[], strategy_type=StrategyType(t.strategy_name or "IRON_CONDOR"), status="CLOSED", entry_time=t.date, expiry_date="2025-01-01", expiry_type="WEEKLY", capital_bucket="WEEKLY")
                        await officer.generate_postmortem(mock_trade, t.net_pnl)

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Daemon Error: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__": asyncio.run(main())
