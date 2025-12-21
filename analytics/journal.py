import logging
import asyncio
from datetime import datetime
from sqlalchemy import select
from core.config import settings
from database.models import DbTradeJournal
from trading.api_client import EnhancedUpstoxAPI

logger = logging.getLogger("TradeJournal")

class JournalManager:
    def __init__(self, db_manager, api_client: EnhancedUpstoxAPI):
        self.db = db_manager
        self.api = api_client

    async def log_entry(self, trade_id: str, strategy: str, metrics: dict, ai_analysis: dict):
        try:
            async with self.db.get_session() as session:
                entry = DbTradeJournal(
                    id=trade_id,
                    strategy_name=strategy,
                    regime_at_entry=metrics.get("regime", "UNKNOWN"),
                    vix_at_entry=metrics.get("vix", 0.0),
                    spot_at_entry=metrics.get("spot_price", 0.0),
                    ai_analysis_json=ai_analysis,
                    entry_rationale=f"IVP: {metrics.get('ivp', 0):.0f} | Skew: {metrics.get('volatility_skew', 0):.2f}",
                    is_reconciled=False
                )
                session.add(entry)
                await self.db.safe_commit(session)
                logger.info(f"📓 Journal Entry Created for {trade_id}")
        except Exception as e:
            logger.error(f"Journal Log Failed: {e}")

    async def reconcile_daily_ledger(self):
        if settings.SAFETY_MODE != "live": return

        today = datetime.now().strftime("%d-%m-%Y")
        logger.info(f"📓 Reconciling Ledger for {today}...")

        charges_res = await self.api._request_with_retry(
            "GET", 
            "profit_loss_charges",
            params={"from_date": today, "to_date": today, "segment": "FO", "financial_year": "2425"} 
        )

        if not charges_res.get("data"):
            logger.warning("📓 No charges found yet.")
            return

        try:
            total_charges = float(charges_res["data"].get("charges_breakdown", {}).get("total", 0.0))
            
            async with self.db.get_session() as session:
                stmt = select(DbTradeJournal).where(DbTradeJournal.is_reconciled == False)
                result = await session.execute(stmt)
                entries = result.scalars().all()
                
                count = len(entries)
                if count > 0:
                    avg_charge = total_charges / count
                    for entry in entries:
                        entry.total_charges = avg_charge
                        entry.net_pnl = entry.gross_pnl - avg_charge
                        entry.is_reconciled = True
                    
                    await self.db.safe_commit(session)
                    logger.info(f"✅ Ledger Reconciled. Total Day Charges: ₹{total_charges}")
        except Exception as e:
            logger.error(f"Reconciliation Failed: {e}")
