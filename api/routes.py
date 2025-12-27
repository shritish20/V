#!/usr/bin/env python3
"""
VolGuard 20.0 – API Routes (Fortress Edition)
- SERVES: All 5 Tabs of the "Data Beast" Dashboard
- TAB 1: Live Quant (VRP, Skew, Option Chain from DbMarketSnapshot)
- TAB 2: Strategies (Active execution from DbStrategy)
- TAB 3: Risk Desk (Drawdown + Capital Locks)
- TAB 4: System (Live Logs)
- TAB 5: Journal (Trade History + Notes)
- CONTROL: Panic Button (Kill Switch)
"""
from __future__ import annotations
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from sqlalchemy import select, desc, text
from sqlalchemy.ext.asyncio import AsyncSession
from database.manager import HybridDatabaseManager
from database.models import (
    DbRiskState, DbMarketContext, DbStrategy,
    DbMarketSnapshot, DbCapitalUsage, DbTradeJournal
)
from core.config import settings

logger = logging.getLogger("API_Routes")
router = APIRouter(prefix="/api", tags=["VolGuard Dashboard"])

class JournalNoteUpdate(BaseModel):
    rationale: str
    tags: Optional[str] = "Neutral"

async def get_db_session() -> AsyncSession:
    db = HybridDatabaseManager()
    async with db.get_session() as session:
        yield session

# -------------------------------------------------------------------------
# TAB 1: LIVE FEED (The "Matrix")
# -------------------------------------------------------------------------
@router.get("/market/live")
async def get_live_quant_feed(session: AsyncSession = Depends(get_db_session)):
    try:
        res = await session.execute(select(DbMarketSnapshot).order_by(DbMarketSnapshot.timestamp.desc()).limit(1))
        data = res.scalars().first()
        if not data:
            return {"status": "waiting_for_engine"}
        return {
            "timestamp": data.timestamp,
            "prices": {"spot": data.spot_price, "vix": data.vix},
            "term_structure": {
                "weekly_iv": data.atm_iv_weekly,
                "monthly_iv": data.atm_iv_monthly,
                "spread": data.iv_spread,
                "tag": data.term_structure_tag,
            },
            "models": {
                "rv_7d": data.rv_7d,
                "garch": data.garch_vol_7d,
                "egarch": data.egarch_vol_1d,
                "ivp": data.iv_percentile,
            },
            "vrp": {
                "spread": data.vrp_spread,
                "zscore": data.vrp_zscore,
                "verdict": data.vrp_verdict,
            },
            "levels": {
                "straddle_wk": data.straddle_cost_weekly,
                "straddle_mo": data.straddle_cost_monthly,
                "be_lower": data.breakeven_lower,
                "be_upper": data.breakeven_upper,
            },
            "chain": data.chain_json,
        }
    except Exception as e:
        logger.error(f"Live Feed Error: {e}")
        return {}

# -------------------------------------------------------------------------
# TAB 2: STRATEGIES (Execution)
# -------------------------------------------------------------------------
@router.get("/strategies/active")
async def get_active_strategies(session: AsyncSession = Depends(get_db_session)):
    try:
        res = await session.execute(
            select(DbStrategy)
            .where(DbStrategy.status.in_(['OPEN', 'PENDING']))
            .order_by(DbStrategy.entry_time.desc())
        )
        strategies = res.scalars().all()
        return [{
            "id": s.id,
            "type": s.type,
            "pnl": s.pnl,
            "status": s.status,
            "capital_bucket": s.capital_bucket,
            "entry_time": s.entry_time,
            "legs": s.metadata_json.get("legs", []) if s.metadata_json else [],
        } for s in strategies]
    except Exception as e:
        logger.error(f"Strategy Fetch Error: {e}")
        return []

# -------------------------------------------------------------------------
# TAB 3: RISK DESK (Sheriff)
# -------------------------------------------------------------------------
@router.get("/risk/detailed")
async def get_risk_desk(session: AsyncSession = Depends(get_db_session)):
    try:
        r_res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
        risk = r_res.scalars().first()
        c_res = await session.execute(select(DbCapitalUsage))
        locks = c_res.scalars().all()
        sheriff_status = "OFFLINE"
        if risk:
            lag = (datetime.utcnow() - risk.sheriff_heartbeat).total_seconds()
            if lag < 15:
                sheriff_status = "ONLINE"
        return {
            "drawdown": {
                "current_pct": risk.drawdown_pct if risk else 0.0,
                "max_limit": 0.03,
                "kill_switch": risk.kill_switch_active if risk else False,
                "sheriff_status": sheriff_status,
            },
            "capital_locks": [{
                "bucket": l.bucket,
                "used": l.used_amount,
                "updated": l.last_updated,
            } for l in locks],
        }
    except Exception as e:
        logger.error(f"Risk Desk Error: {e}")
        return {}

# -------------------------------------------------------------------------
# TAB 4: SYSTEM (Logs)
# -------------------------------------------------------------------------
@router.get("/system/logs")
async def get_system_logs(lines: int = 100):
    possible_paths = [
        Path("logs/engine.out.log"),
        Path("logs/volguard.log"),
        Path("/var/log/volguard.log"),
        Path("logs/engine.out.log"),
    ]
    log_file = next((p for p in possible_paths if p.exists()), None)
    if not log_file:
        return {"logs": ["[WARN] Log file not found in standard paths."]}
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
        return {"logs": [line.strip() for line in all_lines[-lines:]]}
    except Exception as e:
        return {"logs": [f"[ERROR] Could not read logs: {str(e)}"]}

# -------------------------------------------------------------------------
# TAB 5: JOURNAL
# -------------------------------------------------------------------------
@router.get("/journal/entries")
async def get_journal_entries(session: AsyncSession = Depends(get_db_session)):
    try:
        res = await session.execute(select(DbTradeJournal).order_by(desc(DbTradeJournal.date)).limit(50))
        return res.scalars().all()
    except Exception as e:
        logger.error(f"Journal Error: {e}")
        return []

@router.patch("/journal/{trade_id}/note")
async def update_journal_note(
    trade_id: str,
    note: JournalNoteUpdate,
    session: AsyncSession = Depends(get_db_session),
):
    try:
        res = await session.execute(select(DbTradeJournal).where(DbTradeJournal.id == trade_id))
        entry = res.scalars().first()
        if not entry:
            raise HTTPException(status_code=404, detail="Trade entry not found")
        entry.entry_rationale = note.rationale
        await session.commit()
        return {"status": "success", "message": "Journal updated"}
    except Exception as e:
        await session.rollback()
        logger.error(f"Journal Update Failed: {e}")
        raise HTTPException(status_code=500, detail="Update failed")

# -------------------------------------------------------------------------
# CONTROL: PANIC BUTTON
# -------------------------------------------------------------------------
@router.post("/emergency/flatten")
async def trigger_emergency_flatten(session: AsyncSession = Depends(get_db_session)):
    logger.critical("🚨 API RECEIVED EMERGENCY FLATTEN COMMAND 🚨")
    try:
        last_res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
        last_state = last_res.scalars().first()
        sod = last_state.sod_equity if last_state else 0.0
        curr = last_state.current_equity if last_state else 0.0
        kill_cmd = DbRiskState(
            timestamp=datetime.utcnow(),
            sheriff_heartbeat=datetime.utcnow(),
            sod_equity=sod,
            current_equity=curr,
            drawdown_pct=0.0,
            kill_switch_active=True,
            is_flattening=True,
        )
        session.add(kill_cmd)
        await session.commit()
        return {"status": "success", "message": "KILL SWITCH ACTIVATED"}
    except Exception as e:
        logger.error(f"Panic Failed: {e}")
        await session.rollback()
        raise HTTPException(status_code=500, detail="Failed to write Panic Command")

@router.post("/emergency/reset")
async def reset_kill_switch(session: AsyncSession = Depends(get_db_session)):
    logger.warning("⚠️ API RESETTING KILL SWITCH")
    try:
        last_res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
        last_state = last_res.scalars().first()
        sod = last_state.sod_equity if last_state else 0.0
        curr = last_state.current_equity if last_state else 0.0
        reset_cmd = DbRiskState(
            timestamp=datetime.utcnow(),
            sheriff_heartbeat=datetime.utcnow(),
            sod_equity=sod,
            current_equity=curr,
            kill_switch_active=False,
            is_flattening=False,
        )
        session.add(reset_cmd)
        await session.commit()
        return {"status": "success", "message": "System Disarmed."}
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail="Failed to Reset")

# -------------------------------------------------------------------------
# NEW: DEEP HEALTH PROBE
# -------------------------------------------------------------------------
@router.get("/health/deep")
async def deep_health(session: AsyncSession = Depends(get_db_session)):
    """
    Returns 503 if any critical fix is broken.
    """
    # 1. Engine running
    engine_pid_file = Path("data/engine.pid")
    if not engine_pid_file.exists():
        raise HTTPException(status_code=503, detail="engine_not_running")

    # 2. Negative capital usage (corruption check)
    row = await session.execute(text(
        "SELECT COUNT(*) FROM capital_usage WHERE used_amount < 0"
    ))
    if row.scalar() > 0:
        raise HTTPException(status_code=503, detail="negative_capital_usage")

    # 3. Duplicate capital ledger rows (idempotency breach)
    row = await session.execute(text("""
        SELECT COUNT(*) FROM (
            SELECT trade_id, bucket FROM capital_ledger 
            GROUP BY trade_id, bucket HAVING COUNT(*) > 1
        ) t
    """))
    if row.scalar() > 0:
        raise HTTPException(status_code=503, detail="duplicate_allocations")

    return {"status": "pass", "timestamp": datetime.utcnow()}
