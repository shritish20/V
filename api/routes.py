#!/usr/bin/env python3
"""
VolGuard 20.0 – API Routes (Intelligence Edition)
- SERVES: Live Quant Feed, Strategies, Risk Desk, System Logs
- NEW: AI Intelligence Endpoints (Briefings, Patterns, Post-Mortems)
"""
from __future__ import annotations
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import select, desc, text
from sqlalchemy.ext.asyncio import AsyncSession

from database.manager import HybridDatabaseManager
from database.models import (
    DbRiskState, DbStrategy, DbMarketSnapshot, DbCapitalUsage, 
    DbTradeJournal, DbOrder
)
# NEW IMPORTS FOR RISK INTELLIGENCE
from database.models_risk import DbRiskBriefing, DbLearnedPattern, DbTradePostmortem
from core.metrics import get_metrics

logger = logging.getLogger("API_Routes")
router = APIRouter(prefix="/api", tags=["VolGuard Dashboard"])

class JournalNoteUpdate(BaseModel):
    rationale: str
    tags: Optional[str] = "Neutral"

async def get_db_session() -> AsyncSession:
    db = HybridDatabaseManager()
    async with db.get_session() as session:
        yield session

# ==============================================================================
# 🧠 NEW: AI INTELLIGENCE ENDPOINTS
# ==============================================================================

@router.get("/risk/briefing")
async def get_latest_briefing(session: AsyncSession = Depends(get_db_session)):
    """Get the latest AI Market Briefing & Risk Score"""
    try:
        res = await session.execute(
            select(DbRiskBriefing).order_by(desc(DbRiskBriefing.timestamp)).limit(1)
        )
        briefing = res.scalars().first()
        if not briefing:
            return {"status": "waiting", "message": "AI is analyzing markets..."}
        
        return {
            "timestamp": briefing.timestamp,
            "score": briefing.risk_score,
            "level": briefing.alert_level,
            "narrative": briefing.briefing_text,
            "risks": briefing.active_risks,
            "context": briefing.market_context
        }
    except Exception as e:
        logger.error(f"Briefing API Error: {e}")
        return {"status": "error", "message": str(e)}

@router.get("/risk/patterns")
async def get_learned_patterns(session: AsyncSession = Depends(get_db_session)):
    """Get AI-learned Success/Failure patterns"""
    try:
        res = await session.execute(
            select(DbLearnedPattern).order_by(desc(DbLearnedPattern.severity))
        )
        patterns = res.scalars().all()
        return [{
            "name": p.pattern_name,
            "type": p.pattern_type,
            "win_rate": p.win_rate,
            "occurrences": p.occurrence_count,
            "lesson": p.lesson_text,
            "severity": p.severity
        } for p in patterns]
    except Exception as e:
        logger.error(f"Pattern API Error: {e}")
        return []

@router.get("/risk/postmortems")
async def get_recent_postmortems(limit: int = 10, session: AsyncSession = Depends(get_db_session)):
    """Get recent AI grades for closed trades"""
    try:
        res = await session.execute(
            select(DbTradePostmortem).order_by(desc(DbTradePostmortem.created_at)).limit(limit)
        )
        pms = res.scalars().all()
        return pms
    except Exception as e:
        logger.error(f"Postmortem API Error: {e}")
        return []

# ==============================================================================
# EXISTING ENDPOINTS (UNCHANGED FUNCTIONALITY)
# ==============================================================================

@router.get("/market/live")
async def get_live_quant_feed(session: AsyncSession = Depends(get_db_session)):
    try:
        res = await session.execute(select(DbMarketSnapshot).order_by(DbMarketSnapshot.timestamp.desc()).limit(1))
        data = res.scalars().first()
        if not data: return {"status": "waiting_for_engine"}
        
        return {
            "timestamp": data.timestamp,
            "prices": {"spot": data.spot_price, "vix": data.vix},
            "vrp": {"zscore": data.vrp_zscore, "verdict": data.vrp_verdict},
            "models": {"ivp": data.iv_percentile, "garch": data.garch_vol_7d},
            "term_structure": {"spread": data.iv_spread, "tag": data.term_structure_tag}
        }
    except Exception as e:
        logger.error(f"Live Feed Error: {e}")
        return {}

@router.get("/strategies/active")
async def get_active_strategies(session: AsyncSession = Depends(get_db_session)):
    try:
        res = await session.execute(
            select(DbStrategy).where(DbStrategy.status.in_(['OPEN', 'PENDING'])).order_by(DbStrategy.entry_time.desc())
        )
        strategies = res.scalars().all()
        return [{
            "id": s.id, "type": s.type, "pnl": s.pnl, "status": s.status,
            "bucket": s.capital_bucket, "entry": s.entry_time
        } for s in strategies]
    except Exception as e:
        logger.error(f"Strategy API Error: {e}")
        return []

@router.get("/risk/detailed")
async def get_risk_desk(session: AsyncSession = Depends(get_db_session)):
    try:
        r_res = await session.execute(select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1))
        risk = r_res.scalars().first()
        
        c_res = await session.execute(select(DbCapitalUsage))
        locks = c_res.scalars().all()
        
        return {
            "drawdown": {
                "current": risk.drawdown_pct if risk else 0.0,
                "max": 0.03,
                "kill_switch": risk.kill_switch_active if risk else False
            },
            "capital": [{"bucket": l.bucket, "used": l.used_amount} for l in locks]
        }
    except Exception as e:
        logger.error(f"Risk Desk Error: {e}")
        return {}

@router.get("/system/logs")
async def get_system_logs(lines: int = 100):
    log_file = Path("logs/risk_officer.log") # Prioritize Intelligence Logs
    if not log_file.exists():
        log_file = Path("logs/engine.out.log")
    
    if not log_file.exists():
        return {"logs": ["[WARN] No log files found."]}
        
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
            return {"logs": [line.strip() for line in all_lines[-lines:]]}
    except Exception as e:
        return {"logs": [f"Error reading logs: {e}"]}

@router.post("/emergency/flatten")
async def trigger_emergency_flatten(session: AsyncSession = Depends(get_db_session)):
    logger.critical("🔥 API RECEIVED EMERGENCY FLATTEN COMMAND")
    try:
        cmd = DbRiskState(
            timestamp=datetime.utcnow(), sheriff_heartbeat=datetime.utcnow(),
            kill_switch_active=True, is_flattening=True
        )
        session.add(cmd)
        await session.commit()
        return {"status": "success", "message": "KILL SWITCH ACTIVATED"}
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health/detailed")
async def detailed_health():
    metrics = get_metrics()
    return {"status": "healthy", "metrics": metrics.to_dict()}
