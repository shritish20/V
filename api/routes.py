#!/usr/bin/env python3
"""
VolGuard 20.0 – API Routes (Fortress Edition)
- Decoupled Architecture: API talks to DB, not Engine Memory
- "Panic Button" writes to DB -> Sheriff picks it up
- Dashboard reads DB state (fast & safe)
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Any, List

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database.manager import HybridDatabaseManager
from database.models import DbRiskState, DbMarketContext, DbStrategy, DbTradeJournal

logger = logging.getLogger("API_Routes")
router = APIRouter(prefix="/api", tags=["VolGuard Dashboard"])

# ---------------------------------------------------------------------------
# Dependency Injection
# ---------------------------------------------------------------------------
async def get_db_session() -> AsyncSession:
    """Provides a transactional scope for API requests."""
    db = HybridDatabaseManager()
    async with db.get_session() as session:
        yield session

# ---------------------------------------------------------------------------
# Dashboard Endpoints (Read-Only)
# ---------------------------------------------------------------------------

@router.get("/health")
async def health_check(session: AsyncSession = Depends(get_db_session)):
    """
    Checks if the system is alive by reading the Sheriff's Heartbeat.
    """
    try:
        # Get latest Sheriff Heartbeat
        res = await session.execute(
            select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
        )
        state = res.scalars().first()
        
        sheriff_status = "offline"
        kill_switch = False
        
        if state:
            # Check if Sheriff updated recently (within 10s)
            lag = (datetime.utcnow() - state.sheriff_heartbeat).total_seconds()
            if lag < 10:
                sheriff_status = "online"
            kill_switch = state.kill_switch_active
            
        return {
            "status": "healthy",
            "sheriff": sheriff_status,
            "kill_switch_active": kill_switch,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database Unreachable"
        )

@router.get("/dashboard/metrics")
async def get_dashboard_metrics(session: AsyncSession = Depends(get_db_session)):
    """
    Aggregates data for the Main Dashboard UI.
    - PnL (Realized + Unrealized)
    - Drawdown
    - AI Narrative
    - Active Alerts
    """
    try:
        # 1. Get Risk State (PnL & Equity)
        risk_res = await session.execute(
            select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
        )
        risk = risk_res.scalars().first()
        
        # 2. Get Market Context (AI View)
        ctx_res = await session.execute(
            select(DbMarketContext).order_by(DbMarketContext.timestamp.desc()).limit(1)
        )
        ctx = ctx_res.scalars().first()
        
        # 3. Get Recent Trades
        trades_res = await session.execute(
            select(DbStrategy).order_by(DbStrategy.entry_time.desc()).limit(5)
        )
        trades = trades_res.scalars().all()

        return {
            "risk": {
                "current_equity": risk.current_equity if risk else 0.0,
                "sod_equity": risk.sod_equity if risk else 0.0,
                "drawdown_pct": risk.drawdown_pct if risk else 0.0,
                "kill_switch": risk.kill_switch_active if risk else False
            },
            "ai": {
                "regime": ctx.regime if ctx else "UNKNOWN",
                "narrative": ctx.ai_narrative if ctx else "Waiting for Analyst...",
                "is_fresh": ctx.is_fresh if ctx else False,
                "last_update": ctx.timestamp.isoformat() if ctx else None
            },
            "recent_trades": [
                {
                    "id": t.id,
                    "strategy": t.type,
                    "pnl": t.pnl,
                    "status": t.status,
                    "time": t.entry_time.isoformat()
                } for t in trades
            ]
        }
    except Exception as e:
        logger.error(f"Dashboard data error: {e}")
        return {"error": str(e)}

# ---------------------------------------------------------------------------
# Control Endpoints (Writes to DB -> Picked up by Sheriff/Engine)
# ---------------------------------------------------------------------------

@router.post("/emergency/flatten")
async def trigger_emergency_flatten(session: AsyncSession = Depends(get_db_session)):
    """
    THE PANIC BUTTON.
    Writes a 'Kill Switch' entry to the DB.
    The Sheriff (Process 3) picks this up in < 2 seconds and flattens everything.
    """
    logger.critical("🚨 API RECEIVED EMERGENCY FLATTEN COMMAND 🚨")
    try:
        # 1. Fetch last state to preserve equity numbers
        last_res = await session.execute(
            select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
        )
        last_state = last_res.scalars().first()
        
        sod = last_state.sod_equity if last_state else 0.0
        curr = last_state.current_equity if last_state else 0.0
        
        # 2. Insert Kill Command
        kill_cmd = DbRiskState(
            timestamp=datetime.utcnow(),
            sheriff_heartbeat=datetime.utcnow(), 
            sod_equity=sod,
            current_equity=curr,
            drawdown_pct=0.0, 
            kill_switch_active=True, # <--- THE TRIGGER
            is_flattening=True
        )
        
        session.add(kill_cmd)
        await session.commit()
        
        return {"status": "success", "message": "KILL SWITCH ACTIVATED. Sheriff has been notified."}
        
    except Exception as e:
        logger.error(f"Failed to trigger kill switch: {e}")
        raise HTTPException(status_code=500, detail="Failed to write to DB")

@router.post("/emergency/reset")
async def reset_kill_switch(session: AsyncSession = Depends(get_db_session)):
    """
    Disarms the Kill Switch. Use with extreme caution.
    """
    logger.warning("⚠️ API RESETTING KILL SWITCH")
    try:
        last_res = await session.execute(
            select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
        )
        last_state = last_res.scalars().first()
        
        sod = last_state.sod_equity if last_state else 0.0
        curr = last_state.current_equity if last_state else 0.0
        
        reset_cmd = DbRiskState(
            timestamp=datetime.utcnow(),
            sheriff_heartbeat=datetime.utcnow(),
            sod_equity=sod,
            current_equity=curr,
            kill_switch_active=False, # <--- DISARM
            is_flattening=False
        )
        
        session.add(reset_cmd)
        await session.commit()
        
        return {"status": "success", "message": "Kill Switch Disarmed."}
        
    except Exception as e:
        logger.error(f"Failed to reset kill switch: {e}")
        raise HTTPException(status_code=500, detail="Failed to write to DB")
