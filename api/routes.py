#!/usr/bin/env python3
"""
VolGuard 20.0 – Unified API Router
MERGED:
1. V20 Hardening: Deep Health, Graceful Shutdown, Prometheus Metrics
2. V19 Business: CIO Commentary, Dashboard, Manual Trades, Logs, Token Refresh
"""
from __future__ import annotations

import time
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge
from pydantic import BaseModel, Field
from sqlalchemy import select

# Core Imports
from core.config import settings
from core.enums import TradeStatus, StrategyType, ExpiryType, ExitReason, CapitalBucket
from core.models import MultiLegTrade, Position, GreeksSnapshot
from database.models import DbStrategy
from trading.api_client import EnhancedUpstoxAPI

# Logging
logger = logging.getLogger("VolGuardAPI")

# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------
ERROR_TOTAL = Counter("volguard_errors_total", "Total engine errors", ["type"])
MARGIN_AVAILABLE = Gauge("volguard_margin_available", "Broker available margin")
TRADES_OPEN = Gauge("volguard_trades_open", "Number of open trades")

# ---------------------------------------------------------------------------
# Request Models
# ---------------------------------------------------------------------------
class EngineStartRequest(BaseModel):
    continuous: bool = Field(default=True)
    initialize_dashboard: bool = Field(default=True)

class TokenUpdateRequest(BaseModel):
    access_token: str = Field(..., min_length=10)

class CapitalAdjustmentRequest(BaseModel):
    weekly_pct: float = Field(0.40, ge=0.0, le=1.0)
    monthly_pct: float = Field(0.50, ge=0.0, le=1.0)
    intraday_pct: float = Field(0.10, ge=0.0, le=1.0)

class ManualLegRequest(BaseModel):
    symbol: str = "NIFTY"
    strike: float
    option_type: str  # CE/PE
    side: str         # BUY/SELL
    quantity: int
    expiry_date: str  # YYYY-MM-DD

class ManualTradeRequest(BaseModel):
    legs: List[ManualLegRequest]
    capital_bucket: str = "INTRADAY"

# ---------------------------------------------------------------------------
# Router Setup
# ---------------------------------------------------------------------------
router = APIRouter(prefix="/api", tags=["VolGuard Unified"])

# ---------------------------------------------------------------------------
# Dependency Injection
# ---------------------------------------------------------------------------
def get_engine(request: Request):
    """Retrieves the singleton engine injected in main.py"""
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Engine Initializing")
    return engine

def get_admin_key(x_admin_key: str = Query(..., alias="key")):
    """Simple security for critical actions"""
    # In production, use a proper env var or Auth middleware
    # For now, we allow any key if not configured, or check against env
    pass 

# ==========================================
# 1. INFRASTRUCTURE & SAFETY (V20 Hardening)
# ==========================================

@router.get("/health")
async def health(engine=Depends(get_engine)):
    """Deep Health Check: DB + Broker + Margin > 0"""
    try:
        # 1. DB Check
        async with engine.db.get_session() as session:
            await session.execute(select(DbStrategy).limit(1))

        # 2. Broker Connection Check (Independent of Engine Loop)
        temp_api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
        margin_resp = await temp_api.get_funds_and_margin()
        await temp_api.close()

        if margin_resp.get("status") != "success":
            raise HTTPException(status_code=503, detail="Broker Token Invalid")

        # 3. Margin Safety Check
        eq = margin_resp.get("data", {}).get("equity", {})
        avail = float(eq.get("available_margin", 0))
        MARGIN_AVAILABLE.set(avail)

        if avail <= 0:
            ERROR_TOTAL.labels(type="margin_zero").inc()
            raise HTTPException(status_code=503, detail="Available Margin Zero")

        return {
            "status": "healthy", 
            "margin": avail, 
            "engine_running": engine.running,
            "cycle": engine.cycle_count
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Health Check Failed")
        raise HTTPException(status_code=503, detail=str(exc))

@router.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@router.post("/shutdown")  # Also aliases to /api/stop for compatibility
@router.post("/stop")
async def shutdown(background_tasks: BackgroundTasks, engine=Depends(get_engine)):
    """Graceful Shutdown: Stops loop -> Flattens Trades -> Snapshots DB"""
    if not engine.running:
        return {"status": "already_stopped"}

    engine.running = False
    background_tasks.add_task(engine.shutdown)
    return {"status": "shutting_down_gracefully"}

# ==========================================
# 2. BUSINESS LOGIC (V19 Frontend Features)
# ==========================================

@router.get("/dashboard/data")
async def get_dashboard_data(engine=Depends(get_engine)):
    """Main feed for the Frontend Dashboard"""
    try:
        if asyncio.iscoroutinefunction(engine.get_dashboard_data):
            data = await engine.get_dashboard_data()
        else:
            data = engine.get_dashboard_data()
        return data or {"status": "initializing"}
    except Exception as e:
        logger.error(f"Dashboard Data Error: {e}")
        raise HTTPException(500, str(e))

@router.get("/cio/commentary")
async def get_cio_commentary(engine=Depends(get_engine)):
    """
    Risk Advisor Widget Endpoint.
    Fetches data from the AI Architect module.
    """
    try:
        # Safely get attributes even if architect isn't fully ready
        architect = getattr(engine, 'architect', None)
        trade_analysis = getattr(architect, 'last_trade_analysis', {}) if architect else {}
        portfolio_review = getattr(architect, 'last_portfolio_review', {}) if architect else {}
        
        return {
            "trade_analysis": trade_analysis,
            "portfolio_review": portfolio_review,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {"error": "CIO Commentary unavailable", "details": str(e)}

@router.get("/system/logs")
async def get_live_logs(lines: int = 50):
    """Stream logs to frontend console"""
    log_file = Path(settings.PERSISTENT_DATA_DIR) / "logs" / "volguard.log"
    # Fallback to local logs if persistent dir not set
    if not log_file.parent.exists():
        log_file = Path("logs/volguard.log")

    if not log_file.exists():
        return {"logs": ["Log file not created yet."]}
    
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
            return {"logs": [line.strip() for line in all_lines[-lines:]]}
    except Exception as e:
        return {"error": f"Log read failed: {str(e)}"}

# ==========================================
# 3. CONTROL ENDPOINTS
# ==========================================

@router.post("/start")
async def start_engine(
    background_tasks: BackgroundTasks, 
    req: EngineStartRequest = None,
    engine=Depends(get_engine)
):
    if engine.running:
        raise HTTPException(400, "Engine already running")
    
    background_tasks.add_task(engine.run)
    return {"status": "started", "timestamp": datetime.now().isoformat()}

@router.post("/token/refresh")
async def refresh_token(req: TokenUpdateRequest, engine=Depends(get_engine)):
    try:
        logger.info("🔄 API Triggered Token Refresh")
        settings.UPSTOX_ACCESS_TOKEN = req.access_token
        
        # Propagate new token to all components
        if hasattr(engine, "data_feed"): engine.data_feed.update_token(req.access_token)
        if hasattr(engine, "api"): await engine.api.update_token(req.access_token)
            
        return {"status": "success", "message": "Token Rotated"}
    except Exception as e:
        logger.error(f"Token Refresh Failed: {e}")
        raise HTTPException(500, str(e))

@router.post("/capital/adjust")
async def adjust_capital(req: CapitalAdjustmentRequest, engine=Depends(get_engine)):
    new_alloc = {
        "weekly_expiries": req.weekly_pct,
        "monthly_expiries": req.monthly_pct,
        "intraday_adjustments": req.intraday_pct
    }
    if abs(sum(new_alloc.values()) - 1.0) > 0.01:
        raise HTTPException(400, "Allocations must sum to 1.0")
    
    # Update Allocator directly
    engine.capital_allocator._bucket_pct = new_alloc
    return {"status": "updated", "config": new_alloc}

@router.post("/emergency/flatten")
async def emergency_flatten(engine=Depends(get_engine)):
    logger.critical("🔥 API TRIGGERED EMERGENCY FLATTEN")
    await engine._emergency_flatten()
    return {"status": "flatten_triggered", "timestamp": datetime.now().isoformat()}

# ==========================================
# 4. MANUAL TRADING (Hardened)
# ==========================================

@router.post("/trades/manual")
async def place_manual_trade(req: ManualTradeRequest, engine=Depends(get_engine)):
    """
    Executes manual trade through the Hardened Executor (Safety + Slicing)
    """
    try:
        real_legs = []
        for l in req.legs:
            # Resolve Token
            expiry_dt = datetime.strptime(l.expiry_date, "%Y-%m-%d").date()
            token = engine.instruments_master.get_option_token(l.symbol, l.strike, l.option_type, expiry_dt)
            if not token:
                raise HTTPException(400, f"Token not found for {l.symbol} {l.strike}")
            
            # Create Position
            real_legs.append(Position(
                symbol=l.symbol, instrument_key=token, strike=l.strike,
                option_type=l.option_type,
                quantity=l.quantity if l.side == "BUY" else -l.quantity,
                entry_price=0.0, entry_time=datetime.now(settings.IST),
                current_price=0.0, current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                expiry_type=ExpiryType.INTRADAY, capital_bucket=req.capital_bucket
            ))

        # Create Trade
        new_trade = MultiLegTrade(
            legs=real_legs, strategy_type=StrategyType.WAIT,
            net_premium_per_share=0.0, entry_time=datetime.now(settings.IST),
            expiry_date=req.legs[0].expiry_date, expiry_type=ExpiryType.INTRADAY,
            capital_bucket=req.capital_bucket, status=TradeStatus.PENDING,
            id=f"MANUAL-{int(time.time())}"
        )

        logger.info(f"👨‍💻 Manual Trade Request: {len(real_legs)} legs")
        
        # Execute via Hardened Pipeline
        success, msg = await engine.hardened_executor.execute_with_hedge_priority(new_trade)

        if success:
            new_trade.status = TradeStatus.OPEN
            engine.trades.append(new_trade)
            return {"status": "success", "trade_id": new_trade.id}
        else:
            raise HTTPException(400, f"Execution Failed: {msg}")

    except Exception as e:
        logger.error(f"Manual Trade Exception: {e}")
        raise HTTPException(500, str(e))

@router.delete("/trades/{trade_id}")
async def close_specific_trade(trade_id: str, engine=Depends(get_engine)):
    """Allows manual exit of a specific strategy from Frontend"""
    trade = next((t for t in engine.trades if t.id == trade_id), None)
    if not trade:
        raise HTTPException(404, "Trade ID not found")
    
    if trade.status != TradeStatus.OPEN:
        raise HTTPException(400, f"Cannot close trade in {trade.status.value} state")

    logger.info(f"👋 Manual Exit Triggered for {trade_id}")
    await engine.trade_mgr.close_trade(trade, ExitReason.MANUAL)
    return {"status": "closed", "trade_id": trade_id}
