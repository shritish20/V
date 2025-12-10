from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, status, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from pathlib import Path
import logging
import time

# Core Imports
from core.engine import VolGuard17Engine
from core.config import settings
from core.enums import CapitalBucket, StrategyType, TradeStatus, ExpiryType, ExitReason
from core.models import ManualTradeRequest, Position, GreeksSnapshot, MultiLegTrade
from api.security import get_admin_key
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

logger = logging.getLogger("VolGuardAPI")

# ==================== FASTAPI APP ====================
app = FastAPI(
    title="VolGuard 19.0 API",
    description="Institutional-Grade Algorithmic Trading System (Terminal Edition)",
    version="19.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# ==================== CORS ====================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== STATIC ASSETS ====================
static_path = Path("./static_dashboard")
static_path.mkdir(exist_ok=True)
app.mount("/dashboard/static", StaticFiles(directory=static_path), name="dashboard_static")

# ==================== DEPENDENCY INJECTION ====================
def get_engine(request: Request) -> VolGuard17Engine:
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Engine initializing. Try again shortly."
        )
    return engine

# ==================== REQUEST MODELS ====================
class EngineStartRequest(BaseModel):
    continuous: bool = Field(default=True)
    initialize_dashboard: bool = Field(default=True)

class TokenUpdateRequest(BaseModel):
    access_token: str = Field(..., min_length=10)

class CapitalAdjustmentRequest(BaseModel):
    weekly_pct: float = Field(0.40, ge=0.0, le=1.0)
    monthly_pct: float = Field(0.50, ge=0.0, le=1.0)
    intraday_pct: float = Field(0.10, ge=0.0, le=1.0)

# ==================== PUBLIC ENDPOINTS (READ-ONLY) ====================
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("""
    <html>
        <body style="text-align:center; font-family: sans-serif; padding:50px; background-color: #111; color:#eee;">
            <h1>🚀 VolGuard 19.0 Active</h1>
            <p>Status: <span style="color:#0f0">OPERATIONAL</span></p>
            <p><a href="/api/docs" style="color:#4af">API Documentation</a></p>
        </body>
    </html>
    """)

@app.get("/health")
async def health_check(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        health = engine.get_system_health()
        ok = health["engine"]["running"] is not False
        
        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "status": "healthy" if ok else "degraded",
                "timestamp": datetime.now().isoformat(),
                "version": "19.0.0",
                "engine_running": health["engine"]["running"],
                "active_trades": health["engine"]["active_trades"]
            }
        )
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/dashboard/data")
async def get_dashboard_data(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        # CRITICAL FIX: Added 'await' here because engine.get_dashboard_data is now async
        data = await engine.get_dashboard_data()
        
        if not data:
            return {"status": "initializing", "timestamp": datetime.now().isoformat()}
        
        status_info = engine.get_status()
        
        return {
            **data,
            "engine_status": {
                "running": status_info.running,
                "circuit_breaker": status_info.circuit_breaker,
                "cycle_count": status_info.cycle_count,
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        raise HTTPException(500, str(e))

@app.get("/api/status")
async def get_status(engine: VolGuard17Engine = Depends(get_engine)):
    return engine.get_status().to_dict()

@app.get("/api/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ==================== PROTECTED ENDPOINTS (ADMIN KEY REQUIRED) ====================
@app.post("/api/start", dependencies=[Depends(get_admin_key)])
async def start_engine(
    background_tasks: BackgroundTasks,
    req: EngineStartRequest = EngineStartRequest(),
    engine: VolGuard17Engine = Depends(get_engine)
):
    if engine.running:
        raise HTTPException(400, "Engine already running")
    
    background_tasks.add_task(engine.run)
    return {"status": "starting", "timestamp": datetime.now().isoformat()}

@app.post("/api/stop", dependencies=[Depends(get_admin_key)])
async def stop_engine(engine: VolGuard17Engine = Depends(get_engine)):
    if not engine.running:
        raise HTTPException(400, "Engine not running")
    
    await engine.shutdown()
    return {"status": "stopping", "timestamp": datetime.now().isoformat()}

@app.post("/api/token/refresh", dependencies=[Depends(get_admin_key)])
async def refresh_token(req: TokenUpdateRequest, engine: VolGuard17Engine = Depends(get_engine)):
    new_token = req.access_token
    logger.info("🔐 Token Refresh Initiated via API")
    try:
        settings.UPSTOX_ACCESS_TOKEN = new_token
        
        if hasattr(engine, "data_feed"):
            engine.data_feed.update_token(new_token)
        if hasattr(engine, "api"):
            await engine.api.update_token(new_token)
        if hasattr(engine, "greek_validator"):
            await engine.greek_validator.update_token(new_token)
            
        return {"status": "success", "message": "Token refreshed", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/capital/adjust", dependencies=[Depends(get_admin_key)])
async def adjust_capital(
    req: CapitalAdjustmentRequest,
    dry_run: bool = Query(False),
    engine: VolGuard17Engine = Depends(get_engine)
):
    try:
        new_alloc = {
            "weekly_expiries": req.weekly_pct,
            "monthly_expiries": req.monthly_pct,
            "intraday_adjustments": req.intraday_pct
        }
        
        total = sum(new_alloc.values())
        if abs(total - 1.0) > 0.01:
            raise HTTPException(400, f"Allocation must sum to 100%. Got {total*100:.1f}%")

        # Async status check
        status_info = await engine.capital_allocator.get_status()
        violations = []

        for bucket, pct in new_alloc.items():
            new_limit = settings.ACCOUNT_SIZE * pct
            used = status_info["used"].get(bucket, 0)
            
            if used > new_limit:
                violations.append({
                    "bucket": bucket,
                    "used": used,
                    "new_limit": new_limit,
                    "shortfall": used - new_limit
                })

        if violations:
            msg = "New limits below current usage. Close positions first."
            if not dry_run:
                raise HTTPException(400, {"error": msg, "violations": violations})
            return {"status": "blocked", "violations": violations}

        if dry_run:
            return {"status": "safe", "allocation": new_alloc}

        # Apply changes
        engine.capital_allocator.bucket_config = new_alloc
        settings.CAPITAL_ALLOCATION = new_alloc
        
        return {
            "status": "updated",
            "new_allocation": new_alloc,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Capital adjustment error: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/emergency/flatten", dependencies=[Depends(get_admin_key)])
async def emergency_flatten(engine: VolGuard17Engine = Depends(get_engine)):
    logger.critical("🚨 API TRIGGERED EMERGENCY FLATTEN")
    await engine._emergency_flatten()
    return {"status": "flatten_triggered", "timestamp": datetime.now().isoformat()}

# ==================== TERMINAL / MANUAL TRADING ====================
@app.post("/api/trades/manual", dependencies=[Depends(get_admin_key)])
async def place_manual_trade(
    req: ManualTradeRequest,
    engine: VolGuard17Engine = Depends(get_engine)
):
    try:
        real_legs = []
        for l in req.legs:
            expiry_dt = datetime.strptime(l.expiry_date, "%Y-%m-%d").date()
            token = engine.instruments_master.get_option_token(
                l.symbol, l.strike, l.option_type, expiry_dt
            )
            
            if not token:
                raise HTTPException(400, f"Instrument Not Found: {l.symbol} {l.strike} {l.option_type}")

            real_legs.append(Position(
                symbol=l.symbol,
                instrument_key=token,
                strike=l.strike,
                option_type=l.option_type,
                quantity=l.quantity if l.side == "BUY" else -l.quantity,
                entry_price=0.0,
                entry_time=datetime.now(settings.IST),
                current_price=0.0,
                current_greeks=GreeksSnapshot(timestamp=datetime.now(settings.IST)),
                capital_bucket=req.capital_bucket,
                expiry_type=ExpiryType.INTRADAY
            ))

        new_trade = MultiLegTrade(
            legs=real_legs,
            strategy_type=StrategyType.WAIT,
            net_premium_per_share=0.0,
            entry_time=datetime.now(settings.IST),
            expiry_date=req.legs[0].expiry_date,
            expiry_type=ExpiryType.INTRADAY,
            capital_bucket=req.capital_bucket,
            status=TradeStatus.PENDING,
            id=f"MANUAL-{int(time.time())}"
        )

        logger.info(f"👨‍💻 Manual Trade Request Received: {len(real_legs)} legs")
        success = await engine.trade_mgr.execute_strategy(new_trade)
        
        if success:
            engine.trades.append(new_trade)
            return {"status": "success", "trade_id": new_trade.id, "message": "Trade Executed"}
        else:
            raise HTTPException(400, "Execution Failed: Check Logs for Margin/Risk reasons")

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Manual Trade Error: {e}")
        raise HTTPException(500, str(e))

@app.delete("/api/trades/{trade_id}", dependencies=[Depends(get_admin_key)])
async def close_specific_trade(
    trade_id: str,
    engine: VolGuard17Engine = Depends(get_engine)
):
    trade = next((t for t in engine.trades if t.id == trade_id), None)
    if not trade:
        raise HTTPException(404, "Trade ID not found")
    
    if trade.status != TradeStatus.OPEN:
        raise HTTPException(400, f"Cannot close trade in {trade.status.value} state")

    logger.info(f"🔪 Manual Exit Triggered for {trade_id}")
    await engine.trade_mgr.close_trade(trade, ExitReason.MANUAL)
    
    return {"status": "closed", "trade_id": trade_id}

@app.get("/api/system/logs", dependencies=[Depends(get_admin_key)])
async def get_live_logs(lines: int = 50):
    log_file = Path(settings.PERSISTENT_DATA_DIR) / "logs" / "volguard.log"
    if not log_file.exists():
        return {"logs": ["Log file not created yet."]}
    
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
            return {"logs": [line.strip() for line in all_lines[-lines:]]}
    except Exception as e:
        return {"error": str(e)}
