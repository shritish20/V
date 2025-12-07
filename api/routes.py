from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, status, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
from pathlib import Path
import logging

from core.engine import VolGuard17Engine
from core.config import DASHBOARD_DATA_DIR, settings
from core.enums import CapitalBucket
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

logger = logging.getLogger("VolGuardAPI")

app = FastAPI(title="VolGuard 19.0 API", version="19.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

dashboard_path = Path(DASHBOARD_DATA_DIR)
dashboard_path.mkdir(exist_ok=True)
app.mount("/dashboard/static", StaticFiles(directory=dashboard_path), name="dashboard_static")

# --- MODELS ---
class EngineStartRequest(BaseModel):
    continuous: bool = Field(default=True)
    initialize_dashboard: bool = Field(default=True)

class TokenUpdateRequest(BaseModel):
    access_token: str = Field(..., min_length=10)

class CapitalAdjustmentRequest(BaseModel):
    weekly_pct: float = Field(0.40, ge=0.0, le=1.0)
    monthly_pct: float = Field(0.50, ge=0.0, le=1.0)
    intraday_pct: float = Field(0.10, ge=0.0, le=1.0)

    @validator('weekly_pct', 'monthly_pct', 'intraday_pct')
    def validate_percentages(cls, v, values, **kwargs):
        if 'weekly_pct' in values and 'monthly_pct' in values and 'intraday_pct' in values:
            total = values['weekly_pct'] + values['monthly_pct'] + values['intraday_pct']
            if abs(total - 1.0) > 0.01:
                raise ValueError(f"Percentages must sum to 100%, got {total*100:.1f}%")
        return v

# --- DEPENDENCIES ---
def get_engine(request: Request) -> VolGuard17Engine:
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Engine initializing...")
    return engine

# --- ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("<h1>VolGuard 19.0 Active</h1>")

@app.get("/health")
async def health_check(engine: VolGuard17Engine = Depends(get_engine)):
    return JSONResponse(content=engine.get_system_health())

@app.get("/api/dashboard/data")
async def get_dashboard_data(engine: VolGuard17Engine = Depends(get_engine)):
    data = engine.get_dashboard_data()
    return data if data else {"status": "init"}

@app.get("/api/status")
async def get_status(engine: VolGuard17Engine = Depends(get_engine)):
    return engine.get_status().to_dict()

@app.post("/api/start")
async def start_engine(bt: BackgroundTasks, engine: VolGuard17Engine = Depends(get_engine)):
    if engine.running: raise HTTPException(400, "Already running")
    bt.add_task(engine.run)
    return {"status": "starting"}

@app.post("/api/stop")
async def stop_engine(engine: VolGuard17Engine = Depends(get_engine)):
    await engine.shutdown()
    return {"status": "stopping"}

@app.post("/api/token/refresh")
async def refresh_token(req: TokenUpdateRequest, engine: VolGuard17Engine = Depends(get_engine)):
    try:
        settings.UPSTOX_ACCESS_TOKEN = req.access_token
        if hasattr(engine, "data_feed"): engine.data_feed.update_token(req.access_token)
        if hasattr(engine, "api"): 
            engine.api.token = req.access_token
            engine.api.headers["Authorization"] = f"Bearer {req.access_token}"
            if engine.api.session: await engine.api.session.close()
            engine.api.session = None
        if hasattr(engine, "greek_validator"): engine.greek_validator.token = req.access_token
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/capital/adjust")
async def adjust_capital(
    request: CapitalAdjustmentRequest,
    dry_run: bool = Query(False),
    engine: VolGuard17Engine = Depends(get_engine)
):
    """
    Safe Capital Rebalancing with Validation.
    """
    try:
        new_allocation = {
            "weekly_expiries": request.weekly_pct,
            "monthly_expiries": request.monthly_pct,
            "intraday_adjustments": request.intraday_pct
        }
        
        # VALIDATION: Check against current usage
        current_status = engine.capital_allocator.get_status()
        violations = []
        
        for bucket, pct in new_allocation.items():
            new_limit = settings.ACCOUNT_SIZE * pct
            current_used = current_status["used"].get(bucket, 0)
            
            if current_used > new_limit:
                violations.append({
                    "bucket": bucket,
                    "used": current_used,
                    "new_limit": new_limit,
                    "shortfall": current_used - new_limit
                })
        
        if violations:
            msg = "New limits are below current usage! Close positions first."
            if not dry_run:
                raise HTTPException(400, detail={"error": msg, "violations": violations})
            return {"status": "blocked", "violations": violations}

        if dry_run:
            return {"status": "safe_to_apply", "allocation": new_allocation}

        # Apply
        engine.capital_allocator.bucket_config = new_allocation
        settings.CAPITAL_ALLOCATION = new_allocation
        
        return {"status": "success", "new_allocation": new_allocation}

    except HTTPException: raise
    except Exception as e:
        logger.error(f"Capital adjustment error: {e}")
        raise HTTPException(500, str(e))

@app.post("/api/emergency/flatten")
async def emergency_flatten(engine: VolGuard17Engine = Depends(get_engine)):
    await engine._emergency_flatten()
    return {"status": "flatten_initiated"}

@app.get("/api/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
