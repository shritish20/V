from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, status, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict
from datetime import datetime
from pathlib import Path
import logging

from core.engine import VolGuard17Engine
from core.config import settings   # FIXED: removed DASHBOARD_DATA_DIR import
from core.enums import CapitalBucket
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

logger = logging.getLogger("VolGuardAPI")

# ==================== FASTAPI APP ====================
app = FastAPI(
    title="VolGuard 19.0 API",
    description="Institutional-Grade Algorithmic Trading System with Capital Allocation (Endgame Architecture)",
    version="19.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# ==================== CORS ====================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== STATIC DASHBOARD PLACEHOLDER ====================
# VolGuard 19 no longer uses a local dashboard_data directory.
# We still mount an empty static folder to avoid 404s when React frontend loads assets.
static_path = Path("./static_dashboard")
static_path.mkdir(exist_ok=True)

app.mount(
    "/dashboard/static",
    StaticFiles(directory=static_path),
    name="dashboard_static"
)

# ==================== PYDANTIC MODELS ====================

class EngineStartRequest(BaseModel):
    continuous: bool = Field(default=True)
    initialize_dashboard: bool = Field(default=True)


class TokenUpdateRequest(BaseModel):
    access_token: str = Field(..., min_length=10)


class TradeRequest(BaseModel):
    strategy: str
    lots: int = Field(1, ge=1, le=10)
    capital_bucket: str

    @validator("capital_bucket")
    def validate_bucket(cls, v):
        valid = [b.value for b in CapitalBucket]
        if v not in valid:
            raise ValueError(f"Invalid bucket. Must be one of: {valid}")
        return v


class CapitalAdjustmentRequest(BaseModel):
    weekly_pct: float = Field(0.40, ge=0.0, le=1.0)
    monthly_pct: float = Field(0.50, ge=0.0, le=1.0)
    intraday_pct: float = Field(0.10, ge=0.0, le=1.0)

    @validator("weekly_pct", "monthly_pct", "intraday_pct")
    def validate_pct(cls, v, values, **kwargs):
        if "weekly_pct" in values and "monthly_pct" in values and "intraday_pct" in values:
            total = values["weekly_pct"] + values["monthly_pct"] + values["intraday_pct"]
            if abs(total - 1.0) > 0.01:
                raise ValueError(f"Allocation must sum to 100%. Got {total*100:.1f}%")
        return v


class StrategyRecommendationRequest(BaseModel):
    regime: Optional[str] = None
    ivp: Optional[float] = Field(None, ge=0, le=100)
    event_risk: Optional[float] = Field(None, ge=0, le=5)
    spot_price: Optional[float] = Field(None, gt=0)

# ==================== DEPENDENCY ====================

def get_engine(request: Request) -> VolGuard17Engine:
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Engine initializing. Try again shortly."
        )
    return engine

# ==================== ROOT & HEALTH ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse("""
    <html>
    <body style="text-align:center;font-family:sans-serif;padding:50px">
        <h1>🛡 VolGuard 19.0 Active</h1>
        <p><a href="/dashboard">Dashboard</a> | <a href="/api/docs">API Docs</a></p>
    </body>
    </html>
    """)


@app.get("/health")
async def health_check(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        health = engine.get_system_health()
        ok = (
            health["engine"]["running"] is not False and
            health["capital_allocation"] is not None
        )

        return JSONResponse(
            status_code=200 if ok else 503,
            content={
                "status": "healthy" if ok else "degraded",
                "timestamp": datetime.now().isoformat(),
                "version": "19.0.0",
                "engine_running": health["engine"]["running"],
                "active_trades": health["engine"]["active_trades"],
                "analytics_healthy": health["analytics"].get("sabr_calibrated", False)
            }
        )
    except Exception as e:
        raise HTTPException(500, str(e))

# ==================== DASHBOARD ====================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_home():
    return HTMLResponse("<h1>VolGuard Dashboard Loading...</h1><script>window.location='/api/dashboard/data'</script>")


@app.get("/api/dashboard/data")
async def get_dashboard_data(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        data = engine.get_dashboard_data()

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

# ==================== ENGINE CONTROL ====================

@app.get("/api/status")
async def get_status(engine: VolGuard17Engine = Depends(get_engine)):
    return engine.get_status().to_dict()


@app.post("/api/start")
async def start_engine(
    background_tasks: BackgroundTasks,
    req: EngineStartRequest = EngineStartRequest(),
    engine: VolGuard17Engine = Depends(get_engine)
):
    if engine.running:
        raise HTTPException(400, "Engine already running")

    background_tasks.add_task(engine.run)
    return {"status": "starting", "timestamp": datetime.now().isoformat()}


@app.post("/api/stop")
async def stop_engine(engine: VolGuard17Engine = Depends(get_engine)):
    if not engine.running:
        raise HTTPException(400, "Engine not running")
    await engine.shutdown()
    return {"status": "stopping", "timestamp": datetime.now().isoformat()}

# ==================== TOKEN REFRESH ====================

@app.post("/api/token/refresh")
async def refresh_token(req: TokenUpdateRequest, engine: VolGuard17Engine = Depends(get_engine)):
    new_token = req.access_token
    logger.info("🔐 Token Refresh Initiated")

    try:
        settings.UPSTOX_ACCESS_TOKEN = new_token

        if hasattr(engine, "data_feed"):
            engine.data_feed.update_token(new_token)

        if hasattr(engine, "api"):
            engine.api.token = new_token
            engine.api.headers = {
                "Authorization": f"Bearer {new_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Api-Version": "2.0",
            }
            if engine.api.session and not engine.api.session.closed:
                await engine.api.session.close()
            engine.api.session = None

        if hasattr(engine, "greek_validator"):
            await engine.greek_validator.update_token(new_token)

        return {
            "status": "success",
            "message": "Token refreshed",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Token refresh failed: {e}")
        raise HTTPException(500, str(e))

# ==================== CAPITAL ALLOCATION ====================

@app.post("/api/capital/adjust")
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

        status_info = engine.capital_allocator.get_status()
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

        engine.capital_allocator.bucket_config = new_alloc
        settings.CAPITAL_ALLOCATION = new_alloc

        return {
            "status": "updated",
            "new_allocation": new_alloc,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Capital adjustment error: {e}")
        raise HTTPException(500, str(e))

# ==================== EMERGENCY ====================

@app.post("/api/emergency/flatten")
async def emergency_flatten(engine: VolGuard17Engine = Depends(get_engine)):
    await engine._emergency_flatten()
    return {"status": "flatten_triggered"}

# ==================== METRICS ====================

@app.get("/api/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
