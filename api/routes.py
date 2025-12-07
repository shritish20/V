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
from core.config import DASHBOARD_DATA_DIR, settings
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

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for dashboard
dashboard_path = Path(DASHBOARD_DATA_DIR)
dashboard_path.mkdir(exist_ok=True)
app.mount("/dashboard/static", StaticFiles(directory=dashboard_path), name="dashboard_static")

# ==================== PYDANTIC MODELS ====================

class EngineStartRequest(BaseModel):
    """Engine start request model"""
    continuous: bool = Field(default=True, description="Run continuously")
    initialize_dashboard: bool = Field(default=True, description="Initialize dashboard on start")

class TokenUpdateRequest(BaseModel):
    """Token update request model"""
    access_token: str = Field(..., min_length=10, description="New Upstox Access Token")

class TradeRequest(BaseModel):
    """Trade request model"""
    strategy: str = Field(..., description="Strategy type")
    lots: int = Field(1, ge=1, le=10, description="Number of lots")
    capital_bucket: str = Field(..., description="Capital bucket")

    @validator('capital_bucket')
    def validate_bucket(cls, v):
        valid_buckets = [b.value for b in CapitalBucket]
        if v not in valid_buckets:
            raise ValueError(f"Invalid bucket. Must be one of: {valid_buckets}")
        return v

class CapitalAdjustmentRequest(BaseModel):
    """Capital adjustment request model"""
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

class StrategyRecommendationRequest(BaseModel):
    regime: Optional[str] = None
    ivp: Optional[float] = Field(None, ge=0.0, le=100.0)
    event_risk: Optional[float] = Field(None, ge=0.0, le=5.0)
    spot_price: Optional[float] = Field(None, gt=0.0)

# ==================== DEPENDENCIES ====================

def get_engine(request: Request) -> VolGuard17Engine:
    """
    Retrieves the single engine instance initialized in main.py.
    """
    engine = getattr(request.app.state, "engine", None)
    if not engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Engine is still initializing. Please wait."
        )
    return engine

# ==================== ROOT & HEALTH ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head><title>VolGuard 19.0</title></head>
    <body style="font-family: sans-serif; text-align: center; padding: 50px;">
        <h1>🛡 VolGuard 19.0 Active</h1>
        <p>Endgame Production Architecture</p>
        <p><a href="/dashboard">Go to Dashboard</a> | <a href="/api/docs">API Docs</a></p>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/health")
async def health_check(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        health_data = engine.get_system_health()
        
        # Check critical components
        is_healthy = (
            health_data["engine"]["running"] is not False and
            health_data["capital_allocation"] is not None
        )
        
        status_code = status.HTTP_200_OK if is_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
        
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "healthy" if is_healthy else "degraded",
                "timestamp": datetime.now().isoformat(),
                "version": "19.0.0",
                "engine_running": health_data["engine"]["running"],
                "active_trades": health_data["engine"]["active_trades"],
                "analytics_healthy": health_data["analytics"].get("sabr_calibrated", False),
            }
        )
    except Exception as e:
        raise HTTPException(500, f"Health check failed: {str(e)}")

# ==================== DASHBOARD ENDPOINTS ====================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_home():
    """Interactive dashboard placeholder"""
    return HTMLResponse(content="<h1>VolGuard Dashboard Loading...</h1><script>window.location='/api/dashboard/data'</script>")

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
    status = engine.get_status()
    return status.to_dict()

@app.post("/api/start")
async def start_engine(
    background_tasks: BackgroundTasks,
    request: EngineStartRequest = EngineStartRequest(),
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

# ==================== MANAGEMENT ENDPOINTS ====================

@app.post("/api/token/refresh")
async def refresh_token(
    request: TokenUpdateRequest,
    engine: VolGuard17Engine = Depends(get_engine)
):
    """
    FIXED: Updates the Upstox Access Token across all modules at runtime.
    Now uses locks to prevent race conditions during token swap.
    Critical for 24/7 operation.
    """
    new_token = request.access_token
    logger.info("🔐 Received Token Refresh Request")

    try:
        # 1. Update Global Settings
        settings.UPSTOX_ACCESS_TOKEN = new_token

        # 2. Update Live Data Feed (Triggers reconnect)
        if hasattr(engine, "data_feed"):
            engine.data_feed.update_token(new_token)
        
        # 3. Update Execution API
        if hasattr(engine, "api"):
            engine.api.token = new_token
            # Rebuild headers dictionary to ensure consistency
            engine.api.headers = {
                "Authorization": f"Bearer {new_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Api-Version": "2.0",
            }
            
            # Force session close to ensure new session gets new headers
            if engine.api.session and not engine.api.session.closed:
                await engine.api.session.close()
            engine.api.session = None
                
        # 4. CRITICAL FIX: Update Greek Validator with Lock
        if hasattr(engine, "greek_validator"):
            await engine.greek_validator.update_token(new_token)

        logger.info("✅ Token updated successfully across all modules.")
        return {
            "status": "success", 
            "message": "Token refreshed. System is using new credentials.",
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.critical(f"❌ Token refresh failed: {e}")
        raise HTTPException(500, f"Token refresh failed: {str(e)}")

@app.post("/api/capital/adjust")
async def adjust_capital(
    request: CapitalAdjustmentRequest,
    dry_run: bool = Query(False),
    engine: VolGuard17Engine = Depends(get_engine)
):
    """
    Safe Capital Rebalancing with Validation.
    Prevents lowering limits below currently used capital.
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
                raise HTTPException(
                    status_code=400, 
                    detail={"error": msg, "violations": violations}
                )
            return {"status": "blocked", "violations": violations}

        if dry_run:
            return {"status": "safe_to_apply", "allocation": new_allocation}

        # Apply changes if safe
        engine.capital_allocator.bucket_config = new_allocation
        settings.CAPITAL_ALLOCATION = new_allocation
        
        logger.info(f"💰 Capital Allocation Updated: {new_allocation}")
        return {
            "status": "success",
            "message": "Capital allocation updated",
            "new_allocation": new_allocation,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Capital adjustment failed: {e}")
        raise HTTPException(500, str(e))

# ==================== EMERGENCY CONTROLS ====================

@app.post("/api/emergency/flatten")
async def emergency_flatten(engine: VolGuard17Engine = Depends(get_engine)):
    await engine._emergency_flatten()
    return {"status": "emergency_flatten_initiated"}

# ==================== METRICS ====================

@app.get("/api/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
