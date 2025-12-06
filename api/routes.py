from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, Depends, status
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
import logging
from core.engine import VolGuard17Engine
from core.models import EngineStatus, DashboardData
from core.config import settings, DASHBOARD_DATA_DIR
from core.enums import CapitalBucket, StrategyType
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

logger = logging.getLogger("VolGuard18")

app = FastAPI(
    title="VolGuard 18.0 API",
    description="Intelligent Trading System with Capital Allocation",
    version="18.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

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

ENGINE: Optional[VolGuard17Engine] = None

# ==================== PYDANTIC MODELS ====================

class EngineStartRequest(BaseModel):
    continuous: bool = Field(default=True, description="Run continuously")
    initialize_dashboard: bool = Field(default=True, description="Initialize dashboard on start")

class TradeRequest(BaseModel):
    strategy: str = Field(..., description="Strategy type")
    lots: int = Field(1, ge=1, le=10, description="Number of lots")
    capital_bucket: str = Field(..., description="Capital bucket")

    @validator('capital_bucket')
    def validate_bucket(cls, v):
        if v not in [b.value for b in CapitalBucket]:
            raise ValueError(f"Invalid bucket. Must be one of: {[b.value for b in CapitalBucket]}")
        return v

class CapitalAdjustmentRequest(BaseModel):
    weekly_pct: float = Field(0.4, ge=0.0, le=1.0, description="Weekly allocation %")
    monthly_pct: float = Field(0.5, ge=0.0, le=1.0, description="Monthly allocation %")
    intraday_pct: float = Field(0.1, ge=0.0, le=1.0, description="Intraday allocation %")

    @validator('weekly_pct', 'monthly_pct', 'intraday_pct')
    def validate_percentages(cls, v, values):
        total = values.get('weekly_pct', 0) + values.get('monthly_pct', 0) + values.get('intraday_pct', 0)
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Percentages must sum to 100%, got {total * 100:.1f}%")
        return v

class StrategyRecommendationRequest(BaseModel):
    regime: Optional[str] = Field(None, description="Market regime")
    ivp: Optional[float] = Field(None, ge=0.0, le=100.0, description="IV percentile")
    event_risk: Optional[float] = Field(None, ge=0.0, le=5.0, description="Event risk score")
    spot_price: Optional[float] = Field(None, gt=0.0, description="Spot price")

# ==================== DEPENDENCIES ====================

def get_engine():
    if not ENGINE:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Engine not initialized")
    return ENGINE

# ==================== STARTUP/SHUTDOWN ====================

@app.on_event("startup")
async def startup_event():
    global ENGINE
    try:
        ENGINE = VolGuard17Engine()
        logger.info("✅ VolGuard 18.0 Engine initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize engine: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    global ENGINE
    if ENGINE:
        await ENGINE.shutdown()
        logger.info("✅ VolGuard 18.0 Engine shutdown complete")

# ==================== ROOT & HEALTH ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>VolGuard 18.0</title></head>
    <body>
    <h1>🚀 VolGuard 18.0 – Intelligent Volatility Trading System</h1>
    <p>✅ System is online. Visit <a href="/dashboard">/dashboard</a> for live dashboard.</p>
    </body>
    </html>
    """

@app.get("/health")
async def health_check(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        health_data = engine.get_system_health()
        is_healthy = (
            health_data["engine"]["running"] is not False and
            health_data["analytics"]["dashboard_ready"] and
            health_data["capital_allocation"] is not None
        )
        return {
            "status": "healthy" if is_healthy else "degraded",
            "timestamp": datetime.now().isoformat(),
            "engine_running": health_data["engine"]["running"],
            "circuit_breaker": health_data["engine"]["circuit_breaker"],
            "active_trades": health_data["engine"]["active_trades"],
            "dashboard_ready": health_data["analytics"]["dashboard_ready"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

# ==================== DASHBOARD ENDPOINTS ====================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_home():
    return """
    <!DOCTYPE html>
    <html>
    <head><title>VolGuard 18.0 Dashboard</title></head>
    <body>
    <h1>📊 VolGuard 18.0 Dashboard</h1>
    <p>Real-time trading analytics with smart capital allocation.</p>
    <ul>
    <li><a href="/api/dashboard/data">📈 Dashboard Data (JSON)</a></li>
    <li><a href="/api/health">🩺 Health Check</a></li>
    <li><a href="/api/status">⚙️ Engine Status</a></li>
    </ul>
    </body>
    </html>
    """

@app.get("/api/dashboard/data")
async def get_dashboard_data(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        data = engine.get_dashboard_data()
        if not data:
            raise HTTPException(status_code=404, detail="Dashboard data not available")
        return {**data, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dashboard data: {str(e)}")

# ==================== ENGINE CONTROL ====================

@app.get("/api/status")
async def get_status(engine: VolGuard17Engine = Depends(get_engine)):
    status = engine.get_status()
    return {
        "running": status.running,
        "circuit_breaker": status.circuit_breaker,
        "cycle_count": status.cycle_count,
        "total_trades": status.total_trades,
        "daily_pnl": status.daily_pnl,
        "dashboard_ready": status.dashboard_ready,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/start")
async def start_engine(background_tasks: BackgroundTasks,
                       request: EngineStartRequest = EngineStartRequest(),
                       engine: VolGuard17Engine = Depends(get_engine)):
    if engine.running:
        raise HTTPException(status_code=400, detail="Engine already running")
    try:
        background_tasks.add_task(engine.run)
        return {
            "status": "starting",
            "continuous": request.continuous,
            "initialize_dashboard": request.initialize_dashboard,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start engine: {str(e)}")

@app.post("/api/stop")
async def stop_engine(engine: VolGuard17Engine = Depends(get_engine)):
    if not engine.running:
        raise HTTPException(status_code=400, detail="Engine not running")
    try:
        await engine.shutdown()
        return {"status": "stopping", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop engine: {str(e)}")

# ==================== CAPITAL ALLOCATION ====================

@app.get("/api/capital/allocation")
async def get_capital_allocation(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        capital_status = engine.capital_allocator.get_allocation_status()
        return {
            "allocation": capital_status,
            "total_capital": engine.capital_allocator.total_capital,
            "total_used": engine.capital_allocator.get_total_used_capital(),
            "total_available": engine.capital_allocator.get_total_available_capital(),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get capital allocation: {str(e)}")

@app.post("/api/capital/adjust")
async def adjust_capital_allocation(request: CapitalAdjustmentRequest,
                                    engine: VolGuard17Engine = Depends(get_engine)):
    try:
        new_allocation = {
            "weekly_expiries": request.weekly_pct,
            "monthly_expiries": request.monthly_pct,
            "intraday_adjustments": request.intraday_pct
        }
        success = engine.capital_allocator.adjust_allocation(new_allocation)
        if success:
            return {"status": "success", "new_allocation": new_allocation, "timestamp": datetime.now().isoformat()}
        else:
            raise HTTPException(status_code=400, detail="Cannot adjust allocation - too much capital in use")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to adjust capital allocation: {str(e)}")

# ==================== TRADES & PORTFOLIO ====================

@app.get("/api/trades/active")
async def get_active_trades(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        active_trades = [
            {
                "id": trade.id,
                "strategy": trade.strategy_type.value,
                "lots": trade.lots,
                "pnl": trade.total_unrealized_pnl(),
                "vega": trade.trade_vega,
                "delta": trade.trade_delta,
                "theta": trade.trade_theta,
                "entry_time": trade.entry_time.isoformat() if trade.entry_time else None,
                "expiry_type": trade.expiry_type.value,
                "capital_bucket": trade.capital_bucket.value,
                "status": trade.status.value
            }
            for trade in engine.trades if trade.status.value in ["OPEN", "EXTERNAL"]
        ]
        return {
            "active_trades": active_trades,
            "count": len(active_trades),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get active trades: {str(e)}")

# ==================== ANALYTICS ====================

@app.get("/api/analytics/volatility")
async def get_volatility_analytics(engine: VolGuard17Engine = Depends(get_engine)):
    if not engine.last_metrics:
        raise HTTPException(status_code=404, detail="No analytics data available")
    m = engine.last_metrics
    return {
        "spot_price": m.spot_price,
        "vix": m.vix,
        "ivp": m.ivp,
        "realized_vol": m.realized_vol_7d,
        "garch_vol": m.garch_vol_7d,
        "iv_rv_spread": m.iv_rv_spread,
        "regime": m.regime.value,
        "event_risk": m.event_risk_score,
        "sabr_parameters": {
            "alpha": m.sabr_alpha,
            "beta": m.sabr_beta,
            "rho": m.sabr_rho,
            "nu": m.sabr_nu
        },
        "timestamp": m.timestamp.isoformat()
    }

# ==================== EMERGENCY CONTROLS ====================

@app.post("/api/emergency/flatten")
async def emergency_flatten(engine: VolGuard17Engine = Depends(get_engine)):
    try:
        await engine._emergency_flatten()
        return {"status": "emergency_flatten_initiated", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Emergency flatten failed: {str(e)}")

# ==================== METRICS ====================

@app.get("/api/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# ==================== UTILITIES ====================

@app.get("/api/version")
async def get_version():
    return {
        "version": "18.0.0",
        "name": "VolGuard 18.0",
        "description": "Intelligent Trading System with Capital Allocation",
        "timestamp": datetime.now().isoformat()
    }
