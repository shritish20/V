from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
from core.engine import VolGuard14Engine
from core.models import EngineStatus
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import logging

logger = logging.getLogger("VolGuard14")

app = FastAPI(
    title="VolGuard 14.00 API",
    description="Ironclad Trading System - Production Grade",
    version="14.0.0"
)

# Global engine instance
ENGINE: Optional[VolGuard14Engine] = None

class EngineStartRequest(BaseModel):
    continuous: bool = True

class TradeRequest(BaseModel):
    strategy: str
    lots: int

@app.on_event("startup")
async def startup_event():
    """Initialize engine on startup"""
    global ENGINE
    try:
        ENGINE = VolGuard14Engine()
        logger.info("VolGuard 14.00 Engine initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize engine: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown engine gracefully"""
    global ENGINE
    if ENGINE:
        await ENGINE.shutdown()
        logger.info("VolGuard 14.00 Engine shutdown complete")

@app.get("/")
async def root():
    """Root endpoint with system info"""
    return {
        "message": "VolGuard 14.00 - Ironclad Trading System",
        "status": "operational",
        "version": "14.0.0"
    }

@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        health_data = ENGINE.get_system_health()
        return {
            "status": "healthy",
            "engine_running": health_data["engine"]["running"],
            "circuit_breaker": health_data["engine"]["circuit_breaker"],
            "active_trades": health_data["engine"]["active_trades"],
            "analytics_healthy": health_data["analytics"]["sabr_calibrated"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/status")
async def get_status():
    """Get detailed engine status"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    status = ENGINE.get_status()
    return {
        "running": status.running,
        "circuit_breaker": status.circuit_breaker,
        "cycle_count": status.cycle_count,
        "total_trades": status.total_trades,
        "daily_pnl": status.daily_pnl,
        "max_equity": status.max_equity,
        "last_metrics_timestamp": status.last_metrics.timestamp.isoformat() if status.last_metrics else None
    }

@app.post("/start")
async def start_engine(background_tasks: BackgroundTasks, request: EngineStartRequest = EngineStartRequest()):
    """Start the trading engine"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    if ENGINE.running:
        raise HTTPException(status_code=400, detail="Engine already running")
    
    try:
        background_tasks.add_task(ENGINE.run, request.continuous)
        return {"status": "starting", "continuous": request.continuous}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start engine: {str(e)}")

@app.post("/stop")
async def stop_engine():
    """Stop the trading engine"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    if not ENGINE.running:
        raise HTTPException(status_code=400, detail="Engine not running")
    
    try:
        await ENGINE.shutdown()
        return {"status": "stopping"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop engine: {str(e)}")

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/risk")
async def get_risk_report():
    """Get comprehensive risk report"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        risk_report = ENGINE.risk_mgr.get_risk_report()
        return risk_report
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get risk report: {str(e)}")

@app.get("/analytics/volatility")
async def get_volatility_metrics():
    """Get current volatility analytics"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    if not ENGINE.last_metrics:
        raise HTTPException(status_code=404, detail="No analytics data available")
    
    return {
        "spot_price": ENGINE.last_metrics.spot_price,
        "vix": ENGINE.last_metrics.vix,
        "ivp": ENGINE.last_metrics.ivp,
        "realized_vol": ENGINE.last_metrics.realized_vol_7d,
        "garch_vol": ENGINE.last_metrics.garch_vol_7d,
        "regime": ENGINE.last_metrics.regime.value,
        "event_risk": ENGINE.last_metrics.event_risk_score
    }

@app.get("/trades/active")
async def get_active_trades():
    """Get all active trades"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        active_trades = []
        for trade in ENGINE.trades:
            if trade.status.value in ["OPEN", "EXTERNAL"]:
                active_trades.append({
                    "id": trade.id,
                    "strategy": trade.strategy_type,
                    "lots": trade.lots,
                    "pnl": trade.total_unrealized_pnl(),
                    "vega": trade.trade_vega,
                    "delta": trade.trade_delta,
                    "entry_time": trade.entry_time.isoformat()
                })
        return {"active_trades": active_trades}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get active trades: {str(e)}")

@app.post("/emergency/flatten")
async def emergency_flatten():
    """Emergency flatten all positions"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        await ENGINE._emergency_flatten()
        return {"status": "emergency_flatten_initiated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Emergency flatten failed: {str(e)}")

@app.get("/system/health/detailed")
async def get_detailed_health():
    """Get detailed system health information"""
    if not ENGINE:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    
    try:
        health_data = ENGINE.get_system_health()
        return health_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get detailed health: {str(e)}")
