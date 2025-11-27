from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Optional
import asyncio
from core.engine import VolGuardHybridUltimate
from core.models import EngineStatus, AdvancedMetrics, PortfolioMetrics
from core.enums import ExitReason, TradeStatus
from .dependencies import get_engine, set_engine

app = FastAPI(
    title="VolGuard Hybrid Ultimate API",
    description="Production-grade options trading engine with comprehensive risk management",
    version="1.0.0"
)

# --- Pydantic Models ---

class EngineStartRequest(BaseModel):
    continuous: bool = True

class TradeCloseRequest(BaseModel):
    trade_id: int
    reason: ExitReason

class StrategyExecuteRequest(BaseModel):
    strategy_name: str
    legs_spec: List[Dict]
    lots: int

class StatusResponse(BaseModel):
    status: str
    engine_status: Optional[EngineStatus] = None
    message: Optional[str] = None

class MetricsResponse(BaseModel):
    market_metrics: Optional[AdvancedMetrics] = None
    portfolio_metrics: Optional[PortfolioMetrics] = None

# --- Application Lifecycle ---

@app.on_event("startup")
async def startup_event():
    """Initialize engine on startup"""
    engine = VolGuardHybridUltimate()
    set_engine(engine)
    
@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown engine gracefully"""
    engine = get_engine()
    if engine:
        await engine.shutdown()

# --- Routes ---

@app.get("/")
async def root():
    return {
        "message": "VolGuard Hybrid Ultimate API",
        "status": "running",
        "version": "1.0.0"
    }

@app.post("/engine/start", response_model=StatusResponse)
async def start_engine(
    request: EngineStartRequest,
    background_tasks: BackgroundTasks,
    engine: VolGuardHybridUltimate = Depends(get_engine)
):
    """Start the trading engine"""
    if engine.running and engine.cycle_count > 0:
        raise HTTPException(status_code=400, detail="Engine is already running")
    
    background_tasks.add_task(engine.run, request.continuous)
    return StatusResponse(
        status="success", 
        message="Engine started successfully"
    )

@app.post("/engine/stop", response_model=StatusResponse)
async def stop_engine(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Stop the trading engine"""
    engine.running = False
    return StatusResponse(
        status="success", 
        message="Engine stop signal sent"
    )

@app.get("/engine/status", response_model=StatusResponse)
async def get_engine_status(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Get current engine status"""
    return StatusResponse(
        status="success", 
        engine_status=engine.get_status()
    )

@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Get current market and portfolio metrics"""
    return MetricsResponse(
        market_metrics=engine.last_metrics,
        portfolio_metrics=engine.risk_mgr.portfolio_metrics
    )

@app.get("/trades")
async def get_trades(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Get all trades"""
    return {
        "status": "success",
        "trades": [
            {
                "id": trade.id,
                "strategy_type": trade.strategy_type,
                "status": trade.status.value,
                "lots": trade.lots,
                "pnl": trade.total_unrealized_pnl(),
                "entry_time": trade.entry_time
            } for trade in engine.trades
        ]
    }

@app.post("/trades/close")
async def close_trade(
    request: TradeCloseRequest, 
    engine: VolGuardHybridUltimate = Depends(get_engine)
):
    """Close a specific trade"""
    trade = next((t for t in engine.trades if t.id == request.trade_id), None)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    
    await engine.trade_mgr.close_trade(trade, request.reason)
    return {"status": "success", "message": f"Trade {request.trade_id} closed"}

@app.post("/trades/execute")
async def execute_strategy(
    request: StrategyExecuteRequest, 
    engine: VolGuardHybridUltimate = Depends(get_engine)
):
    """Execute a specific strategy (manual override)"""
    # NOTE: Spot price lookup for manual execution
    current_spot = engine.rt_quotes.get(engine.MARKET_KEY_INDEX, engine.last_metrics.spot_price if engine.last_metrics else 0.0)
    
    trade = await engine.trade_mgr.execute_strategy(
        request.strategy_name,
        request.legs_spec,
        request.lots,
        current_spot
    )
    if trade:
        engine.trades.append(trade)
        return {"status": "success", "trade_id": trade.id}
    else:
        raise HTTPException(status_code=400, detail="Strategy execution failed")

@app.post("/emergency/flatten")
async def emergency_flatten(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Emergency flatten all positions"""
    await engine._emergency_flatten()
    return {"status": "success", "message": "Emergency flatten executed"}

@app.get("/health")
async def health_check(engine: VolGuardHybridUltimate = Depends(get_engine)):
    """Health check endpoint"""
    return {
        "status": "healthy",
        "engine_running": engine.running,
        "market_open": engine._is_market_open(),
        "cycle_count": engine.cycle_count,
        "active_trades": len([t for t in engine.trades if t.status in [TradeStatus.OPEN, TradeStatus.EXTERNAL]])
    }
