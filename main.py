#!/usr/bin/env python3
"""
VolGuard 20.0 – Main Entry Point
- Initializes the Hardened VolGuard20Engine
- Mounts the Unified API Router
- Manages Lifecycle (Startup/Shutdown) via FastAPI
"""
import uvicorn
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

# Import Hardened Components
from core.engine import VolGuard20Engine
from core.config import settings
from utils.logger import setup_logger
from api.routes import router as api_router

# Setup Logging
logger = setup_logger("Main")

# Global Engine Instance
engine_instance = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of the Engine (Startup/Shutdown).
    This replaces your old signal handlers and startup_sequence.
    """
    global engine_instance
    logger.info("🚀 VolGuard 20.0 System Initializing...")
    
    # 1. Initialize Engine
    # This automatically connects to DB, Broker, and sets up the Allocator
    engine_instance = VolGuard20Engine()
    
    # 2. Inject into App State so API endpoints can access it
    app.state.engine = engine_instance
    
    # 3. Boot Engine Components
    try:
        await engine_instance.initialize()
        
        # Optional: Auto-start the loop if configured, otherwise wait for /api/start
        if settings.ENV == "production":
            logger.info("⚡ Auto-starting Engine Loop...")
            import asyncio
            asyncio.create_task(engine_instance.run())
            
    except Exception as e:
        logger.critical(f"🔥 Startup Failed: {e}")
        raise e
    
    yield
    
    # 4. Cleanup on Shutdown
    logger.info("🛑 System Shutdown Initiated...")
    if engine_instance and engine_instance.running:
        await engine_instance.shutdown()
    logger.info("✅ System Shutdown Complete")

# ==========================================
# FASTAPI APP DEFINITION
# ==========================================
app = FastAPI(
    title="VolGuard 20.0 (Hardened)",
    description="Institutional-Grade Algorithmic Trading System",
    version="20.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static Files (Dashboard)
static_path = Path("./static_dashboard")
static_path.mkdir(exist_ok=True)
app.mount("/dashboard/static", StaticFiles(directory=static_path), name="static")

# Include the Unified Router
app.include_router(api_router)

# Root Redirect
@app.get("/")
async def root():
    return {"message": "VolGuard 20.0 Active", "docs": "/docs"}

if __name__ == "__main__":
    logger.info(f"🔥 Starting VolGuard Server on Port {settings.PORT} [ENV: {settings.ENV}]")
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=settings.PORT, 
        log_level="info",
        reload=False # False for production stability
    )
