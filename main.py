#!/usr/bin/env python3
"""
VolGuard 20.0 – Web API Gateway (Pure Quant Edition)
- SERVES: REST API + Static Dashboard
- SINGLETON DB: Connects via shared pool.
- NO AI: Purely serves market data and control endpoints.
"""
import uvicorn
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

# Core Imports
from core.config import settings
from utils.logger import setup_logger
from api.routes import router as api_router
from database.manager import HybridDatabaseManager

# Setup directories
os.makedirs("logs", exist_ok=True)
os.makedirs("static_dashboard", exist_ok=True)

logger = setup_logger("API_Gateway")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle: Initializes the Singleton DB connection for the API.
    """
    logger.info("🚀 VolGuard API Gateway Initializing...")
    
    # Initialize Singleton Database
    db = HybridDatabaseManager()
    await db.init_db() 
    
    logger.info("✅ Database Pool Ready (Singleton)")
    
    yield
    
    # Cleanup
    logger.info("🛑 Web API Shutdown...")

app = FastAPI(
    title="VolGuard 20.0 (Pure Quant)",
    description="API Layer for VolGuard Fortress (No AI)",
    version="20.0.1",
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

# Static Dashboard (Optional: requires React build in /static_dashboard)
static_path = Path("./static_dashboard")
if static_path.exists():
    app.mount("/dashboard", StaticFiles(directory=static_path, html=True), name="dashboard")

# Routes
app.include_router(api_router)

@app.get("/")
async def root():
    return {
        "system": "VolGuard 20.0",
        "mode": "Pure Quant (Hardened)",
        "status": "Online",
        "docs": "/docs"
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=settings.PORT, 
        log_level="info",
        reload=False # False for production stability
    )
