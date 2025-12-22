#!/usr/bin/env python3
"""
VolGuard 20.0 – Web API Server (Microservice)
- Roles: Serves Frontend API, Serves Static React Files
- DOES NOT run the Trading Engine (Run core/engine.py for that)
"""
import uvicorn
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

# Setup Logging
logger = setup_logger("API_Server")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle Manager for the Web Server.
    Initializes DB Connection Pool for the API.
    """
    logger.info("🚀 VolGuard Web API Initializing...")
    
    # 1. Initialize Database Manager (Shared Pool)
    db = HybridDatabaseManager()
    await db.init_db() # Ensure tables exist
    
    logger.info("✅ Database Connected")
    
    yield
    
    logger.info("🛑 Web API Shutdown...")

# ==========================================
# FASTAPI APP DEFINITION
# ==========================================
app = FastAPI(
    title="VolGuard 20.0 (Command Center)",
    description="API Layer for VolGuard Fortress Architecture",
    version="20.0.1",
    lifespan=lifespan
)

# CORS (Allow Frontend to talk to Backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static Files (Placeholder for your React Build)
# When you build React, put the 'build' folder contents in './static_dashboard'
static_path = Path("./static_dashboard")
static_path.mkdir(exist_ok=True)
app.mount("/dashboard", StaticFiles(directory=static_path, html=True), name="dashboard")

# Include the Unified Router (The Logic)
app.include_router(api_router)

# Root Redirect
@app.get("/")
async def root():
    return {
        "system": "VolGuard 20.0 Fortress",
        "status": "API Online",
        "dashboard_url": f"http://localhost:{settings.PORT}/dashboard"
    }

if __name__ == "__main__":
    logger.info(f"🔥 Starting API Server on Port {settings.PORT}")
    # We use reload=True for dev so you can see changes instantly
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=settings.PORT, 
        log_level="info",
        reload=True 
    )
