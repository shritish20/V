#!/usr/bin/env python3
"""
VolGuard 20.0 – Web API Server (Microservice)
- Roles: Serves Frontend API, Serves Static React Files
- DOES NOT run the Trading Engine (Run core/engine.py for that)
- Hardened: Auto-creates directories, Graceful DB shutdown
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
# We will fix api/routes.py next, but main.py needs it to start
from api.routes import router as api_router
from database.manager import HybridDatabaseManager

# --- FIX: Create essential directories before Logger/App start ---
os.makedirs("logs", exist_ok=True)
os.makedirs("static_dashboard", exist_ok=True)

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
    # This ensures the API can talk to the tables created by the Engine/Sheriff
    db = HybridDatabaseManager()
    await db.init_db() 
    
    logger.info("✅ Database Connected")
    
    yield
    
    # Cleanup
    await db.close()
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

# Static Files (The React Frontend)
# This serves the files from the 'static_dashboard' folder
static_path = Path("./static_dashboard")
app.mount("/dashboard", StaticFiles(directory=static_path, html=True), name="dashboard")

# Include the Unified Router (The Logic)
app.include_router(api_router)

# Root Redirect / Health Check
@app.get("/")
async def root():
    return {
        "system": "VolGuard 20.0 Fortress",
        "role": "API Gateway",
        "status": "Online",
        "dashboard_url": f"http://localhost:{settings.PORT}/dashboard"
    }

if __name__ == "__main__":
    logger.info(f"🔥 Starting API Server on Port {settings.PORT}")
    # reload=True is enabled for easier debugging during setup
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=settings.PORT, 
        log_level="info",
        reload=True 
    )
