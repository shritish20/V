#!/usr/bin/env python3
import asyncio
import signal
import uvicorn
import sys
import os

# Add project root to python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.config import settings
from core.engine import VolGuard17Engine
from database.manager import HybridDatabaseManager
from utils.logger import setup_logger
from api.routes import app

logger = setup_logger("Main")

engine_instance = None

async def startup_sequence():
    """Initializes DB and Engine, then attaches Engine to API state"""
    global engine_instance
    
    logger.info("🛡️ STARTING VOLGUARD 19.0 (PRODUCTION)")
    logger.info(f"💰 Account: {settings.ACCOUNT_SIZE:,.0f} | Mode: {settings.SAFETY_MODE}")

    # 1. Initialize Database
    db_manager = HybridDatabaseManager()
    await db_manager.init_db()

    # 2. Initialize Engine
    engine_instance = VolGuard17Engine()
    
    # 3. Attach to API State (Crucial for API control)
    app.state.engine = engine_instance
    
    return engine_instance

async def shutdown_sequence(sig=None):
    if sig:
        logger.info(f"🛑 Signal received: {sig.name}")
    
    if engine_instance:
        logger.info("Shutting down Trading Engine...")
        await engine_instance.shutdown()
    
    logger.info("👋 System Shutdown Complete.")

async def main():
    loop = asyncio.get_running_loop()
    
    # Signal Handlers
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            s, lambda sig=s: asyncio.create_task(shutdown_sequence(sig))
        )

    try:
        # Bootup
        active_engine = await startup_sequence()

        # Run Engine and API concurrently
        # Engine runs in background, API blocks until exit
        await asyncio.gather(
            active_engine.run(),
            uvicorn.Server(
                uvicorn.Config(app, host="0.0.0.0", port=settings.PORT, log_level="warning")
            ).serve(),
        )
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"🔥 FATAL ERROR: {e}")
    finally:
        await shutdown_sequence()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
