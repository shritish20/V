#!/usr/bin/env python3
import asyncio
import signal
import uvicorn
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.config import settings
from core.engine import VolGuard17Engine
from database.manager import HybridDatabaseManager
from utils.logger import setup_logger
from api.routes import app

logger = setup_logger("Main")
engine_instance = None

async def startup_sequence():
    global engine_instance
    logger.info("🚀 STARTING VOLGUARD 19.0 (PROP EDITION)")
    
    db_manager = HybridDatabaseManager()
    await db_manager.init_db()
    
    engine_instance = VolGuard17Engine()
    
    app.state.engine = engine_instance
    return engine_instance

async def shutdown_sequence(sig=None):
    if engine_instance:
        logger.info("🛑 Shutting down Trading Engine...")
        await engine_instance.shutdown()
    logger.info("✅ System Shutdown Complete.")

async def main():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_sequence(sig)))

    try:
        active_engine = await startup_sequence()
        await asyncio.gather(
            active_engine.run(),
            uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=settings.PORT)).serve(),
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
