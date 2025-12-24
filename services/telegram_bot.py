#!/usr/bin/env python3
"""
VolGuard Sentinel - External Heartbeat Monitor
Polls the database to ensure the Sheriff (Risk Watchdog) is alive.
Sends Telegram alerts if the system goes silent.
"""
import asyncio
import logging
import os
import sys
import aiohttp
from datetime import datetime
from sqlalchemy import text

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.manager import HybridDatabaseManager

# CONFIGURATION
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
# Alert if Sheriff hasn't reported in for this many seconds
MAX_SILENCE_SECONDS = 120 

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🛡️ SENTINEL | %(levelname)s | %(message)s"
)
logger = logging.getLogger("Sentinel")

async def send_alert(message: str):
    """Sends a critical alert to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning(f"Telegram not configured. Alert suppressed: {message}")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID, 
        "text": f"🚨 VOLGUARD ALERT: {message}"
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to send Telegram alert: {resp.status}")
        except Exception as e:
            logger.error(f"Telegram connection failed: {e}")

async def watch_sheriff_heartbeat():
    """Main loop: Checks DB for fresh heartbeat timestamps."""
    logger.info("🛡️ Sentinel is watching the Sheriff...")
    
    db = HybridDatabaseManager()
    await db.init_db()

    while True:
        try:
            async with db.get_session() as session:
                # Check the last time Sheriff updated the risk_state table
                # We use raw SQL for speed and simplicity
                query = text("SELECT sheriff_heartbeat FROM risk_state ORDER BY timestamp DESC LIMIT 1")
                result = await session.execute(query)
                last_beat = result.scalar()
                
                if last_beat:
                    # Calculate silence duration
                    # Note: Ensure DB timezone matches (UTC is standard)
                    delta = (datetime.utcnow() - last_beat).total_seconds()
                    
                    if delta > MAX_SILENCE_SECONDS:
                        msg = (
                            f"SHERIFF IS DEAD/STUCK. \n"
                            f"Last heartbeat: {int(delta)}s ago. \n"
                            f"Engine might be running UNPROTECTED. \n"
                            f"IMMEDIATE INTERVENTION REQUIRED."
                        )
                        logger.critical(msg)
                        await send_alert(msg)
                    else:
                        logger.debug(f"Sheriff is alive. Last beat: {int(delta)}s ago")
                else:
                    logger.warning("No heartbeat data found yet. Waiting...")
                    
        except Exception as e:
            logger.error(f"Sentinel Loop Error: {e}")
        
        # Check every 60 seconds
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        if not BOT_TOKEN:
            logger.warning("⚠️ TELEGRAM_BOT_TOKEN missing. Alerts will only log to console.")
        asyncio.run(watch_sheriff_heartbeat())
    except KeyboardInterrupt:
        logger.info("Sentinel stopped.")
