#!/usr/bin/env python3
"""
VolGuard 20.0 – The Sheriff (Risk Watchdog)
- Independent Process (Process 3)
- Monitors Realized + Unrealized PnL via Broker API
- Listens for "PANIC" signal from Frontend/DB
- Auto-Flattens if Limits Breached
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, time

# Path Hack to ensure we find core modules
sys.path.append(os.getcwd())

from core.config import settings
from trading.api_client import EnhancedUpstoxAPI
from database.manager import HybridDatabaseManager
from database.models import DbRiskState
from sqlalchemy import select

# Structured Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | 🤠 SHERIFF | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/sheriff.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Sheriff")

# CONFIGURATION (Hardcoded fallbacks for safety)
MAX_DRAWDOWN_PCT = getattr(settings, 'DAILY_LOSS_LIMIT_PCT', 0.03) # Default 3%
MARKET_OPEN_TIME = time(9, 15)
HEARTBEAT_INTERVAL = 2

async def run_watchdog():
    db = HybridDatabaseManager()
    api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
    
    logger.info(f"🤠 Sheriff Online. Max Drawdown Limit: {MAX_DRAWDOWN_PCT*100}%")
    
    # State Variables
    sod_equity = 0.0
    kill_switch_triggered = False
    last_flatten_time = datetime.min
    sod_locked_today = False

    while True:
        try:
            # --- 1. BROKER DATA (SOURCE OF TRUTH) ---
            funds_resp = await api.get_funds_and_margin()
            if funds_resp.get("status") != "success":
                logger.warning("Broker API glitch. Retrying...")
                await asyncio.sleep(1)
                continue
                
            data = funds_resp.get("data", {}).get("equity", {})
            used_margin = float(data.get("used_margin", 0.0))
            avail_margin = float(data.get("available_margin", 0.0))
            current_equity = avail_margin + used_margin
            
            now = datetime.now().time()

            # --- 2. SOD RE-LOCK LOGIC (9:15 AM Reset) ---
            if sod_equity == 0.0 or (now >= MARKET_OPEN_TIME and now < time(9, 16) and not sod_locked_today):
                sod_equity = current_equity
                sod_locked_today = True
                logger.info(f"🌅 SOD Equity Locked: ₹{sod_equity:,.2f}")
            
            if now > time(23, 0): sod_locked_today = False

            # --- 3. CALCULATE DRAWDOWN ---
            drawdown_pct = 0.0
            if sod_equity > 0:
                drawdown_pct = (current_equity - sod_equity) / sod_equity
            
            # --- 4. CHECK DB FOR MANUAL KILL SWITCH ---
            # This allows the Frontend "PANIC" button to work
            manual_kill = False
            async with db.get_session() as session:
                res = await session.execute(
                    select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
                )
                latest_state = res.scalars().first()
                if latest_state and latest_state.kill_switch_active:
                    manual_kill = True

            # --- 5. DECISION LOGIC ---
            should_flatten = False
            
            # Condition A: PnL Breach
            if drawdown_pct < -MAX_DRAWDOWN_PCT:
                logger.critical(f"🚨 MAX DRAWDOWN BREACHED! {drawdown_pct*100:.2f}%")
                should_flatten = True
                kill_switch_triggered = True
            
            # Condition B: Manual Panic Button
            if manual_kill:
                logger.critical("🚨 MANUAL KILL SWITCH DETECTED FROM DB")
                should_flatten = True
                kill_switch_triggered = True

            # Condition C: Recovery (Auto-Reset)
            if kill_switch_triggered and not manual_kill and drawdown_pct > -0.01:
                logger.info(f"✅ Equity Recovered ({drawdown_pct*100:.2f}%). Resetting Kill Switch.")
                kill_switch_triggered = False
                should_flatten = False

            # --- 6. WRITE HEARTBEAT (So Engine knows Sheriff is alive) ---
            state = DbRiskState(
                sheriff_heartbeat=datetime.utcnow(),
                sod_equity=sod_equity,
                current_equity=current_equity,
                drawdown_pct=drawdown_pct,
                kill_switch_active=kill_switch_triggered,
                is_flattening=should_flatten
            )
            
            # Retry DB write 3 times
            for attempt in range(3):
                try:
                    async with db.get_session() as session:
                        session.add(state)
                        await db.safe_commit(session)
                    break
                except Exception:
                    await asyncio.sleep(0.5)

            # --- 7. EXECUTE FLATTENING ---
            if should_flatten:
                # Throttle to every 5 seconds to avoid API ban
                if (datetime.utcnow() - last_flatten_time).total_seconds() > 5:
                    positions = await api.get_short_term_positions()
                    open_positions = [p for p in positions if int(p['quantity']) != 0]
                    
                    if open_positions:
                        logger.warning(f"📉 FLATTENING {len(open_positions)} POSITIONS...")
                        for pos in open_positions:
                            await api.place_order({
                                "instrument_token": pos['instrument_token'],
                                "quantity": abs(int(pos['quantity'])),
                                "transaction_type": "SELL" if int(pos['quantity']) > 0 else "BUY",
                                "order_type": "MARKET",
                                "product": "I",
                                "validity": "DAY",
                                "tag": "SHERIFF_KILL"
                            })
                        last_flatten_time = datetime.utcnow()
                    else:
                        logger.info("✅ Account Flattened. Holding Kill Switch.")

            await asyncio.sleep(HEARTBEAT_INTERVAL)

        except Exception as e:
            logger.error(f"Sheriff Loop Crash: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_watchdog())
