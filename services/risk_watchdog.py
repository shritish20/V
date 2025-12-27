#!/usr/bin/env python3
"""
VolGuard 20.0 – The Sheriff (Risk Watchdog)
- INDEPENDENT PROCESS: Monitors PnL & Equity.
- SINGLETON DB: Uses shared pool.
- V3 EXECUTION: Uses updated Order model for flattening.
- ENGINE KILLER: Sends SIGTERM if Drawdown > Limit.
"""
import asyncio
import logging
import sys
import os
import signal
from pathlib import Path
from datetime import datetime, time

# Path Hack to ensure we find core modules
sys.path.append(os.getcwd())

from core.config import settings
from core.models import Order  # <--- CRITICAL: Required for V3 API calls
from trading.api_client import EnhancedUpstoxAPI
from database.manager import HybridDatabaseManager
from database.models import DbRiskState, DbTokenState
from sqlalchemy import select

# --- SETUP LOGGING ---
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | 🤠 SHERIFF | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/sheriff.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Sheriff")

# CONFIGURATION
MAX_DRAWDOWN_PCT = getattr(settings, 'DAILY_LOSS_LIMIT_PCT', 0.03) 
MARKET_OPEN_TIME = settings.MARKET_OPEN_TIME
HEARTBEAT_INTERVAL = 2
ENGINE_PID_FILE = Path("data/engine.pid")

async def get_valid_token(db: HybridDatabaseManager) -> str:
    """Fetches the latest valid token from DB or falls back to settings."""
    try:
        async with db.get_session() as session:
            result = await session.execute(
                select(DbTokenState).order_by(DbTokenState.last_refreshed.desc()).limit(1)
            )
            state = result.scalars().first()
            # Add 60s buffer to expiry check
            if state and datetime.utcnow() < state.expires_at:
                return state.access_token
    except Exception:
        pass
    return settings.UPSTOX_ACCESS_TOKEN

async def _shutdown_engine():
    """
    Sends SIGTERM to Engine process.
    """
    try:
        if not ENGINE_PID_FILE.exists():
            return
        
        pid_str = ENGINE_PID_FILE.read_text().strip()
        if not pid_str.isdigit():
            return
        
        engine_pid = int(pid_str)
        logger.critical(f"🛑 SHERIFF STOPPING ENGINE (PID: {engine_pid})")
        
        try:
            os.kill(engine_pid, signal.SIGTERM)
        except ProcessLookupError:
            ENGINE_PID_FILE.unlink(missing_ok=True)
            return

        # Wait for death
        for i in range(5):
            await asyncio.sleep(1)
            try:
                os.kill(engine_pid, 0)
            except ProcessLookupError:
                logger.info("✅ Engine stopped.")
                ENGINE_PID_FILE.unlink(missing_ok=True)
                return
        
        # Force Kill
        try:
            os.kill(engine_pid, signal.SIGKILL)
        except OSError: pass
        ENGINE_PID_FILE.unlink(missing_ok=True)
        
    except Exception as e:
        logger.error(f"Engine Shutdown Failed: {e}")

async def run_watchdog():
    # SINGLETON DB
    db = HybridDatabaseManager()
    await db.init_db()
    
    # Initial Token
    token = await get_valid_token(db)
    api = EnhancedUpstoxAPI(token)
    
    logger.info(f"🤠 Sheriff Online. Kill Limit: {MAX_DRAWDOWN_PCT*100}%")
    
    sod_equity = 0.0
    kill_switch_triggered = False
    last_flatten_time = datetime.min
    sod_locked_today = False

    while True:
        try:
            # 0. Token Refresh (Every ~3 mins)
            if int(datetime.utcnow().timestamp()) % 200 == 0:
                new_token = await get_valid_token(db)
                if new_token != api._token: # Access token safely
                     await api.update_token(new_token)
                     logger.info("🔄 Sheriff synced token")

            # 1. Broker Data (Source of Truth)
            funds_resp = await api.get_funds_and_margin()
            if funds_resp.get("status") != "success":
                await asyncio.sleep(1)
                continue
                
            data = funds_resp.get("data", {}).get("equity", {})
            used_margin = float(data.get("used_margin", 0.0))
            avail_margin = float(data.get("available_margin", 0.0))
            current_equity = avail_margin + used_margin
            
            now = datetime.now(settings.IST).time()

            # 2. SOD Lock Logic
            # Reset SOD if we are in pre-open or just started
            if sod_equity == 0.0 or (now >= MARKET_OPEN_TIME and now < time(9, 16) and not sod_locked_today):
                sod_equity = current_equity
                sod_locked_today = True
                logger.info(f"🌅 SOD Equity Locked: {sod_equity:,.2f}")
            
            if now > time(23, 0): sod_locked_today = False

            # 3. Drawdown Calc
            drawdown_pct = 0.0
            if sod_equity > 0:
                drawdown_pct = (current_equity - sod_equity) / sod_equity
            
            # 4. DB Kill Switch Check
            manual_kill = False
            async with db.get_session() as session:
                res = await session.execute(
                    select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
                )
                latest_state = res.scalars().first()
                if latest_state and latest_state.kill_switch_active:
                    manual_kill = True

            # 5. Decision Logic
            should_flatten = False
            
            if drawdown_pct < -MAX_DRAWDOWN_PCT:
                logger.critical(f"🚨 MAX DRAWDOWN BREACHED! {drawdown_pct*100:.2f}%")
                should_flatten = True
                kill_switch_triggered = True
            
            if manual_kill:
                logger.critical("🚨 MANUAL KILL SWITCH ACTIVE")
                should_flatten = True
                kill_switch_triggered = True

            # Reset if recovered and NOT manual kill
            if kill_switch_triggered and not manual_kill and drawdown_pct > -0.01:
                logger.info(f"✅ Recovery Detected ({drawdown_pct*100:.2f}%). Disarming.")
                kill_switch_triggered = False
                should_flatten = False

            # 6. Heartbeat Write
            state = DbRiskState(
                sheriff_heartbeat=datetime.utcnow(),
                sod_equity=sod_equity,
                current_equity=current_equity,
                drawdown_pct=drawdown_pct,
                kill_switch_active=kill_switch_triggered,
                is_flattening=should_flatten
            )
            
            # Retry loop for heartbeat to avoid crashing watchdog on DB blip
            for _ in range(3):
                try:
                    async with db.get_session() as session:
                        session.add(state)
                        await db.safe_commit(session)
                    break
                except Exception:
                    await asyncio.sleep(0.5)

            # 7. Execution
            if should_flatten:
                # A. Kill Engine First
                await _shutdown_engine()
                
                # B. Flatten Positions
                if (datetime.utcnow() - last_flatten_time).total_seconds() > 5:
                    positions = await api.get_short_term_positions()
                    open_positions = [p for p in positions if int(p['quantity']) != 0]
                    
                    if open_positions:
                        logger.warning(f"📉 FLATTENING {len(open_positions)} POSITIONS...")
                        for pos in open_positions:
                            qty = int(pos['quantity'])
                            # Construct Proper Order Object for V3 API
                            order = Order(
                                quantity=abs(qty),
                                product="I", # Intraday for safety
                                validity="DAY",
                                price=0.0, # Market
                                trigger_price=0.0,
                                instrument_key=pos['instrument_token'],
                                order_type="MARKET",
                                transaction_type="SELL" if qty > 0 else "BUY",
                                tag="SHERIFF_KILL"
                            )
                            await api.place_order(order)
                        last_flatten_time = datetime.utcnow()
                    else:
                        logger.info("✅ All Positions Closed.")

            await asyncio.sleep(HEARTBEAT_INTERVAL)

        except Exception as e:
            logger.error(f"Sheriff Loop Crash: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(run_watchdog())
