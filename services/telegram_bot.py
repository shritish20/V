#!/usr/bin/env python3
"""
VolGuard Sentinel (Fortress Edition)
- Monitors: Sheriff Heartbeat (System Health)
- Monitors: Market Volatility (VIX, VRP)
- Monitors: Account Risk (Drawdown, Kill Switch)
- Features: Anti-Spam (Debouncing), Pure Quant Logic (No AI)
"""
import asyncio
import logging
import os
import sys
import aiohttp
from datetime import datetime, timedelta
from sqlalchemy import select, text, desc

# Path Hack
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.manager import HybridDatabaseManager
from database.models import DbRiskState, DbMarketSnapshot

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Thresholds
MAX_SILENCE_SECONDS = 120    # Alert if Sheriff is dead for > 2 mins
VIX_HIGH_THRESHOLD = 20.0    # Alert if VIX > 20
VRP_DANGER_THRESHOLD = -1.5  # Alert if VRP Z-Score < -1.5 (Market Mispricing)
DRAWDOWN_WARN_PCT = 0.015    # Warn at 1.5% Drawdown (Halfway to 3% Limit)

# Anti-Spam (Seconds to wait before repeating an alert)
COOLDOWN_VOLATILITY = 1800   # 30 Mins
COOLDOWN_RISK = 600          # 10 Mins

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | 🛡️ SENTINEL | %(levelname)s | %(message)s"
)
logger = logging.getLogger("Sentinel")

class VolGuardSentinel:
    def __init__(self):
        self.db = HybridDatabaseManager()
        self.last_alerts = {
            "vix": datetime.min,
            "vrp": datetime.min,
            "drawdown": datetime.min,
            "kill_switch": datetime.min
        }

    async def send_alert(self, title: str, message: str, level: str = "🚨"):
        """Sends a formatted alert to Telegram."""
        if not BOT_TOKEN or not CHAT_ID:
            logger.warning(f"Telegram Missing. Suppressed: {title} - {message}")
            return

        text_msg = f"{level} *{title}*\n{message}"
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": text_msg, "parse_mode": "Markdown"}
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.error(f"Telegram Fail: {resp.status}")
            except Exception as e:
                logger.error(f"Telegram Connection Error: {e}")

    async def check_system_health(self, session):
        """1. CHECK SHERIFF HEARTBEAT"""
        try:
            query = text("SELECT sheriff_heartbeat FROM risk_state ORDER BY timestamp DESC LIMIT 1")
            result = await session.execute(query)
            last_beat = result.scalar()
            
            if last_beat:
                delta = (datetime.utcnow() - last_beat).total_seconds()
                if delta > MAX_SILENCE_SECONDS:
                    await self.send_alert(
                        "SYSTEM FAILURE", 
                        f"Sheriff is DEAD. Last beat: {int(delta)}s ago.\nIMMEDIATE ACTION REQUIRED.",
                        level="💀"
                    )
        except Exception as e:
            logger.error(f"Health Check Failed: {e}")

    async def check_market_conditions(self, session):
        """2. CHECK VOLATILITY & VRP"""
        try:
            res = await session.execute(
                select(DbMarketSnapshot).order_by(DbMarketSnapshot.timestamp.desc()).limit(1)
            )
            market = res.scalars().first()
            if not market: return

            now = datetime.utcnow()

            # A. High VIX Alert
            if market.vix > VIX_HIGH_THRESHOLD:
                if (now - self.last_alerts["vix"]).total_seconds() > COOLDOWN_VOLATILITY:
                    await self.send_alert(
                        "HIGH VOLATILITY",
                        f"VIX has spiked to *{market.vix:.2f}*.\nOptions premiums are expensive.",
                        level="⚡"
                    )
                    self.last_alerts["vix"] = now

            # B. VRP Danger (Negative Z-Score)
            # This means Realized Vol > Implied Vol (Selling is dangerous)
            if market.vrp_zscore < VRP_DANGER_THRESHOLD:
                if (now - self.last_alerts["vrp"]).total_seconds() > COOLDOWN_VOLATILITY:
                    await self.send_alert(
                        "VRP DANGER",
                        f"VRP Z-Score: *{market.vrp_zscore:.2f}*\nMarket is moving faster than priced.\nStrategies may switch to CASH.",
                        level="⚠️"
                    )
                    self.last_alerts["vrp"] = now
                    
        except Exception as e:
            logger.error(f"Market Check Failed: {e}")

    async def check_account_risk(self, session):
        """3. CHECK DRAWDOWN & KILL SWITCH"""
        try:
            res = await session.execute(
                select(DbRiskState).order_by(DbRiskState.timestamp.desc()).limit(1)
            )
            risk = res.scalars().first()
            if not risk: return

            now = datetime.utcnow()

            # A. Kill Switch Activation (Immediate Alert, No Cooldown)
            if risk.kill_switch_active:
                if (now - self.last_alerts["kill_switch"]).total_seconds() > 300: # Remind every 5 mins
                    await self.send_alert(
                        "KILL SWITCH ACTIVE",
                        f"Bot has flattened all positions.\nDrawdown: {risk.drawdown_pct*100:.2f}%",
                        level="🛑"
                    )
                    self.last_alerts["kill_switch"] = now

            # B. Drawdown Warning
            # Alert if we are halfway to the limit (e.g. down 1.5%)
            if risk.drawdown_pct < -DRAWDOWN_WARN_PCT and not risk.kill_switch_active:
                 if (now - self.last_alerts["drawdown"]).total_seconds() > COOLDOWN_RISK:
                    await self.send_alert(
                        "DRAWDOWN WARNING",
                        f"Current Drawdown: *{risk.drawdown_pct*100:.2f}%*\nApproaching limits.",
                        level="📉"
                    )
                    self.last_alerts["drawdown"] = now

        except Exception as e:
            logger.error(f"Risk Check Failed: {e}")

    async def run(self):
        logger.info("🛡️ Sentinel is watching...")
        await self.db.init_db()

        while True:
            try:
                async with self.db.get_session() as session:
                    await self.check_system_health(session)
                    await self.check_market_conditions(session)
                    await self.check_account_risk(session)
            except Exception as e:
                logger.error(f"Sentinel Loop Error: {e}")
            
            await asyncio.sleep(60) # Check every minute

if __name__ == "__main__":
    sentinel = VolGuardSentinel()
    try:
        if not BOT_TOKEN:
            logger.warning("⚠️ TELEGRAM_BOT_TOKEN missing. Alerts will only log to console.")
        asyncio.run(sentinel.run())
    except KeyboardInterrupt:
        logger.info("Sentinel stopped.")
