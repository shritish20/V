import time 
import threading
import smtplib
import os 
import logging
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Set
from threading import Lock
from datetime import datetime
from collections import deque
from core.config import ALERT_EMAIL, EMAIL_PASSWORD

logger = logging.getLogger("VolGuard14")

class CriticalAlertSystem:
    """Production-grade alert system with multiple channels - Enhanced"""
    
    def __init__(self):
        self.alert_queue = deque(maxlen=1000)
        self._alert_lock = Lock()
        self.sent_alerts: Set[str] = set()  # Prevent duplicate alerts
        self.alert_cooldown = 300  # 5 minutes
        self.last_alert_time: Dict[str, float] = {}
        
    async def send_alert(self, alert_type: str, message: str, urgent: bool = False):
        """Send alert through multiple channels with cooldown management"""
        alert_id = f"{alert_type}_{hash(message)}"
        
        # Cooldown check
        with self._alert_lock:
            now = time.time()
            last_time = self.last_alert_time.get(alert_type, 0)
            if now - last_time < self.alert_cooldown and not urgent:
                return
            self.last_alert_time[alert_type] = now
            
            if alert_id in self.sent_alerts:
                return  # Prevent duplicates
            self.sent_alerts.add(alert_id)
            self.alert_queue.append({
                'type': alert_type,
                'message': message,
                'timestamp': datetime.now(),
                'urgent': urgent
            })
        
        # Send via available channels
        tasks = []
        if ALERT_EMAIL and EMAIL_PASSWORD:
            tasks.append(self._send_email_alert(alert_type, message, urgent))
        
        # Console logging (always)
        log_level = logging.CRITICAL if urgent else logging.WARNING
        logger.log(log_level, f"ALERT [{alert_type}]: {message}")
        
        # Run all alert methods concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_email_alert(self, alert_type: str, message: str, urgent: bool):
        """Send email alert"""
        try:
            if not ALERT_EMAIL or not EMAIL_PASSWORD:
                return
                
            msg = MIMEMultipart()
            msg['From'] = ALERT_EMAIL
            msg['To'] = ALERT_EMAIL
            msg['Subject'] = f"{'🚨 URGENT: ' if urgent else '⚠️ '}VolGuard 14.00 Alert: {alert_type}"
            
            body = f"""
            VolGuard 14.00 System Alert
            
            Type: {alert_type}
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            Urgent: {urgent}
            
            Message:
            {message}
            
            ---
            VolGuard 14.00 - Ironclad Trading System
            Automated Alert - Do not reply
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(ALERT_EMAIL, EMAIL_PASSWORD)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent for {alert_type}")
            
        except Exception as e:
            logger.error(f"Email alert failed: {e}")

    async def circuit_breaker_alert(self, pnl: float, limit: float, urgent: bool = False):
        await self.send_alert(
            "CIRCUIT_BREAKER",
            f"Daily loss limit breached! PnL: ₹{pnl:,.0f}, Limit: ₹{limit:,.0f}",
            urgent=urgent
        )

    async def partial_fill_alert(self, failed_step: str, trade_id: str, urgent: bool = False):
        await self.send_alert(
            "PARTIAL_FILL",
            f"CRITICAL: Failed during {failed_step} for Trade ID: {trade_id}",
            urgent=urgent
        )

    async def data_feed_alert(self, feed_type: str, error: str, urgent: bool = False):
        await self.send_alert(
            "DATA_FEED_ERROR",
            f"{feed_type} data feed error: {error}",
            urgent=urgent
        )

    def get_alert_stats(self) -> Dict[str, any]:
        """Get alert system statistics"""
        with self._alert_lock:
            return {
                "queue_size": len(self.alert_queue),
                "sent_alerts": len(self.sent_alerts),
                "recent_alerts": list(self.alert_queue)[-10:] if self.alert_queue else []
            }
