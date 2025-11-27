import time 
import threading
import smtplib
import os 
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List
from threading import Lock
from datetime import datetime
from core.config import IST

logger = logging.getLogger("VolGuardHybrid")

class CriticalAlertSystem:
    """Production-grade alert system"""
    def __init__(self):
        self.last_alert_time: Dict[str, float] = {}
        self.alert_cooldown = 300 # 5 minutes
        self._lock = Lock()
        self.alert_email = os.getenv("ALERT_EMAIL")
        self.email_password = os.getenv("EMAIL_PASSWORD")

    async def send_alert(self, alert_type: str, message: str, urgent: bool = False):
        """Send alert with cooldown management"""
        with self._lock:
            now = time.time()
            last_time = self.last_alert_time.get(alert_type, 0)
            if now - last_time < self.alert_cooldown and not urgent:
                return
            self.last_alert_time[alert_type] = now
            
            print(f"🚨 VOLGUARD ALERT [{alert_type}]: {message}")
            logger.critical(f"ALERT_{alert_type}: {message}")
            
            if urgent and self.alert_email and self.email_password:
                threading.Thread(target=self._send_email_alert, args=(alert_type, message), daemon=True).start()

    def _send_email_alert(self, subject: str, body: str):
        """Send email alert"""
        try:
            msg = MIMEMultipart()
            msg['From'] = self.alert_email
            msg['To'] = self.alert_email
            msg['Subject'] = f"VOLGUARD HYBRID: {subject}"
            
            email_body = f"""
VolGuard Hybrid Critical Alert
Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}

{body}

---
Automated alert from VolGuard Hybrid Ultimate
"""
            msg.attach(MIMEText(email_body, 'plain'))
            
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(self.alert_email, self.email_password)
            server.send_message(msg)
            server.quit()
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

    async def risk_limit_alert(self, metric: str, value: float, limit: float, urgent: bool = False):
        await self.send_alert(
            "RISK_LIMIT",
            f"Risk limit breached! {metric}: {value:.1f}, Limit: {limit:.1f}",
            urgent=urgent
        )
