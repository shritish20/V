import smtplib
from email.mime.text import MIMEText
from core.config import settings
from utils.logger import setup_logger

logger = setup_logger()

class AlertSystem:
    def __init__(self):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.sender_email = settings.ALERT_EMAIL
        self.sender_password = settings.EMAIL_PASSWORD
        self.recipient_email = settings.ALERT_EMAIL

    def _send_email(self, subject: str, body: str):
        try:
            msg = MIMEText(body)
            msg["Subject"] = f"[VolGuard ALERT] {subject}"
            msg["From"] = self.sender_email
            msg["To"] = self.recipient_email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, self.recipient_email, msg.as_string())
            logger.success(f"Alert email sent: {subject}")
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")

    def send_critical_alert(self, message: str):
        subject = "CRITICAL SYSTEM FAILURE"
        body = f"A critical event has occurred in VolGuard 18.0:\n\n{message}"
        logger.critical(body)
        self._send_email(subject, body)

    def send_risk_breach_alert(self, risk_metric: str, value: float, limit: float):
        subject = f"RISK BREACH: {risk_metric}"
        body = (
            f"The {risk_metric} limit has been breached.\n"
            f"Current Value: {value:.2f}\n"
            f"Limit: {limit:.2f}\n"
            "Immediate action may be required."
        )
        logger.warning(body)
        self._send_email(subject, body)
