import logging
import sys
import re
from pathlib import Path
from core.config import settings

class SanitizingFormatter(logging.Formatter):
    """
    Security Formatter: Redacts sensitive API tokens from logs.
    """
    # Patterns to catch Bearer tokens and Access tokens
    SENSITIVE_PATTERNS = [
        (r'Bearer\s+[\w\-]+', 'Bearer [REDACTED]'),
        (r'"access_token":\s*"[^"]+"', '"access_token": "[REDACTED]"'),
        (r'Authorization:\s*[^\s]+', 'Authorization: [REDACTED]'),
        (r'token=[^&\s]+', 'token=[REDACTED]')
    ]
    
    def format(self, record):
        msg = super().format(record)
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            msg = re.sub(pattern, replacement, msg)
        return msg

def setup_logger(name: str = "VolGuard18") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
        
    logger.setLevel(logging.DEBUG)
    
    # Use Sanitizing Formatter
    formatter = SanitizingFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File
    logs_dir = Path(settings.PERSISTENT_DATA_DIR) / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    file_handler = logging.FileHandler(logs_dir / "volguard.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
