import logging 
import os
from core.config import TRADE_LOG_FILE

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\x1b[38;20m',
        'INFO': '\x1b[38;20m',
        'WARNING': '\x1b[33;20m',
        'ERROR': '\x1b[31;20m',
        'CRITICAL': '\x1b[31;1m'
    }
    RESET = '\x1b[0m'
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)

def setup_logger():
    """Setup enhanced logging"""
    logger = logging.getLogger("VolGuardHybrid")
    logger.setLevel(logging.INFO)
    
    fh = logging.FileHandler(TRADE_LOG_FILE)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    sh = logging.StreamHandler()
    sh.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(sh)
        
    return logger
