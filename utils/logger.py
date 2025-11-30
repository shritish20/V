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
    """Setup enhanced logging for VolGuard 14.00"""
    logger = logging.getLogger("VolGuard14")
    
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(TRADE_LOG_FILE)
    fh.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(file_formatter)
    
    # Console handler with colors
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(fh)
    logger.addHandler(sh)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger
