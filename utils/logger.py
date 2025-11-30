import logging

def setup_logger():
    """Simple console logging for Render deployment"""
    logger = logging.getLogger("VolGuard14")
    
    if logger.handlers:
        return logger
        
    logger.setLevel(logging.INFO)
    
    # Console handler only (Render doesn't allow file writes in app directory)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.propagate = False
    
    return logger
