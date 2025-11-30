import uvicorn
import os
import sys
from pathlib import Path
import asyncio
import logging
from utils.logger import setup_logger

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

if __name__ == "__main__":
    logger = setup_logger()
    
    print("🚀 VOLGUARD 14.00 - IRONCLAD ARCHITECTURE")
    print("✅ PRODUCTION-GRADE TRADING ENGINE")
    print("✅ ADVANCED VOLATILITY ANALYTICS")
    print("✅ COMPREHENSIVE RISK MANAGEMENT")
    print("✅ REAL-TIME ANALYTICS & ALERTS")
    print("✅ PRODUCTION MONITORING & METRICS")
    print("🎯 READY FOR LIVE DEPLOYMENT")

    # Create persistent storage directory
    PERSISTENT_DIR = os.getenv("PERSISTENT_DATA_DIR", "./data")
    Path(PERSISTENT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create required files
    for file in [
        os.path.join(PERSISTENT_DIR, "volguard_14.db"), 
        os.path.join(PERSISTENT_DIR, "volguard_14_log.txt"), 
        os.path.join(PERSISTENT_DIR, "volguard_14_journal.csv")
    ]:
        if not os.path.exists(file):
            try:
                open(file, 'a').close()
            except IOError as e:
                print(f"WARNING: Could not create file {file}. Error: {e}")

    # Configuration
    ENV = os.getenv("ENV", "production")
    PORT = int(os.getenv("PORT", 8000))

    # Run the FastAPI server
    uvicorn.run(
        "api.routes:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        reload=(ENV == "development"),
        loop="uvloop" if sys.platform != "win32" else "asyncio"
    )
