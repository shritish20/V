import uvicorn
import os
import sys
from pathlib import Path
import asyncio

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

if __name__ == "__main__":
    print("🚀 VOLGUARD HYBRID ULTIMATE - FASTAPI EDITION")
    print("✅ MODULAR ARCHITECTURE")
    print("✅ PRODUCTION-GRADE API")
    print("✅ COMPREHENSIVE RISK MANAGEMENT")
    print("🎯 READY FOR LIVE TRADING")

    # CRITICAL FIX 1: Use persistent storage path
    PERSISTENT_DIR = os.getenv("PERSISTENT_DATA_DIR", "./data")
    Path(PERSISTENT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create required files in the persistent directory
    for file in [
        os.path.join(PERSISTENT_DIR, "volguard_hybrid.db"), 
        os.path.join(PERSISTENT_DIR, "volguard_hybrid_log.txt"), 
        os.path.join(PERSISTENT_DIR, "volguard_hybrid_journal.csv")
    ]:
        if not os.path.exists(file):
            try:
                open(file, 'a').close()
            except IOError as e:
                print(f"WARNING: Could not create file {file}. Error: {e}")

    # Run the FastAPI server using the PORT environment variable
    uvicorn.run(
        "api.routes:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        log_level="info",
    )
