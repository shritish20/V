import uvicorn
import os
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

if __name__ == "__main__":
    print("🚀 VOLGUARD 14.00 - IRONCLAD ARCHITECTURE")
    print("✅ PRODUCTION-GRADE TRADING ENGINE")
    print("✅ ADVANCED VOLATILITY ANALYTICS")
    print("✅ COMPREHENSIVE RISK MANAGEMENT")
    print("✅ REAL-TIME ANALYTICS & ALERTS")
    print("✅ PRODUCTION MONITORING & METRICS")
    print("🎯 READY FOR LIVE DEPLOYMENT")

    # Create persistent storage directory - RENDER COMPATIBLE
    PERSISTENT_DIR = os.getenv("PERSISTENT_DATA_DIR", "/tmp/data")
    Path(PERSISTENT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create required files with better error handling
    for file in [
        os.path.join(PERSISTENT_DIR, "volguard_14.db"), 
        os.path.join(PERSISTENT_DIR, "volguard_14_log.txt"), 
        os.path.join(PERSISTENT_DIR, "volguard_14_journal.csv")
    ]:
        try:
            # Use pathlib for better file creation
            Path(file).touch(exist_ok=True)
            print(f"✅ Created file: {file}")
        except Exception as e:
            print(f"⚠️ WARNING: Could not create file {file}. Error: {e}")
            # Continue anyway - some files might not be critical

    # Configuration
    ENV = os.getenv("ENV", "production")
    PORT = int(os.getenv("PORT", 8000))

    # Run the FastAPI server - SIMPLE VERSION (NO UVLOOP)
    print(f"🚀 Starting VolGuard 14.00 on port {PORT}...")
    uvicorn.run(
        "api.routes:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info"
    )
