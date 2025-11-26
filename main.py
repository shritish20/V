import uvicorn
import os
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent))

if __name__ == "__main__":
    print("🚀 VOLGUARD HYBRID ULTIMATE - FASTAPI EDITION")
    print("✅ MODULAR ARCHITECTURE")
    print("✅ PRODUCTION-GRADE API")
    print("✅ COMPREHENSIVE RISK MANAGEMENT")
    print("🎯 READY FOR LIVE TRADING")
    
    # Create required files
    for file in ["volguard_hybrid.db", "volguard_hybrid_log.txt", "volguard_hybrid_journal.csv"]:
        if not os.path.exists(file):
            open(file, 'a').close()
    
    # Run the FastAPI server
    uvicorn.run(
        "api.routes:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
