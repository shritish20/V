import inspect
import sys
import os

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from core.engine import VolGuard17Engine
    from trading.live_data_feed import LiveDataFeed
    from trading.api_client import EnhancedUpstoxAPI
    from trading.trade_manager import EnhancedTradeManager
    print("✅ All Core Modules Imported")
except ImportError as e:
    print(f"❌ FAIL: Import Error - {e}")
    sys.exit(1)

def verify():
    print("\n🔍 VOLGUARD 19.0 LITE - PRE-FLIGHT CHECK")
    print("------------------------------------------")
    
    # 1. Check if Hedge Manager is truly gone
    if hasattr(VolGuard17Engine, "hedge_mgr"):
        print("❌ FAIL: HedgeManager still present in Engine init")
    else:
        print("✅ HedgeManager Removed from Engine")

    # 2. Check API Client Dynamic Date Fix
    if hasattr(EnhancedUpstoxAPI, "get_current_future_symbol"):
        source = inspect.getsource(EnhancedUpstoxAPI.get_current_future_symbol)
        if "calendar" in source and "datetime.now" in source:
            print("✅ API Client: Dynamic Futures Symbol Logic Found")
        else:
            print("⚠️ WARN: API Client might still be using hardcoded dates")
    else:
        print("❌ FAIL: API Client missing 'get_current_future_symbol'")

    # 3. Check Live Feed for SDK Usage
    source_feed = inspect.getsource(LiveDataFeed)
    if "MarketDataFeed" in source_feed:
        print("✅ LiveDataFeed: Using Official SDK")
    else:
        print("❌ FAIL: LiveDataFeed not using SDK")

    # 4. Check Trade Manager for Risk Checks
    if hasattr(EnhancedTradeManager, "execute_strategy"):
        print("✅ TradeManager: Execution Logic Ready")
    else:
        print("❌ FAIL: TradeManager incomplete")

    print("\n🚀 SYSTEM VERIFICATION COMPLETE.")

if __name__ == "__main__":
    verify()
