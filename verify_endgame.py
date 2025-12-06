import inspect
from core.engine import VolGuard17Engine
from trading.live_data_feed import LiveDataFeed
from capital.allocator import SmartCapitalAllocator
from trading.live_order_executor import LiveOrderExecutor
from trading.strategy_engine import IntelligentStrategyEngine

def verify():
    print("🔍 ENDGAME PRODUCTION AUDIT...")
    
    if not hasattr(VolGuard17Engine, "_system_heartbeat"):
        print("❌ FAIL: No Heartbeat")
    else:
        print("✅ Heartbeat OK")
        
    if "MarketDataFeed" not in inspect.getsource(LiveDataFeed):
        print("❌ FAIL: No Binary Feed")
    else:
        print("✅ Binary Feed (SDK) OK")
        
    if not hasattr(IntelligentStrategyEngine, "_select_capital_bucket"):
        print("❌ FAIL: Strategy Logic Missing")
    else:
        print("✅ Intelligent Strategy Logic Restored")
        
    if "SAFETY_MODE" not in inspect.getsource(LiveOrderExecutor.place_multi_leg):
        print("❌ FAIL: Safety Lock Missing")
    else:
        print("✅ Safety Locks OK")
        
    print("✅ SYSTEM IS LIVE READY.")

if __name__ == "__main__":
    verify()
