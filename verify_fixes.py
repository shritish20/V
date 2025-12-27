#!/usr/bin/env python3
"""
VolGuard 20.0 – **One-shot verification**
Run:  python verify_fixes.py
"""
import asyncio, sys, os, time, requests, json
from datetime import datetime as dt
sys.path.insert(0, os.getcwd())

from core.engine      import VolGuard20Engine, StaleDataError
from core.metrics     import get_metrics
from capital.allocator import SmartCapitalAllocator
from database.manager import HybridDatabaseManager
from core.config      import settings

PASS, FAIL = 0, 1
metrics = get_metrics()

async def test_stale_data_raises():
    """Fix #1 – stale data must raise, not return 0.0"""
    engine = VolGuard20Engine()
    engine.rt_quotes['TEST'] = {'ltp': 21000.0, 'last_updated': time.time()-10}
    try:
        price = engine._get_safe_price('TEST')
        print("❌ stale-data did NOT raise")
        return FAIL
    except StaleDataError:
        print("✅ stale-data raises StaleDataError")
        return PASS

async def test_rollback_metric():
    """Fix #2 – rollback increments counter"""
    before = metrics.rollback_attempts
    metrics.log_rollback("TEST-ROLL", 4, success=True)
    after  = metrics.rollback_attempts
    if after == before+1:
        print("✅ rollback metric increments")
        return PASS
    print("❌ rollback metric broken")
    return FAIL

async def test_capital_lock():
    """Fix #3 – concurrent alloc uses SELECT FOR UPDATE"""
    db  = HybridDatabaseManager()
    await db.init_db()
    alloc = SmartCapitalAllocator(1_000_000, {"WEEKLY": 0.5}, db)
    # two coroutines try to grab the same 600k
    tasks = [
        alloc.allocate_capital("WEEKLY", 600_000, "T1"),
        alloc.allocate_capital("WEEKLY", 600_000, "T2")
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # exactly one must succeed
    ok = sum(1 for r in results if r is True)
    if ok == 1:
        print("✅ capital lock prevents double-spend")
        return PASS
    print("❌ capital race condition")
    return FAIL

async def test_order_id_uniqueness():
    """Fix #4 – 1 000 000 ids must be unique"""
    from trading.live_order_executor import LiveOrderExecutor
    seen = set()
    for i in range(1_000_000):
        oid = LiveOrderExecutor._client_order_id("T","HEDGE",0,i)
        if oid in seen:
            print("❌ order-id collision")
            return FAIL
        seen.add(oid)
    print("✅ order-id unique over 1M samples")
    return PASS

async def test_margin_zero_div():
    """Fix #5 – zero legs / zero qty must not div-by-zero"""
    from trading.margin_guard import MarginGuard
    from core.models import MultiLegTrade, Position
    mg = MarginGuard(None, None)
    empty_trade = MultiLegTrade(legs=[], strategy_type="IRON_CONDOR", id="TEST", status="PENDING", entry_time=dt.utcnow(), expiry_date="2025-12-31", expiry_type="WEEKLY", capital_bucket="WEEKLY")
    ok, req = await mg._enhanced_fallback_margin(empty_trade, 20)
    if not ok and req == float('inf'):
        print("✅ margin guard handles empty trade")
        return PASS
    print("❌ margin zero-div guard missing")
    return FAIL

async def main():
    tests = [
        test_stale_data_raises,
        test_rollback_metric,
        test_capital_lock,
        test_order_id_uniqueness,
        test_margin_zero_div,
    ]
    results = await asyncio.gather(*(t() for t in tests))
    sys.exit(0 if all(r==PASS for r in results) else 1)

if __name__ == "__main__":
    asyncio.run(main())
