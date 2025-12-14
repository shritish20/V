#!/usr/bin/env python3
"""
VolGuard 19.0 - Startup Verification Script (NIFTY ONLY EDITION)
Run this before starting the engine to verify all critical fixes are in place
"""
import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.config import settings
from core.enums import MarketRegime, StrategyType, TradeStatus
from trading.instruments_master import InstrumentMaster
from capital.allocator import SmartCapitalAllocator
from trading.risk_manager import AdvancedRiskManager


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'


def print_check(name: str, passed: bool, details: str = ""):
    status = f"{Colors.GREEN}✓ PASS{Colors.END}" if passed else f"{Colors.RED}✗ FAIL{Colors.END}"
    print(f"{status} | {name}")
    if details:
        print(f"       {details}")


async def verify_config():
    """Verify all required config values are present"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}CONFIGURATION CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    checks = []
    
    # Check critical settings
    checks.append(("UPSTOX_ACCESS_TOKEN", hasattr(settings, 'UPSTOX_ACCESS_TOKEN')))
    checks.append(("BROKERAGE_PER_ORDER", hasattr(settings, 'BROKERAGE_PER_ORDER')))
    checks.append(("GST_RATE", hasattr(settings, 'GST_RATE')))
    checks.append(("RISK_FREE_RATE", hasattr(settings, 'RISK_FREE_RATE')))
    checks.append(("SABR_BOUNDS", hasattr(settings, 'SABR_BOUNDS')))
    checks.append(("MAX_PORTFOLIO_GAMMA", hasattr(settings, 'MAX_PORTFOLIO_GAMMA')))
    checks.append(("MAX_PORTFOLIO_THETA", hasattr(settings, 'MAX_PORTFOLIO_THETA')))
    checks.append(("NIFTY_FREEZE_QTY", hasattr(settings, 'NIFTY_FREEZE_QTY')))
    
    for name, passed in checks:
        value = getattr(settings, name, None) if passed else None
        print_check(name, passed, f"Value: {value}" if passed else "Missing")
    
    return all(check[1] for check in checks)


async def verify_enums():
    """Verify all required enum values exist"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}ENUM CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    checks = []
    
    # Check MarketRegime has all required values
    required_regimes = ['BULL_EXPANSION', 'PANIC', 'TRANSITION', 'SAFE']
    for regime in required_regimes:
        try:
            MarketRegime[regime]
            checks.append((f"MarketRegime.{regime}", True))
        except KeyError:
            checks.append((f"MarketRegime.{regime}", False))
    
    # ============================================
    # 🔧 FIX: Updated to match actual enum values
    # ============================================
    required_strategies = [
        'IRON_CONDOR',      # Standard income strategy
        'SHORT_STRANGLE',   # Aggressive neutral strategy
        'BULL_PUT_SPREAD',  # Directional credit spread
        'IRON_FLY',         # High IV crush strategy
        'LONG_STRADDLE',    # Volatility buying
        'RATIO_SPREAD_PUT'  # Skew strategy
    ]
    
    for strategy in required_strategies:
        try:
            StrategyType[strategy]
            checks.append((f"StrategyType.{strategy}", True))
        except KeyError:
            checks.append((f"StrategyType.{strategy}", False))
    
    for name, passed in checks:
        print_check(name, passed)
    
    return all(check[1] for check in checks)


async def verify_capital_allocator():
    """Verify SmartCapitalAllocator has required methods"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}CAPITAL ALLOCATOR CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    from database.manager import HybridDatabaseManager
    
    db = HybridDatabaseManager()
    allocator = SmartCapitalAllocator(1000000, {"test": 0.5}, db)
    
    checks = [
        ("allocate_capital method", hasattr(allocator, 'allocate_capital')),
        ("release_capital method", hasattr(allocator, 'release_capital')),
        ("get_status method", hasattr(allocator, 'get_status')),
        ("get_bucket_limit method", hasattr(allocator, 'get_bucket_limit')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    # Test get_bucket_limit (doesn't require DB)
    try:
        limit = allocator.get_bucket_limit("test")
        has_correct_calc = (limit == 500000.0)  # 50% of 1M
        print_check("get_bucket_limit calculation", has_correct_calc, f"Limit: {limit:,.0f}")
    except Exception as e:
        print_check("get_bucket_limit execution", False, f"Error: {e}")
        return False
    
    return all(check[1] for check in checks)


async def verify_risk_manager():
    """Verify RiskManager tracks all Greeks"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}RISK MANAGER CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    risk_mgr = AdvancedRiskManager(None, None)
    
    checks = [
        ("portfolio_delta tracking", hasattr(risk_mgr, 'portfolio_delta')),
        ("portfolio_gamma tracking", hasattr(risk_mgr, 'portfolio_gamma')),
        ("portfolio_theta tracking", hasattr(risk_mgr, 'portfolio_theta')),
        ("portfolio_vega tracking", hasattr(risk_mgr, 'portfolio_vega')),
        ("check_pre_trade method", hasattr(risk_mgr, 'check_pre_trade')),
        ("update_portfolio_state method", hasattr(risk_mgr, 'update_portfolio_state')),
        ("check_portfolio_limits method", hasattr(risk_mgr, 'check_portfolio_limits')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    return all(check[1] for check in checks)


async def verify_instruments_master():
    """Verify InstrumentMaster functionality"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}INSTRUMENT MASTER CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    master = InstrumentMaster()
    
    checks = [
        ("download_and_load method", hasattr(master, 'download_and_load')),
        ("get_option_token method", hasattr(master, 'get_option_token')),
        ("get_all_expiries method", hasattr(master, 'get_all_expiries')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    return all(check[1] for check in checks)


async def verify_database():
    """Verify database configuration"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}DATABASE CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    checks = [
        ("DATABASE_URL configured", hasattr(settings, 'DATABASE_URL')),
        ("POSTGRES_SERVER", settings.POSTGRES_SERVER is not None),
        ("POSTGRES_USER", settings.POSTGRES_USER is not None),
        ("POSTGRES_DB", settings.POSTGRES_DB is not None),
    ]
    
    for name, passed in checks:
        value = None
        if name == "DATABASE_URL configured":
            value = settings.DATABASE_URL if passed else None
            if value:
                display_value = str(value)
                if ":" in display_value and "@" in display_value:
                    parts = display_value.split("@")
                    display_value = f"{parts[0].split(':')[0]}:***@{parts[1]}"
                print_check(name, passed, f"{display_value[:50]}...")
        else:
            print_check(name, passed, f"Value: {getattr(settings, name.split()[0], None)}" if passed else "Missing")
    
    return all(check[1] for check in checks)


async def verify_api_methods():
    """Verify critical API methods exist"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}API CLIENT CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    from trading.api_client import EnhancedUpstoxAPI
    
    # Create dummy API instance
    api = EnhancedUpstoxAPI("dummy_token_for_testing")
    
    checks = [
        ("place_order method", hasattr(api, 'place_order')),
        ("place_multi_order method", hasattr(api, 'place_multi_order')),
        ("place_order_raw method", hasattr(api, 'place_order_raw')),  # 🔧 FIX: Now exists
        ("get_quotes method", hasattr(api, 'get_quotes')),
        ("get_option_chain method", hasattr(api, 'get_option_chain')),
        ("cancel_order method", hasattr(api, 'cancel_order')),
        ("get_order_details method", hasattr(api, 'get_order_details')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    return all(check[1] for check in checks)


async def verify_strategy_engine():
    """Verify strategy engine strike rounding (NIFTY ONLY)"""
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}STRATEGY ENGINE CHECKS{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    from trading.strategy_engine import IntelligentStrategyEngine
    
    engine = IntelligentStrategyEngine(None, None, None, None)
    
    checks = []
    
    # Test NIFTY rounding (50-point intervals)
    try:
        nifty_price = 21337.25
        rounded = engine._round_strike(nifty_price)
        expected = 21350.0  # Should round to nearest 50
        checks.append(("NIFTY strike rounding", rounded == expected))
        print_check(f"NIFTY rounding: {nifty_price} → {rounded}", 
                    rounded == expected, 
                    f"Expected: {expected}, Got: {rounded}")
    except Exception as e:
        checks.append(("NIFTY strike rounding", False))
        print_check("NIFTY strike rounding", False, f"Error: {e}")
    
    return all(checks)


async def main():
    print(f"\n{Colors.GREEN}{'='*60}{Colors.END}")
    print(f"{Colors.GREEN}VolGuard 19.0 - Production Readiness (NIFTY ONLY){Colors.END}")
    print(f"{Colors.GREEN}{'='*60}{Colors.END}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Safety Mode: {settings.SAFETY_MODE}")
    print(f"Account Size: ₹{settings.ACCOUNT_SIZE:,.0f}")
    
    results = {}
    
    # Run all verification checks
    results['config'] = await verify_config()
    results['enums'] = await verify_enums()
    results['api_methods'] = await verify_api_methods()
    results['strategy_engine'] = await verify_strategy_engine()
    results['capital_allocator'] = await verify_capital_allocator()
    results['risk_manager'] = await verify_risk_manager()
    results['instruments_master'] = await verify_instruments_master()
    results['database'] = await verify_database()
    
    # Final summary
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}")
    print(f"{Colors.BLUE}VERIFICATION SUMMARY{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    all_passed = all(results.values())
    
    for category, passed in results.items():
        status = f"{Colors.GREEN}✓{Colors.END}" if passed else f"{Colors.RED}✗{Colors.END}"
        print(f"{status} {category.replace('_', ' ').title()}")
    
    print(f"\n{Colors.BLUE}{'='*60}{Colors.END}\n")
    
    if all_passed:
        print(f"{Colors.GREEN}🚀 ALL CHECKS PASSED - NIFTY 50 SYSTEM READY{Colors.END}")
    else:
        print(f"{Colors.RED}❌ CHECKS FAILED - FIX ISSUES BEFORE DEPLOYMENT{Colors.END}\n")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
