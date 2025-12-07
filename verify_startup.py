#!/usr/bin/env python3
"""
VolGuard 19.0 - Startup Verification Script
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
        print(f"      {details}")

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
    required_regimes = ['BULL_EXPANSION', 'PANIC', 'TRANSITION']
    for regime in required_regimes:
        try:
            MarketRegime[regime]
            checks.append((f"MarketRegime.{regime}", True))
        except KeyError:
            checks.append((f"MarketRegime.{regime}", False))
    
    # Check StrategyType has all required values
    required_strategies = ['DEFENSIVE_IRON_CONDOR', 'SHORT_STRANGLE', 'BULL_PUT_SPREAD']
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
    
    allocator = SmartCapitalAllocator(1000000, {"test": 0.5})
    
    checks = [
        ("allocate_capital method", hasattr(allocator, 'allocate_capital')),
        ("release_capital method", hasattr(allocator, 'release_capital')),
        ("get_status method", hasattr(allocator, 'get_status')),
        ("get_bucket_limit method", hasattr(allocator, 'get_bucket_limit')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    # Test get_status method
    try:
        status = allocator.get_status()
        has_required_keys = all(k in status for k in ['available', 'used', 'limit'])
        print_check("get_status returns correct structure", has_required_keys,
                   f"Keys: {list(status.keys())}")
    except Exception as e:
        print_check("get_status execution", False, f"Error: {e}")
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
        ("get_current_future method", hasattr(master, 'get_current_future')),
        ("get_all_expiries method", hasattr(master, 'get_all_expiries')),
    ]
    
    for name, passed in checks:
        print_check(name, passed)
    
    # Test download (optional - comment out if you don't want to test now)
    # print(f"\n{Colors.YELLOW}Testing Instrument Download (may take 60s)...{Colors.END}")
    # try:
    #     await master.download_and_load()
    #     has_data = master.df is not None and len(master.df) > 0
    #     print_check("Instrument download successful", has_data, 
    #                f"Loaded {len(master.df) if master.df is not None else 0} contracts")
    #     checks.append(("Instrument download", has_data))
    # except Exception as e:
    #     print_check("Instrument download", False, f"Error: {e}")
    #     checks.append(("Instrument download", False))
    
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
        print_check(name, passed, f"{value[:50]}..." if value and len(value) > 50 else str(value))
    
    return all(check[1] for check in checks)

async def main():
    print(f"\n{Colors.GREEN}{'='*60}{Colors.END}")
    print(f"{Colors.GREEN}VolGuard 19.0 - Pre-Flight Verification{Colors.END}")
    print(f"{Colors.GREEN}{'='*60}{Colors.END}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Safety Mode: {settings.SAFETY_MODE}")
    
    results = {}
    
    results['config'] = await verify_config()
    results['enums'] = await verify_enums()
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
        print(f"{Colors.GREEN}🚀 ALL CHECKS PASSED - READY FOR LAUNCH{Colors.END}\n")
        return 0
    else:
        print(f"{Colors.RED}❌ SOME CHECKS FAILED - FIX ISSUES BEFORE LAUNCH{Colors.END}\n")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
