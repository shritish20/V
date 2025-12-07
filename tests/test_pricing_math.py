import pytest
from datetime import datetime, timedelta
from analytics.pricing import HybridPricingEngine
from analytics.sabr_model import EnhancedSABRModel
from core.config import settings

@pytest.fixture
def engine():
    return HybridPricingEngine(EnhancedSABRModel())

def test_expiry_day_math_safety(engine):
    """
    CRITICAL: Verify math doesn't blow up when time_to_expiry is near zero.
    """
    # 1. Setup Scenario: Thursday 3:29 PM (1 minute to close)
    expiry = datetime.now(settings.IST).date().strftime("%Y-%m-%d")
    spot = 20000
    strike = 20000
    
    # 2. Run Calc
    greeks = engine.calculate_greeks(spot, strike, "CE", expiry)
    
    # 3. Assertions
    assert greeks.delta is not None
    assert greeks.gamma < 1000000 # Should not be infinite
    assert greeks.theta != 0 # Should still have decay
    print(f"✅ 0DTE Math Check: Delta={greeks.delta:.2f}, Gamma={greeks.gamma:.4f}")

def test_expired_option_safety(engine):
    """
    Verify expired options return 0 instead of crashing.
    """
    # 1. Setup: Yesterday
    yesterday = (datetime.now(settings.IST) - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 2. Run Calc
    greeks = engine.calculate_greeks(20000, 20000, "CE", yesterday)
    
    # 3. Assertions
    assert greeks.delta == 0.0
    assert greeks.vega == 0.0
    print("✅ Expired Option handled correctly (Zero Greeks)")
