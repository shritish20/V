import pytest
import sys
import os

def run_comprehensive_suite():
    print("🛡️ VOLGUARD 19.0 TEST SUITE")
    print("===========================")
    
    # Run pytest programmatically
    # -v: verbose
    # -s: show print output
    # --disable-warnings: clean output
    args = ["-v", "-s", "--disable-warnings", "tests/"]
    
    result = pytest.main(args)
    
    if result == 0:
        print("\n✅ ALL SYSTEMS GO. Ready for Deployment.")
    else:
        print("\n❌ TESTS FAILED. Do NOT Deploy.")
        sys.exit(1)

if __name__ == "__main__":
    # Ensure we can import core modules
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    run_comprehensive_suite()
