import pytest, time
from core.engine import VolGuard20Engine, StaleDataError

@pytest.mark.asyncio
async def test_stale_data_raises_exception():
    engine = VolGuard20Engine()
    engine.rt_quotes['TEST'] = {'ltp': 21000.0, 'last_updated': time.time()-10}
    with pytest.raises(StaleDataError):
        engine._get_safe_price('TEST')

@pytest.mark.asyncio  
async def test_missing_data_raises_exception():
    engine = VolGuard20Engine()
    with pytest.raises(StaleDataError, match="No market data"):
        engine._get_safe_price('MISSING')
