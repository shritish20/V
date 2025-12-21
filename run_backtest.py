import asyncio
from core.config import settings
from trading.api_client import EnhancedUpstoxAPI
from utils.data_fetcher import DashboardDataFetcher
from backtesting.vectorized_backtester import VectorizedBacktester

async def main():
    print("🚀 Initializing Backtest Engine...")
    
    # 1. Setup Data Layer
    api = EnhancedUpstoxAPI(settings.UPSTOX_ACCESS_TOKEN)
    fetcher = DashboardDataFetcher(api)
    
    # 2. Fetch Live/History Data
    await fetcher.load_all_data()
    
    # 3. Run Backtest
    bt = VectorizedBacktester(fetcher)
    await bt.run_analysis(days=365)
    
    await api.close()

if __name__ == "__main__":
    asyncio.run(main())
