import asyncio
import sys, os
from sqlalchemy import text
sys.path.append(os.getcwd())
from database.manager import HybridDatabaseManager
from database.models_risk import Base

async def init():
    print("Initializing Risk DB & Indexes...")
    db = HybridDatabaseManager()
    await db.init_db()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # R4: Ensure GIN extension exists for JSONB indexing
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin;"))
    print("✅ Schema & Indexes Applied.")

if __name__ == "__main__": asyncio.run(init())
