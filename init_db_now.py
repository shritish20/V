#!/usr/bin/env python3
"""
VolGuard 20.0 - Database Initializer
Run this ONCE to create all tables in your PostgreSQL/SQLite database.
"""
import asyncio
import os
import sys

# Ensure we can find the core modules
sys.path.append(os.getcwd())

from database.manager import HybridDatabaseManager
from database.models import Base
from core.config import settings

async def init_tables():
    print(f"🚀 Initializing Database: {settings.POSTGRES_DB}")
    print(f"📍 URL: {settings.DATABASE_URL}")
    
    db = HybridDatabaseManager()
    
    try:
        # This function creates tables based on the models in database/models.py
        await db.init_db()
        print("✅ Tables Created Successfully!")
        print("   - strategies")
        print("   - orders")
        print("   - capital_usage")
        print("   - capital_ledger")
        print("   - risk_state")
        print("   - market_context")
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(init_tables())
