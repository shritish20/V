from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON, Date, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime
from database.manager import Base

class DbStrategy(Base):
    __tablename__ = "strategies"
    id = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    entry_time = Column(DateTime, default=datetime.utcnow)
    exit_time = Column(DateTime, nullable=True)
    capital_bucket = Column(String)
    pnl = Column(Float, default=0.0)
    expiry_date = Column(Date, nullable=True)
    broker_ref_id = Column(String, nullable=True)
    metadata_json = Column(JSON)
    orders = relationship("DbOrder", back_populates="strategy", cascade="all, delete-orphan")

class DbOrder(Base):
    __tablename__ = "orders"
    order_id = Column(String, primary_key=True)
    strategy_id = Column(String, ForeignKey("strategies.id"))
    instrument_token = Column(String, nullable=False)
    transaction_type = Column(String)
    quantity = Column(Integer)
    price = Column(Float)
    status = Column(String)
    filled_qty = Column(Integer, default=0)
    avg_price = Column(Float, default=0.0)
    tag = Column(String)
    placed_at = Column(DateTime, default=datetime.utcnow)
    strategy = relationship("DbStrategy", back_populates="orders")

class DbCapitalUsage(Base):
    __tablename__ = "capital_usage"
    bucket = Column(String, primary_key=True)
    used_amount = Column(Float, default=0.0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# --- NEW TABLE FOR KIMI'S ALLOCATOR ---
class DbCapitalLedger(Base):
    """Tracks individual allocations for Idempotency and SOD Balance"""
    __tablename__ = "capital_ledger"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_id = Column(String, index=True) # "SOD" for Start-of-Day balance
    bucket = Column(String)
    amount = Column(Float)
    date = Column(Date, default=datetime.utcnow)
    timestamp = Column(DateTime, default=datetime.utcnow)

class DbTradeJournal(Base):
    __tablename__ = "trade_journal"
    id = Column(String, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow)
    # Context
    strategy_name = Column(String)
    regime_at_entry = Column(String)
    vix_at_entry = Column(Float)
    spot_at_entry = Column(Float)
    # Intelligence
    ai_analysis_json = Column(JSON)
    entry_rationale = Column(String)
    # Financials
    gross_pnl = Column(Float, default=0.0)
    total_charges = Column(Float, default=0.0)
    net_pnl = Column(Float, default=0.0)
    # Status
    is_reconciled = Column(Boolean, default=False)
    slippage = Column(Float, default=0.0)
