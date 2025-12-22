#!/usr/bin/env python3
"""
VolGuard 20.0 – Database Models (Hardened)
- SQLAlchemy 2.0 Type-Safe Syntax
- Unique Constraints for Allocation Idempotency
- Indexed for High-Performance Queries
- Includes Process Communication Tables (RiskState, MarketContext)
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Integer, String, Float, DateTime, ForeignKey, JSON, Date, Boolean,
    UniqueConstraint, Index,
)
from sqlalchemy.orm import relationship, DeclarativeBase, Mapped, mapped_column

# ---------------------------------------------------------------------------
# Base – SQLAlchemy 2.0 style
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass

# ---------------------------------------------------------------------------
# Core Trading Tables
# ---------------------------------------------------------------------------
class DbStrategy(Base):
    __tablename__ = "strategies"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    entry_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    exit_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    capital_bucket: Mapped[str] = mapped_column(String)
    pnl: Mapped[float] = mapped_column(Float, default=0.0)
    expiry_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    broker_ref_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    metadata_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)

    orders: Mapped[List["DbOrder"]] = relationship(back_populates="strategy", cascade="all, delete-orphan")

class DbOrder(Base):
    __tablename__ = "orders"
    
    order_id: Mapped[str] = mapped_column(String, primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String, ForeignKey("strategies.id"))
    instrument_token: Mapped[str] = mapped_column(String, nullable=False)
    transaction_type: Mapped[str] = mapped_column(String)
    quantity: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String)
    filled_qty: Mapped[int] = mapped_column(Integer, default=0)
    avg_price: Mapped[float] = mapped_column(Float, default=0.0)
    tag: Mapped[Optional[str]] = mapped_column(String)
    placed_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    strategy: Mapped["DbStrategy"] = relationship(back_populates="orders")

# ---------------------------------------------------------------------------
# Capital Management Tables (The "Hardened" Layer)
# ---------------------------------------------------------------------------
class DbCapitalUsage(Base):
    """Real-time view of used capital per bucket."""
    __tablename__ = "capital_usage"
    
    bucket: Mapped[str] = mapped_column(String, primary_key=True)
    used_amount: Mapped[float] = mapped_column(Float, default=0.0)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

class DbCapitalLedger(Base):
    """
    Historical Ledger for Idempotency.
    Constraint: A Trade ID cannot allocate from the same bucket twice.
    """
    __tablename__ = "capital_ledger"
    __table_args__ = (
        UniqueConstraint("trade_id", "bucket", name="uq_trade_bucket"),
        Index("ix_capledger_bucket_date", "bucket", "date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String, index=True) 
    bucket: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    date: Mapped[date] = mapped_column(Date, default=date.today, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class DbStartOfDayMargin(Base):
    """
    Snapshots start-of-day margin for accurate Drawdown calculations.
    """
    __tablename__ = "sod_margin"
    __table_args__ = (UniqueConstraint("date", name="uq_sod_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, unique=True, index=True)
    margin: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# ---------------------------------------------------------------------------
# Journal & Analytics
# ---------------------------------------------------------------------------
class DbTradeJournal(Base):
    __tablename__ = "trade_journal"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    strategy_name: Mapped[Optional[str]] = mapped_column(String)
    regime_at_entry: Mapped[Optional[str]] = mapped_column(String)
    vix_at_entry: Mapped[Optional[float]] = mapped_column(Float)
    spot_at_entry: Mapped[Optional[float]] = mapped_column(Float)
    ai_analysis_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    entry_rationale: Mapped[Optional[str]] = mapped_column(String)
    gross_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    total_charges: Mapped[float] = mapped_column(Float, default=0.0)
    net_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    is_reconciled: Mapped[bool] = mapped_column(Boolean, default=False)
    slippage: Mapped[float] = mapped_column(Float, default=0.0)

# ---------------------------------------------------------------------------
# Process Communication (The Nervous System)
# ---------------------------------------------------------------------------
class DbRiskState(Base):
    """
    Shared memory between Sheriff (Process 3) and Core Engine (Process 1).
    Tracks PnL, Equity, and Kill Switch status.
    """
    __tablename__ = "risk_state"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # HARDENING: Proof of Life (Engine checks this to ensure Sheriff is alive)
    sheriff_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # HARDENING: Equity Tracking (Realized + Unrealized)
    sod_equity: Mapped[float] = mapped_column(Float, default=0.0) # Start of Day
    current_equity: Mapped[float] = mapped_column(Float, default=0.0)
    drawdown_pct: Mapped[float] = mapped_column(Float, default=0.0)
    
    # STATE FLAGS
    kill_switch_active: Mapped[bool] = mapped_column(Boolean, default=False)
    is_flattening: Mapped[bool] = mapped_column(Boolean, default=False) 
    flatten_order_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # Added real_time_pnl for Dashboard compat (Optional)
    real_time_pnl: Mapped[Optional[float]] = mapped_column(Float, default=0.0)

class DbMarketContext(Base):
    """
    Shared memory between AI Analyst (Process 2) and Core Engine (Process 1).
    Stores the 'Brain's' view of the market.
    """
    __tablename__ = "market_context"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Analysis
    regime: Mapped[str] = mapped_column(String) # "SAFE", "PANIC", "VOLATILE"
    ai_narrative: Mapped[str] = mapped_column(String) 
    is_high_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # HARDENING: Stale Data Protection (True if AI is online and fresh)
    is_fresh: Mapped[bool] = mapped_column(Boolean, default=True)
