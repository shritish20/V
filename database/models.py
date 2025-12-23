#!/usr/bin/env python3
"""
VolGuard 20.0 – Database Models (Fortress Edition)
- Includes Token State for OAuth Management
- Includes Margin History for Sanity Checks
- Includes Market Snapshot for Quant Dashboard
- Uses SQLAlchemy 2.0 Type-Safe Syntax
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
# Authentication & Token Management (NEW)
# ---------------------------------------------------------------------------
class DbTokenState(Base):
    """
    Stores the active Upstox access token and refresh token.
    Used by the TokenManager to persist authentication across restarts.
    """
    __tablename__ = "token_state"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    access_token: Mapped[str] = mapped_column(String, nullable=False)
    refresh_token: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_refreshed: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# ---------------------------------------------------------------------------
# Risk & Analytics Tables
# ---------------------------------------------------------------------------
class DbMarginHistory(Base):
    """
    Tracks real margin requirements from Upstox API.
    Used by MarginGuard to 'Sanity Check' fallback calculations during high VIX.
    """
    __tablename__ = "margin_history"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_type: Mapped[str] = mapped_column(String, nullable=False)
    lots: Mapped[int] = mapped_column(Integer, default=1)
    required_margin: Mapped[float] = mapped_column(Float, nullable=False)
    vix_at_calc: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

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

# ---------------------------------------------------------------------------
# NEW: Market Snapshot for Quant Dashboard
# ---------------------------------------------------------------------------
class DbMarketSnapshot(Base):
    """
    Stores the full 'Quant Matrix' from the Engine/Script.
    The API reads this to populate the 'Live Feed' tab on the Dashboard.
    """
    __tablename__ = "market_snapshot"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # --- 1. CORE PRICES ---
    spot_price: Mapped[float] = mapped_column(Float)
    vix: Mapped[float] = mapped_column(Float)
    
    # --- 2. IV TERM STRUCTURE ---
    atm_iv_weekly: Mapped[float] = mapped_column(Float)
    atm_iv_monthly: Mapped[float] = mapped_column(Float)
    iv_spread: Mapped[float] = mapped_column(Float)  # Weekly - Monthly
    term_structure_tag: Mapped[str] = mapped_column(String) # "Backwardation" / "Contango"
    
    # --- 3. QUANT MODELS ---
    rv_7d: Mapped[float] = mapped_column(Float)
    garch_vol_7d: Mapped[float] = mapped_column(Float)
    egarch_vol_1d: Mapped[float] = mapped_column(Float)
    iv_percentile: Mapped[float] = mapped_column(Float)
    
    # --- 4. VRP SIGNALS ---
    vrp_spread: Mapped[float] = mapped_column(Float) # IV - RV
    vrp_zscore: Mapped[float] = mapped_column(Float)
    vrp_verdict: Mapped[str] = mapped_column(String) # "STRONG SELL", "NEUTRAL", etc.
    
    # --- 5. EXECUTION CONTEXT ---
    straddle_cost_weekly: Mapped[float] = mapped_column(Float)
    straddle_cost_monthly: Mapped[float] = mapped_column(Float)
    breakeven_lower: Mapped[float] = mapped_column(Float)
    breakeven_upper: Mapped[float] = mapped_column(Float)
    
    # --- 6. THE MATRIX (Option Chain) ---
    # Stores the full chain metrics (efficiency table or full chain) as JSON list
    chain_json: Mapped[List[Dict[str, Any]]] = mapped_column(JSON)
