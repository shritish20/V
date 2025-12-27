#!/usr/bin/env python3
"""
VolGuard 20.0 – Database Models (Fortress Edition)
- Includes Token State for OAuth Management
- Includes Margin History for Sanity Checks
- Includes Market Snapshot for Quant Dashboard
- Includes Historical Candles for Data Persistence (NEW)
"""
from __future__ import annotations
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    Integer, String, Float, DateTime, ForeignKey, JSON, Date, Boolean,
    UniqueConstraint, Index,
)
from sqlalchemy.orm import relationship, DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

# --- CORE TRADING TABLES ---
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

# --- CAPITAL MANAGEMENT ---
class DbCapitalUsage(Base):
    __tablename__ = "capital_usage"
    bucket: Mapped[str] = mapped_column(String, primary_key=True)
    used_amount: Mapped[float] = mapped_column(Float, default=0.0)
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DbCapitalLedger(Base):
    __tablename__ = "capital_ledger"
    __table_args__ = (UniqueConstraint("trade_id", "bucket", name="uq_trade_bucket"), Index("ix_capledger_bucket_date", "bucket", "date"))
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String, index=True) 
    bucket: Mapped[str] = mapped_column(String)
    amount: Mapped[float] = mapped_column(Float)
    date: Mapped[date] = mapped_column(Date, default=date.today, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class DbStartOfDayMargin(Base):
    __tablename__ = "sod_margin"
    __table_args__ = (UniqueConstraint("date", name="uq_sod_date"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, unique=True, index=True)
    margin: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# --- AUTHENTICATION ---
class DbTokenState(Base):
    __tablename__ = "token_state"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    access_token: Mapped[str] = mapped_column(String, nullable=False)
    refresh_token: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_refreshed: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# --- RISK & ANALYTICS ---
class DbMarginHistory(Base):
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

# --- NEW: PERSISTENT HISTORICAL DATA (MISSING LAYER) ---
class DbHistoricalCandle(Base):
    """
    Stores daily OHLCV data for NIFTY and VIX.
    Prevents re-downloading 365 days on every restart.
    """
    __tablename__ = "historical_candles"
    __table_args__ = (
        UniqueConstraint("instrument_key", "date", name="uq_instrument_date"),
        Index("ix_candles_instrument_date", "instrument_key", "date"),
    )
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_key: Mapped[str] = mapped_column(String, nullable=False, index=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    
    # OHLCV
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float, default=0.0)
    oi: Mapped[float] = mapped_column(Float, default=0.0)
    
    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# --- PROCESS COMMUNICATION ---
class DbRiskState(Base):
    __tablename__ = "risk_state"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    sheriff_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    sod_equity: Mapped[float] = mapped_column(Float, default=0.0)
    current_equity: Mapped[float] = mapped_column(Float, default=0.0)
    drawdown_pct: Mapped[float] = mapped_column(Float, default=0.0)
    kill_switch_active: Mapped[bool] = mapped_column(Boolean, default=False)
    is_flattening: Mapped[bool] = mapped_column(Boolean, default=False) 
    flatten_order_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    real_time_pnl: Mapped[Optional[float]] = mapped_column(Float, default=0.0)

class DbMarketContext(Base):
    __tablename__ = "market_context"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    regime: Mapped[str] = mapped_column(String)
    ai_narrative: Mapped[str] = mapped_column(String) 
    is_high_risk: Mapped[bool] = mapped_column(Boolean, default=False)
    is_fresh: Mapped[bool] = mapped_column(Boolean, default=True)

class DbMarketSnapshot(Base):
    __tablename__ = "market_snapshot"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    spot_price: Mapped[float] = mapped_column(Float)
    vix: Mapped[float] = mapped_column(Float)
    atm_iv_weekly: Mapped[float] = mapped_column(Float)
    atm_iv_monthly: Mapped[float] = mapped_column(Float)
    iv_spread: Mapped[float] = mapped_column(Float)
    term_structure_tag: Mapped[str] = mapped_column(String)
    rv_7d: Mapped[float] = mapped_column(Float)
    garch_vol_7d: Mapped[float] = mapped_column(Float)
    egarch_vol_1d: Mapped[float] = mapped_column(Float)
    iv_percentile: Mapped[float] = mapped_column(Float)
    vrp_spread: Mapped[float] = mapped_column(Float)
    vrp_zscore: Mapped[float] = mapped_column(Float)
    vrp_verdict: Mapped[str] = mapped_column(String)
    straddle_cost_weekly: Mapped[float] = mapped_column(Float)
    straddle_cost_monthly: Mapped[float] = mapped_column(Float)
    breakeven_lower: Mapped[float] = mapped_column(Float)
    breakeven_upper: Mapped[float] = mapped_column(Float)
    chain_json: Mapped[List[Dict[str, Any]]] = mapped_column(JSON)
