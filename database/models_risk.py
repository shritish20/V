#!/usr/bin/env python3
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import Integer, String, Float, DateTime, JSON, ForeignKey, Index, text
from sqlalchemy.orm import Mapped, mapped_column
from database.models import Base

class DbLearnedPattern(Base):
    __tablename__ = "learned_patterns"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pattern_type: Mapped[str] = mapped_column(String(20))
    pattern_name: Mapped[str] = mapped_column(String(200))
    # R4: JSONB is required for GIN indexing
    conditions_json: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=True)
    
    occurrence_count: Mapped[int] = mapped_column(Integer, default=0)
    win_rate: Mapped[float] = mapped_column(Float, default=0.0)
    avg_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    matching_trade_ids: Mapped[List[str]] = mapped_column(JSON, nullable=True)
    lesson_text: Mapped[str] = mapped_column(String, nullable=True)
    severity: Mapped[str] = mapped_column(String(10), default="MEDIUM")
    
    # Y3: Pattern Expiry
    last_occurrence: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    valid_until: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # R4: GIN Index for fast JSON searching
    __table_args__ = (
        Index('idx_patterns_gin', 'conditions_json', postgresql_using='gin'),
    )

class DbPatternWarning(Base):
    __tablename__ = "pattern_warnings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(50))
    pattern_id: Mapped[int] = mapped_column(Integer, ForeignKey("learned_patterns.id"), nullable=True)
    warning_text: Mapped[str] = mapped_column(String)
    similarity_score: Mapped[float] = mapped_column(Float)
    user_action: Mapped[str] = mapped_column(String(20), default="PENDING")
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class DbRiskBriefing(Base):
    __tablename__ = "risk_briefings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    briefing_text: Mapped[str] = mapped_column(String)
    risk_score: Mapped[float] = mapped_column(Float)
    alert_level: Mapped[str] = mapped_column(String(10))
    market_context: Mapped[Dict[str, Any]] = mapped_column(JSON)
    active_risks: Mapped[List[str]] = mapped_column(JSON)
    system_health: Mapped[Dict[str, Any]] = mapped_column(JSON)

class DbTradePostmortem(Base):
    __tablename__ = "trade_postmortems"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(50), unique=True)
    grade: Mapped[str] = mapped_column(String(2))
    what_went_right: Mapped[List[str]] = mapped_column(JSON, nullable=True)
    what_went_wrong: Mapped[List[str]] = mapped_column(JSON, nullable=True)
    lessons_learned: Mapped[str] = mapped_column(String, nullable=True)
    ai_analysis: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
