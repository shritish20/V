from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON, Date
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

    # Hydration-critical fields
    expiry_date = Column(Date, nullable=True)
    broker_ref_id = Column(String, nullable=True)
    metadata_json = Column(JSON)

    orders = relationship(
        "DbOrder", back_populates="strategy", cascade="all, delete-orphan"
    )

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

# --- NEW: Institutional Capital Tracking Table ---
class DbCapitalUsage(Base):
    __tablename__ = "capital_usage"

    bucket = Column(String, primary_key=True)
    used_amount = Column(Float, default=0.0)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
