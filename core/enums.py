# File: core/enums.py

from enum import Enum

class StrategyType(Enum):
    WAIT = "WAIT"
    
    # --- INCOME (High Probability, Theta Focus) ---
    IRON_CONDOR = "IRON_CONDOR"          # Neutral, Defined Risk, Standard Income
    IRON_FLY = "IRON_FLY"                # Neutral, Centered Risk, High IV Crush
    SHORT_STRANGLE = "SHORT_STRANGLE"    # Neutral, Undefined Risk, Aggressive
    SHORT_STRADDLE = "SHORT_STRADDLE"    # Neutral, Max Theta, Binary Event Capture

    # --- DIRECTIONAL (Delta Focus) ---
    BULL_PUT_SPREAD = "BULL_PUT_SPREAD"  # Bullish, Credit
    BEAR_CALL_SPREAD = "BEAR_CALL_SPREAD" # Bearish, Credit
    BULL_CALL_SPREAD = "BULL_CALL_SPREAD" # Bullish, Debit (Low Vol)
    BEAR_PUT_SPREAD = "BEAR_PUT_SPREAD"   # Bearish, Debit (Low Vol)

    # --- SKEW / VOLATILITY (Vega/Gamma Focus) ---
    RATIO_SPREAD_PUT = "RATIO_SPREAD_PUT" # Bullish/Neutral, financed by OTM Panic
    RATIO_SPREAD_CALL = "RATIO_SPREAD_CALL" # Bearish/Neutral, financed by Call FOMO
    JADE_LIZARD = "JADE_LIZARD"           # Bullish/Neutral, No Upside Risk
    REVERSE_JADE_LIZARD = "REVERSE_JADE_LIZARD" # Bearish/Neutral, No Downside Risk

    # --- TERM STRUCTURE (Calendar Focus) ---
    # Used when Volatility is CHEAP (VRP < 0)
    LONG_CALENDAR_CALL = "LONG_CALENDAR_CALL" 
    LONG_CALENDAR_PUT = "LONG_CALENDAR_PUT"   
    LONG_STRADDLE = "LONG_STRADDLE"       # Buying Volatility (Gamma Long)

class TradeStatus(Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    EXTERNAL = "EXTERNAL"
    REJECTED = "REJECTED"

class OrderStatus(Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"

class ExitReason(Enum):
    PROFIT_TARGET = "PROFIT_TARGET"
    STOP_LOSS = "STOP_LOSS"
    EXPIRY = "EXPIRY"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    RISK_BREACH = "RISK_BREACH"
    MANUAL = "MANUAL"
    GAMMA_HEDGE = "GAMMA_HEDGE"

class CapitalBucket(Enum):
    WEEKLY = "weekly_expiries"
    MONTHLY = "monthly_expiries"
    INTRADAY = "intraday_adjustments"

class ExpiryType(Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    INTRADAY = "INTRADAY"

class MarketRegime(Enum):
    # RISK STATES
    PANIC = "PANIC"                       # Crash Mode (VIX > 25, Backwardation)
    BINARY_EVENT = "BINARY_EVENT"         # Pre-Budget/Election (Do Not Trade)
    MACRO_RISK = "MACRO_RISK"             # Fed/RBI Day (Defined Risk Only)
    
    # VOLATILITY STATES
    FEAR_BACKWARDATION = "FEAR_BACKWARDATION" # Short term panic
    LOW_VOL_COMPRESSION = "LOW_VOL_COMPRESSION" # VIX < 12, Contango
    CALM_COMPRESSION = "CALM_COMPRESSION"     # Standard grind
    
    # TREND STATES
    BULL_EXPANSION = "BULL_EXPANSION"
    BEAR_CONTRACTION = "BEAR_CONTRACTION"
    TRANSITION = "TRANSITION"
    
    SAFE = "SAFE" # Default
