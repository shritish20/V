from enum import Enum

class StrategyType(Enum):
    WAIT = "WAIT"
    ATM_STRADDLE = "ATM_STRADDLE"
    ATM_STRANGLE = "ATM_STRANGLE"
    IRON_CONDOR = "IRON_CONDOR"
    CALENDAR_SPREAD = "CALENDAR_SPREAD"
    RATIO_SPREAD = "RATIO_SPREAD"

class TradeStatus(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    EXTERNAL = "EXTERNAL"

class ExitReason(Enum):
    PROFIT_TARGET = "PROFIT_TARGET"
    STOP_LOSS = "STOP_LOSS"
    EXPIRY = "EXPIRY"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"
    MANUAL = "MANUAL"

class CapitalBucket(Enum):
    WEEKLY = "weekly_expiries"
    MONTHLY = "monthly_expiries"
    INTRADAY = "intraday_adjustments"

class ExpiryType(Enum):
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    INTRADAY = "INTRADAY"

class MarketRegime(Enum):
    PANIC = "PANIC"
    FEAR_BACKWARDATION = "FEAR_BACKWARDATION"
    LOW_VOL_COMPRESSION = "LOW_VOL_COMPRESSION"
    CALM_COMPRESSION = "CALM_COMPRESSION"
    DEFENSIVE_EVENT = "DEFENSIVE_EVENT"
    TRANSITION = "TRANSITION"
