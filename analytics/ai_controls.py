from enum import Enum
from dataclasses import dataclass
from typing import Optional

class AIActionType(Enum):
    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    DOWNGRADE = "DOWNGRADE"
    WARN = "WARN"

@dataclass
class AIDecision:
    action: AIActionType
    reason: str
    confidence: float
    alternative_strategy: Optional[str] = None
    
    def should_proceed(self) -> bool:
        """Safety Gate: Only ALLOW, WARN, or DOWNGRADE pass."""
        return self.action in [AIActionType.ALLOW, AIActionType.WARN, AIActionType.DOWNGRADE]
