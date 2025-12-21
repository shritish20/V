import asyncio
import logging
from datetime import datetime, time, timedelta
from typing import List, Tuple
from core.models import MultiLegTrade
from core.enums import TradeStatus, ExitReason, ExpiryType
from core.config import settings, IST

logger = logging.getLogger("PositionLifecycle")

class PositionLifecycleManager:
    """
    CRITICAL RULES:
    1. Exit ALL positions 1 day before expiry (3:15 PM on T-1)
    2. On expiry day: Only allow intraday trades
    3. Force close ALL positions at 3:15 PM on expiry day
    4. Max hold time enforcement
    """
    
    def __init__(self, trade_manager):
        self.trade_mgr = trade_manager
        
        # Configurable limits
        self.exit_before_expiry_days = 1  # Exit 1 day before expiry
        self.force_close_time = time(15, 15)  # 3:15 PM
        self.max_hold_hours = {
            ExpiryType.WEEKLY: 48,   # 2 days max
            ExpiryType.MONTHLY: 120,  # 5 days max
            ExpiryType.INTRADAY: 6    # 6 hours max
        }
        
    async def monitor_lifecycle(self, trades: List[MultiLegTrade]):
        """
        Main lifecycle check - run this in your engine loop
        """
        now = datetime.now(IST)
        today = now.date()
        current_time = now.time()
        
        for trade in trades:
            if trade.status != TradeStatus.OPEN:
                continue
            
            try:
                expiry_date = datetime.strptime(trade.expiry_date, "%Y-%m-%d").date()
                days_to_expiry = (expiry_date - today).days
                
                # RULE 1: Exit 1 day before expiry at 3:15 PM
                if days_to_expiry == 1 and current_time >= self.force_close_time:
                    logger.critical(f"🔴 T-1 EXIT: Closing {trade.id} (Expiry tomorrow)")
                    await self.trade_mgr.close_trade(trade, ExitReason.EXPIRY)
                    continue
                
                # RULE 2: Force close on expiry day
                if days_to_expiry == 0:
                    if trade.expiry_type == ExpiryType.INTRADAY:
                        if current_time >= self.force_close_time:
                            logger.critical(f"🔴 EXPIRY FORCE CLOSE: {trade.id}")
                            await self.trade_mgr.close_trade(trade, ExitReason.EXPIRY)
                    else:
                        logger.critical(f"🚨 EMERGENCY: Position held into expiry day! {trade.id}")
                        await self.trade_mgr.close_trade(trade, ExitReason.EXPIRY)
                    continue
                
                # RULE 3: Max hold time enforcement
                hold_hours = (now - trade.entry_time).total_seconds() / 3600
                max_hold = self.max_hold_hours.get(trade.expiry_type, 48)
                
                if hold_hours > max_hold:
                    logger.warning(f"⏰ MAX HOLD TIME: Closing {trade.id} (held {hold_hours:.1f}h)")
                    await self.trade_mgr.close_trade(trade, ExitReason.MANUAL)
                    continue
            except Exception as e:
                logger.error(f"Lifecycle Check Error {trade.id}: {e}")
    
    def can_enter_new_trade(self, proposed_expiry: str, expiry_type: ExpiryType) -> Tuple[bool, str]:
        """
        Pre-trade check: Should we allow this trade?
        """
        now = datetime.now(IST)
        today = now.date()
        current_time = now.time()
        
        try:
            expiry_date = datetime.strptime(proposed_expiry, "%Y-%m-%d").date()
            days_to_expiry = (expiry_date - today).days
            
            # RULE: On expiry day, only allow INTRADAY trades
            if days_to_expiry == 0:
                if expiry_type != ExpiryType.INTRADAY:
                    return False, "Expiry day: Only intraday trades allowed"
                
                if current_time >= time(14, 30):  # After 2:30 PM
                    return False, "Too close to market close on expiry day"
            
            # RULE: Don't enter if expiry is tomorrow (T-1) and it's late
            if days_to_expiry == 1 and current_time >= time(14, 0):
                return False, "Too close to T-1 exit time"
            
            return True, "Trade allowed"
        except:
            return True, "Date Parse Error (Allowed)"
