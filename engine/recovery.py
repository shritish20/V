import logging
from database.manager import HybridDatabaseManager
from core.models import TradeStatus, ExitReason, MultiLegTrade, Position, GreeksSnapshot
from trading.api_client import HybridUpstoxAPI
from datetime import datetime
from core.config import IST, LOT_SIZE, MARKET_KEY_INDEX

logger = logging.getLogger("VolGuard14")

class StateRecoveryService:
    """Advanced state recovery service for crash resilience"""
    
    def __init__(self, db: HybridDatabaseManager, api: HybridUpstoxAPI):
        self.db = db
        self.api = api

    async def recover_open_trades(self):
        """Recover and reconcile open trades after system restart"""
        logger.info("⚡ VOLGUARD 14.00 STATE RECOVERY INITIATED")
        
        try:
            # Get broker positions
            broker_positions = await self.api.get_short_term_positions()
            broker_keys = {p['instrument_key']: p for p in broker_positions} if broker_positions else {}
            
            # Get database active trades
            db_active_trades = self.db.get_active_trades()
            
            recovered_trades = []
            
            for trade_data in db_active_trades:
                trade_row = trade_data['trade']
                legs_data = trade_data['legs']
                trade_id = trade_row[0]
                
                # Check if trade still exists on broker
                trade_alive = False
                for leg in legs_data:
                    if leg[2] in broker_keys:  # instrument_key
                        trade_alive = True
                        break
                
                if not trade_alive:
                    # Trade doesn't exist on broker - mark as closed
                    logger.warning(f"⚠️ Trade {trade_id} ghost found. Cleaning up.")
                    self.db.update_trade_status(trade_id, TradeStatus.CLOSED, 0.0, ExitReason.HEALTH_CHECK)
                else:
                    # Trade exists - reconstruct for engine
                    reconstructed_trade = await self._reconstruct_trade(trade_row, legs_data)
                    if reconstructed_trade:
                        recovered_trades.append(reconstructed_trade)
                        logger.info(f"✅ Recovered trade {trade_id}")
            
            # Handle broker positions not in database (external trades)
            broker_instruments = set(broker_keys.keys())
            db_instruments = set()
            for trade_data in db_active_trades:
                for leg in trade_data['legs']:
                    db_instruments.add(leg[2])
            
            external_instruments = broker_instruments - db_instruments
            
            for instrument_key in external_instruments:
                pos_data = broker_keys[instrument_key]
                external_trade = await self._create_external_trade(pos_data)
                if external_trade:
                    recovered_trades.append(external_trade)
                    logger.info(f"🔗 Created external trade for {instrument_key}")
            
            logger.info(f"🎯 Recovery complete: {len(recovered_trades)} trades recovered")
            return recovered_trades
            
        except Exception as e:
            logger.critical(f"❌ RECOVERY FAILED: {e}")
            return []

    async def _reconstruct_trade(self, trade_row, legs_data) -> MultiLegTrade:
        """Reconstruct a trade from database rows"""
        try:
            positions = []
            for leg in legs_data:
                position = Position(
                    symbol=MARKET_KEY_INDEX,
                    instrument_key=leg[2],  # instrument_key
                    strike=leg[3],  # strike
                    option_type=leg[4],  # option_type
                    quantity=leg[5],  # quantity
                    entry_price=leg[6],  # entry_price
                    entry_time=datetime.fromisoformat(leg[11]),  # created_at as entry_time
                    current_price=leg[7],  # current_price
                    current_greeks=GreeksSnapshot(
                        timestamp=datetime.now(IST),
                        delta=leg[8],  # delta
                        gamma=leg[9],  # gamma
                        theta=leg[10],  # theta
                        vega=leg[11] if len(leg) > 11 else 0.0  # vega
                    )
                )
                positions.append(position)
            
            trade = MultiLegTrade(
                legs=positions,
                strategy_type=trade_row[1],  # strategy_type
                net_premium_per_share=trade_row[6],  # net_premium
                entry_time=datetime.fromisoformat(trade_row[3]),  # entry_time
                lots=trade_row[5],  # lots
                status=TradeStatus(trade_row[2]),  # status
                expiry_date=trade_row[9],  # expiry_date
                id=trade_row[0]  # id
            )
            
            return trade
            
        except Exception as e:
            logger.error(f"Failed to reconstruct trade: {e}")
            return None

    async def _create_external_trade(self, pos_data: dict) -> MultiLegTrade:
        """Create external trade for broker positions not in database"""
        try:
            position = Position(
                symbol=pos_data.get('symbol', MARKET_KEY_INDEX),
                instrument_key=pos_data.get('instrument_key', ''),
                strike=pos_data.get('strike_price', 0.0),
                option_type=pos_data.get('option_type', 'CE'),
                quantity=pos_data.get('net_quantity', 0),
                entry_price=pos_data.get('average_price', 0.0),
                entry_time=datetime.now(IST),
                current_price=pos_data.get('last_price', 0.0),
                current_greeks=GreeksSnapshot(timestamp=datetime.now(IST))
            )
            
            external_trade = MultiLegTrade(
                legs=[position],
                strategy_type="EXTERNAL_RECONCILED",
                net_premium_per_share=0.0,
                entry_time=datetime.now(IST),
                lots=abs(position.quantity) // LOT_SIZE,
                status=TradeStatus.EXTERNAL,
                expiry_date=pos_data.get('expiry_date', '2099-12-31')
            )
            
            return external_trade
            
        except Exception as e:
            logger.error(f"Failed to create external trade: {e}")
            return None
