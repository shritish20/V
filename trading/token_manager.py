#!/usr/bin/env python3
"""
VolGuard 20.0 – OAuth Token Manager
Handles automatic token refresh to prevent daily expiry crashes.
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import aiohttp
from sqlalchemy import select, delete
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import DbTokenState

logger = logging.getLogger("TokenManager")

class OAuthTokenManager:
    """
    Manages Upstox OAuth tokens with automatic refresh.
    
    Features:
    - Stores refresh_token securely in database
    - Auto-refreshes 1 hour before expiry
    - Updates all services (Engine, Sheriff, Analyst) on refresh
    - Graceful fallback if refresh fails
    """
    
    def __init__(self, db_manager: HybridDatabaseManager):
        self.db = db_manager
        self.api_key = settings.UPSTOX_API_KEY
        self.api_secret = settings.UPSTOX_API_SECRET
        self.redirect_uri = getattr(settings, 'REDIRECT_URI', 'http://localhost:8000/callback')
        
        # Callbacks to notify services of token change
        self.subscribers = []
        
        self._running = False
        self._refresh_interval = 1800  # Check every 30 minutes
        
    def subscribe(self, callback):
        """Register a service to be notified on token refresh."""
        self.subscribers.append(callback)
        
    async def initialize_from_env(self):
        """
        One-time setup: Store initial token from .env to database.
        Checks if DB is empty first to avoid overwriting a fresher token.
        """
        initial_token = settings.UPSTOX_ACCESS_TOKEN
        if not initial_token or "TEST" in initial_token:
            logger.warning("⚠️ No valid token in .env to initialize DB.")
            return False
            
        try:
            async with self.db.get_session() as session:
                # Check if we already have a valid token
                result = await session.execute(
                    select(DbTokenState).order_by(DbTokenState.last_refreshed.desc()).limit(1)
                )
                existing = result.scalars().first()
                
                # If DB is empty or env token is clearly newer (manual override), update it
                if not existing:
                    logger.info("📥 Importing Token from .env to Database...")
                    # Upstox tokens expire after 24 hours. We assume env token is fresh now.
                    expires_at = datetime.utcnow() + timedelta(hours=24)
                    
                    token_state = DbTokenState(
                        access_token=initial_token,
                        refresh_token=None,  # Will be set if we had the full oauth response
                        expires_at=expires_at,
                        last_refreshed=datetime.utcnow()
                    )
                    session.add(token_state)
                    await self.db.safe_commit(session)
                    logger.info("✅ Initial token stored in database")
                    return True
                else:
                    logger.info("✅ Database already has a token. Skipping import.")
                    return True
            
        except Exception as e:
            logger.error(f"Failed to store initial token: {e}")
            return False
    
    async def get_current_token(self) -> Optional[str]:
        """Retrieve the current valid access token."""
        try:
            async with self.db.get_session() as session:
                result = await session.execute(
                    select(DbTokenState).order_by(DbTokenState.last_refreshed.desc()).limit(1)
                )
                token_state = result.scalars().first()
                
                if not token_state:
                    # Fallback to env if DB is empty
                    return settings.UPSTOX_ACCESS_TOKEN
                    
                # Check if expired
                if datetime.utcnow() >= token_state.expires_at:
                    logger.warning("⚠️ Token in DB is expired - attempting refresh")
                    refreshed = await self._refresh_token(token_state.refresh_token)
                    if refreshed:
                        return refreshed
                    else:
                        logger.critical("🚨 REFRESH FAILED: Please update .env manually!")
                        return None
                    
                return token_state.access_token
                
        except Exception as e:
            logger.error(f"Failed to get token: {e}")
            # Emergency Fallback
            return settings.UPSTOX_ACCESS_TOKEN
    
    async def _refresh_token(self, refresh_token: Optional[str]) -> Optional[str]:
        """
        Refresh the access token using refresh_token.
        """
        if not refresh_token:
            logger.error("❌ No refresh_token available in DB. Manual re-authentication required.")
            return None
            
        try:
            # Upstox API Endpoint for Token
            url = "https://api.upstox.com/v2/login/authorization/token"
            headers = {
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.api_key,
                "client_secret": self.api_secret,
                "redirect_uri": self.redirect_uri
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=data, timeout=10) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        new_access_token = result.get("access_token")
                        new_refresh_token = result.get("refresh_token")
                        
                        if not new_access_token:
                            logger.error("❌ Refresh response missing access_token")
                            return None
                            
                        # Update database
                        await self._store_refreshed_token(
                            new_access_token, 
                            new_refresh_token
                        )
                        
                        # Notify all subscribers
                        await self._notify_subscribers(new_access_token)
                        
                        logger.info("✅ Token refreshed successfully via Upstox API")
                        return new_access_token
                    else:
                        body = await resp.text()
                        logger.error(f"❌ Token refresh failed: {resp.status} - {body}")
                        return None
                        
        except Exception as e:
            logger.error(f"Token refresh exception: {e}")
            return None
    
    async def _store_refreshed_token(self, access_token: str, refresh_token: Optional[str]):
        """Store refreshed token in database."""
        expires_at = datetime.utcnow() + timedelta(hours=24)
        
        async with self.db.get_session() as session:
            # Clear old state first to keep table clean
            await session.execute(delete(DbTokenState))
            
            # Insert new state
            token_state = DbTokenState(
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at,
                last_refreshed=datetime.utcnow()
            )
            session.add(token_state)
            await self.db.safe_commit(session)
    
    async def _notify_subscribers(self, new_token: str):
        """Notify all registered services of token change."""
        for callback in self.subscribers:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(new_token)
                else:
                    callback(new_token)
            except Exception as e:
                logger.error(f"Subscriber notification failed: {e}")
    
    async def start_refresh_loop(self):
        """
        Background task to check and refresh tokens proactively.
        """
        self._running = True
        logger.info("🔄 Token refresh loop started")
        
        while self._running:
            try:
                async with self.db.get_session() as session:
                    result = await session.execute(
                        select(DbTokenState).order_by(DbTokenState.last_refreshed.desc()).limit(1)
                    )
                    token_state = result.scalars().first()
                    
                    if not token_state:
                        # If no token, check .env again (maybe user updated it manually)
                        await self.initialize_from_env()
                        await asyncio.sleep(60)
                        continue
                    
                    # Check if we should refresh (1 hour before expiry)
                    time_until_expiry = (token_state.expires_at - datetime.utcnow()).total_seconds()
                    
                    if time_until_expiry < 3600 and token_state.refresh_token:
                        logger.info(f"⏰ Token expires in {time_until_expiry/60:.0f} minutes - attempting refresh")
                        await self._refresh_token(token_state.refresh_token)
                    elif time_until_expiry < 0:
                        logger.critical("🚨 Token EXPIRED. Manual update required.")
                    else:
                        logger.debug(f"✅ Token valid for {time_until_expiry/3600:.1f} hours")
                
                await asyncio.sleep(self._refresh_interval)
                
            except Exception as e:
                logger.error(f"Refresh loop error: {e}")
                await asyncio.sleep(60)
    
    async def stop(self):
        """Stop the refresh loop."""
        self._running = False
        logger.info("🛑 Token refresh loop stopped")

# ---------------------------------------------------------------------------
# Integration Helper
# ---------------------------------------------------------------------------
async def setup_token_manager(db: HybridDatabaseManager, api_client) -> OAuthTokenManager:
    """
    Helper to initialize token manager.
    """
    token_mgr = OAuthTokenManager(db)
    
    # Initialize from .env (first time or manual override)
    await token_mgr.initialize_from_env()
    
    # Register API client to receive updates
    async def update_api_token(new_token: str):
        # Assuming api_client has an update_token method
        if hasattr(api_client, 'update_token'):
            await api_client.update_token(new_token)
            logger.info("🔄 API Client token updated")
        else:
            # Fallback: update the attribute directly
            api_client.access_token = new_token
            api_client.headers["Authorization"] = f"Bearer {new_token}"
    
    token_mgr.subscribe(update_api_token)
    
    return token_mgr
