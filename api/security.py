from fastapi import Security, HTTPException, status, Depends
from fastapi.security import APIKeyHeader
import os
from dotenv import load_dotenv

load_dotenv()

# Define the expected header name
# Client must send header: "X-VolGuard-Key: your_password"
api_key_header = APIKeyHeader(name="X-VolGuard-Key", auto_error=False)

async def get_admin_key(api_key_header: str = Security(api_key_header)):
    """
    Security Dependency:
    Validates that the request contains the correct VOLGUARD_ADMIN_KEY.
    If the key is missing or wrong, it rejects the request instantly.
    """
    # Load secret from .env
    admin_secret = os.getenv("VOLGUARD_ADMIN_KEY")
    
    # Safety Check: If no key is set in .env, lock everything down to be safe.
    if not admin_secret:
        raise HTTPException(
            status_code=500, 
            detail="Server Security Error: VOLGUARD_ADMIN_KEY not set in environment variables."
        )

    # Validate Key
    if api_key_header == admin_secret:
        return api_key_header
    
    # Reject Unauthorized
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="⛔ ACCESS DENIED: Invalid or Missing Admin Key"
    )
