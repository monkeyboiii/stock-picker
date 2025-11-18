"""
Authentication Module - Logto OIDC Integration

This module provides:
- JWT token validation (local JWKS-based verification)
- User info validation against Logto /userinfo endpoint
- FastAPI dependencies for protected routes

Environment Variables Required:
    LOGTO_ENDPOINT: Logto server endpoint (e.g., https://your-logto.com)
    LOGTO_APP_ID: Application ID from Logto
    LOGTO_RESOURCE: API resource identifier
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from jose.backends import RSAKey
from loguru import logger
from pydantic import BaseModel


# Environment configuration
LOGTO_ENDPOINT = os.getenv("LOGTO_ENDPOINT", "")
LOGTO_APP_ID = os.getenv("LOGTO_APP_ID", "")
LOGTO_RESOURCE = os.getenv("LOGTO_RESOURCE", "")

# Security scheme
security = HTTPBearer()

# JWKS cache
_jwks_cache: Optional[dict] = None
_jwks_cache_expiry: Optional[datetime] = None


class User(BaseModel):
    """User model from token claims"""
    sub: str  # Subject (user ID)
    username: Optional[str] = None
    email: Optional[str] = None
    aud: Optional[str] = None  # Audience
    exp: Optional[int] = None  # Expiration
    iat: Optional[int] = None  # Issued at
    iss: Optional[str] = None  # Issuer


class AuthenticationError(Exception):
    """Custom exception for authentication errors"""
    pass


async def get_jwks() -> dict:
    """
    Fetch JWKS (JSON Web Key Set) from Logto

    Caches the result for 1 hour to reduce external API calls.

    Returns:
        JWKS dictionary

    Raises:
        AuthenticationError: If JWKS cannot be fetched
    """
    global _jwks_cache, _jwks_cache_expiry

    # Return cached JWKS if still valid
    if _jwks_cache and _jwks_cache_expiry and datetime.now() < _jwks_cache_expiry:
        return _jwks_cache

    # Fetch new JWKS
    jwks_url = f"{LOGTO_ENDPOINT}/oidc/jwks"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(jwks_url, timeout=10.0)
            response.raise_for_status()

            _jwks_cache = response.json()
            _jwks_cache_expiry = datetime.now() + timedelta(hours=1)

            logger.info(f"JWKS fetched and cached from {jwks_url}")
            return _jwks_cache

    except httpx.HTTPError as e:
        logger.error(f"Failed to fetch JWKS from {jwks_url}: {e}")
        raise AuthenticationError(f"Cannot fetch JWKS: {e}")


async def verify_token_local(token: str) -> dict:
    """
    Verify JWT token locally using JWKS

    Args:
        token: JWT access token

    Returns:
        Decoded token claims

    Raises:
        AuthenticationError: If token is invalid
    """
    try:
        # Get JWKS
        jwks = await get_jwks()

        # Decode token header to get kid (key ID)
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")

        if not kid:
            raise AuthenticationError("Token missing 'kid' in header")

        # Find matching key in JWKS
        key = None
        for jwk in jwks.get("keys", []):
            if jwk.get("kid") == kid:
                key = jwk
                break

        if not key:
            raise AuthenticationError(f"No matching key found for kid: {kid}")

        # Verify and decode token
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=LOGTO_RESOURCE if LOGTO_RESOURCE else None,
            issuer=f"{LOGTO_ENDPOINT}/oidc",
        )

        logger.debug(f"Token verified locally for sub: {claims.get('sub')}")
        return claims

    except JWTError as e:
        logger.warning(f"JWT verification failed: {e}")
        raise AuthenticationError(f"Invalid token: {e}")


async def verify_token_userinfo(token: str) -> dict:
    """
    Verify token by calling Logto /userinfo endpoint

    This provides additional validation and fetches user information.

    Args:
        token: JWT access token

    Returns:
        User info from Logto

    Raises:
        AuthenticationError: If token is invalid or userinfo fails
    """
    userinfo_url = f"{LOGTO_ENDPOINT}/oidc/me"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                userinfo_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=10.0
            )

            if response.status_code == 401:
                raise AuthenticationError("Token rejected by /userinfo endpoint")

            response.raise_for_status()
            userinfo = response.json()

            logger.debug(f"User info validated for sub: {userinfo.get('sub')}")
            return userinfo

    except httpx.HTTPError as e:
        logger.error(f"Failed to validate token with /userinfo: {e}")
        raise AuthenticationError(f"Userinfo validation failed: {e}")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> User:
    """
    FastAPI dependency to get current authenticated user

    Performs two-stage validation:
    1. Local JWT verification using JWKS (fast, no network call if cached)
    2. Remote validation via /userinfo endpoint (ensures token not revoked)

    Args:
        credentials: HTTP Bearer token from request

    Returns:
        User object with claims

    Raises:
        HTTPException: 401 if authentication fails

    Usage:
        @app.get("/protected")
        async def protected_route(user: User = Depends(get_current_user)):
            return {"message": f"Hello {user.sub}"}
    """
    if not LOGTO_ENDPOINT:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication not configured (LOGTO_ENDPOINT missing)",
        )

    token = credentials.credentials

    try:
        # Stage 1: Local JWT verification (fast)
        claims = await verify_token_local(token)

        # Stage 2: Remote validation (ensures not revoked)
        userinfo = await verify_token_userinfo(token)

        # Merge claims and userinfo
        user_data = {**claims, **userinfo}

        return User(**user_data)

    except AuthenticationError as e:
        logger.warning(f"Authentication failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.error(f"Unexpected authentication error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication service error",
        )


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
) -> Optional[User]:
    """
    Optional authentication dependency

    Returns User if valid token provided, None otherwise.
    Useful for endpoints that support both authenticated and anonymous access.

    Args:
        credentials: Optional HTTP Bearer token

    Returns:
        User object or None

    Usage:
        @app.get("/public-or-private")
        async def mixed_route(user: Optional[User] = Depends(get_current_user_optional)):
            if user:
                return {"message": f"Hello {user.sub}"}
            return {"message": "Hello anonymous"}
    """
    if not credentials or not LOGTO_ENDPOINT:
        return None

    try:
        token = credentials.credentials
        claims = await verify_token_local(token)
        userinfo = await verify_token_userinfo(token)
        user_data = {**claims, **userinfo}
        return User(**user_data)
    except Exception as e:
        logger.debug(f"Optional auth failed (expected for anonymous): {e}")
        return None


def require_scopes(required_scopes: list[str]):
    """
    Dependency factory for scope-based authorization

    Args:
        required_scopes: List of required scopes

    Returns:
        Dependency function that checks scopes

    Usage:
        @app.get("/admin")
        async def admin_route(
            user: User = Depends(get_current_user),
            _: None = Depends(require_scopes(["admin:write"]))
        ):
            return {"message": "Admin access granted"}
    """
    async def check_scopes(user: User = Depends(get_current_user)):
        # Scopes are typically in 'scope' claim as space-separated string
        token_scopes = getattr(user, 'scope', '').split()

        missing_scopes = [s for s in required_scopes if s not in token_scopes]

        if missing_scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required scopes: {missing_scopes}",
            )

        return None

    return check_scopes


# Health check for auth system
async def check_auth_health() -> dict:
    """
    Check if authentication system is properly configured

    Returns:
        Dict with health status
    """
    health = {
        "configured": bool(LOGTO_ENDPOINT and LOGTO_APP_ID),
        "endpoint": LOGTO_ENDPOINT if LOGTO_ENDPOINT else "NOT_SET",
        "jwks_cached": _jwks_cache is not None,
    }

    if health["configured"]:
        try:
            jwks = await get_jwks()
            health["jwks_keys_count"] = len(jwks.get("keys", []))
            health["status"] = "healthy"
        except Exception as e:
            health["status"] = "unhealthy"
            health["error"] = str(e)
    else:
        health["status"] = "not_configured"

    return health
