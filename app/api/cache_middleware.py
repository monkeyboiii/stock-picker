"""
HTTP Caching Middleware - Adds Cache-Control and ETag headers to responses

This middleware improves API performance by enabling client-side caching of responses.
Reduces server load and network bandwidth usage.
"""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from app.constant.trading import CACHE_TTL_LONG, CACHE_TTL_MEDIUM, CACHE_TTL_SHORT


class HTTPCacheMiddleware(BaseHTTPMiddleware):
    """
    Adds HTTP caching headers to GET requests

    Features:
    - Cache-Control headers based on endpoint patterns
    - ETag generation for cache validation
    - Only applies to successful GET requests (200 OK)
    - Respects cache-busting parameters
    """

    def __init__(self, app: ASGIApp):
        super().__init__(app)

        # Define cache policies for different endpoint patterns
        # Pattern: (path_pattern, cache_ttl_seconds, is_public)
        self.cache_policies = [
            # Long-lived static resources (24 hours)
            ("/api/v1/strategies", CACHE_TTL_LONG, True),

            # Medium-lived data (1 hour)
            ("/api/v1/backtest/runs", CACHE_TTL_MEDIUM, True),
            ("/api/v1/backtest/compare", CACHE_TTL_MEDIUM, True),

            # Short-lived data (5 minutes)
            ("/api/v1/backtest/runs/.*/metrics", CACHE_TTL_SHORT, True),
            ("/api/v1/backtest/runs/.*/charts", CACHE_TTL_SHORT, True),
        ]

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and add caching headers to GET responses"""

        # Only cache GET requests
        if request.method != "GET":
            return await call_next(request)

        # Process the request
        response = await call_next(request)

        # Only add caching headers to successful responses
        if response.status_code != 200:
            return response

        # Skip caching if explicitly disabled (e.g., via query param)
        if "no-cache" in request.query_params:
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response

        # Determine cache policy for this endpoint
        cache_policy = self._get_cache_policy(request.url.path)

        if cache_policy:
            ttl, is_public = cache_policy

            # Add Cache-Control header
            cache_directive = "public" if is_public else "private"
            response.headers["Cache-Control"] = f"{cache_directive}, max-age={ttl}, stale-while-revalidate={ttl // 2}"

            # Generate and add ETag for cache validation
            etag = self._generate_etag(response)
            if etag:
                response.headers["ETag"] = etag

            # Add Vary header to indicate cache varies by Accept header
            response.headers["Vary"] = "Accept, Accept-Encoding"
        else:
            # Default: no caching for endpoints without explicit policy
            response.headers["Cache-Control"] = "no-cache"

        return response

    def _get_cache_policy(self, path: str) -> tuple[int, bool] | None:
        """
        Get cache policy for a given path

        Returns:
            Tuple of (ttl_seconds, is_public) or None if no policy matches
        """
        import re

        for pattern, ttl, is_public in self.cache_policies:
            if re.match(pattern, path):
                return (ttl, is_public)

        return None

    def _generate_etag(self, response: Response) -> str | None:
        """
        Generate ETag based on response body

        Returns:
            ETag string or None if body cannot be hashed
        """
        try:
            # Get response body
            body = response.body
            if not body:
                return None

            # Generate MD5 hash of body
            etag_hash = hashlib.md5(body).hexdigest()

            # Return ETag in quoted format
            return f'"{etag_hash}"'

        except Exception:
            # If we can't generate ETag, just skip it
            return None


def add_http_cache_middleware(app):
    """
    Add HTTP caching middleware to FastAPI app

    Usage:
        from app.api.cache_middleware import add_http_cache_middleware

        app = FastAPI()
        add_http_cache_middleware(app)
    """
    app.add_middleware(HTTPCacheMiddleware)
