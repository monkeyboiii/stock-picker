"""
API Middleware - Rate limiting, metrics, and request tracking

This module provides:
- Rate limiting (using slowapi or custom implementation)
- Prometheus metrics collection
- Request ID tracking
- Request/response logging
"""

import time
from typing import Callable

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

# Optional: Prometheus metrics (requires prometheus_client)
try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True

    # Define metrics
    REQUEST_COUNT = Counter(
        'http_requests_total',
        'Total HTTP requests',
        ['method', 'endpoint', 'status']
    )

    REQUEST_DURATION = Histogram(
        'http_request_duration_seconds',
        'HTTP request duration in seconds',
        ['method', 'endpoint']
    )

    ACTIVE_REQUESTS = Gauge(
        'http_requests_active',
        'Number of active HTTP requests'
    )

    BACKTEST_RUNS_ACTIVE = Gauge(
        'backtest_runs_active',
        'Number of active backtest runs'
    )

except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("Prometheus metrics disabled - install prometheus_client")


# Optional: Rate limiting (using slowapi or custom implementation)
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded
    from slowapi.util import get_remote_address

    RATE_LIMITING_AVAILABLE = True

    # Create limiter
    limiter = Limiter(
        key_func=get_remote_address,
        default_limits=["100/minute", "1000/hour"],
    )

except ImportError:
    RATE_LIMITING_AVAILABLE = False
    limiter = None
    logger.warning("Rate limiting disabled - install slowapi")


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    Middleware for collecting Prometheus metrics

    Tracks:
    - Request count by method, endpoint, status
    - Request duration
    - Active requests
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and collect metrics"""

        if not PROMETHEUS_AVAILABLE:
            return await call_next(request)

        # Skip metrics endpoint itself
        if request.url.path == "/metrics":
            return await call_next(request)

        # Track active requests
        ACTIVE_REQUESTS.inc()

        start_time = time.time()

        try:
            response = await call_next(request)

            # Record metrics
            duration = time.time() - start_time

            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=self._get_endpoint_pattern(request),
                status=response.status_code
            ).inc()

            REQUEST_DURATION.labels(
                method=request.method,
                endpoint=self._get_endpoint_pattern(request)
            ).observe(duration)

            return response

        except Exception as e:
            # Record error metrics
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=self._get_endpoint_pattern(request),
                status=500
            ).inc()

            raise

        finally:
            ACTIVE_REQUESTS.dec()

    @staticmethod
    def _get_endpoint_pattern(request: Request) -> str:
        """
        Extract endpoint pattern from request

        Converts /api/v1/backtest/runs/123 to /api/v1/backtest/runs/{id}
        """
        path = request.url.path

        # Simple pattern matching (improve with route matching if needed)
        if "/runs/" in path:
            return path.rsplit("/", 1)[0] + "/{id}"
        if "/strategies/" in path:
            return path.rsplit("/", 1)[0] + "/{id}"

        return path


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware for adding request ID to all requests

    Adds X-Request-ID header to both request and response
    for request tracking and debugging.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add request ID to request and response"""
        import uuid

        # Check if request already has ID
        request_id = request.headers.get("X-Request-ID")

        if not request_id:
            request_id = str(uuid.uuid4())

        # Add to request state for access in routes
        request.state.request_id = request_id

        # Process request
        response = await call_next(request)

        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id

        return response


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware for logging requests and responses

    Logs:
    - Request method, path, client IP
    - Response status code, duration
    - Errors and exceptions
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Log request and response"""

        start_time = time.time()

        # Log request
        logger.info(
            f"Request: {request.method} {request.url.path}",
            extra={
                "method": request.method,
                "path": request.url.path,
                "client": request.client.host if request.client else "unknown",
                "request_id": getattr(request.state, "request_id", None),
            }
        )

        try:
            response = await call_next(request)
            duration = time.time() - start_time

            # Log response
            logger.info(
                f"Response: {request.method} {request.url.path} - {response.status_code} ({duration:.3f}s)",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration": duration,
                    "request_id": getattr(request.state, "request_id", None),
                }
            )

            return response

        except Exception as e:
            duration = time.time() - start_time

            logger.error(
                f"Error: {request.method} {request.url.path} - {type(e).__name__} ({duration:.3f}s)",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "error": str(e),
                    "duration": duration,
                    "request_id": getattr(request.state, "request_id", None),
                },
                exc_info=True
            )

            raise


# Helper functions

def get_metrics_response() -> Response:
    """
    Generate Prometheus metrics response

    Returns:
        Response with Prometheus metrics in text format
    """
    if not PROMETHEUS_AVAILABLE:
        return JSONResponse(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            content={"error": "Prometheus metrics not available"}
        )

    from fastapi.responses import Response as FastAPIResponse

    metrics_data = generate_latest()

    return FastAPIResponse(
        content=metrics_data,
        media_type=CONTENT_TYPE_LATEST
    )


def configure_rate_limiting(app):
    """
    Configure rate limiting for FastAPI app

    Args:
        app: FastAPI application instance

    Usage:
        from app.api.middleware import configure_rate_limiting
        configure_rate_limiting(app)
    """
    if not RATE_LIMITING_AVAILABLE:
        logger.warning("Rate limiting not configured - slowapi not installed")
        return

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    logger.info("Rate limiting configured: 100/minute, 1000/hour")


def configure_middleware(app):
    """
    Configure all middleware for FastAPI app

    Args:
        app: FastAPI application instance

    Usage:
        from app.api.middleware import configure_middleware
        configure_middleware(app)
    """
    # Add middleware in reverse order (last added = first executed)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(MetricsMiddleware)
    app.add_middleware(RequestIDMiddleware)

    logger.info("Middleware configured: RequestID, Metrics, Logging")
