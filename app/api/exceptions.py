"""
Custom Exceptions and Error Handlers for Stock Picker API

This module provides:
- Custom exception hierarchy
- Standardized error responses
- FastAPI exception handlers
- HTTP status code mapping
"""

from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from loguru import logger


# ============================================================================
# Base Exceptions
# ============================================================================

class StockPickerException(Exception):
    """Base exception for all Stock Picker errors"""

    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


# ============================================================================
# Domain Exceptions
# ============================================================================

class BacktestException(StockPickerException):
    """Backtest-related errors"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )


class StrategyException(StockPickerException):
    """Strategy-related errors"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details,
        )


class DataException(StockPickerException):
    """Data access/validation errors"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=details,
        )


# ============================================================================
# HTTP Exceptions
# ============================================================================

class NotFoundException(StockPickerException):
    """Resource not found (404)"""

    def __init__(self, resource: str, resource_id: Optional[str] = None):
        message = f"{resource} not found"
        if resource_id:
            message = f"{resource} with ID '{resource_id}' not found"

        super().__init__(
            message=message,
            status_code=status.HTTP_404_NOT_FOUND,
            details={"resource": resource, "resource_id": resource_id},
        )


class ValidationException(StockPickerException):
    """Validation error (422)"""

    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        details = {}
        if field:
            details["field"] = field
        if value is not None:
            details["value"] = str(value)

        super().__init__(
            message=message,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            details=details,
        )


class ConflictException(StockPickerException):
    """Resource conflict (409)"""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_409_CONFLICT,
            details=details,
        )


class ForbiddenException(StockPickerException):
    """Forbidden access (403)"""

    def __init__(self, message: str = "Forbidden", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_403_FORBIDDEN,
            details=details,
        )


class UnauthorizedException(StockPickerException):
    """Unauthorized access (401)"""

    def __init__(self, message: str = "Unauthorized", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_401_UNAUTHORIZED,
            details=details,
        )


class RateLimitException(StockPickerException):
    """Rate limit exceeded (429)"""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: Optional[int] = None):
        details = {}
        if retry_after:
            details["retry_after"] = retry_after

        super().__init__(
            message=message,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            details=details,
        )


# ============================================================================
# Specific Business Logic Exceptions
# ============================================================================

class InvalidStrategyException(StrategyException):
    """Invalid strategy configuration"""

    def __init__(self, message: str, strategy_data: Optional[Dict] = None):
        super().__init__(
            message=f"Invalid strategy: {message}",
            details={"strategy_data": strategy_data} if strategy_data else None,
        )


class InvalidDateRangeException(ValidationException):
    """Invalid date range (start > end)"""

    def __init__(self, start_date: str, end_date: str):
        super().__init__(
            message=f"Invalid date range: start_date ({start_date}) must be before end_date ({end_date})",
            field="date_range",
            value=f"{start_date} to {end_date}",
        )


class InsufficientDataException(DataException):
    """Insufficient data for operation"""

    def __init__(self, message: str, required_days: Optional[int] = None):
        details = {}
        if required_days:
            details["required_days"] = required_days

        super().__init__(
            message=f"Insufficient data: {message}",
            details=details,
        )


class BacktestRunningException(BacktestException):
    """Backtest already running"""

    def __init__(self, run_id: str):
        super().__init__(
            message=f"Backtest {run_id} is already running",
            details={"run_id": run_id, "status": "running"},
        )


# ============================================================================
# Error Response Model
# ============================================================================

class ErrorResponse:
    """Standardized error response"""

    @staticmethod
    def create(
        message: str,
        status_code: int,
        details: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create standardized error response

        Returns:
            Dict with error information
        """
        response = {
            "success": False,
            "error": {
                "message": message,
                "status_code": status_code,
            },
        }

        if details:
            response["error"]["details"] = details

        if request_id:
            response["request_id"] = request_id

        return response


# ============================================================================
# Exception Handlers
# ============================================================================

async def stock_picker_exception_handler(request: Request, exc: StockPickerException) -> JSONResponse:
    """
    Handle custom StockPickerException

    Args:
        request: FastAPI request
        exc: StockPickerException instance

    Returns:
        JSONResponse with error details
    """
    logger.warning(
        f"StockPickerException: {exc.message}",
        extra={
            "status_code": exc.status_code,
            "details": exc.details,
            "path": request.url.path,
        },
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse.create(
            message=exc.message,
            status_code=exc.status_code,
            details=exc.details,
            request_id=request.headers.get("X-Request-ID"),
        ),
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Handle FastAPI HTTPException

    Args:
        request: FastAPI request
        exc: HTTPException instance

    Returns:
        JSONResponse with error details
    """
    logger.warning(
        f"HTTPException: {exc.detail}",
        extra={
            "status_code": exc.status_code,
            "path": request.url.path,
        },
    )

    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse.create(
            message=str(exc.detail),
            status_code=exc.status_code,
            request_id=request.headers.get("X-Request-ID"),
        ),
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle unhandled exceptions

    Args:
        request: FastAPI request
        exc: Exception instance

    Returns:
        JSONResponse with generic error message
    """
    logger.error(
        f"Unhandled exception: {str(exc)}",
        extra={
            "exception_type": type(exc).__name__,
            "path": request.url.path,
        },
        exc_info=True,
    )

    # Don't expose internal error details in production
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse.create(
            message="Internal server error",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"exception_type": type(exc).__name__},
            request_id=request.headers.get("X-Request-ID"),
        ),
    )


# ============================================================================
# Helper Functions
# ============================================================================

def register_exception_handlers(app):
    """
    Register all exception handlers with FastAPI app

    Args:
        app: FastAPI application instance

    Usage:
        from app.api.exceptions import register_exception_handlers
        register_exception_handlers(app)
    """
    # Custom exceptions
    app.add_exception_handler(StockPickerException, stock_picker_exception_handler)

    # FastAPI exceptions
    app.add_exception_handler(HTTPException, http_exception_handler)

    # Generic exceptions
    app.add_exception_handler(Exception, generic_exception_handler)

    logger.info("Exception handlers registered")
