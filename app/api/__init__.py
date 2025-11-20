"""
API Package - REST API for Stock Picker Backtesting

This package provides a FastAPI-based REST API for:
- Running backtests asynchronously
- Managing strategies (CRUD operations)
- Querying backtest results
- Retrieving trade history and equity curves

Run the API server:
    uvicorn app.api.main:app --reload
    # or
    python -m app.api.main

API Documentation:
    http://localhost:8000/api/docs (Swagger UI)
    http://localhost:8000/api/redoc (ReDoc)
"""

__all__ = ["main"]
