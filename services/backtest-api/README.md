# Backtest API

Strategy backtesting and performance analysis service

## Overview

Backtest API is a FastAPI-based microservice that provides:

- **Strategy Backtesting**: Run backtests on historical stock data
- **Performance Analytics**: Calculate returns, Sharpe ratio, drawdown, win rate
- **Trade Log Management**: Store and retrieve backtest results
- **Feed Access**: Query filtered stock feeds for backtesting

## Tech Stack

- **Framework**: FastAPI 0.115+ (async Python web framework)
- **Database**: PostgreSQL 16+ with SQLAlchemy 2.0
- **Analysis**: pandas, numpy for performance calculations
- **Python**: 3.13+

## API Endpoints

### Health Checks

- `GET /` - Basic health check
- `GET /health` - Health check with database connectivity test

### Backtesting

- `POST /backtest` - Run a backtest
  - Parameters:
    - `strategy_name` - Name of the strategy
    - `start_date` - Backtest start date
    - `end_date` - Backtest end date
    - `initial_capital` - Initial capital in CNY (default: 100,000)
    - `filter_id` - Filter ID to use (T2-T8)
  - Returns: Backtest results with total trades and return

### Performance Analytics

- `GET /backtest/{strategy_name}/performance` - Get performance metrics
  - Parameters: `start_date`, `end_date`, `filter_id`
  - Returns: Performance metrics (return, Sharpe, drawdown, win rate)

### Feed Queries

- `GET /feed/{trade_date}` - Get filtered stock feed for a specific date
  - Parameters: `trade_date`, `filter_id`
  - Returns: List of stocks matching the filter

## Development

### Prerequisites

- Python 3.13+
- PostgreSQL 16+
- uv (Python package manager)

### Setup

```bash
# Install dependencies
cd services/backtest-api
uv sync

# Set environment variables
cp ../../example.env .env
# Edit .env with your database credentials

# Run the service
uv run uvicorn app.main:app --reload --port 8001
```

### Testing

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html
```

### Docker

```bash
# Build image
docker build -t backtest-api:latest .

# Run container
docker run -p 8001:8001 \
  -e POSTGRES_USERNAME=user \
  -e POSTGRES_PASSWORD=pass \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_DATABASE=stock_picker \
  backtest-api:latest
```

## API Documentation

Once running, visit:

- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc
- **OpenAPI JSON**: http://localhost:8001/openapi.json

## Environment Variables

Required:
- `POSTGRES_USERNAME` - Database username
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_HOST` - Database host (default: localhost)
- `POSTGRES_PORT` - Database port (default: 5432)
- `POSTGRES_DATABASE` - Database name

## Backtest Workflow

1. **Prepare Data**: Ensure stock data is ingested via Trading API
2. **Define Strategy**: Create a strategy with entry/exit conditions
3. **Run Backtest**: POST to `/backtest` with date range and parameters
4. **Analyze Results**: GET performance metrics via `/backtest/{name}/performance`
5. **Iterate**: Adjust strategy parameters and re-run

## Current Implementation

This is Phase 2 implementation with basic backtest infrastructure. Full features coming in later phases:

- ✅ Basic backtest endpoint
- ✅ Performance metrics calculation (placeholders)
- ✅ Feed access for backtesting
- ⏳ Strategy DSL (Phase 4)
- ⏳ Advanced analytics (Sharpe, drawdown, etc.) - Phase 4
- ⏳ Trade log storage and playback - Phase 4
- ⏳ Parameter optimization - Phase 5
- ⏳ LLM integration for strategy generation - Phase 6

## License

MIT
