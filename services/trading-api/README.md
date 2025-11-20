# Trading API

Stock data ingestion, filtering, and analysis service

## Overview

Trading API is a FastAPI-based microservice that provides:

- **Stock Data Ingestion**: Fetch stock data from AKShare (Chinese markets)
- **Technical Filtering**: Apply tail scraper filters (T2-T8) to find stocks
- **Data Management**: CRUD operations for stocks and daily data
- **Display Integration**: Support for Google Sheets and TDX format output

## Tech Stack

- **Framework**: FastAPI 0.115+ (async Python web framework)
- **Database**: PostgreSQL 16+ with SQLAlchemy 2.0
- **Data Source**: AKShare (Chinese stock market data)
- **Python**: 3.13+

## API Endpoints

### Health Checks

- `GET /` - Basic health check
- `GET /health` - Health check with database connectivity test

### Data Ingestion

- `POST /ingest` - Ingest stock data for a trading day
  - Parameters: `trade_date` (optional), `force` (bool)
  - Returns: Number of stocks updated

### Metrics & Filtering

- `POST /update-metrics` - Update derived metrics (ma250)
  - Parameters: `trade_date` (optional)
  - Returns: Number of stocks updated

- `POST /filter` - Filter stocks using tail scraper
  - Parameters: `trade_date` (optional), `filter_id` (T2-T8)
  - Returns: Number of stocks found

### Stock Queries

- `GET /stocks` - List all stocks (paginated)
  - Parameters: `limit` (1-1000), `offset`

- `GET /stocks/{code}` - Get detailed stock information
  - Parameters: `code` (e.g., "600000")

- `GET /stocks/{code}/daily` - Get daily price data
  - Parameters: `start_date`, `end_date`, `limit`

- `GET /feed` - Get filtered stock feed
  - Parameters: `trade_date`, `filter_id`, `limit`

## Development

### Prerequisites

- Python 3.13+
- PostgreSQL 16+
- uv (Python package manager)

### Setup

```bash
# Install dependencies
cd services/trading-api
uv sync

# Set environment variables
cp ../../example.env .env
# Edit .env with your database credentials

# Run the service
uv run uvicorn app.main:app --reload --port 8000
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
docker build -t trading-api:latest .

# Run container
docker run -p 8000:8000 \
  -e POSTGRES_USERNAME=user \
  -e POSTGRES_PASSWORD=pass \
  -e POSTGRES_HOST=localhost \
  -e POSTGRES_DATABASE=stock_picker \
  trading-api:latest
```

## API Documentation

Once running, visit:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Environment Variables

Required:
- `POSTGRES_USERNAME` - Database username
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_HOST` - Database host (default: localhost)
- `POSTGRES_PORT` - Database port (default: 5432)
- `POSTGRES_DATABASE` - Database name

Optional:
- `GOOGLE_SHEET_ID` - Google Sheets ID for display integration

## Filter IDs

- **T2**: Quantity relative ratio ≥ 1.0
- **T3**: Turnover rate > 5.0%
- **T4**: Circulation capital between 20M - 2B (in 万元)
- **T6**: Exclude ST stocks and stocks with "*"
- **T7**: MA250 exists and low price > MA250
- **T8**: Close price > Open price (positive day)

## License

MIT
