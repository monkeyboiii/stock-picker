# Backend Services

This directory contains all Python backend services (FastAPI microservices).

## Services

### `trading-api/` (Phase 2 - Extract from current stock-picker)
- **Tech Stack**: Python 3.13 + FastAPI + SQLAlchemy
- **Database**: PostgreSQL + QuestDB + Redis
- **Purpose**: Stock data, filtering, portfolios, collections
- **Port**: 8001

### `backtest-api/` (Phase 2 - Extract from current stock-picker)
- **Tech Stack**: Python 3.13 + FastAPI + WebSocket
- **Database**: PostgreSQL + QuestDB + Redis
- **Purpose**: Strategy execution, backtesting, analytics
- **Port**: 8002

### `calculation-api/` (Phase 4)
- **Tech Stack**: Python 3.13 + FastAPI + pandas/numpy
- **Database**: Redis (caching)
- **Purpose**: Technical indicators, risk analytics, optimization
- **Port**: 8005

### `auth-api/` (Phase 3)
- **Tech Stack**: Python 3.13 + FastAPI + JWT
- **Database**: PostgreSQL + Redis
- **Purpose**: Authentication, OAuth2, RBAC
- **Port**: 8003

### `notification-api/` (Phase 3)
- **Tech Stack**: Python 3.13 + FastAPI + Celery
- **Database**: PostgreSQL + Redis
- **Purpose**: Email, SMS, push notifications
- **Port**: 8004

### `data-ingestion/` (Phase 2)
- **Tech Stack**: Python 3.13 + Celery Beat
- **Database**: PostgreSQL + QuestDB
- **Purpose**: Scheduled data ingestion from AKShare

## Development

Each service has its own:
- `pyproject.toml` - Python dependencies (uv managed)
- `Dockerfile` - Container image
- `tests/` - Unit tests (pytest)
- `app/` - Application code

```bash
# Install dependencies for a service
cd services/trading-api && uv sync

# Run a service locally
cd services/trading-api && uv run uvicorn app.main:app --reload --port 8001

# Run all services with Docker Compose
docker-compose -f infrastructure/docker/docker-compose.dev.yml up
```

## OpenAPI Specs

Each service auto-generates OpenAPI specs at:
- Trading API: http://localhost:8001/openapi.json
- Backtest API: http://localhost:8002/openapi.json
- Calculation API: http://localhost:8005/openapi.json
- Auth API: http://localhost:8003/openapi.json

These are used to generate TypeScript clients in `packages/api/`.
