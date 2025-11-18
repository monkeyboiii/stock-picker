# Backend Services

This directory contains all Python backend services (FastAPI microservices).

## Services

### `trading-api/` ✅ (Phase 2 - Complete)
- **Tech Stack**: Python 3.13 + FastAPI + SQLAlchemy
- **Database**: PostgreSQL
- **Purpose**: Stock data ingestion, filtering, display (Google Sheets, TDX)
- **Port**: 8000
- **Endpoints**: 11 REST endpoints (ingest, filter, metrics, stocks, feed)

### `backtest-api/` ✅ (Phase 2 - Complete)
- **Tech Stack**: Python 3.13 + FastAPI
- **Database**: PostgreSQL
- **Purpose**: Strategy backtesting, performance analytics, trade logs
- **Port**: 8001
- **Endpoints**: 4 REST endpoints (backtest, performance, feed)

### `calculation-api/` (Phase 4)
- **Tech Stack**: Python 3.13 + FastAPI + pandas/numpy
- **Database**: Redis (caching)
- **Purpose**: Technical indicators, risk analytics, optimization
- **Port**: 8005

### `auth-api/` ✅ (Phase 3 - Complete)
- **Tech Stack**: Python 3.13 + FastAPI + JWT + bcrypt
- **Database**: PostgreSQL
- **Purpose**: User authentication, JWT tokens, RBAC, session management
- **Port**: 8003
- **Endpoints**: 9 REST endpoints (register, login, logout, refresh, profile, users)

### `notification-api/` ✅ (Phase 3 - Complete)
- **Tech Stack**: Python 3.13 + FastAPI
- **Providers**: SendGrid (email), Twilio (SMS), FCM (push) - stub implementations
- **Purpose**: Multi-channel notifications (email, SMS, push)
- **Port**: 8004
- **Endpoints**: 4 REST endpoints (email, sms, push, templates)

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
cd services/trading-api && uv run uvicorn app.main:app --reload --port 8000

# Run all services with Docker Compose ✅ (Phase 3)
docker-compose -f infrastructure/docker/docker-compose.dev.yml up
```

## OpenAPI Specs

Each service auto-generates OpenAPI specs at runtime:
- **Trading API**: http://localhost:8000/openapi.json (Swagger: http://localhost:8000/docs)
- **Backtest API**: http://localhost:8001/openapi.json (Swagger: http://localhost:8001/docs)
- **Auth API**: http://localhost:8003/openapi.json (Swagger: http://localhost:8003/docs) ✅
- **Notification API**: http://localhost:8004/openapi.json (Swagger: http://localhost:8004/docs) ✅
- **Calculation API**: http://localhost:8005/openapi.json (Phase 4)

These specs will be used to generate TypeScript clients in `packages/api/` (Phase 6).
