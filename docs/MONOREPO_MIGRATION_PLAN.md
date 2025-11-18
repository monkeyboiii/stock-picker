# Monorepo Migration Plan

**Stock Picker Platform - Monorepo Architecture Design**

---

## Executive Summary

This document outlines the migration plan to transform the current `stock-picker` backend service into a comprehensive monorepo architecture suitable for a production-grade stock analysis and backtesting platform.

**Goals:**
- Separate concerns into microservices for independent scaling
- Support multiple frontend applications (web, mobile, admin)
- Enable high-performance calculation services
- Establish proper infrastructure and DevOps practices
- Maintain backward compatibility during migration

**Timeline:** 8-12 weeks (phased migration)

---

## Table of Contents

1. [Target Architecture](#target-architecture)
2. [Monorepo Structure](#monorepo-structure)
3. [Service Breakdown](#service-breakdown)
4. [Technology Stack](#technology-stack)
5. [Migration Strategy](#migration-strategy)
6. [Infrastructure Setup](#infrastructure-setup)
7. [Development Workflow](#development-workflow)
8. [Deployment Strategy](#deployment-strategy)
9. [Risk Analysis](#risk-analysis)
10. [Timeline & Milestones](#timeline--milestones)

---

## Target Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Layer                            │
├─────────────┬─────────────┬──────────────┬─────────────────┤
│  Web App    │  Admin      │  Mobile App  │  Trading        │
│  (React)    │  Dashboard  │  (RN/Flutter)│  Terminal       │
│             │  (React)    │              │  (Desktop)      │
└─────────────┴─────────────┴──────────────┴─────────────────┘
                           │
                    ┌──────▼──────┐
                    │  API Gateway │
                    │  (Kong/Nginx)│
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼─────┐   ┌───────▼─────┐   ┌───────▼─────┐
│ Trading API │   │ Backtest API│   │  Data API   │
│ (FastAPI)   │   │ (FastAPI)   │   │  (FastAPI)  │
└───────┬─────┘   └───────┬─────┘   └───────┬─────┘
        │                  │                  │
        │         ┌────────▼────────┐         │
        │         │ Calculation Svc │         │
        │         │ (Rust/Go/C++)  │         │
        │         └────────┬────────┘         │
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼─────┐   ┌───────▼─────┐   ┌───────▼─────┐
│ PostgreSQL  │   │    Redis    │   │  TimescaleDB│
│ (Master+RR) │   │   (Cache)   │   │ (Time Series│
└─────────────┘   └─────────────┘   └─────────────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    ┌──────▼──────┐
                    │   Message   │
                    │   Queue     │
                    │ (RabbitMQ)  │
                    └─────────────┘
```

### Key Principles

1. **Separation of Concerns**: Each service has a single responsibility
2. **Independent Scalability**: Services scale independently based on load
3. **Polyglot Architecture**: Use the best language for each task
4. **API-First Design**: All services expose well-defined APIs
5. **Event-Driven**: Asynchronous communication via message queues
6. **Cloud-Native**: Containerized, orchestrated with Kubernetes

---

## Monorepo Structure

```
stock-analysis-platform/
├── .github/                          # GitHub Actions workflows
│   ├── workflows/
│   │   ├── ci-backend.yml           # Backend CI/CD
│   │   ├── ci-frontend.yml          # Frontend CI/CD
│   │   ├── ci-calculation.yml       # Calculation service CI/CD
│   │   └── deploy.yml               # Deployment workflows
│   └── CODEOWNERS                   # Code ownership
│
├── .vscode/                         # VS Code workspace settings
│   ├── settings.json
│   ├── extensions.json
│   └── launch.json
│
├── services/                        # Backend microservices
│   ├── trading-api/                # Trading & market data API (renamed from stock-picker)
│   │   ├── app/
│   │   │   ├── api/
│   │   │   ├── db/
│   │   │   ├── data/              # AKShare integration
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── backtest-api/               # Backtesting service
│   │   ├── app/
│   │   │   ├── api/
│   │   │   ├── backtest/
│   │   │   ├── strategy/
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── data-ingestion/             # Data pipeline service
│   │   ├── app/
│   │   │   ├── ingestion/
│   │   │   ├── schedulers/
│   │   │   └── main.py
│   │   ├── Dockerfile
│   │   └── README.md
│   │
│   ├── notification-service/       # Email, SMS, push notifications
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── package.json           # Node.js service
│   │   └── README.md
│   │
│   └── auth-service/               # Authentication & authorization
│       ├── src/
│       ├── Dockerfile
│       ├── package.json           # Node.js + Passport.js
│       └── README.md
│
├── calculation/                    # High-performance calculation services
│   ├── indicators/                # Technical indicators (Rust)
│   │   ├── src/
│   │   │   ├── moving_average.rs
│   │   │   ├── rsi.rs
│   │   │   ├── macd.rs
│   │   │   └── lib.rs
│   │   ├── Cargo.toml
│   │   ├── Dockerfile
│   │   └── README.md
│   │
│   ├── risk-analytics/            # Risk calculations (Go)
│   │   ├── cmd/
│   │   ├── pkg/
│   │   ├── go.mod
│   │   ├── Dockerfile
│   │   └── README.md
│   │
│   └── optimization/              # Portfolio optimization (C++ with Python bindings)
│       ├── src/
│       ├── CMakeLists.txt
│       ├── Dockerfile
│       └── README.md
│
├── frontends/                     # Frontend applications
│   ├── web-app/                  # Main web application (React + TypeScript)
│   │   ├── src/
│   │   │   ├── components/
│   │   │   ├── pages/
│   │   │   ├── api/
│   │   │   ├── hooks/
│   │   │   ├── stores/
│   │   │   └── App.tsx
│   │   ├── public/
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   ├── tsconfig.json
│   │   ├── vite.config.ts
│   │   └── README.md
│   │
│   ├── admin-dashboard/          # Admin panel (React + Ant Design)
│   │   ├── src/
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   └── README.md
│   │
│   ├── mobile-app/               # Mobile app (React Native or Flutter)
│   │   ├── src/
│   │   ├── android/
│   │   ├── ios/
│   │   ├── package.json
│   │   └── README.md
│   │
│   └── trading-terminal/         # Desktop trading terminal (Electron)
│       ├── src/
│       ├── package.json
│       └── README.md
│
├── shared/                       # Shared libraries and utilities
│   ├── types/                   # Shared TypeScript types
│   │   ├── src/
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── python-common/           # Shared Python utilities
│   │   ├── common/
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   └── proto/                   # Protocol Buffers definitions (for gRPC)
│       ├── trading.proto
│       ├── backtest.proto
│       └── README.md
│
├── infrastructure/              # Infrastructure as Code
│   ├── docker/                 # Docker configurations
│   │   ├── docker-compose.yml
│   │   ├── docker-compose.dev.yml
│   │   └── docker-compose.prod.yml
│   │
│   ├── kubernetes/             # Kubernetes manifests
│   │   ├── base/
│   │   │   ├── namespace.yaml
│   │   │   ├── configmap.yaml
│   │   │   └── secrets.yaml
│   │   ├── services/
│   │   │   ├── trading-api/
│   │   │   ├── backtest-api/
│   │   │   └── web-app/
│   │   └── kustomization.yaml
│   │
│   ├── terraform/              # Terraform for cloud infrastructure
│   │   ├── modules/
│   │   ├── environments/
│   │   │   ├── dev/
│   │   │   ├── staging/
│   │   │   └── production/
│   │   └── main.tf
│   │
│   ├── helm/                   # Helm charts
│   │   ├── stock-platform/
│   │   │   ├── Chart.yaml
│   │   │   ├── values.yaml
│   │   │   └── templates/
│   │   └── README.md
│   │
│   └── scripts/                # Infrastructure scripts
│       ├── setup-cluster.sh
│       ├── deploy.sh
│       └── backup.sh
│
├── docs/                       # Documentation
│   ├── architecture/
│   │   ├── adr/               # Architecture Decision Records
│   │   ├── diagrams/
│   │   └── README.md
│   ├── api/                   # API documentation
│   │   ├── trading-api.md
│   │   ├── backtest-api.md
│   │   └── openapi/
│   ├── development/
│   │   ├── setup.md
│   │   ├── contributing.md
│   │   └── coding-standards.md
│   └── deployment/
│       ├── local.md
│       ├── staging.md
│       └── production.md
│
├── scripts/                    # Development and deployment scripts
│   ├── dev/
│   │   ├── setup-env.sh
│   │   ├── start-services.sh
│   │   └── generate-types.sh
│   ├── ci/
│   │   ├── run-tests.sh
│   │   └── build-images.sh
│   └── deployment/
│       ├── deploy-dev.sh
│       ├── deploy-staging.sh
│       └── deploy-prod.sh
│
├── config/                     # Configuration files
│   ├── nginx/
│   │   ├── nginx.conf
│   │   └── ssl/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── dashboards/
│   └── redis/
│       └── redis.conf
│
├── tests/                      # Integration and E2E tests
│   ├── integration/
│   │   ├── test_trading_flow.py
│   │   └── test_backtest_flow.py
│   ├── e2e/
│   │   ├── cypress/           # Cypress E2E tests
│   │   └── playwright/        # Playwright tests
│   └── load/
│       └── k6/                # K6 load tests
│
├── .editorconfig              # Editor configuration
├── .gitignore                 # Git ignore rules
├── .prettierrc                # Prettier configuration (frontend)
├── .eslintrc.js              # ESLint configuration (frontend)
├── lerna.json                # Lerna monorepo management (optional)
├── package.json              # Root package.json for workspace
├── pnpm-workspace.yaml       # PNPM workspace configuration
├── Makefile                  # Common commands
├── README.md                 # Main README
├── LICENSE                   # License file
└── CHANGELOG.md              # Changelog

```

---

## Service Breakdown

### 1. Trading API Service (`services/trading-api/`)

**Renamed from:** `stock-picker`

**Responsibilities:**
- Stock data ingestion (AKShare integration)
- Real-time market data
- Stock filtering and screening
- Portfolio management
- Trade execution simulation

**Tech Stack:**
- Python 3.13+
- FastAPI + Uvicorn
- SQLAlchemy 2.0 + PostgreSQL
- Redis caching
- Celery for async tasks

**Key Modules (migrated from current app/):**
- `app/data/` → Data fetching from AKShare
- `app/filter/` → Stock filtering logic
- `app/db/` → Database models and access
- `app/display/` → Output formatters

**API Endpoints:**
```
/api/v1/stocks
/api/v1/markets
/api/v1/filters
/api/v1/portfolios
/api/v1/collections
```

---

### 2. Backtest API Service (`services/backtest-api/`)

**Responsibilities:**
- Strategy management (YAML DSL)
- Backtest execution engine
- Performance analytics
- Strategy comparison
- Historical replay

**Tech Stack:**
- Python 3.13+
- FastAPI + Uvicorn
- PostgreSQL (backtest results)
- Redis (caching)
- WebSocket (real-time updates)

**Key Modules (migrated from current app/):**
- `app/backtest/` → Backtesting engine
- `app/strategy/` → Strategy DSL parser
- `app/api/` → REST API + WebSocket

**API Endpoints:**
```
/api/v1/strategies
/api/v1/backtest/run
/api/v1/backtest/runs/{id}
/api/v1/backtest/runs/{id}/trades
/api/v1/backtest/runs/{id}/equity-curve
/ws/v1/backtest/{run_id}
```

---

### 3. Data Ingestion Service (`services/data-ingestion/`)

**Responsibilities:**
- Scheduled data ingestion (daily market data)
- Historical data backfill
- Data validation and cleaning
- Derived metrics calculation (MA250, etc.)
- Database state management

**Tech Stack:**
- Python 3.13+
- APScheduler or Celery Beat
- PostgreSQL
- TimescaleDB (for time-series optimization)

**Key Modules (migrated from current app/):**
- `app/utils/ingest.py` → Ingestion logic
- `app/utils/update.py` → Metrics calculation
- `app/constant/schedule.py` → Trade day scheduling

**Internal API:**
```
/internal/v1/ingest/trigger
/internal/v1/ingest/status
/internal/v1/metrics/calculate
```

---

### 4. Calculation Services (`calculation/`)

High-performance calculation services for computationally intensive tasks.

#### 4a. Indicators Service (Rust)

**Responsibilities:**
- Technical indicators (MA, RSI, MACD, Bollinger Bands, etc.)
- High-frequency calculations
- Bulk processing

**Tech Stack:**
- Rust 1.70+
- Actix-web or Axum
- Rayon (parallel processing)
- TA-lib bindings

**API:**
```
gRPC or REST
/api/v1/indicators/ma
/api/v1/indicators/rsi
/api/v1/indicators/batch
```

---

#### 4b. Risk Analytics Service (Go)

**Responsibilities:**
- VaR (Value at Risk) calculation
- Sharpe ratio, Sortino ratio
- Monte Carlo simulations
- Correlation matrices

**Tech Stack:**
- Go 1.21+
- Gin or Fiber
- gonum (numerical computing)

**API:**
```
/api/v1/risk/var
/api/v1/risk/sharpe
/api/v1/risk/monte-carlo
```

---

#### 4c. Portfolio Optimization Service (C++ with Python bindings)

**Responsibilities:**
- Mean-variance optimization
- Black-Litterman model
- Efficient frontier calculation
- Constraint-based optimization

**Tech Stack:**
- C++17
- pybind11 (Python bindings)
- Eigen library
- CVXOPT

**API:**
- Python API wrapper
- gRPC for inter-service communication

---

### 5. Auth Service (`services/auth-service/`)

**Responsibilities:**
- User authentication (JWT)
- Authorization (RBAC)
- Session management
- OAuth integration (Google, GitHub)

**Tech Stack:**
- Node.js 20+ (TypeScript)
- Express or NestJS
- Passport.js
- PostgreSQL (user data)
- Redis (sessions)

**API:**
```
/api/v1/auth/login
/api/v1/auth/logout
/api/v1/auth/refresh
/api/v1/auth/register
/api/v1/auth/verify
```

---

### 6. Notification Service (`services/notification-service/`)

**Responsibilities:**
- Email notifications (SendGrid/SES)
- SMS alerts (Twilio)
- Push notifications (FCM)
- In-app notifications

**Tech Stack:**
- Node.js 20+ (TypeScript)
- Bull (job queue)
- Redis
- WebSocket (real-time)

**API:**
```
/api/v1/notifications/send
/api/v1/notifications/preferences
/ws/v1/notifications
```

---

## Technology Stack

### Frontend Technologies

| Application | Framework | Language | UI Library | State Management |
|------------|-----------|----------|------------|------------------|
| Web App | React 18 + Vite | TypeScript | Tailwind CSS + shadcn/ui | Zustand / React Query |
| Admin Dashboard | React 18 + Vite | TypeScript | Ant Design | Redux Toolkit |
| Mobile App | React Native or Flutter | TypeScript / Dart | Native components | Redux / Riverpod |
| Trading Terminal | Electron + React | TypeScript | Custom components | Redux Toolkit |

**Common Frontend Dependencies:**
- **Charts**: Recharts, Plotly.js, TradingView Lightweight Charts
- **Tables**: TanStack Table (React Table)
- **Forms**: React Hook Form + Zod validation
- **HTTP Client**: Axios or Fetch API
- **WebSocket**: Native WebSocket API or Socket.io
- **Date**: date-fns or Day.js

---

### Backend Technologies

| Service | Language | Framework | Database | Cache | Queue |
|---------|----------|-----------|----------|-------|-------|
| Trading API | Python 3.13 | FastAPI | PostgreSQL 16 | Redis 7 | RabbitMQ |
| Backtest API | Python 3.13 | FastAPI | PostgreSQL 16 | Redis 7 | RabbitMQ |
| Data Ingestion | Python 3.13 | Celery | PostgreSQL 16 | Redis 7 | RabbitMQ |
| Auth Service | Node.js 20 (TS) | NestJS | PostgreSQL 16 | Redis 7 | - |
| Notification | Node.js 20 (TS) | Express | - | Redis 7 | Bull |
| Indicators | Rust 1.70+ | Axum | - | Redis 7 | - |
| Risk Analytics | Go 1.21+ | Gin | - | Redis 7 | - |

---

### Infrastructure & DevOps

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Container Runtime** | Docker 24+ | Containerization |
| **Orchestration** | Kubernetes 1.28+ | Container orchestration |
| **Service Mesh** | Istio or Linkerd | Service-to-service communication |
| **API Gateway** | Kong or Nginx | API routing, rate limiting |
| **Load Balancer** | Nginx or HAProxy | Load distribution |
| **Message Queue** | RabbitMQ or Kafka | Async messaging |
| **Databases** | PostgreSQL 16, TimescaleDB | Data storage |
| **Caching** | Redis 7 | In-memory cache |
| **Monitoring** | Prometheus + Grafana | Metrics and dashboards |
| **Logging** | ELK Stack (Elasticsearch, Logstash, Kibana) | Centralized logging |
| **Tracing** | Jaeger or Zipkin | Distributed tracing |
| **CI/CD** | GitHub Actions | Continuous integration |
| **IaC** | Terraform + Helm | Infrastructure provisioning |
| **Secret Management** | HashiCorp Vault or Kubernetes Secrets | Secrets management |

---

## Migration Strategy

### Phase 1: Monorepo Setup (Week 1-2)

**Objectives:**
- Create monorepo structure
- Set up tooling and workspace management
- Establish CI/CD pipelines

**Tasks:**
1. Create new repository structure
2. Configure PNPM/Yarn workspaces
3. Set up root-level tooling:
   - ESLint, Prettier (frontend)
   - Ruff, MyPy (Python)
   - Docker, Docker Compose
4. Create Makefile with common commands
5. Set up GitHub Actions workflows
6. Document development workflow

**Deliverables:**
- Monorepo skeleton
- Developer setup guide
- CI/CD pipelines (basic)

---

### Phase 2: Extract Trading API (Week 3-4)

**Objectives:**
- Extract core trading functionality into `services/trading-api/`
- Maintain backward compatibility
- Establish service boundaries

**Tasks:**
1. Copy current `stock-picker` to `services/trading-api/`
2. Remove backtest-related code (to be moved to backtest-api)
3. Extract shared code to `shared/python-common/`
4. Create Dockerfile for trading-api
5. Update pyproject.toml dependencies
6. Write service-specific tests
7. Create OpenAPI documentation
8. Set up service in docker-compose

**Migration Checklist:**
- [ ] Database models (Market, Stock, Collection, StockDaily, CollectionDaily)
- [ ] AKShare integration (`app/data/ak.py`)
- [ ] Filtering logic (`app/filter/`)
- [ ] Display formatters (`app/display/`)
- [ ] Constants (`app/constant/`)
- [ ] Database utilities (`app/db/`)

**Deliverables:**
- Trading API service (containerized)
- API documentation
- Unit tests

---

### Phase 3: Extract Backtest API (Week 5-6)

**Objectives:**
- Create standalone backtest service
- Move backtesting engine and strategy DSL
- Implement WebSocket for real-time updates

**Tasks:**
1. Create `services/backtest-api/` structure
2. Move backtest-related code:
   - `app/backtest/` → Backtesting engine
   - `app/strategy/` → Strategy DSL
   - `app/api/` → REST API + WebSocket
3. Create separate database schema for backtest results
4. Implement inter-service communication with Trading API
5. Set up WebSocket manager
6. Create Dockerfile
7. Update documentation

**Migration Checklist:**
- [ ] Backtest engine (`app/backtest/engine.py`)
- [ ] Strategy parser (`app/strategy/parser.py`)
- [ ] Analytics (`app/backtest/analytics.py`)
- [ ] Benchmark comparison (`app/backtest/comparison.py`)
- [ ] WebSocket manager (`app/api/websocket_manager.py`)
- [ ] REST API routes (`app/api/routes/backtest.py`)

**Deliverables:**
- Backtest API service
- WebSocket implementation
- API documentation
- Integration tests

---

### Phase 4: Create Data Ingestion Service (Week 7)

**Objectives:**
- Separate data ingestion from trading API
- Implement scheduled jobs
- Set up monitoring

**Tasks:**
1. Create `services/data-ingestion/` structure
2. Move ingestion logic:
   - `app/utils/ingest.py`
   - `app/utils/update.py`
3. Set up Celery Beat for scheduling
4. Implement health checks
5. Create monitoring dashboard
6. Set up alerting

**Deliverables:**
- Data Ingestion service
- Scheduled jobs
- Monitoring setup

---

### Phase 5: Build Calculation Services (Week 8-9)

**Objectives:**
- Implement high-performance calculation services
- Establish gRPC communication
- Benchmark performance

**Tasks:**

**Rust Indicators Service:**
1. Create `calculation/indicators/` project
2. Implement core indicators (MA, RSI, MACD, Bollinger Bands)
3. Create gRPC or REST API
4. Write benchmarks
5. Create Dockerfile

**Go Risk Analytics Service:**
1. Create `calculation/risk-analytics/` project
2. Implement risk metrics (VaR, Sharpe, Sortino)
3. Create REST API
4. Write benchmarks
5. Create Dockerfile

**Deliverables:**
- Indicators service (Rust)
- Risk Analytics service (Go)
- Performance benchmarks
- API documentation

---

### Phase 6: Develop Frontend Applications (Week 10-11)

**Objectives:**
- Build web application
- Create admin dashboard
- Establish design system

**Tasks:**

**Web App (`frontends/web-app/`):**
1. Set up React + Vite + TypeScript
2. Install dependencies (Recharts, Axios, Zustand)
3. Implement core features:
   - Stock screening
   - Backtest creation and monitoring
   - Equity curve visualization
   - Trade log
4. Set up authentication
5. Create Dockerfile

**Admin Dashboard (`frontends/admin-dashboard/`):**
1. Set up React + Ant Design
2. Implement admin features:
   - User management
   - System monitoring
   - Data ingestion control
3. Create Dockerfile

**Shared Types:**
1. Create `shared/types/` package
2. Define TypeScript interfaces for API responses
3. Set up type generation from OpenAPI specs

**Deliverables:**
- Web application
- Admin dashboard
- Shared TypeScript types
- UI component library

---

### Phase 7: Infrastructure & Deployment (Week 12)

**Objectives:**
- Set up Kubernetes cluster
- Implement CI/CD
- Deploy to staging environment

**Tasks:**
1. Create Kubernetes manifests (`infrastructure/kubernetes/`)
2. Set up Helm charts
3. Configure Terraform for cloud infrastructure
4. Implement GitHub Actions for CI/CD:
   - Build and test on PR
   - Deploy to staging on merge to `develop`
   - Deploy to production on tag
5. Set up monitoring (Prometheus + Grafana)
6. Set up logging (ELK stack)
7. Configure API Gateway (Kong or Nginx)
8. Set up TLS/SSL certificates

**Deliverables:**
- Kubernetes cluster (staging)
- CI/CD pipelines
- Monitoring and logging
- Deployment documentation

---

### Phase 8: Testing & Optimization (Ongoing)

**Objectives:**
- Comprehensive testing
- Performance optimization
- Documentation

**Tasks:**
1. Write integration tests (`tests/integration/`)
2. Write E2E tests (Cypress or Playwright)
3. Implement load tests (K6)
4. Optimize database queries
5. Implement caching strategies
6. Create architecture diagrams
7. Write deployment guides

**Deliverables:**
- Test suites (unit, integration, E2E)
- Performance benchmarks
- Comprehensive documentation

---

## Infrastructure Setup

### Docker Compose (Local Development)

**`infrastructure/docker/docker-compose.yml`:**

```yaml
version: '3.9'

services:
  # Databases
  postgres:
    image: postgres:16-alpine
    container_name: stock-platform-postgres
    environment:
      POSTGRES_USER: stock_user
      POSTGRES_PASSWORD: stock_pass
      POSTGRES_DB: stock_platform
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - stock-network

  timescaledb:
    image: timescale/timescaledb:latest-pg16
    container_name: stock-platform-timescale
    environment:
      POSTGRES_USER: timescale_user
      POSTGRES_PASSWORD: timescale_pass
      POSTGRES_DB: stock_timeseries
    ports:
      - "5433:5432"
    volumes:
      - timescale_data:/var/lib/postgresql/data
    networks:
      - stock-network

  redis:
    image: redis:7-alpine
    container_name: stock-platform-redis
    command: redis-server --appendonly yes
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - stock-network

  rabbitmq:
    image: rabbitmq:3-management-alpine
    container_name: stock-platform-rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: rabbitmq_user
      RABBITMQ_DEFAULT_PASS: rabbitmq_pass
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    networks:
      - stock-network

  # Backend Services
  trading-api:
    build:
      context: ../../services/trading-api
      dockerfile: Dockerfile
    container_name: trading-api
    environment:
      - DATABASE_URL=postgresql://stock_user:stock_pass@postgres:5432/stock_platform
      - REDIS_URL=redis://redis:6379/0
      - RABBITMQ_URL=amqp://rabbitmq_user:rabbitmq_pass@rabbitmq:5672/
    ports:
      - "8001:8000"
    depends_on:
      - postgres
      - redis
      - rabbitmq
    volumes:
      - ../../services/trading-api:/app
    networks:
      - stock-network

  backtest-api:
    build:
      context: ../../services/backtest-api
      dockerfile: Dockerfile
    container_name: backtest-api
    environment:
      - DATABASE_URL=postgresql://stock_user:stock_pass@postgres:5432/stock_platform
      - REDIS_URL=redis://redis:6379/1
      - TRADING_API_URL=http://trading-api:8000
    ports:
      - "8002:8000"
    depends_on:
      - postgres
      - redis
      - trading-api
    volumes:
      - ../../services/backtest-api:/app
    networks:
      - stock-network

  data-ingestion:
    build:
      context: ../../services/data-ingestion
      dockerfile: Dockerfile
    container_name: data-ingestion
    environment:
      - DATABASE_URL=postgresql://stock_user:stock_pass@postgres:5432/stock_platform
      - TIMESCALE_URL=postgresql://timescale_user:timescale_pass@timescaledb:5432/stock_timeseries
      - REDIS_URL=redis://redis:6379/2
      - RABBITMQ_URL=amqp://rabbitmq_user:rabbitmq_pass@rabbitmq:5672/
    depends_on:
      - postgres
      - timescaledb
      - redis
      - rabbitmq
    volumes:
      - ../../services/data-ingestion:/app
    networks:
      - stock-network

  # Calculation Services
  indicators:
    build:
      context: ../../calculation/indicators
      dockerfile: Dockerfile
    container_name: indicators-service
    ports:
      - "50051:50051"
    networks:
      - stock-network

  risk-analytics:
    build:
      context: ../../calculation/risk-analytics
      dockerfile: Dockerfile
    container_name: risk-analytics-service
    ports:
      - "8003:8000"
    networks:
      - stock-network

  # Frontend
  web-app:
    build:
      context: ../../frontends/web-app
      dockerfile: Dockerfile
      args:
        - VITE_API_BASE_URL=http://localhost:8080
    container_name: web-app
    ports:
      - "3000:80"
    depends_on:
      - trading-api
      - backtest-api
    networks:
      - stock-network

  # API Gateway
  nginx:
    image: nginx:alpine
    container_name: api-gateway
    ports:
      - "8080:80"
    volumes:
      - ../../config/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - trading-api
      - backtest-api
    networks:
      - stock-network

  # Monitoring
  prometheus:
    image: prom/prometheus:latest
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ../../config/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - stock-network

  grafana:
    image: grafana/grafana:latest
    container_name: grafana
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ../../config/grafana/dashboards:/etc/grafana/provisioning/dashboards
    depends_on:
      - prometheus
    networks:
      - stock-network

volumes:
  postgres_data:
  timescale_data:
  redis_data:
  rabbitmq_data:
  prometheus_data:
  grafana_data:

networks:
  stock-network:
    driver: bridge
```

---

### Kubernetes Deployment

**`infrastructure/kubernetes/services/trading-api/deployment.yaml`:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trading-api
  namespace: stock-platform
  labels:
    app: trading-api
    tier: backend
spec:
  replicas: 3
  selector:
    matchLabels:
      app: trading-api
  template:
    metadata:
      labels:
        app: trading-api
        tier: backend
    spec:
      containers:
      - name: trading-api
        image: gcr.io/your-project/trading-api:latest
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: database-credentials
              key: url
        - name: REDIS_URL
          valueFrom:
            configMapKeyRef:
              name: redis-config
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: trading-api
  namespace: stock-platform
spec:
  selector:
    app: trading-api
  ports:
  - protocol: TCP
    port: 8000
    targetPort: 8000
  type: ClusterIP
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: trading-api-hpa
  namespace: stock-platform
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: trading-api
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

---

## Development Workflow

### Setup Local Environment

```bash
# Clone repository
git clone https://github.com/monkeyboiii/stock-analysis-platform.git
cd stock-analysis-platform

# Install dependencies
make install

# Start all services
make dev

# Or start specific services
make dev-backend
make dev-frontend
make dev-calculation
```

### Makefile Commands

**`Makefile`:**

```makefile
.PHONY: help install dev test lint clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install all dependencies
	@echo "Installing dependencies..."
	pnpm install
	cd services/trading-api && uv sync
	cd services/backtest-api && uv sync
	cd services/data-ingestion && uv sync
	cd calculation/indicators && cargo build
	cd calculation/risk-analytics && go mod download

dev: ## Start all services in development mode
	docker-compose -f infrastructure/docker/docker-compose.yml up

dev-backend: ## Start backend services only
	docker-compose -f infrastructure/docker/docker-compose.yml up postgres redis rabbitmq trading-api backtest-api data-ingestion

dev-frontend: ## Start frontend services only
	cd frontends/web-app && pnpm dev

dev-calculation: ## Start calculation services
	docker-compose -f infrastructure/docker/docker-compose.yml up indicators risk-analytics

test: ## Run all tests
	@echo "Running backend tests..."
	cd services/trading-api && uv run pytest
	cd services/backtest-api && uv run pytest
	@echo "Running frontend tests..."
	cd frontends/web-app && pnpm test
	@echo "Running integration tests..."
	pytest tests/integration

test-unit: ## Run unit tests only
	cd services/trading-api && uv run pytest tests/
	cd services/backtest-api && uv run pytest tests/

test-integration: ## Run integration tests
	pytest tests/integration -v

test-e2e: ## Run E2E tests
	cd tests/e2e/cypress && pnpm cypress run

lint: ## Run linters
	@echo "Linting Python code..."
	cd services/trading-api && ruff check .
	cd services/backtest-api && ruff check .
	@echo "Linting TypeScript code..."
	cd frontends/web-app && pnpm lint

format: ## Format code
	cd services/trading-api && ruff check . --fix
	cd services/backtest-api && ruff check . --fix
	cd frontends/web-app && pnpm format

build: ## Build all services
	docker-compose -f infrastructure/docker/docker-compose.yml build

deploy-dev: ## Deploy to development environment
	./scripts/deployment/deploy-dev.sh

deploy-staging: ## Deploy to staging environment
	./scripts/deployment/deploy-staging.sh

deploy-prod: ## Deploy to production environment
	./scripts/deployment/deploy-prod.sh

clean: ## Clean build artifacts
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "node_modules" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	docker-compose -f infrastructure/docker/docker-compose.yml down -v

logs: ## Show logs from all services
	docker-compose -f infrastructure/docker/docker-compose.yml logs -f

logs-backend: ## Show backend logs
	docker-compose -f infrastructure/docker/docker-compose.yml logs -f trading-api backtest-api data-ingestion

shell-trading-api: ## Open shell in trading-api container
	docker-compose -f infrastructure/docker/docker-compose.yml exec trading-api /bin/bash

shell-backtest-api: ## Open shell in backtest-api container
	docker-compose -f infrastructure/docker/docker-compose.yml exec backtest-api /bin/bash

db-migrate: ## Run database migrations
	cd services/trading-api && uv run alembic upgrade head
	cd services/backtest-api && uv run alembic upgrade head

db-reset: ## Reset database
	docker-compose -f infrastructure/docker/docker-compose.yml down -v postgres
	docker-compose -f infrastructure/docker/docker-compose.yml up -d postgres
	sleep 5
	make db-migrate
```

---

## Deployment Strategy

### CI/CD Pipeline

**`.github/workflows/ci-backend.yml`:**

```yaml
name: Backend CI

on:
  push:
    branches: [ main, develop ]
    paths:
      - 'services/**'
      - 'shared/python-common/**'
  pull_request:
    branches: [ main, develop ]
    paths:
      - 'services/**'
      - 'shared/python-common/**'

jobs:
  test-trading-api:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: test_user
          POSTGRES_PASSWORD: test_pass
          POSTGRES_DB: test_db
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432

      redis:
        image: redis:7-alpine
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 6379:6379

    steps:
    - uses: actions/checkout@v4

    - name: Install uv
      run: curl -LsSf https://astral.sh/uv/install.sh | sh

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.13'

    - name: Install dependencies
      working-directory: services/trading-api
      run: uv sync

    - name: Run linters
      working-directory: services/trading-api
      run: |
        uv run ruff check .
        uv run mypy app/

    - name: Run tests
      working-directory: services/trading-api
      env:
        DATABASE_URL: postgresql://test_user:test_pass@localhost:5432/test_db
        REDIS_URL: redis://localhost:6379/0
      run: uv run pytest --cov=app --cov-report=xml

    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        files: services/trading-api/coverage.xml
        flags: trading-api

  build-docker-images:
    needs: test-trading-api
    runs-on: ubuntu-latest
    if: github.event_name == 'push'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Login to Container Registry
      uses: docker/login-action@v3
      with:
        registry: gcr.io
        username: _json_key
        password: ${{ secrets.GCR_JSON_KEY }}

    - name: Build and push Trading API
      uses: docker/build-push-action@v5
      with:
        context: services/trading-api
        push: true
        tags: |
          gcr.io/${{ secrets.GCP_PROJECT }}/trading-api:${{ github.sha }}
          gcr.io/${{ secrets.GCP_PROJECT }}/trading-api:latest
        cache-from: type=gha
        cache-to: type=gha,mode=max
```

---

### Deployment Environments

| Environment | Branch | Auto-Deploy | Purpose |
|------------|--------|-------------|---------|
| **Development** | `develop` | Yes | Feature testing |
| **Staging** | `staging` | Yes | Pre-production testing |
| **Production** | `main` | Manual (tag) | Live environment |

**Deployment Process:**

1. **Development**: Auto-deploy on push to `develop`
2. **Staging**: Auto-deploy on push to `staging`
3. **Production**: Manual approval required, triggered by Git tag (e.g., `v1.0.0`)

---

## Risk Analysis

### Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Service communication latency | High | Medium | Implement caching, use gRPC for internal comms |
| Database migration complexity | High | High | Use Alembic, test migrations thoroughly |
| Frontend-backend version mismatch | Medium | Medium | API versioning, OpenAPI spec generation |
| Calculation service performance | High | Medium | Benchmark early, optimize hot paths |
| Data consistency across services | High | Medium | Implement distributed transactions (Saga pattern) |

### Operational Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Increased infrastructure costs | Medium | High | Right-size resources, use auto-scaling |
| Complexity in debugging | High | High | Comprehensive logging and tracing |
| Developer onboarding time | Medium | High | Detailed documentation, Makefile automation |
| Deployment failures | High | Medium | Canary deployments, rollback procedures |

---

## Timeline & Milestones

### Phase-by-Phase Timeline

| Phase | Duration | Start | End | Key Deliverable |
|-------|----------|-------|-----|----------------|
| 1. Monorepo Setup | 2 weeks | Week 1 | Week 2 | Monorepo structure + CI/CD |
| 2. Trading API | 2 weeks | Week 3 | Week 4 | Containerized Trading API |
| 3. Backtest API | 2 weeks | Week 5 | Week 6 | Backtest service + WebSocket |
| 4. Data Ingestion | 1 week | Week 7 | Week 7 | Data ingestion service |
| 5. Calculation Services | 2 weeks | Week 8 | Week 9 | Rust + Go services |
| 6. Frontend Apps | 2 weeks | Week 10 | Week 11 | Web app + Admin dashboard |
| 7. Infrastructure | 1 week | Week 12 | Week 12 | K8s cluster + monitoring |
| 8. Testing & Optimization | Ongoing | Week 13+ | - | Comprehensive testing |

### Major Milestones

- **M1 (Week 2)**: Monorepo structure complete, CI/CD running
- **M2 (Week 4)**: Trading API extracted and containerized
- **M3 (Week 6)**: Backtest API functional with WebSocket
- **M4 (Week 9)**: All calculation services operational
- **M5 (Week 11)**: Web application deployed
- **M6 (Week 12)**: Staging environment live
- **M7 (Week 16)**: Production-ready release

---

## Success Criteria

### Technical Metrics

- **API Response Time**: p95 < 200ms for Trading API, p95 < 500ms for Backtest API
- **Throughput**: Handle 1000 req/s per service
- **Uptime**: 99.9% availability
- **Test Coverage**: >80% for all services
- **Build Time**: <10 minutes for full CI/CD pipeline

### Business Metrics

- **Developer Productivity**: 50% faster feature development after migration
- **Infrastructure Cost**: <20% increase in costs
- **Deployment Frequency**: Daily deployments to staging
- **Mean Time to Recovery (MTTR)**: <30 minutes

---

## Appendix

### A. Naming Conventions

**Services:**
- Use kebab-case: `trading-api`, `backtest-api`
- Suffix with `-api` for API services, `-service` for workers

**Databases:**
- Prefix tables with service name: `trading_stocks`, `backtest_runs`
- Use snake_case for table and column names

**Docker Images:**
- Format: `gcr.io/project/service-name:tag`
- Tags: `latest`, `vX.Y.Z`, `git-SHA`

**Environment Variables:**
- Use SCREAMING_SNAKE_CASE: `DATABASE_URL`, `REDIS_URL`

### B. Security Considerations

1. **API Gateway**: Rate limiting, IP whitelisting, DDoS protection
2. **Service-to-Service**: Mutual TLS (mTLS) with service mesh
3. **Secrets**: Store in Vault or Kubernetes Secrets, never in code
4. **Database**: Encrypted connections, row-level security
5. **Frontend**: CSP headers, XSS protection, HTTPS only

### C. Monitoring & Observability

**Metrics to Track:**
- Request rate, error rate, duration (RED method)
- CPU, memory, disk usage
- Database connection pool stats
- Queue depth, message processing rate
- Cache hit rate

**Logging Standards:**
- Structured logging (JSON format)
- Include trace ID, service name, timestamp
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

**Alerting:**
- High error rate (>5% for 5 minutes)
- High latency (p95 >500ms for 10 minutes)
- Low availability (<99.5%)
- Resource exhaustion (>85% CPU/memory)

### D. Disaster Recovery

**Backup Strategy:**
- PostgreSQL: Daily full backup, hourly incremental
- Redis: RDB snapshots every 6 hours
- Config: Version-controlled in Git

**Recovery Procedures:**
- RTO (Recovery Time Objective): 1 hour
- RPO (Recovery Point Objective): 1 hour
- Automated failover for databases

---

## Conclusion

This migration plan provides a comprehensive roadmap for transforming the current `stock-picker` backend into a scalable, production-ready monorepo platform. The phased approach minimizes risk while enabling incremental value delivery.

**Next Steps:**
1. Review and approve migration plan
2. Assign team members to each phase
3. Set up initial monorepo structure
4. Begin Phase 1 implementation

**Questions or Concerns:**
- Please open a GitHub issue or discussion
- Contact: monkeyboiii

---

**Document Version:** 1.0.0
**Last Updated:** 2025-11-18
**Author:** Claude (AI Assistant) with monkeyboiii
