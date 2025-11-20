# Monorepo Migration Plan

**Stock Picker Platform - Pragmatic Monorepo Architecture**

---

## Executive Summary

This document outlines a **pragmatic migration plan** to transform the current `stock-picker` backend service into a production-grade stock analysis and backtesting platform using a carefully selected, unified technology stack.

**Core Philosophy:**
- **Python-first approach**: All backend services in Python (FastAPI) including calculations
- **Incremental optimization**: Start with pandas/numpy, optimize with Rust later
- **Maximize code reuse**: Shared TypeScript packages across web, mobile, and desktop
- **Type-safe everything**: OpenAPI-generated TypeScript clients from Python services
- **Modern full-stack**: Next.js/Remix for web, React Native (Expo) for mobile, Electron for desktop
- **Proven databases**: PostgreSQL for relational data, QuestDB for time-series, Redis for caching

**Timeline:** 8-9 weeks (core platform) + optional Rust optimization phase

---

## Table of Contents

1. [Target Architecture](#target-architecture)
2. [Technology Stack Rationale](#technology-stack-rationale)
3. [Monorepo Structure](#monorepo-structure)
4. [Service Breakdown](#service-breakdown)
5. [Migration Strategy](#migration-strategy)
6. [Development Workflow](#development-workflow)
7. [Type Safety & Code Generation](#type-safety--code-generation)
8. [Timeline & Milestones](#timeline--milestones)

---

## Target Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Layer                            │
├──────────────────┬──────────────────┬───────────────────────┤
│   Web App        │   Mobile App     │   Desktop App         │
│   (Next.js/      │   (React Native  │   (Electron +         │
│    Remix)        │    + Expo)       │    RN Web)            │
└──────────────────┴──────────────────┴───────────────────────┘
         │                   │                    │
         └───────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Shared TS     │ ← OpenAPI codegen
                    │   Packages      │   from Python
                    │  (API clients,  │
                    │   auth, types)  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  API Gateway    │
                    │  (Caddy/Nginx)  │
                    └────────┬────────┘
                             │
    ┌───────────────────────┼───────────────────────┐
    │                       │                       │
┌───▼─────────┐  ┌─────────▼────────┐  ┌──────────▼────────┐
│ Trading API │  │  Backtest API    │  │   Auth API        │
│  (FastAPI)  │  │   (FastAPI)      │  │   (FastAPI)       │
└───┬─────────┘  └─────────┬────────┘  └──────────┬────────┘
    │                       │                       │
    │              ┌────────▼────────┐              │
    │              │ Calculation API │              │
    │              │ (FastAPI +      │              │
    │              │  pandas/numpy)  │ ← Phase 11-12: Rust
    │              └────────┬────────┘   (optional)
    │                       │                       │
    └───────────────────────┼───────────────────────┘
                            │
    ┌───────────────────────┼───────────────────────┐
    │                       │                       │
┌───▼─────────┐  ┌─────────▼────────┐  ┌──────────▼────────┐
│ PostgreSQL  │  │    QuestDB       │  │     Redis         │
│(Relational) │  │  (Time Series)   │  │    (Cache)        │
└─────────────┘  └──────────────────┘  └───────────────────┘
    │                       │                       │
    └───────────────────────┼───────────────────────┘
                            │
                   ┌────────▼────────┐
                   │   RabbitMQ      │
                   │  (Message Bus)  │
                   └─────────────────┘
```

### Key Principles

1. **Python everywhere**: All API services in Python (FastAPI) - Trading, Backtest, Auth, Notification, Calculation
2. **Start simple, optimize later**: Use pandas/numpy initially, migrate to Rust when needed
3. **TypeScript everywhere frontend**: Next.js/Remix (web) + React Native (mobile/desktop)
4. **Type safety across boundaries**: OpenAPI → TypeScript codegen
5. **Maximum code reuse**: Shared packages for API client, auth, UI components
6. **Cloud-native**: Containerized, orchestrated with Kubernetes

---

## Technology Stack Rationale

### Backend: Python (FastAPI) for Everything

**Why Python for auth/notifications instead of Node.js?**
- ✅ **Single backend language**: Easier hiring, onboarding, knowledge sharing
- ✅ **FastAPI excellence**: Built-in OpenAPI spec generation for TypeScript codegen
- ✅ **Async support**: asyncio handles auth/notification workloads efficiently
- ✅ **Rich ecosystem**: Battle-tested libraries for email (SendGrid), SMS (Twilio), JWT (python-jose)
- ✅ **Team familiarity**: Your team already knows Python deeply

**Services:**
- Trading API (FastAPI)
- Backtest API (FastAPI)
- Auth API (FastAPI) - JWT, OAuth2, session management
- Notification API (FastAPI) - Email, SMS, push notifications
- Data Ingestion (FastAPI + Celery)

### Calculations: Python First, Rust Later

**Phase 1 (Weeks 1-9): Python Calculation API**
- ✅ **Immediate value**: Leverage existing calculation code (pandas/numpy)
- ✅ **Faster to market**: No need to rewrite in Rust initially
- ✅ **Single language**: All backend in Python for simplicity
- ✅ **Good enough**: pandas/numpy handle most workloads well
- ✅ **Easy to evolve**: Can refactor to Rust later when bottlenecks identified

**Calculation API (Python + FastAPI):**
- Technical indicators (MA, RSI, MACD, Bollinger Bands, etc.)
- Risk analytics (VaR, Sharpe, Sortino, Monte Carlo)
- Portfolio optimization (mean-variance, efficient frontier)
- Built with pandas, numpy, scipy, ta-lib
- REST API for now, can add gRPC later

**Phase 2 (Optional - Weeks 11-12): Rust Optimization**
- ⚡ **When needed**: After profiling shows performance bottlenecks
- ⚡ **Incremental**: Replace hot paths one at a time
- ⚡ **Rust benefits**: 10-100x speedup for calculation-heavy workloads
- ⚡ **Polars**: Rust DataFrame library (similar to pandas)
- ⚡ **gRPC**: Fast binary protocol for internal communication

### Time-Series: QuestDB

**Why QuestDB instead of TimescaleDB?**
- ✅ **SQL compatible**: No PostgreSQL extension needed, standalone
- ✅ **Blazing fast**: 10x faster ingestion than TimescaleDB for time-series
- ✅ **Built for finance**: Designed for tick data, OHLC, time-series analytics
- ✅ **Better compression**: Optimized columnar storage
- ✅ **Simpler ops**: Less PostgreSQL-specific tuning required
- ✅ **Great SQL extensions**: `SAMPLE BY`, `LATEST ON`, time-series JOINs

**Use cases:**
- Stock daily prices (OHLC + volume)
- Minute/tick data (if real-time trading added later)
- Backtest equity curves (date + portfolio value)
- Historical metrics (calculated indicators)

### Frontend: Next.js/Remix + React Native

**Why Next.js/Remix instead of Vite?**
- ✅ **SEO-first**: Server-side rendering for marketing pages, docs
- ✅ **API routes**: Built-in API endpoints if needed (proxy, webhooks)
- ✅ **File-based routing**: Intuitive page structure
- ✅ **Production-ready**: Built-in optimizations, image optimization
- ✅ **Flexible rendering**: SSR, SSG, ISR based on page needs

**Why React Native + Expo?**
- ✅ **Maximum code reuse**: Share 80%+ code with web via react-native-web
- ✅ **Expo simplicity**: No native code setup, OTA updates, managed workflow
- ✅ **Shared TypeScript**: Same API clients, state management, business logic
- ✅ **Cross-platform**: iOS + Android from single codebase

**Why Electron + React Native Web?**
- ✅ **Almost free**: Reuse web app as-is with minimal Electron wrapper
- ✅ **Native feel**: Desktop-specific features (notifications, menubar)
- ✅ **Shared codebase**: Same components as web and mobile

### Type Safety: OpenAPI → TypeScript Codegen

**Why this approach?**
- ✅ **FastAPI generates OpenAPI**: Built-in, always up-to-date
- ✅ **Automatic codegen**: `openapi-typescript` or `openapi-typescript-codegen`
- ✅ **Type-safe APIs**: Frontend knows exactly what backend returns
- ✅ **No manual typing**: API changes propagate automatically
- ✅ **Error prevention**: Catch breaking changes at compile time

**Workflow:**
```bash
# FastAPI generates openapi.json automatically
curl http://localhost:8000/openapi.json -o openapi.json

# Generate TypeScript types and clients
npx openapi-typescript openapi.json -o shared/types/api.ts

# Use in frontend with full type safety
import type { paths } from '@/shared/types/api'
```

---

## Monorepo Structure

```
stock-analysis-platform/
├── .github/                          # GitHub Actions workflows
│   ├── workflows/
│   │   ├── ci-backend.yml           # Python services CI
│   │   ├── ci-calculation.yml       # Rust service CI
│   │   ├── ci-frontend.yml          # Next.js/RN CI
│   │   └── deploy.yml               # Deployment workflows
│   └── CODEOWNERS                   # Code ownership
│
├── services/                        # Python backend services
│   ├── trading-api/                # Trading & market data API
│   │   ├── app/
│   │   │   ├── api/                # FastAPI routes
│   │   │   ├── db/                 # SQLAlchemy models
│   │   │   ├── data/               # AKShare integration
│   │   │   ├── filter/             # Stock filtering
│   │   │   └── main.py             # Entry point
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── backtest-api/               # Backtesting service
│   │   ├── app/
│   │   │   ├── api/                # REST + WebSocket
│   │   │   ├── backtest/           # Engine
│   │   │   ├── strategy/           # DSL parser
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── calculation-api/            # Calculation service (Python)
│   │   ├── app/
│   │   │   ├── api/
│   │   │   │   ├── indicators.py   # Technical indicators endpoints
│   │   │   │   ├── risk.py         # Risk analytics endpoints
│   │   │   │   └── optimization.py # Portfolio optimization
│   │   │   ├── core/
│   │   │   │   ├── indicators/     # MA, RSI, MACD, etc. (pandas/numpy)
│   │   │   │   ├── risk/           # VaR, Sharpe, Monte Carlo
│   │   │   │   └── optimization/   # Mean-variance, efficient frontier
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── auth-api/                   # Authentication service
│   │   ├── app/
│   │   │   ├── api/
│   │   │   │   ├── auth.py         # Login, logout, refresh
│   │   │   │   ├── users.py        # User management
│   │   │   │   └── oauth.py        # Google/GitHub OAuth
│   │   │   ├── models/
│   │   │   │   ├── user.py
│   │   │   │   └── session.py
│   │   │   ├── security/
│   │   │   │   ├── jwt.py          # JWT utilities
│   │   │   │   ├── password.py     # Hashing
│   │   │   │   └── rbac.py         # Role-based access
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   ├── notification-api/           # Notification service
│   │   ├── app/
│   │   │   ├── api/
│   │   │   │   ├── email.py        # SendGrid integration
│   │   │   │   ├── sms.py          # Twilio integration
│   │   │   │   └── push.py         # FCM integration
│   │   │   ├── templates/          # Email templates
│   │   │   ├── tasks/              # Celery tasks
│   │   │   └── main.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   ├── pyproject.toml
│   │   └── README.md
│   │
│   └── data-ingestion/             # Data pipeline service
│       ├── app/
│       │   ├── schedulers/         # Celery Beat tasks
│       │   ├── ingestion/          # AKShare data fetching
│       │   ├── processing/         # Data validation
│       │   └── main.py
│       ├── Dockerfile
│       ├── pyproject.toml
│       └── README.md
│
├── apps/                          # Frontend applications
│   ├── web/                      # Next.js web application
│   │   ├── src/
│   │   │   ├── app/              # App router (Next.js 14+)
│   │   │   │   ├── (marketing)/  # Marketing pages
│   │   │   │   ├── (dashboard)/  # Dashboard pages
│   │   │   │   ├── api/          # API routes
│   │   │   │   └── layout.tsx
│   │   │   ├── components/
│   │   │   │   ├── ui/           # shadcn/ui components
│   │   │   │   ├── charts/       # Chart components
│   │   │   │   └── features/     # Feature components
│   │   │   ├── lib/
│   │   │   │   ├── api.ts        # API client (uses @repo/api)
│   │   │   │   └── utils.ts
│   │   │   └── styles/
│   │   ├── public/
│   │   ├── Dockerfile
│   │   ├── package.json
│   │   ├── next.config.js
│   │   ├── tailwind.config.ts
│   │   └── tsconfig.json
│   │
│   ├── mobile/                   # React Native (Expo) app
│   │   ├── src/
│   │   │   ├── screens/
│   │   │   │   ├── HomeScreen.tsx
│   │   │   │   ├── BacktestScreen.tsx
│   │   │   │   └── ProfileScreen.tsx
│   │   │   ├── navigation/
│   │   │   │   └── AppNavigator.tsx
│   │   │   ├── components/       # RN-specific components
│   │   │   └── App.tsx
│   │   ├── app.json              # Expo config
│   │   ├── Dockerfile            # For EAS build
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   └── desktop/                  # Electron + React Native Web
│       ├── src/
│       │   ├── main/             # Electron main process
│       │   │   ├── main.ts
│       │   │   └── preload.ts
│       │   └── renderer/         # React Native Web (reuse web app)
│       ├── package.json
│       └── electron-builder.yml
│
├── packages/                     # Shared TypeScript packages
│   ├── api/                     # API client (OpenAPI-generated)
│   │   ├── src/
│   │   │   ├── client.ts        # HTTP client setup
│   │   │   ├── types.ts         # Generated types
│   │   │   └── index.ts
│   │   ├── scripts/
│   │   │   └── generate.sh      # OpenAPI codegen script
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── auth/                    # Auth utilities
│   │   ├── src/
│   │   │   ├── hooks/
│   │   │   │   ├── useAuth.ts
│   │   │   │   └── useSession.ts
│   │   │   ├── context/
│   │   │   │   └── AuthProvider.tsx
│   │   │   ├── storage.ts       # Token storage (web/RN)
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── ui/                      # Shared UI components
│   │   ├── src/
│   │   │   ├── components/
│   │   │   │   ├── Button.tsx   # RN + Web compatible
│   │   │   │   ├── Input.tsx
│   │   │   │   └── Card.tsx
│   │   │   ├── icons/           # Unified icon set
│   │   │   ├── tokens/          # Design tokens
│   │   │   │   ├── colors.ts
│   │   │   │   ├── spacing.ts
│   │   │   │   └── typography.ts
│   │   │   └── index.ts
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   ├── charts/                  # Shared chart components
│   │   ├── src/
│   │   │   ├── EquityCurve.tsx  # Recharts (web) + RN compatible
│   │   │   ├── TradeLog.tsx
│   │   │   └── PerformanceMetrics.tsx
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   └── config/                  # Shared configuration
│       ├── eslint-config/       # ESLint presets
│       ├── tsconfig/            # TypeScript configs
│       └── tailwind-config/     # Tailwind presets
│
├── infrastructure/              # Infrastructure as Code
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   ├── docker-compose.dev.yml
│   │   └── docker-compose.prod.yml
│   │
│   ├── kubernetes/             # K8s manifests
│   │   ├── base/
│   │   ├── overlays/
│   │   │   ├── dev/
│   │   │   ├── staging/
│   │   │   └── production/
│   │   └── kustomization.yaml
│   │
│   ├── terraform/              # Cloud infrastructure
│   │   ├── modules/
│   │   └── environments/
│   │
│   └── helm/                   # Helm charts
│       └── stock-platform/
│
├── docs/                       # Documentation
│   ├── architecture/
│   ├── api/
│   ├── development/
│   └── deployment/
│
├── scripts/                    # Development scripts
│   ├── dev/
│   │   ├── setup.sh
│   │   ├── codegen.sh          # OpenAPI → TypeScript
│   │   └── db-seed.sh
│   └── deployment/
│
├── tests/                      # E2E and integration tests
│   ├── integration/
│   ├── e2e/
│   │   ├── playwright/         # Playwright E2E
│   │   └── maestro/            # Mobile E2E
│   └── load/
│
├── .editorconfig
├── .gitignore
├── pnpm-workspace.yaml         # PNPM workspace config
├── package.json                # Root package.json
├── turbo.json                  # Turborepo config
├── Makefile
├── README.md
├── LICENSE
└── CHANGELOG.md

# Optional: Rust Calculation Service (Phase 11-12)
# Only add this when performance optimization is needed
#
# calculation-rust/              # Rust calculation service (optional)
# ├── src/
# │   ├── indicators/             # Technical indicators (Polars)
# │   │   ├── moving_average.rs
# │   │   ├── rsi.rs
# │   │   ├── macd.rs
# │   │   └── mod.rs
# │   ├── risk/                   # Risk analytics
# │   │   ├── var.rs
# │   │   ├── sharpe.rs
# │   │   └── mod.rs
# │   ├── grpc/                   # gRPC server (Tonic)
# │   │   └── server.rs
# │   └── main.rs
# ├── proto/                      # Protocol Buffers
# │   └── calculation.proto
# ├── Cargo.toml
# ├── Dockerfile
# └── README.md
```

---

## Service Breakdown

### 1. Trading API Service

**Responsibilities:**
- Stock data ingestion (AKShare)
- Real-time market data
- Stock filtering and screening
- Portfolio management
- Collection management

**Tech Stack:**
- Python 3.13 + FastAPI
- SQLAlchemy 2.0 + PostgreSQL
- QuestDB for time-series
- Redis caching
- Celery for async tasks

**API Endpoints:**
```
GET  /api/v1/stocks
GET  /api/v1/stocks/{code}
GET  /api/v1/markets
POST /api/v1/filters
GET  /api/v1/portfolios
GET  /api/v1/collections
```

---

### 2. Backtest API Service

**Responsibilities:**
- Strategy management (YAML DSL)
- Backtest execution
- Performance analytics
- Real-time progress via WebSocket

**Tech Stack:**
- Python 3.13 + FastAPI
- PostgreSQL + QuestDB
- Redis + WebSocket
- gRPC client to Calculation Service

**API Endpoints:**
```
POST   /api/v1/strategies
POST   /api/v1/backtest/run
GET    /api/v1/backtest/runs/{id}
WS     /ws/v1/backtest/{run_id}
```

---

### 3. Auth API Service

**Responsibilities:**
- User authentication (JWT, OAuth2)
- Session management
- Role-based access control

**Tech Stack:**
- Python 3.13 + FastAPI
- PostgreSQL + Redis
- python-jose (JWT)
- passlib (password hashing)

**API Endpoints:**
```
POST   /api/v1/auth/register
POST   /api/v1/auth/login
POST   /api/v1/auth/refresh
GET    /api/v1/auth/me
POST   /api/v1/auth/oauth/{provider}
```

---

### 4. Notification API Service

**Responsibilities:**
- Email (SendGrid)
- SMS (Twilio)
- Push notifications (FCM)

**Tech Stack:**
- Python 3.13 + FastAPI
- PostgreSQL + Redis + Celery
- SendGrid, Twilio, FCM SDKs

**API Endpoints:**
```
POST   /api/v1/notifications/email
POST   /api/v1/notifications/sms
POST   /api/v1/notifications/push
GET    /api/v1/notifications/preferences
```

---

### 5. Calculation API Service (Python)

**Python-based calculation service using pandas/numpy/scipy.**

**Responsibilities:**
- Technical indicators (MA, RSI, MACD, Bollinger Bands, Stochastic, etc.)
- Risk analytics (VaR, Sharpe, Sortino, Calmar, Monte Carlo)
- Portfolio optimization (mean-variance, efficient frontier)
- Bulk data processing with pandas vectorization

**Tech Stack:**
- Python 3.13 + FastAPI
- pandas (DataFrames)
- numpy (numerical computing)
- scipy (statistical functions)
- ta-lib Python bindings (optional)
- Redis caching for computed results

**API Endpoints:**
```
POST   /api/v1/indicators/ma
POST   /api/v1/indicators/rsi
POST   /api/v1/indicators/macd
POST   /api/v1/indicators/batch

POST   /api/v1/risk/var
POST   /api/v1/risk/sharpe
POST   /api/v1/risk/monte-carlo

POST   /api/v1/optimization/mean-variance
POST   /api/v1/optimization/efficient-frontier
```

**Why Python First:**
- ✅ Leverage existing calculation code from current stock-picker
- ✅ Faster time to market (no rewrite needed)
- ✅ pandas/numpy are "fast enough" for most use cases
- ✅ Can optimize to Rust later if bottlenecks identified

---

## Migration Strategy

### Phase 1: Monorepo Setup (Week 1-2)

**Tasks:**
1. Create monorepo structure
2. Configure PNPM workspace + Turborepo
3. Set up ESLint, Prettier, TypeScript configs
4. Create GitHub Actions CI/CD
5. Create Makefile

**Deliverables:**
- ✅ Monorepo skeleton
- ✅ CI/CD pipelines
- ✅ Developer guide

---

### Phase 2: Extract Backend Services (Week 3-4)

**Tasks:**
1. Split current stock-picker into Trading API + Backtest API
2. Create shared Python utilities
3. Generate OpenAPI specs
4. Write tests

**Deliverables:**
- ✅ Trading API service
- ✅ Backtest API service
- ✅ OpenAPI specs

---

### Phase 3: Create Auth & Notification Services (Week 5)

**Tasks:**
1. Build Auth API (JWT, OAuth2, RBAC)
2. Build Notification API (email, SMS, push)
3. Integrate with existing services

**Deliverables:**
- ✅ Auth API service
- ✅ Notification API service

---

### Phase 4: Build Calculation API (Week 6)

**Tasks:**
1. Create `services/calculation-api/` structure
2. Extract existing calculation logic from backtest engine
3. Implement indicator endpoints (MA, RSI, MACD, etc.)
4. Implement risk analytics endpoints (VaR, Sharpe, etc.)
5. Add Redis caching for computed results
6. Write tests and benchmarks
7. Create Dockerfile

**Deliverables:**
- ✅ Calculation API service (Python + pandas/numpy)
- ✅ REST API endpoints
- ✅ Performance benchmarks (baseline for future Rust comparison)

---

### Phase 5: Migrate to QuestDB (Week 7)

**Tasks:**
1. Set up QuestDB
2. Design time-series schema
3. Migrate stock daily prices
4. Update APIs to query QuestDB

**Deliverables:**
- ✅ QuestDB setup
- ✅ Migrated time-series data

---

### Phase 6: Build Shared TypeScript Packages (Week 8)

**Tasks:**
1. Create OpenAPI-generated API client
2. Build auth utilities (@repo/auth)
3. Create universal UI components (@repo/ui)
4. Build chart components (@repo/charts)

**Deliverables:**
- ✅ @repo/api (type-safe clients)
- ✅ @repo/auth
- ✅ @repo/ui
- ✅ @repo/charts

---

### Phase 7: Build Web Application (Week 9)

**Tasks:**
1. Initialize Next.js 14 project
2. Implement dashboard pages
3. Integrate backend APIs
4. Write E2E tests (Playwright)

**Deliverables:**
- ✅ Next.js web app
- ✅ Full backend integration

---

### Phase 8: Build Mobile Application (Week 10)

**Tasks:**
1. Initialize Expo project
2. Implement screens (reuse shared packages)
3. Add push notifications
4. Write E2E tests (Maestro)

**Deliverables:**
- ✅ React Native mobile app
- ✅ iOS + Android builds

---

### Phase 9: Build Desktop Application (Optional)

**Tasks:**
1. Set up Electron
2. Reuse web app via React Native Web
3. Add desktop features (tray, notifications)

**Deliverables:**
- ✅ Electron desktop app

---

### Phase 10: Infrastructure & Deployment (Week 11-12)

**Tasks:**
1. Create Kubernetes manifests
2. Set up Helm charts
3. Configure Terraform
4. Deploy to staging + production
5. Set up monitoring (Prometheus + Grafana)

**Deliverables:**
- ✅ K8s cluster
- ✅ Production deployment

---

## Development Workflow

### Makefile Commands

```makefile
install:    ## Install all dependencies
dev:        ## Start all services
test:       ## Run all tests
lint:       ## Run linters
codegen:    ## Generate TypeScript from OpenAPI
build:      ## Build all services
deploy-dev: ## Deploy to dev environment
clean:      ## Clean build artifacts
```

---

## Type Safety & Code Generation

### OpenAPI → TypeScript Codegen

**Codegen Script (`scripts/dev/codegen.sh`):**

```bash
#!/bin/bash
set -e

# Fetch OpenAPI specs
curl http://localhost:8001/openapi.json -o /tmp/trading-api.json
curl http://localhost:8002/openapi.json -o /tmp/backtest-api.json

# Generate TypeScript types
npx openapi-typescript /tmp/trading-api.json -o packages/api/src/generated/trading.ts
npx openapi-typescript /tmp/backtest-api.json -o packages/api/src/generated/backtest.ts

echo "✅ Types generated!"
```

**Type-Safe Client:**

```typescript
import createClient from 'openapi-fetch'
import type { paths as TradingPaths } from './generated/trading'

export const tradingApi = createClient<TradingPaths>({
  baseUrl: 'http://localhost:8001',
})

// Usage (fully typed!)
const { data } = await tradingApi.GET('/api/v1/stocks/{code}', {
  params: { path: { code: '600000' } },
})
```

---

## Timeline & Milestones

### Core Platform (8-9 Weeks)

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| 1. Monorepo Setup | Week 1-2 | Structure + CI/CD |
| 2. Backend Services | Week 3-4 | Trading + Backtest APIs |
| 3. Auth + Notifications | Week 5 | Auth + Notification APIs |
| 4. Calculation API | Week 6 | Python calculation service |
| 5. QuestDB Migration | Week 7 | Time-series database |
| 6. Shared Packages | Week 8 | TypeScript packages |
| 7. Web Application | Week 9 | Next.js app |
| 8. Mobile Application | Week 10 | React Native app (optional) |
| 9. Infrastructure | Week 11-12 | K8s deployment |

**Major Milestones:**
- **M1 (Week 2)**: Monorepo operational
- **M2 (Week 4)**: All backend APIs running
- **M3 (Week 6)**: Calculation API integrated (Python)
- **M4 (Week 8)**: Type-safe API clients ready
- **M5 (Week 9)**: Web app deployed
- **M6 (Week 12)**: Production ready with Python stack

---

### Optional: Performance Optimization Phase (2-4 Weeks)

**When to do this:** After production launch, when profiling identifies bottlenecks

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| 11. Rust Calculation Service | Week 1-2 | Rust service with gRPC |
| 12. Gradual Migration | Week 3-4 | Replace hot paths incrementally |

**Rust Migration Approach:**
1. Profile Python calculation API in production
2. Identify bottlenecks (e.g., Monte Carlo with 100k simulations)
3. Implement hot paths in Rust (use Polars for DataFrames)
4. Deploy Rust service alongside Python
5. Gradually route traffic to Rust for specific endpoints
6. Keep Python as fallback

**Expected Performance Gains:**
- 10-100x faster for CPU-intensive calculations
- Lower memory usage with Rust
- Better concurrency with async Rust (Tokio)

---

## Technology Stack Summary

| Layer | Technology | Justification |
|-------|-----------|---------------|
| **Backend APIs** | Python 3.13 + FastAPI | Consistency, OpenAPI codegen, team expertise |
| **Calculations (Phase 1)** | Python + pandas/numpy | Faster to market, leverage existing code |
| **Calculations (Phase 2)** | Rust + Polars (optional) | 10-100x speedup when needed |
| **Relational DB** | PostgreSQL 16 | Battle-tested, ACID compliance |
| **Time-Series DB** | QuestDB | 10x faster ingestion, built for finance |
| **Cache** | Redis 7 | Industry standard, fast |
| **Web Frontend** | Next.js 14 + React | SSR/SSG, SEO, production-ready |
| **Mobile** | React Native + Expo | Code reuse, OTA updates |
| **Desktop** | Electron + RN Web | Reuse web app, minimal effort |
| **Type Safety** | OpenAPI → TS codegen | Automatic sync, zero manual typing |
| **Monorepo** | PNPM + Turborepo | Fast builds, efficient workspace |

---

## Next Steps

1. **Review and approve** this plan
2. **Assign team members** to phases
3. **Begin Phase 1**: Monorepo setup

**Questions:**
- Open GitHub issue
- Contact: monkeyboiii

---

**Version:** 2.0.0 (Pragmatic Edition)
**Updated:** 2025-11-18
**Authors:** Claude + monkeyboiii
**License:** MIT
