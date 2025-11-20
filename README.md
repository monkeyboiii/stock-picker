# Stock Analysis Platform

**Modern stock analysis and backtesting platform for Chinese markets**

[![CI Frontend](https://github.com/monkeyboiii/stock-picker/workflows/Frontend%20CI/badge.svg)](https://github.com/monkeyboiii/stock-picker/actions)
[![CI Backend](https://github.com/monkeyboiii/stock-picker/workflows/Backend%20CI/badge.svg)](https://github.com/monkeyboiii/stock-picker/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🚀 Project Status

**Current Phase**: Phase 1 - Monorepo Setup ✅

This project is being migrated to a modern monorepo architecture. See the [Migration Plan](docs/MONOREPO_MIGRATION_PLAN.md) for details.

---

## 📋 Overview

A comprehensive platform for stock analysis and backtesting, featuring:

- **Real-time market data** from Chinese stock exchanges (Shanghai, Shenzhen, Beijing, Hong Kong)
- **Technical indicators** (MA, RSI, MACD, Bollinger Bands, etc.)
- **Stock filtering** based on multiple criteria
- **Backtesting engine** with strategy DSL
- **Risk analytics** (VaR, Sharpe ratio, Monte Carlo simulations)
- **Web & mobile apps** for visualization and management

---

## 🏗️ Architecture

### Current Status (Monorepo Phase 1 Complete)

The monorepo structure is now set up with:
- ✅ PNPM workspace configuration
- ✅ Turborepo for build orchestration
- ✅ Shared TypeScript configs
- ✅ ESLint & Prettier configs
- ✅ Makefile with common commands
- ✅ GitHub Actions CI/CD workflows
- ✅ Developer setup guide

### Planned Services (Phases 2-10)

```
┌─────────────────────────────────────────────┐
│  Frontend                                   │
│  • Web (Next.js)                           │
│  • Mobile (React Native)                   │
│  • Desktop (Electron)                      │
└─────────────────────────────────────────────┘
                  │
┌─────────────────┼─────────────────────────┐
│                 │                         │
│  Trading API  Backtest API   Auth API    │
│  (FastAPI)    (FastAPI)      (FastAPI)   │
│                 │                         │
│        Calculation API (Python/Rust)     │
└─────────────────────────────────────────────┘
                  │
┌─────────────────┼─────────────────────────┐
│  PostgreSQL   QuestDB        Redis        │
└─────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend APIs** | Python 3.13 + FastAPI |
| **Calculations** | Python (pandas/numpy) → Rust (Polars) |
| **Databases** | PostgreSQL 16, QuestDB, Redis 7 |
| **Web Frontend** | Next.js 14 + React 18 + TypeScript |
| **Mobile** | React Native + Expo |
| **Monorepo** | PNPM + Turborepo |
| **DevOps** | Docker, Kubernetes, GitHub Actions |

---

## 🚀 Quick Start

### Prerequisites

- Node.js 20+ and pnpm 8+
- Python 3.13+ and uv
- Docker and Docker Compose
- PostgreSQL 16+

### Installation

```bash
# Clone repository
git clone https://github.com/monkeyboiii/stock-picker.git
cd stock-picker

# Install all dependencies
make install

# Set up environment
cp example.env .env
# Edit .env with your configuration
```

### Development

```bash
# Start all services (Docker Compose)
make dev

# Or start specific services
make dev-backend    # Databases + backend APIs
make dev-frontend   # Web and mobile apps

# Run tests
make test

# Lint and format
make lint
make format
```

### Current Stock Picker (Legacy CLI)

The original stock-picker CLI is still available:

```bash
# Install Python dependencies
cd app && uv sync

# Initialize database
uv run stock-picker init -r -lll

# Run daily stock picking
uv run stock-picker run

# For detailed usage, see CLAUDE.md
```

---

## 📚 Documentation

- **[Migration Plan](docs/MONOREPO_MIGRATION_PLAN.md)** - Comprehensive monorepo migration guide
- **[Developer Guide](DEVELOPER_GUIDE.md)** - Setup and development workflow
- **[CLAUDE.md](CLAUDE.md)** - AI assistant guide (current stock-picker)
- **[API Documentation](docs/api/)** - REST API specs (coming in Phase 2)
- **[Architecture](docs/architecture/)** - System design and ADRs

---

## 📂 Repository Structure

```
stock-analysis-platform/
├── services/        # Python backend services (FastAPI)
│   ├── trading-api/        # Phase 2: Stock data & filtering
│   ├── backtest-api/       # Phase 2: Backtesting engine
│   ├── calculation-api/    # Phase 4: Indicators & risk
│   ├── auth-api/          # Phase 3: Authentication
│   └── notification-api/  # Phase 3: Notifications
├── apps/           # Frontend applications
│   ├── web/               # Phase 7: Next.js web app
│   ├── mobile/            # Phase 8: React Native mobile
│   └── desktop/           # Optional: Electron desktop
├── packages/       # Shared TypeScript packages
│   ├── api/               # Phase 6: Type-safe API clients
│   ├── auth/              # Phase 6: Auth utilities
│   ├── ui/                # Phase 6: UI components
│   └── charts/            # Phase 6: Chart components
├── infrastructure/ # Docker, K8s, Terraform
├── app/           # Legacy stock-picker code (will be migrated)
└── docs/          # Documentation
```

---

## 🎯 Roadmap

### Phase 1: Monorepo Setup ✅ (Week 1-2)
- [x] PNPM workspace + Turborepo
- [x] TypeScript configs
- [x] ESLint & Prettier
- [x] CI/CD workflows
- [x] Developer guide

### Phase 2: Backend Services (Week 3-4)
- [ ] Extract Trading API
- [ ] Extract Backtest API
- [ ] OpenAPI specs

### Phase 3: Auth & Notifications (Week 5)
- [ ] Auth API (JWT, OAuth2)
- [ ] Notification API

### Phase 4: Calculation API (Week 6)
- [ ] Python calculation service
- [ ] REST API endpoints

### Phase 5-10: See [Migration Plan](docs/MONOREPO_MIGRATION_PLAN.md)

---

## 🧪 Testing

```bash
# Run all tests
make test

# Python tests
make test-python

# Frontend tests
make test-frontend

# E2E tests
make test-e2e

# With coverage
pnpm turbo test -- --coverage
```

---

## 🤝 Contributing

1. Read the [Developer Guide](DEVELOPER_GUIDE.md)
2. Check [open issues](https://github.com/monkeyboiii/stock-picker/issues)
3. Create a feature branch (`feature/my-feature`)
4. Make your changes with tests
5. Run `make lint` and `make test`
6. Submit a pull request

---

## 📄 License

[MIT License](LICENSE)

---

## 🙏 Acknowledgments

- **AKShare** - Chinese stock market data
- **FastAPI** - Modern Python web framework
- **Next.js** - React framework
- **Turborepo** - High-performance build system

---

## 📞 Support

- **Issues**: https://github.com/monkeyboiii/stock-picker/issues
- **Discussions**: https://github.com/monkeyboiii/stock-picker/discussions
- **Documentation**: See `docs/` directory

---

**Built with ❤️ for stock traders and developers**
