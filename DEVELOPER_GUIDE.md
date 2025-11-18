# Developer Setup Guide

Welcome to the Stock Analysis Platform monorepo! This guide will help you get started with development.

## Prerequisites

### Required Software

1. **Node.js** 20+ and **pnpm** 8+
   ```bash
   # Install Node.js from https://nodejs.org
   # Install pnpm
   npm install -g pnpm@8
   ```

2. **Python** 3.13+ and **uv**
   ```bash
   # Install uv (Python package manager)
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. **Docker** and **Docker Compose**
   ```bash
   # Install from https://docs.docker.com/get-docker/
   ```

4. **PostgreSQL** 16+ (optional - can use Docker)

5. **Make** (usually pre-installed on Unix systems)

### Optional Tools

- **Visual Studio Code** with recommended extensions (see `.vscode/extensions.json`)
- **Git** 2.40+

---

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/monkeyboiii/stock-picker.git
cd stock-picker
```

### 2. Install Dependencies

```bash
# Install all dependencies (Node.js + Python)
make install

# Or manually:
pnpm install  # Frontend dependencies
cd services/trading-api && uv sync  # Backend dependencies
```

### 3. Configure Environment

```bash
# Copy environment template
cp example.env .env

# Edit .env with your configuration
nano .env
```

### 4. Start Development Environment

```bash
# Start all services (Docker Compose)
make dev

# Or start specific services:
make dev-backend    # Databases + backend APIs only
make dev-frontend   # Frontend apps only
```

### 5. Access Services

- **Web App**: http://localhost:3000 (when created in Phase 7)
- **Trading API**: http://localhost:8001
- **Backtest API**: http://localhost:8002
- **Auth API**: http://localhost:8003
- **PostgreSQL**: localhost:5432
- **QuestDB Console**: http://localhost:9000
- **Redis**: localhost:6379

---

## Monorepo Structure

```
stock-analysis-platform/
├── services/        # Python backend services (FastAPI)
├── apps/           # Frontend applications (Next.js, React Native)
├── packages/       # Shared TypeScript packages
├── infrastructure/ # Docker, K8s, Terraform
├── scripts/        # Development and deployment scripts
├── tests/          # Integration and E2E tests
└── docs/           # Documentation
```

---

## Development Workflow

### Backend Development (Python)

```bash
# Navigate to a service
cd services/trading-api

# Install dependencies
uv sync

# Run service locally
uv run uvicorn app.main:app --reload --port 8001

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Format code
uv run ruff check . --fix
```

### Frontend Development (TypeScript)

```bash
# Navigate to an app
cd apps/web

# Install dependencies (done at root)
pnpm install

# Start dev server
pnpm dev

# Run tests
pnpm test

# Lint
pnpm lint

# Build
pnpm build
```

### Using Turborepo (from root)

```bash
# Run dev for all apps
pnpm turbo dev

# Build all apps
pnpm turbo build

# Test all apps
pnpm turbo test

# Lint all apps
pnpm turbo lint
```

---

## Common Tasks

### Generate TypeScript Types from OpenAPI

```bash
# Make sure backend services are running
make codegen

# Or manually:
./scripts/dev/codegen.sh
```

### Database Operations

```bash
# Run migrations
make db-migrate

# Reset database (WARNING: deletes all data)
make db-reset

# Seed test data
make db-seed
```

### Run Tests

```bash
# All tests
make test

# Python tests only
make test-python

# Frontend tests only
make test-frontend

# E2E tests
make test-e2e
```

### Code Formatting

```bash
# Format all code
make format

# Or separately:
make format-python   # Python code
make format-ts       # TypeScript code
```

### View Logs

```bash
# All services
make logs

# Backend only
make logs-backend

# Specific service
docker-compose -f infrastructure/docker/docker-compose.dev.yml logs -f trading-api
```

---

## Project Conventions

### Python Code Style

- **Formatter**: Ruff (minimal, non-invasive)
- **Linter**: Ruff + MyPy
- **Type hints**: Use throughout
- **Imports**: Absolute imports from `app.`
- **Testing**: pytest with fixtures

### TypeScript Code Style

- **Formatter**: Prettier
- **Linter**: ESLint
- **Type safety**: Strict TypeScript
- **Imports**: ESM imports
- **Testing**: Jest or Vitest

### Git Workflow

1. **Branch naming**: `feature/description` or `fix/description`
2. **Commit messages**: Imperative mood ("Add feature" not "Added feature")
3. **Pull requests**: Required for main branch
4. **CI checks**: All tests must pass

---

## Troubleshooting

### Port Already in Use

```bash
# Find process using port 8001
lsof -i :8001

# Kill process
kill -9 <PID>
```

### Database Connection Errors

```bash
# Check if PostgreSQL is running
docker-compose -f infrastructure/docker/docker-compose.dev.yml ps

# Restart database
docker-compose -f infrastructure/docker/docker-compose.dev.yml restart postgres
```

### Node Modules Issues

```bash
# Clean and reinstall
rm -rf node_modules pnpm-lock.yaml
pnpm install
```

### Python Virtual Environment Issues

```bash
# Remove and recreate
rm -rf .venv
uv sync
```

---

## IDE Setup

### Visual Studio Code

Recommended extensions (install automatically with workspace):
- **Python**: ms-python.python
- **Pylance**: ms-python.vscode-pylance
- **ESLint**: dbaeumer.vscode-eslint
- **Prettier**: esbenp.prettier-vscode
- **Docker**: ms-azuretools.vscode-docker

Settings (`.vscode/settings.json`):
```json
{
  "editor.formatOnSave": true,
  "editor.codeActionsOnSave": {
    "source.fixAll.eslint": true
  },
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true
  },
  "[typescript]": {
    "editor.defaultFormatter": "esbenp.prettier-vscode"
  }
}
```

---

## Useful Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make install` | Install all dependencies |
| `make dev` | Start all services |
| `make build` | Build all services |
| `make test` | Run all tests |
| `make lint` | Run all linters |
| `make format` | Format all code |
| `make clean` | Clean build artifacts |

---

## Getting Help

- **Documentation**: See `docs/` directory
- **Issues**: https://github.com/monkeyboiii/stock-picker/issues
- **Migration Plan**: See `docs/MONOREPO_MIGRATION_PLAN.md`

---

## Next Steps

1. Read the [Migration Plan](docs/MONOREPO_MIGRATION_PLAN.md)
2. Check the [Architecture Documentation](docs/architecture/)
3. Review the [API Documentation](docs/api/)
4. Start contributing!

---

**Happy coding! 🚀**
