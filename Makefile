.PHONY: help install dev build test lint format clean

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Stock Analysis Platform - Monorepo Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Usage:$(NC) make [target]"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ============================================================================
# Installation & Setup
# ============================================================================

install: ## Install all dependencies (Python + Node.js)
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@echo "$(YELLOW)Installing Node.js dependencies (pnpm)...$(NC)"
	pnpm install
	@echo "$(YELLOW)Installing Python dependencies (uv)...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv sync; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv sync; fi
	@if [ -d "services/calculation-api" ]; then cd services/calculation-api && uv sync; fi
	@if [ -d "services/auth-api" ]; then cd services/auth-api && uv sync; fi
	@if [ -d "services/notification-api" ]; then cd services/notification-api && uv sync; fi
	@echo "$(GREEN)✓ All dependencies installed$(NC)"

setup: install ## Run initial setup (install + database init)
	@echo "$(BLUE)Running initial setup...$(NC)"
	@echo "$(YELLOW)Creating .env file from example...$(NC)"
	@if [ ! -f .env ]; then cp example.env .env; fi
	@echo "$(GREEN)✓ Setup complete! Edit .env with your configuration$(NC)"

# ============================================================================
# Development
# ============================================================================

dev: ## Start all services in development mode
	@echo "$(BLUE)Starting all services...$(NC)"
	docker-compose -f infrastructure/docker/docker-compose.dev.yml up

dev-backend: ## Start backend services only (databases + APIs)
	@echo "$(BLUE)Starting backend services...$(NC)"
	docker-compose -f infrastructure/docker/docker-compose.dev.yml up postgres questdb redis rabbitmq

dev-frontend: ## Start frontend applications (Next.js, React Native)
	@echo "$(BLUE)Starting frontend services...$(NC)"
	pnpm turbo dev --filter=web --filter=mobile

# ============================================================================
# Build & Test
# ============================================================================

build: ## Build all services and applications
	@echo "$(BLUE)Building all services...$(NC)"
	@echo "$(YELLOW)Building frontend applications...$(NC)"
	pnpm turbo build
	@echo "$(YELLOW)Building Docker images...$(NC)"
	docker-compose -f infrastructure/docker/docker-compose.yml build
	@echo "$(GREEN)✓ Build complete$(NC)"

test: ## Run all tests (Python + TypeScript)
	@echo "$(BLUE)Running all tests...$(NC)"
	@echo "$(YELLOW)Running Python tests...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv run pytest; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv run pytest; fi
	@echo "$(YELLOW)Running TypeScript tests...$(NC)"
	pnpm turbo test
	@echo "$(GREEN)✓ All tests passed$(NC)"

test-python: ## Run Python tests only
	@echo "$(BLUE)Running Python tests...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv run pytest -v; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv run pytest -v; fi

test-frontend: ## Run frontend tests only
	@echo "$(BLUE)Running frontend tests...$(NC)"
	pnpm turbo test

test-e2e: ## Run end-to-end tests
	@echo "$(BLUE)Running E2E tests...$(NC)"
	pnpm turbo test:e2e

# ============================================================================
# Code Quality
# ============================================================================

lint: ## Run linters (Python + TypeScript)
	@echo "$(BLUE)Running linters...$(NC)"
	@echo "$(YELLOW)Linting Python code...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv run ruff check .; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv run ruff check .; fi
	@echo "$(YELLOW)Linting TypeScript code...$(NC)"
	pnpm turbo lint

format: ## Format all code (Python + TypeScript)
	@echo "$(BLUE)Formatting code...$(NC)"
	@echo "$(YELLOW)Formatting Python code...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv run ruff check . --fix; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv run ruff check . --fix; fi
	@echo "$(YELLOW)Formatting TypeScript code...$(NC)"
	pnpm prettier --write "**/*.{ts,tsx,js,jsx,json,md}"
	@echo "$(GREEN)✓ Code formatted$(NC)"

typecheck: ## Run TypeScript type checking
	@echo "$(BLUE)Running type checks...$(NC)"
	pnpm turbo typecheck

# ============================================================================
# Code Generation
# ============================================================================

codegen: ## Generate TypeScript types from OpenAPI specs
	@echo "$(BLUE)Generating TypeScript types from OpenAPI...$(NC)"
	@if [ -f "scripts/dev/codegen.sh" ]; then \
		./scripts/dev/codegen.sh; \
	else \
		echo "$(YELLOW)Codegen script not yet created$(NC)"; \
	fi

# ============================================================================
# Database
# ============================================================================

db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(NC)"
	@if [ -d "services/trading-api" ]; then cd services/trading-api && uv run alembic upgrade head; fi
	@if [ -d "services/backtest-api" ]; then cd services/backtest-api && uv run alembic upgrade head; fi

db-reset: ## Reset databases (WARNING: destructive)
	@echo "$(YELLOW)WARNING: This will delete all data!$(NC)"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "$(BLUE)Resetting databases...$(NC)"
	docker-compose -f infrastructure/docker/docker-compose.dev.yml down -v postgres questdb
	docker-compose -f infrastructure/docker/docker-compose.dev.yml up -d postgres questdb
	@sleep 5
	$(MAKE) db-migrate

db-seed: ## Seed databases with test data
	@echo "$(BLUE)Seeding databases...$(NC)"
	@if [ -f "scripts/dev/db-seed.sh" ]; then \
		./scripts/dev/db-seed.sh; \
	else \
		echo "$(YELLOW)Seed script not yet created$(NC)"; \
	fi

# ============================================================================
# Logs & Debugging
# ============================================================================

logs: ## Show logs from all services
	docker-compose -f infrastructure/docker/docker-compose.dev.yml logs -f

logs-backend: ## Show backend service logs only
	docker-compose -f infrastructure/docker/docker-compose.dev.yml logs -f postgres questdb redis trading-api backtest-api

shell-trading: ## Open shell in trading-api container
	docker-compose -f infrastructure/docker/docker-compose.dev.yml exec trading-api /bin/bash

shell-backtest: ## Open shell in backtest-api container
	docker-compose -f infrastructure/docker/docker-compose.dev.yml exec backtest-api /bin/bash

# ============================================================================
# Deployment
# ============================================================================

deploy-dev: ## Deploy to development environment
	@echo "$(BLUE)Deploying to development...$(NC)"
	@if [ -f "scripts/deployment/deploy-dev.sh" ]; then \
		./scripts/deployment/deploy-dev.sh; \
	else \
		echo "$(YELLOW)Deploy script not yet created$(NC)"; \
	fi

deploy-staging: ## Deploy to staging environment
	@echo "$(BLUE)Deploying to staging...$(NC)"
	@if [ -f "scripts/deployment/deploy-staging.sh" ]; then \
		./scripts/deployment/deploy-staging.sh; \
	else \
		echo "$(YELLOW)Deploy script not yet created$(NC)"; \
	fi

deploy-prod: ## Deploy to production environment
	@echo "$(BLUE)Deploying to production...$(NC)"
	@read -p "Are you sure you want to deploy to PRODUCTION? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@if [ -f "scripts/deployment/deploy-prod.sh" ]; then \
		./scripts/deployment/deploy-prod.sh; \
	else \
		echo "$(YELLOW)Deploy script not yet created$(NC)"; \
	fi

# ============================================================================
# Cleanup
# ============================================================================

clean: ## Clean build artifacts and dependencies
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@echo "$(YELLOW)Cleaning Python artifacts...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(YELLOW)Cleaning Node.js artifacts...$(NC)"
	find . -type d -name "node_modules" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".next" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".turbo" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

clean-docker: ## Clean Docker containers and volumes
	@echo "$(BLUE)Cleaning Docker resources...$(NC)"
	docker-compose -f infrastructure/docker/docker-compose.dev.yml down -v
	@echo "$(GREEN)✓ Docker cleanup complete$(NC)"
