# Infrastructure

Infrastructure as Code (IaC) for the stock analysis platform.

## Directory Structure

### `docker/`
Docker Compose configurations for local development and production.

**Files:**
- `docker-compose.dev.yml` - Local development (all services)
- `docker-compose.yml` - Production configuration
- `docker-compose.prod.yml` - Production with monitoring

**Usage:**
```bash
# Start all services for local development
docker-compose -f docker-compose.dev.yml up

# Start specific services
docker-compose -f docker-compose.dev.yml up postgres questdb redis

# Production deployment
docker-compose -f docker-compose.prod.yml up -d
```

### `kubernetes/`
Kubernetes manifests for production deployment.

**Structure:**
- `base/` - Base Kubernetes resources
- `overlays/dev/` - Development environment
- `overlays/staging/` - Staging environment
- `overlays/production/` - Production environment

**Usage:**
```bash
# Apply development configuration
kubectl apply -k overlays/dev

# Apply production configuration
kubectl apply -k overlays/production
```

### `terraform/`
Terraform configurations for cloud infrastructure provisioning.

**Structure:**
- `modules/` - Reusable Terraform modules
- `environments/dev/` - Dev environment
- `environments/staging/` - Staging environment
- `environments/production/` - Production environment

**Usage:**
```bash
cd environments/production
terraform init
terraform plan
terraform apply
```

### `helm/`
Helm charts for Kubernetes deployment.

**Charts:**
- `stock-platform/` - Main platform chart (umbrella)

**Usage:**
```bash
# Install chart in dev environment
helm install stock-platform helm/stock-platform -f helm/values-dev.yaml

# Upgrade existing deployment
helm upgrade stock-platform helm/stock-platform -f helm/values-prod.yaml
```

## Deployment Environments

| Environment | Purpose | Infrastructure |
|-------------|---------|----------------|
| **Development** | Local development | Docker Compose |
| **Staging** | Pre-production testing | Kubernetes (GKE/EKS) |
| **Production** | Live environment | Kubernetes (GKE/EKS) |

## Required Resources

**Development:**
- Docker Desktop
- 8GB RAM minimum, 16GB recommended
- 20GB disk space

**Production (Kubernetes):**
- 3+ nodes (t3.medium or equivalent)
- PostgreSQL (managed service recommended)
- QuestDB (self-hosted or managed)
- Redis (managed service recommended)
