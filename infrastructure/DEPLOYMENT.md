# Stock Picker - Deployment Guide

Comprehensive deployment guide for the Stock Picker platform.

## Quick Start

```bash
# 1. Install dependencies
make install

# 2. Configure environment
cp infrastructure/docker/.env.prod.example infrastructure/docker/.env.prod
# Edit .env.prod with your configuration

# 3. Deploy
make deploy-prod
```

## Deployment Options

### 1. Docker Compose (Recommended for getting started)

**Best for:** Small to medium deployments, single server

```bash
# Build and start all services
docker-compose -f infrastructure/docker/docker-compose.prod.yml build
docker-compose -f infrastructure/docker/docker-compose.prod.yml up -d

# View logs
docker-compose -f infrastructure/docker/docker-compose.prod.yml logs -f

# Stop services
docker-compose -f infrastructure/docker/docker-compose.prod.yml down
```

**Services exposed:**
- Web App: http://localhost:3000
- Trading API: http://localhost:8000/docs
- Backtest API: http://localhost:8001/docs
- Auth API: http://localhost:8003/docs
- Notification API: http://localhost:8004/docs
- Calculation API: http://localhost:8005/docs

### 2. Kubernetes (Recommended for production)

**Best for:** Large scale, high availability

```bash
# Create namespace
kubectl apply -f infrastructure/kubernetes/base/namespace.yaml

# Deploy database
kubectl apply -f infrastructure/kubernetes/base/postgres.yaml

# Deploy APIs
kubectl apply -f infrastructure/kubernetes/base/trading-api.yaml
# ... apply other services

# Check status
kubectl get all -n stock-picker
```

## Environment Configuration

### Required Variables

```env
# Database
POSTGRES_PASSWORD=<strong_password>
POSTGRES_USER=stockpicker
POSTGRES_DB=stock_picker

# JWT
JWT_SECRET=<strong_random_secret>

# API URLs (for web app)
NEXT_PUBLIC_TRADING_API_URL=http://localhost:8000
NEXT_PUBLIC_BACKTEST_API_URL=http://localhost:8001
NEXT_PUBLIC_AUTH_API_URL=http://localhost:8003
NEXT_PUBLIC_NOTIFICATION_API_URL=http://localhost:8004
NEXT_PUBLIC_CALCULATION_API_URL=http://localhost:8005
```

### Generate Secrets

```bash
# PostgreSQL password
openssl rand -base64 32

# JWT secret
openssl rand -base64 64
```

## CI/CD

### GitHub Actions

Workflows are configured in `.github/workflows/`:

1. **ci.yml** - Runs on every push/PR
   - Lints code
   - Type checks
   - Builds packages

2. **deploy.yml** - Manual or automatic deployment
   - Builds Docker images
   - Pushes to registry
   - Deploys to environment

### Manual Deployment

```bash
# Via GitHub Actions UI
Actions → Deploy → Run workflow → Select environment

# Via Make
make deploy-dev    # Development
make deploy-prod   # Production (requires confirmation)
```

## Monitoring

### Health Checks

All APIs expose `/health` endpoints for health monitoring.

### Logs

**Docker:**
```bash
docker-compose logs -f [service]
```

**Kubernetes:**
```bash
kubectl logs -f deployment/[service] -n stock-picker
```

## Scaling

### Docker Compose

Limited scaling support:
```bash
docker-compose up -d --scale trading-api=3
```

### Kubernetes

Full autoscaling support:
```bash
# Manual scaling
kubectl scale deployment trading-api --replicas=5 -n stock-picker

# Auto-scaling
kubectl autoscale deployment trading-api \
  --cpu-percent=70 \
  --min=2 \
  --max=10 \
  -n stock-picker
```

## Backup & Recovery

### Database Backup

```bash
# Docker
docker exec stock-picker-postgres pg_dump -U stockpicker stock_picker > backup.sql

# Kubernetes
kubectl exec -it postgres-0 -n stock-picker -- pg_dump -U stockpicker stock_picker > backup.sql
```

### Database Restore

```bash
# Docker
docker exec -i stock-picker-postgres psql -U stockpicker stock_picker < backup.sql

# Kubernetes
kubectl exec -i postgres-0 -n stock-picker -- psql -U stockpicker stock_picker < backup.sql
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker logs stock-picker-trading-api

# Or in Kubernetes
kubectl logs deployment/trading-api -n stock-picker

# Check environment variables
docker exec stock-picker-trading-api env
```

### Database Connection Issues

```bash
# Test connection
docker exec stock-picker-postgres psql -U stockpicker -d stock_picker -c "SELECT 1;"

# Check if PostgreSQL is running
docker ps | grep postgres
```

### Port Conflicts

```bash
# Check what's using the port
sudo lsof -i :8000

# Kill the process
sudo kill -9 <PID>
```

## Security Checklist

- [ ] Change all default passwords
- [ ] Use strong JWT secrets
- [ ] Enable HTTPS/TLS
- [ ] Configure firewall rules
- [ ] Use secret management
- [ ] Enable rate limiting
- [ ] Regular security updates
- [ ] Backup automation
- [ ] Access control (RBAC)
- [ ] Audit logging

## Performance Tuning

### Database
- Connection pooling
- Query optimization
- Proper indexing
- Regular VACUUM

### APIs
- Response caching
- Request batching
- Connection pooling
- Async processing

### Frontend
- CDN for static assets
- Image optimization
- Code splitting
- SSR/ISR with Next.js

---

For detailed infrastructure configuration, see [infrastructure/README.md](./README.md).
