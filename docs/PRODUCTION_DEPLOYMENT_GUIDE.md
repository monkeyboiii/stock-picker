# Production Deployment Guide

Complete guide for deploying Stock Picker to production environments using Docker, Kubernetes, or bare metal.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Docker Deployment](#docker-deployment)
3. [Kubernetes Deployment](#kubernetes-deployment)
4. [Database Setup](#database-setup)
5. [Monitoring](#monitoring)
6. [Security](#security)
7. [Performance Tuning](#performance-tuning)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

**Minimum:**
- CPU: 4 cores
- RAM: 8 GB
- Storage: 100 GB SSD
- OS: Ubuntu 22.04 LTS or later

**Recommended (Production):**
- CPU: 8+ cores
- RAM: 16+ GB
- Storage: 500 GB NVMe SSD
- OS: Ubuntu 22.04 LTS

### Software Requirements

- Docker 24.0+
- Docker Compose 2.20+
- Kubernetes 1.28+ (for K8s deployment)
- PostgreSQL 16+
- Redis 7+

## Docker Deployment

### Quick Start

1. **Clone the repository:**
```bash
git clone https://github.com/monkeyboiii/stock-picker.git
cd stock-picker
```

2. **Create environment file:**
```bash
cp .env.production .env
# Edit .env with your configuration
vi .env
```

3. **Start all services:**
```bash
docker-compose up -d
```

4. **Verify services:**
```bash
docker-compose ps
docker-compose logs -f api
```

5. **Initialize database:**
```bash
docker-compose exec api stock-picker init -r -lll
```

6. **Access services:**
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- MCP Server: http://localhost:8001
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

### Production Configuration

**Edit .env file:**
```bash
# Change all passwords
POSTGRES_PASSWORD=$(openssl rand -base64 32)
REDIS_PASSWORD=$(openssl rand -base64 32)
SECRET_KEY=$(openssl rand -hex 32)

# Set production log level
LOG_LEVEL=INFO

# Enable monitoring
ENABLE_MONITORING=true
```

**Scale services:**
```bash
# Scale API to 4 instances
docker-compose up -d --scale api=4
```

**SSL/TLS Setup:**
```bash
# Add nginx reverse proxy
docker-compose -f docker-compose.yml -f docker-compose.nginx.yml up -d
```

## Kubernetes Deployment

### Prerequisites

- kubectl configured
- Helm 3+ installed
- Ingress controller (nginx)
- cert-manager for SSL

### Deployment Steps

1. **Create namespace:**
```bash
kubectl create namespace stock-picker
```

2. **Create secrets:**
```bash
kubectl create secret generic stock-picker-secrets \
  --from-literal=POSTGRES_PASSWORD=$(openssl rand -base64 32) \
  --from-literal=REDIS_PASSWORD=$(openssl rand -base64 32) \
  --from-literal=SECRET_KEY=$(openssl rand -hex 32) \
  -n stock-picker
```

3. **Deploy PostgreSQL:**
```bash
kubectl apply -f deployment/kubernetes/deployment.yml
```

4. **Deploy services:**
```bash
kubectl apply -f deployment/kubernetes/service.yml
```

5. **Verify deployment:**
```bash
kubectl get pods -n stock-picker
kubectl get svc -n stock-picker
kubectl logs -f deployment/stock-picker-api -n stock-picker
```

6. **Initialize database:**
```bash
kubectl exec -it deployment/stock-picker-api -n stock-picker -- stock-picker init -r -lll
```

### Ingress Configuration

**Prerequisites:**
```bash
# Install nginx ingress controller
helm install nginx-ingress ingress-nginx/ingress-nginx

# Install cert-manager for SSL
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
```

**Update ingress hosts:**
```yaml
# Edit deployment/kubernetes/service.yml
spec:
  rules:
  - host: api.yourdomain.com  # Change this
    ...
```

**Apply ingress:**
```bash
kubectl apply -f deployment/kubernetes/service.yml
```

### Horizontal Autoscaling

```bash
# HPA is already configured in deployment.yml
kubectl get hpa -n stock-picker

# Monitor autoscaling
kubectl describe hpa stock-picker-api-hpa -n stock-picker
```

## Database Setup

### PostgreSQL Optimization

**Apply indexes:**
```bash
# Via Docker
docker-compose exec postgres psql -U stockpicker -d stock_picker -f /app/sql/phase7_optimization.sql

# Via Kubernetes
kubectl exec -it statefulset/postgres -n stock-picker -- \
  psql -U stockpicker -d stock_picker -f /app/sql/phase7_optimization.sql
```

**Configure PostgreSQL:**
```bash
# Edit postgresql.conf
shared_buffers = 4GB
effective_cache_size = 12GB
maintenance_work_mem = 1GB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 20MB
min_wal_size = 2GB
max_wal_size = 8GB
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
```

**Regular maintenance:**
```bash
# Run weekly
docker-compose exec postgres psql -U stockpicker -d stock_picker -c "VACUUM ANALYZE;"

# Or schedule with cron
0 2 * * 0 docker-compose exec -T postgres psql -U stockpicker -d stock_picker -c "VACUUM ANALYZE;"
```

### Redis Configuration

**Enable persistence:**
```bash
# Ensure redis.conf has:
appendonly yes
appendfsync everysec
```

**Monitor memory:**
```bash
docker-compose exec redis redis-cli INFO memory
```

## Monitoring

### Prometheus Setup

**Access Prometheus:**
```
http://localhost:9090
```

**Key queries:**
```promql
# Request rate
rate(http_requests_total[5m])

# Error rate
rate(http_requests_total{status=~"5.."}[5m])

# Response time
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Database connections
pg_stat_database_numbackends
```

### Grafana Setup

1. **Access Grafana:**
```
http://localhost:3000
User: admin
Pass: (from .env)
```

2. **Add Prometheus data source:**
- Configuration → Data Sources → Add Prometheus
- URL: http://prometheus:9090

3. **Import dashboards:**
- Dashboard ID 1860 (Node Exporter)
- Dashboard ID 9628 (PostgreSQL)
- Dashboard ID 11835 (Redis)

### Custom Metrics

**Add to FastAPI app:**
```python
from prometheus_client import Counter, Histogram

REQUEST_COUNT = Counter('requests_total', 'Total requests')
REQUEST_LATENCY = Histogram('request_duration_seconds', 'Request latency')

@app.middleware("http")
async def metrics_middleware(request, call_next):
    REQUEST_COUNT.inc()
    with REQUEST_LATENCY.time():
        response = await call_next(request)
    return response
```

### Health Checks

**API health check:**
```bash
curl http://localhost:8000/health
```

**Database health check:**
```bash
docker-compose exec postgres pg_isready -U stockpicker
```

**Redis health check:**
```bash
docker-compose exec redis redis-cli ping
```

## Security

### SSL/TLS

**Using Let's Encrypt:**
```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Get certificate
sudo certbot --nginx -d api.yourdomain.com -d mcp.yourdomain.com

# Auto-renewal
sudo certbot renew --dry-run
```

### Firewall Rules

```bash
# Allow only necessary ports
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp  # SSH
sudo ufw allow 80/tcp  # HTTP
sudo ufw allow 443/tcp  # HTTPS
sudo ufw enable
```

### API Authentication

**Add JWT authentication:**
```python
# See app/api/auth.py for implementation
# Requires SECRET_KEY in .env
```

### Database Security

**Restrict PostgreSQL access:**
```bash
# Edit pg_hba.conf
host    stock_picker    stockpicker    10.0.0.0/8    scram-sha-256
```

**Use SSL for PostgreSQL:**
```bash
# Generate certificates
openssl req -new -x509 -days 365 -nodes -text -out server.crt -keyout server.key

# Configure PostgreSQL
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
```

## Performance Tuning

### API Performance

**Worker configuration:**
```bash
# Adjust based on CPU cores
API_WORKERS=$(( 2 * CPU_CORES + 1 ))
```

**Connection pooling:**
```python
# app/db/engine.py
create_engine(
    url,
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True
)
```

### Redis Caching

**Enable caching:**
```python
from app.cache import get_cache

cache = get_cache()
cache.set("backtest_run", run_data, ttl=3600)
```

**Monitor hit rate:**
```bash
docker-compose exec redis redis-cli INFO stats | grep keyspace
```

### Database Query Optimization

**Enable query logging:**
```sql
ALTER SYSTEM SET log_min_duration_statement = 1000;  -- Log queries > 1s
SELECT pg_reload_conf();
```

**View slow queries:**
```sql
SELECT * FROM v_table_stats ORDER BY total_size DESC LIMIT 10;
SELECT * FROM v_index_usage WHERE idx_scan < 100;
```

## Backup & Recovery

### PostgreSQL Backup

**Daily backup script:**
```bash
#!/bin/bash
BACKUP_DIR=/backups/postgres
DATE=$(date +%Y%m%d_%H%M%S)

docker-compose exec -T postgres pg_dump -U stockpicker stock_picker | \
  gzip > $BACKUP_DIR/stock_picker_$DATE.sql.gz

# Keep last 7 days
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete
```

**Restore from backup:**
```bash
gunzip < backup.sql.gz | \
  docker-compose exec -T postgres psql -U stockpicker stock_picker
```

### Redis Backup

**Backup RDB file:**
```bash
docker-compose exec redis redis-cli SAVE
docker cp stock-picker-redis:/data/dump.rdb ./backups/redis_$(date +%Y%m%d).rdb
```

### Application Data

**Backup reports:**
```bash
tar -czf reports_$(date +%Y%m%d).tar.gz reports/
```

## Troubleshooting

### Common Issues

**1. Database Connection Refused**
```bash
# Check PostgreSQL is running
docker-compose ps postgres
docker-compose logs postgres

# Check connection string
echo $POSTGRES_HOST $POSTGRES_PORT
```

**2. High Memory Usage**
```bash
# Check container resources
docker stats

# Restart containers
docker-compose restart
```

**3. Slow Queries**
```bash
# Check database indexes
docker-compose exec postgres psql -U stockpicker -d stock_picker -c "\di"

# Apply missing indexes
docker-compose exec postgres psql -U stockpicker -d stock_picker -f /app/sql/phase7_optimization.sql
```

**4. Redis Connection Issues**
```bash
# Test Redis connection
docker-compose exec redis redis-cli ping

# Check authentication
docker-compose exec redis redis-cli --askpass
```

### Logs

**View all logs:**
```bash
docker-compose logs -f
```

**View specific service:**
```bash
docker-compose logs -f api
docker-compose logs -f mcp
docker-compose logs -f postgres
```

**Kubernetes logs:**
```bash
kubectl logs -f deployment/stock-picker-api -n stock-picker
kubectl logs --previous deployment/stock-picker-api -n stock-picker  # Previous crash
```

### Performance Monitoring

**Check CPU/Memory:**
```bash
docker stats
kubectl top pods -n stock-picker
```

**Database connections:**
```sql
SELECT count(*) FROM pg_stat_activity;
SELECT * FROM pg_stat_activity WHERE state = 'active';
```

**API response times:**
```bash
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/health
```

## Maintenance

### Weekly Tasks

- [ ] Review Grafana dashboards
- [ ] Check error logs
- [ ] Verify backups
- [ ] Update security patches

### Monthly Tasks

- [ ] Review and optimize slow queries
- [ ] Clean up old backtest results
- [ ] Update dependencies
- [ ] Review resource usage and scale if needed

### Quarterly Tasks

- [ ] Security audit
- [ ] Disaster recovery test
- [ ] Performance benchmarking
- [ ] Architecture review

## Support

**Documentation:**
- [API Documentation](./FRONTEND_API_SPEC.md)
- [n8n Integration](./N8N_INTEGRATION_GUIDE.md)
- [Frontend Integration](./FRONTEND_INTEGRATION_GUIDE.md)

**Monitoring:**
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000

**Logs:**
```bash
docker-compose logs -f
kubectl logs -f deployment/stock-picker-api -n stock-picker
```

---

**Last Updated:** 2025-11-17
**Version:** 1.0.0
