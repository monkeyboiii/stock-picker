# TLS/SSL Configuration Guide

**Version:** 1.0
**Date:** 2025-11-19
**Status:** Production-Ready
**Priority:** P2 (Medium Priority - Infrastructure Security)

## Table of Contents

1. [Overview](#overview)
2. [Certificate Management](#certificate-management)
3. [FastAPI Services (HTTPS)](#fastapi-services-https)
4. [Nginx Reverse Proxy](#nginx-reverse-proxy)
5. [PostgreSQL SSL Connections](#postgresql-ssl-connections)
6. [Docker & Kubernetes](#docker--kubernetes)
7. [Security Best Practices](#security-best-practices)
8. [Testing & Verification](#testing--verification)
9. [Troubleshooting](#troubleshooting)

---

## Overview

### Why TLS/SSL?

TLS (Transport Layer Security) provides:
- **Encryption**: Protects data in transit from eavesdropping
- **Authentication**: Verifies server identity via certificates
- **Integrity**: Prevents data tampering during transmission

### Architecture

```
Internet → Nginx (TLS termination) → FastAPI services (HTTP)
                                   ↓
                              PostgreSQL (SSL)
```

**TLS Termination Point:** Nginx reverse proxy
**Internal Communication:** HTTP (within Kubernetes cluster) or HTTPS (production)
**Database:** SSL/TLS required for production

---

## Certificate Management

### Option 1: Let's Encrypt (Recommended for Production)

**Automatic certificate renewal with certbot:**

```bash
# Install certbot
apt-get update
apt-get install -y certbot python3-certbot-nginx

# Obtain certificate
certbot --nginx -d yourdomain.com -d www.yourdomain.com \
  --email admin@yourdomain.com \
  --agree-tos \
  --non-interactive \
  --redirect

# Auto-renewal (cron job)
certbot renew --dry-run
# Add to crontab: 0 3 * * * certbot renew --quiet --post-hook "systemctl reload nginx"
```

**Pros:**
- Free, automated, trusted CA
- 90-day certificates with auto-renewal
- Widely supported

**Cons:**
- Requires public domain
- 90-day expiration (requires automation)

### Option 2: Self-Signed Certificates (Development/Testing)

**Generate self-signed certificate:**

```bash
# Create directory
mkdir -p /etc/ssl/private /etc/ssl/certs

# Generate private key and certificate
openssl req -x509 -nodes -days 365 \
  -newkey rsa:4096 \
  -keyout /etc/ssl/private/stock-picker.key \
  -out /etc/ssl/certs/stock-picker.crt \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# Set permissions
chmod 600 /etc/ssl/private/stock-picker.key
chmod 644 /etc/ssl/certs/stock-picker.crt
```

**Pros:**
- No external dependencies
- Works for local development

**Cons:**
- Browser warnings (untrusted)
- Not suitable for production

### Option 3: Commercial CA (Enterprise)

Purchase from Digicert, GlobalSign, etc.

**Installation:**
```bash
# Place certificate files
cp yourdomain.crt /etc/ssl/certs/
cp yourdomain.key /etc/ssl/private/
cp ca-bundle.crt /etc/ssl/certs/

# Set permissions
chmod 600 /etc/ssl/private/yourdomain.key
chmod 644 /etc/ssl/certs/yourdomain.crt
```

### Certificate Storage in Kubernetes

**Using Kubernetes Secrets:**

```bash
# Create TLS secret
kubectl create secret tls stock-picker-tls \
  --namespace=stock-picker \
  --cert=/path/to/cert.crt \
  --key=/path/to/cert.key

# Verify
kubectl get secret stock-picker-tls -n stock-picker -o yaml
```

**Using cert-manager (Automated Let's Encrypt):**

```yaml
# infrastructure/kubernetes/base/cert-manager-issuer.yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: admin@yourdomain.com
    privateKeySecretRef:
      name: letsencrypt-prod
    solvers:
    - http01:
        ingress:
          class: nginx
```

---

## FastAPI Services (HTTPS)

### Option 1: Uvicorn with SSL (Development)

**Direct HTTPS in FastAPI:**

```python
# services/auth-api/app/main.py
import uvicorn
import ssl

if __name__ == "__main__":
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(
        certfile="/etc/ssl/certs/stock-picker.crt",
        keyfile="/etc/ssl/private/stock-picker.key"
    )

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8443,  # HTTPS port
        ssl_keyfile="/etc/ssl/private/stock-picker.key",
        ssl_certfile="/etc/ssl/certs/stock-picker.crt",
        ssl_keyfile_password=None,  # If key is encrypted
    )
```

**Run with SSL:**
```bash
cd services/auth-api
uvicorn app.main:app \
  --host 0.0.0.0 \
  --port 8443 \
  --ssl-keyfile /etc/ssl/private/stock-picker.key \
  --ssl-certfile /etc/ssl/certs/stock-picker.crt
```

**Environment Configuration:**

```bash
# services/auth-api/.env
SSL_ENABLED=true
SSL_CERTFILE=/etc/ssl/certs/stock-picker.crt
SSL_KEYFILE=/etc/ssl/private/stock-picker.key
```

### Option 2: Nginx Reverse Proxy (Recommended for Production)

**Pros:**
- Centralized TLS termination
- Better performance (async I/O)
- Load balancing capabilities
- Static file serving

**Configuration:** See [Nginx Reverse Proxy](#nginx-reverse-proxy) section

---

## Nginx Reverse Proxy

### Installation

```bash
apt-get update
apt-get install -y nginx
systemctl enable nginx
systemctl start nginx
```

### Configuration

**Create Nginx configuration:**

```nginx
# /etc/nginx/sites-available/stock-picker
upstream auth_api {
    server 127.0.0.1:8001;  # Local
    # server auth-api:8001;  # Docker
    # server auth-api-service:8001;  # Kubernetes
}

upstream calculation_api {
    server 127.0.0.1:8002;
}

upstream notification_api {
    server 127.0.0.1:8003;
}

# HTTP → HTTPS redirect
server {
    listen 80;
    listen [::]:80;
    server_name yourdomain.com www.yourdomain.com;

    # Let's Encrypt ACME challenge
    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    # Redirect all other traffic to HTTPS
    location / {
        return 301 https://$host$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name yourdomain.com www.yourdomain.com;

    # SSL Certificate Configuration
    ssl_certificate /etc/ssl/certs/stock-picker.crt;
    ssl_certificate_key /etc/ssl/private/stock-picker.key;

    # SSL Security Settings (Mozilla Modern Configuration)
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384';
    ssl_prefer_server_ciphers on;

    # HSTS (HTTP Strict Transport Security)
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains; preload" always;

    # OCSP Stapling
    ssl_stapling on;
    ssl_stapling_verify on;
    ssl_trusted_certificate /etc/ssl/certs/ca-bundle.crt;
    resolver 8.8.8.8 8.8.4.4 valid=300s;
    resolver_timeout 5s;

    # Session Configuration
    ssl_session_cache shared:SSL:50m;
    ssl_session_timeout 1d;
    ssl_session_tickets off;

    # Security Headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "no-referrer-when-downgrade" always;

    # Auth API
    location /api/auth/ {
        proxy_pass http://auth_api/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Calculation API
    location /api/calculation/ {
        proxy_pass http://calculation_api/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Notification API
    location /api/notification/ {
        proxy_pass http://notification_api/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Static files (if needed)
    location /static/ {
        alias /var/www/stock-picker/static/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # Health check
    location /health {
        access_log off;
        return 200 "healthy\n";
        add_header Content-Type text/plain;
    }
}
```

**Enable site:**

```bash
# Symlink configuration
ln -s /etc/nginx/sites-available/stock-picker /etc/nginx/sites-enabled/

# Test configuration
nginx -t

# Reload Nginx
systemctl reload nginx
```

### Nginx TLS Best Practices

**Test SSL Configuration:**
```bash
# Using SSL Labs (online)
# https://www.ssllabs.com/ssltest/analyze.html?d=yourdomain.com

# Using testssl.sh (local)
git clone https://github.com/drwetter/testssl.sh.git
cd testssl.sh
./testssl.sh https://yourdomain.com
```

**Expected Grade:** A+ on SSL Labs

---

## PostgreSQL SSL Connections

### Server Configuration

**Enable SSL in PostgreSQL:**

```bash
# Generate server certificate
cd /var/lib/postgresql/data
openssl req -new -x509 -days 365 -nodes -text \
  -out server.crt \
  -keyout server.key \
  -subj "/CN=postgres-server"

# Set permissions
chown postgres:postgres server.crt server.key
chmod 600 server.key
chmod 644 server.crt

# Edit postgresql.conf
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
ssl_ca_file = 'root.crt'  # Optional: client certificate verification
ssl_ciphers = 'HIGH:MEDIUM:+3DES:!aNULL'  # Strong ciphers only
ssl_prefer_server_ciphers = on
ssl_min_protocol_version = 'TLSv1.2'

# Edit pg_hba.conf (require SSL)
# TYPE  DATABASE        USER            ADDRESS                 METHOD
hostssl all             all             0.0.0.0/0               scram-sha-256
```

**Restart PostgreSQL:**
```bash
systemctl restart postgresql
```

### Client Configuration

**Connection String (Python):**

```python
# Using psycopg2
import psycopg2

conn = psycopg2.connect(
    host="postgres-host",
    port=5432,
    database="stock_picker",
    user="your_user",
    password="your_password",
    sslmode="require",  # Options: disable, allow, prefer, require, verify-ca, verify-full
    sslrootcert="/path/to/root.crt",  # CA certificate (for verify-ca/verify-full)
)

# Using SQLAlchemy
from sqlalchemy import create_engine

DATABASE_URL = (
    "postgresql://user:password@host:5432/database"
    "?sslmode=require"
    "&sslrootcert=/path/to/root.crt"
)
engine = create_engine(DATABASE_URL)
```

**Environment Variables:**

```bash
# .env
DATABASE_URL=postgresql://user:password@host:5432/database?sslmode=require
PGSSLMODE=require
PGSSLROOTCERT=/path/to/root.crt
PGSSLCERT=/path/to/client.crt  # Optional: client certificate
PGSSLKEY=/path/to/client.key   # Optional: client key
```

**SSL Modes:**

| Mode | Description | Security Level |
|------|-------------|----------------|
| `disable` | No SSL | ❌ None |
| `allow` | SSL if server supports | ⚠️ Low |
| `prefer` | SSL preferred, fallback to non-SSL | ⚠️ Medium |
| `require` | **SSL required** | ✅ Good |
| `verify-ca` | Verify server certificate against CA | ✅ Better |
| `verify-full` | Verify certificate and hostname | ✅ Best |

**Recommended for Production:** `verify-full`

### Kubernetes PostgreSQL SSL

**Store certificates in secrets:**

```bash
# Create secret with PostgreSQL certificates
kubectl create secret generic postgres-ssl \
  --namespace=stock-picker \
  --from-file=root.crt=/path/to/root.crt \
  --from-file=client.crt=/path/to/client.crt \
  --from-file=client.key=/path/to/client.key
```

**Mount in deployment:**

```yaml
# infrastructure/kubernetes/base/auth-api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: auth-api
spec:
  template:
    spec:
      containers:
      - name: auth-api
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: DATABASE_URL
        - name: PGSSLMODE
          value: "verify-full"
        - name: PGSSLROOTCERT
          value: "/etc/ssl/postgres/root.crt"
        volumeMounts:
        - name: postgres-ssl
          mountPath: /etc/ssl/postgres
          readOnly: true
      volumes:
      - name: postgres-ssl
        secret:
          secretName: postgres-ssl
```

---

## Docker & Kubernetes

### Docker Compose (Development)

```yaml
# docker-compose.yml
services:
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - /etc/ssl/certs/stock-picker.crt:/etc/ssl/certs/stock-picker.crt:ro
      - /etc/ssl/private/stock-picker.key:/etc/ssl/private/stock-picker.key:ro
    networks:
      - stock-picker-network

  auth-api:
    build:
      context: ./services/auth-api
    environment:
      - SSL_ENABLED=false  # SSL terminated at Nginx
      - DATABASE_URL=postgresql://user:pass@postgres:5432/db?sslmode=require
    networks:
      - stock-picker-network

  postgres:
    image: postgres:16
    volumes:
      - ./postgres/server.crt:/var/lib/postgresql/data/server.crt:ro
      - ./postgres/server.key:/var/lib/postgresql/data/server.key:ro
    command: postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/data/server.crt -c ssl_key_file=/var/lib/postgresql/data/server.key
    networks:
      - stock-picker-network

networks:
  stock-picker-network:
    driver: bridge
```

### Kubernetes Ingress (Production)

**Using Nginx Ingress Controller:**

```yaml
# infrastructure/kubernetes/base/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: stock-picker-ingress
  namespace: stock-picker
  annotations:
    # Nginx-specific annotations
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/force-ssl-redirect: "true"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
    nginx.ingress.kubernetes.io/proxy-body-size: "10m"
    nginx.ingress.kubernetes.io/rate-limit: "100"

    # Security headers
    nginx.ingress.kubernetes.io/configuration-snippet: |
      more_set_headers "Strict-Transport-Security: max-age=31536000; includeSubDomains; preload";
      more_set_headers "X-Frame-Options: SAMEORIGIN";
      more_set_headers "X-Content-Type-Options: nosniff";
      more_set_headers "X-XSS-Protection: 1; mode=block";
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - yourdomain.com
    - www.yourdomain.com
    secretName: stock-picker-tls  # Created by cert-manager or manually
  rules:
  - host: yourdomain.com
    http:
      paths:
      - path: /api/auth
        pathType: Prefix
        backend:
          service:
            name: auth-api-service
            port:
              number: 8001
      - path: /api/calculation
        pathType: Prefix
        backend:
          service:
            name: calculation-api-service
            port:
              number: 8002
      - path: /api/notification
        pathType: Prefix
        backend:
          service:
            name: notification-api-service
            port:
              number: 8003
```

**Deploy:**
```bash
kubectl apply -f infrastructure/kubernetes/base/ingress.yaml
kubectl get ingress -n stock-picker
kubectl describe ingress stock-picker-ingress -n stock-picker
```

---

## Security Best Practices

### 1. Strong Cipher Suites

**Recommended Ciphers (Mozilla Modern):**
```
ECDHE-ECDSA-AES128-GCM-SHA256
ECDHE-RSA-AES128-GCM-SHA256
ECDHE-ECDSA-AES256-GCM-SHA384
ECDHE-RSA-AES256-GCM-SHA384
ECDHE-ECDSA-CHACHA20-POLY1305
ECDHE-RSA-CHACHA20-POLY1305
```

**Disable Weak Ciphers:**
- ❌ SSLv2, SSLv3
- ❌ TLS 1.0, TLS 1.1
- ❌ RC4, DES, 3DES
- ❌ MD5-based ciphers
- ❌ Anonymous ciphers (aNULL)

### 2. HSTS (HTTP Strict Transport Security)

```nginx
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains; preload" always;
```

**Benefits:**
- Forces HTTPS for all future visits
- Prevents downgrade attacks
- Prevents SSL stripping

**HSTS Preload:** Submit to https://hstspreload.org/

### 3. Certificate Pinning (Optional)

**Public Key Pinning (HPKP) - DEPRECATED**
- Not recommended (too risky if misconfigured)
- Use Certificate Transparency instead

**Certificate Transparency:**
```nginx
ssl_ct on;
ssl_ct_static_scts /path/to/scts;
```

### 4. OCSP Stapling

```nginx
ssl_stapling on;
ssl_stapling_verify on;
ssl_trusted_certificate /etc/ssl/certs/ca-bundle.crt;
```

**Benefits:**
- Faster certificate validation
- Privacy (no OCSP queries to CA)
- Reliability (cached responses)

### 5. Perfect Forward Secrecy (PFS)

**Ensure ephemeral key exchange:**
- ✅ ECDHE (Elliptic Curve Diffie-Hellman Ephemeral)
- ✅ DHE (Diffie-Hellman Ephemeral)

**Generate DH parameters:**
```bash
openssl dhparam -out /etc/ssl/dhparam.pem 4096
```

**Nginx configuration:**
```nginx
ssl_dhparam /etc/ssl/dhparam.pem;
```

### 6. Regular Security Audits

**Automated Scanning:**
- SSL Labs: https://www.ssllabs.com/ssltest/
- testssl.sh: https://github.com/drwetter/testssl.sh
- Mozilla Observatory: https://observatory.mozilla.org/

**Manual Checks:**
```bash
# Check certificate validity
openssl s_client -connect yourdomain.com:443 -servername yourdomain.com

# Check supported protocols
nmap --script ssl-enum-ciphers -p 443 yourdomain.com
```

---

## Testing & Verification

### 1. Local Testing

**Test HTTPS endpoint:**
```bash
# Basic connectivity
curl -v https://localhost:8443/health

# With self-signed cert (skip verification)
curl -k https://localhost:8443/api/auth/health

# With client certificate
curl --cert client.crt --key client.key https://localhost:8443/health
```

**Test PostgreSQL SSL:**
```bash
# Using psql
psql "host=localhost port=5432 dbname=stock_picker user=postgres sslmode=require"

# Check SSL connection
\conninfo
# Expected: SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256)
```

### 2. SSL Labs Test

**Online test:**
```
https://www.ssllabs.com/ssltest/analyze.html?d=yourdomain.com
```

**Expected Grade:** A+ or A

**Checklist:**
- ✅ Certificate valid and trusted
- ✅ TLS 1.2+ only
- ✅ Strong cipher suites
- ✅ Perfect Forward Secrecy
- ✅ HSTS enabled
- ✅ No vulnerabilities (BEAST, POODLE, Heartbleed, etc.)

### 3. testssl.sh

**Local scanning:**
```bash
git clone https://github.com/drwetter/testssl.sh.git
cd testssl.sh
./testssl.sh --full https://yourdomain.com

# Quick check
./testssl.sh --quick https://yourdomain.com

# Check specific vulnerability
./testssl.sh --heartbleed --crime --breach https://yourdomain.com
```

### 4. Integration Tests

**Python test script:**
```python
# tests/test_ssl.py
import requests
import ssl
import socket

def test_https_auth_api():
    """Test Auth API HTTPS endpoint"""
    response = requests.get("https://yourdomain.com/api/auth/health")
    assert response.status_code == 200
    assert response.url.startswith("https://")

def test_ssl_version():
    """Verify TLS 1.2+ is used"""
    context = ssl.create_default_context()
    with socket.create_connection(("yourdomain.com", 443)) as sock:
        with context.wrap_socket(sock, server_hostname="yourdomain.com") as ssock:
            version = ssock.version()
            assert version in ("TLSv1.2", "TLSv1.3")

def test_hsts_header():
    """Verify HSTS header is set"""
    response = requests.get("https://yourdomain.com/api/auth/health")
    assert "Strict-Transport-Security" in response.headers
    assert "max-age=31536000" in response.headers["Strict-Transport-Security"]

def test_postgres_ssl():
    """Test PostgreSQL SSL connection"""
    import psycopg2
    conn = psycopg2.connect(
        host="localhost",
        database="stock_picker",
        user="postgres",
        password="password",
        sslmode="require"
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    assert cur.fetchone() is not None
    conn.close()
```

---

## Troubleshooting

### Common Issues

#### 1. Certificate Not Trusted

**Error:**
```
SSL certificate problem: self signed certificate
```

**Solutions:**
- Use a trusted CA (Let's Encrypt)
- Add CA certificate to system trust store
- For testing: Use `-k` flag in curl

**Add CA to system:**
```bash
# Ubuntu/Debian
cp ca-cert.crt /usr/local/share/ca-certificates/
update-ca-certificates

# CentOS/RHEL
cp ca-cert.crt /etc/pki/ca-trust/source/anchors/
update-ca-trust
```

#### 2. Certificate Expired

**Error:**
```
SSL certificate problem: certificate has expired
```

**Solutions:**
- Renew certificate with Let's Encrypt: `certbot renew`
- Check expiration: `openssl x509 -in cert.crt -noout -dates`
- Set up auto-renewal cron job

#### 3. Certificate Hostname Mismatch

**Error:**
```
SSL: certificate subject name 'example.com' does not match target host name 'yourdomain.com'
```

**Solutions:**
- Generate new certificate with correct CN (Common Name)
- Use SAN (Subject Alternative Name) for multiple domains
- Update DNS to match certificate

#### 4. Weak Cipher Suites

**Error:**
```
sslv3 alert handshake failure
```

**Solutions:**
- Update cipher suite configuration
- Disable old protocols (TLS 1.0, 1.1)
- Use Mozilla SSL Configuration Generator: https://ssl-config.mozilla.org/

#### 5. PostgreSQL SSL Connection Failed

**Error:**
```
psycopg2.OperationalError: FATAL: no pg_hba.conf entry for host
```

**Solutions:**
- Check `pg_hba.conf` has `hostssl` entries
- Verify `postgresql.conf` has `ssl = on`
- Check certificate permissions (600 for key)
- Ensure client has CA certificate (for verify-ca/verify-full)

**Debug:**
```bash
# Check PostgreSQL SSL status
psql -h localhost -U postgres -c "SHOW ssl;"

# Check active connections
psql -h localhost -U postgres -c "SELECT * FROM pg_stat_ssl;"
```

#### 6. Nginx SSL Configuration Error

**Error:**
```
nginx: [emerg] cannot load certificate
```

**Solutions:**
- Check file paths are correct
- Verify file permissions (readable by nginx user)
- Test certificate format: `openssl x509 -in cert.crt -text -noout`
- Check Nginx error logs: `tail -f /var/log/nginx/error.log`

---

## Monitoring & Alerts

### Certificate Expiration Monitoring

**Prometheus exporter:**
```yaml
# docker-compose.yml
services:
  ssl_exporter:
    image: ribbybibby/ssl-exporter
    ports:
      - "9219:9219"
    command:
      - --config.file=/config.yaml
    volumes:
      - ./ssl-exporter-config.yaml:/config.yaml
```

**Alert rule:**
```yaml
# prometheus/alerts.yml
groups:
- name: ssl_alerts
  rules:
  - alert: CertificateExpiringSoon
    expr: ssl_certificate_not_after - time() < 86400 * 30  # 30 days
    for: 1h
    labels:
      severity: warning
    annotations:
      summary: "SSL certificate expiring soon"
      description: "Certificate for {{ $labels.instance }} expires in {{ $value | humanizeDuration }}"
```

### Nagios/Icinga Check

```bash
#!/bin/bash
# check_ssl_expiry.sh
DOMAIN=$1
DAYS_WARN=30
DAYS_CRIT=7

expiry_date=$(echo | openssl s_client -servername $DOMAIN -connect $DOMAIN:443 2>/dev/null | openssl x509 -noout -enddate | cut -d= -f2)
expiry_epoch=$(date -d "$expiry_date" +%s)
now_epoch=$(date +%s)
days_remaining=$(( ($expiry_epoch - $now_epoch) / 86400 ))

if [ $days_remaining -lt $DAYS_CRIT ]; then
    echo "CRITICAL: Certificate expires in $days_remaining days"
    exit 2
elif [ $days_remaining -lt $DAYS_WARN ]; then
    echo "WARNING: Certificate expires in $days_remaining days"
    exit 1
else
    echo "OK: Certificate expires in $days_remaining days"
    exit 0
fi
```

---

## Appendix

### Quick Reference

**Generate self-signed certificate:**
```bash
openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
  -keyout key.pem -out cert.pem \
  -subj "/CN=localhost"
```

**Check certificate:**
```bash
openssl x509 -in cert.pem -text -noout
openssl x509 -in cert.pem -noout -dates
openssl x509 -in cert.pem -noout -subject
```

**Test SSL connection:**
```bash
openssl s_client -connect domain.com:443 -servername domain.com
```

**Convert certificate formats:**
```bash
# PEM to DER
openssl x509 -in cert.pem -outform der -out cert.der

# DER to PEM
openssl x509 -in cert.der -inform der -out cert.pem

# PEM to PKCS12
openssl pkcs12 -export -out cert.p12 -inkey key.pem -in cert.pem
```

### Resources

**Tools:**
- SSL Labs: https://www.ssllabs.com/ssltest/
- testssl.sh: https://github.com/drwetter/testssl.sh
- Mozilla SSL Config Generator: https://ssl-config.mozilla.org/
- certbot (Let's Encrypt): https://certbot.eff.org/

**Documentation:**
- Mozilla SSL Configuration: https://wiki.mozilla.org/Security/Server_Side_TLS
- OWASP TLS Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Transport_Layer_Protection_Cheat_Sheet.html
- PostgreSQL SSL: https://www.postgresql.org/docs/current/ssl-tcp.html
- Nginx SSL: https://nginx.org/en/docs/http/configuring_https_servers.html

**Standards:**
- RFC 8446 (TLS 1.3): https://tools.ietf.org/html/rfc8446
- RFC 6797 (HSTS): https://tools.ietf.org/html/rfc6797
- RFC 6960 (OCSP): https://tools.ietf.org/html/rfc6960

---

**Document Version:** 1.0
**Last Updated:** 2025-11-19
**Author:** Security Review - P2 TLS/SSL Configuration
**Status:** ✅ Production-Ready
