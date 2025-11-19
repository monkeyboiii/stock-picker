# Auth API

Authentication and authorization service with JWT and OAuth2

## Overview

Auth API is a FastAPI-based microservice that provides:

- **User Registration & Login**: Email/password authentication
- **JWT Tokens**: Access tokens (30 min) and refresh tokens (7 days)
- **Role-Based Access Control (RBAC)**: User, Analyst, Admin roles
- **User Management**: Profile updates, password changes
- **Session Management**: Refresh token storage and validation

## Tech Stack

- **Framework**: FastAPI 0.115+ (async Python web framework)
- **Database**: PostgreSQL 16+ with SQLAlchemy 2.0
- **Authentication**: JWT (python-jose), bcrypt password hashing
- **Python**: 3.13+

## API Endpoints

### Public Endpoints

- `POST /register` - Register a new user
  - Parameters: `email`, `username`, `password`, `full_name` (optional)
  - Returns: User object

- `POST /login` - Login with email and password
  - Parameters: `email`, `password`
  - Returns: Access token, refresh token, user object

- `POST /refresh` - Refresh access token
  - Parameters: `refresh_token`
  - Returns: New access token, user object

### Protected Endpoints (Require Authentication)

- `GET /me` - Get current user profile
  - Requires: Bearer token
  - Returns: User object

- `PATCH /me` - Update current user profile
  - Parameters: `full_name`, `email`
  - Requires: Bearer token
  - Returns: Updated user object

- `POST /change-password` - Change password
  - Parameters: `current_password`, `new_password`
  - Requires: Bearer token

- `POST /logout` - Logout (invalidate refresh token)
  - Parameters: `refresh_token`
  - Requires: Bearer token

### Admin Endpoints

- `GET /users` - List all users (admin only)
  - Parameters: `skip`, `limit`
  - Requires: Bearer token + admin role
  - Returns: List of users

### Health Checks

- `GET /` - Basic health check
- `GET /health` - Health check with database connectivity test

## Roles & Permissions

### Roles

- **user** (default) - Regular user access
- **analyst** - Can create strategies and backtests
- **admin** - Full system access

### Role Hierarchy

```
admin > analyst > user
```

### Permissions by Role

**User:**
- `read:stocks`
- `read:backtests`

**Analyst:**
- All user permissions +
- `write:stocks`
- `write:backtests`
- `delete:backtests`

**Admin:**
- All analyst permissions +
- `read:users`
- `write:users`
- `delete:users`
- `admin:access`

## Development

### Prerequisites

- Python 3.13+
- PostgreSQL 16+
- uv (Python package manager)

### Setup

```bash
# Install dependencies
cd services/auth-api
uv sync

# Set environment variables (use strong values in production!)
export DATABASE_URL="postgresql://your_user:your_password@localhost:5432/auth_db"
export JWT_SECRET="$(openssl rand -base64 64)"  # Generate a secure secret

# Run the service
uv run uvicorn app.main:app --reload --port 8003
```

### Testing

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html
```

### Docker

```bash
# Build image
docker build -t auth-api:latest .

# Run container
docker run -p 8003:8003 \
  -e DATABASE_URL=postgresql://your_user:your_password@db:5432/auth_db \
  -e JWT_SECRET="$(openssl rand -base64 64)" \
  auth-api:latest
```

## API Documentation

Once running, visit:

- **Swagger UI**: http://localhost:8003/docs
- **ReDoc**: http://localhost:8003/redoc
- **OpenAPI JSON**: http://localhost:8003/openapi.json

## Environment Variables

Required:
- `DATABASE_URL` - PostgreSQL connection string
- `SECRET_KEY` - Secret key for JWT signing (use strong random value)

Optional:
- `ACCESS_TOKEN_EXPIRE_MINUTES` - Access token expiration (default: 30)
- `REFRESH_TOKEN_EXPIRE_DAYS` - Refresh token expiration (default: 7)
- `ALGORITHM` - JWT algorithm (default: HS256)

## Authentication Flow

### Registration
1. User submits email, username, password
2. Password is hashed with bcrypt
3. User record created in database with role="user"

### Login
1. User submits email and password
2. Password verified against hashed password
3. Access token (JWT) and refresh token (random) generated
4. Refresh token stored in database with expiration
5. Both tokens returned to client

### Token Refresh
1. Client submits refresh token
2. Token validated against database
3. New access token generated
4. Access token returned (refresh token remains valid)

### Authenticated Requests
1. Client includes access token in Authorization header: `Bearer <token>`
2. Server validates JWT signature and expiration
3. User extracted from token and loaded from database
4. Request processed with user context

## Security Features

- ✅ **Password hashing**: bcrypt with salt
- ✅ **JWT tokens**: Signed with HS256 algorithm
- ✅ **Token expiration**: Short-lived access tokens (30 min)
- ✅ **Refresh token rotation**: Secure random tokens
- ✅ **Session management**: Refresh tokens stored in database
- ✅ **Account status**: Active/inactive flag
- ✅ **Role-based permissions**: RBAC enforcement

## Future Enhancements (Phase 4+)

- [ ] OAuth2 integration (Google, GitHub)
- [ ] Email verification
- [ ] Password reset flow
- [ ] Two-factor authentication (2FA)
- [ ] Account lockout after failed attempts
- [ ] Audit logging
- [ ] Redis caching for session lookups

## License

MIT
