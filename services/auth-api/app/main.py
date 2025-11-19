"""
Auth API - Authentication and Authorization Service

FastAPI service for:
- User registration and login
- JWT token generation and refresh
- OAuth2 integration (Google, GitHub)
- Role-based access control (RBAC)
- User management
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.models import Base, Session as DBSession, User
from app.schemas.auth import (
    ChangePasswordRequest,
    RefreshTokenRequest,
    TokenResponse,
    UserCreate,
    UserLogin,
    UserResponse,
    UserUpdate,
)
from app.security import (
    TokenData,
    create_access_token,
    create_refresh_token,
    hash_password,
    verify_password,
    verify_token,
)

# Database configuration - MUST be set via environment variables
import os

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    # Construct from individual components if DATABASE_URL not provided
    db_user = os.getenv("POSTGRES_USERNAME")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DATABASE", "auth_db")

    if not db_user or not db_pass:
        raise ValueError(
            "Database credentials required. Set either DATABASE_URL or "
            "POSTGRES_USERNAME and POSTGRES_PASSWORD environment variables."
        )

    DATABASE_URL = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# HTTP Bearer security scheme
security = HTTPBearer()


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle handler for FastAPI app"""
    logger.info("Starting Auth API...")
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    yield
    logger.info("Shutting down Auth API...")


# FastAPI app
app = FastAPI(
    title="Auth API",
    description="Authentication and authorization service with JWT and OAuth2",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS middleware - Security: Whitelist specific origins only
# Get allowed origins from environment variable (comma-separated)
allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000")
allowed_origins = [origin.strip() for origin in allowed_origins_str.split(",")]

logger.info(f"CORS allowed origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Whitelist only trusted origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],  # Explicit methods only
    allow_headers=["Content-Type", "Authorization"],  # Explicit headers only
    max_age=600,  # Cache preflight requests for 10 minutes
)

# Cookie security configuration
# SECURITY: Make cookie secure flag configurable for development vs production
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() == "true"
IS_PRODUCTION = os.getenv("ENVIRONMENT", "development").lower() == "production"
# Auto-enable secure cookies in production, or when explicitly enabled
SECURE_COOKIES = COOKIE_SECURE or IS_PRODUCTION

logger.info(f"Cookie security: secure={SECURE_COOKIES}, environment={os.getenv('ENVIRONMENT', 'development')}")


# Dependency to get database session
def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Dependency to get current user from JWT token
def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    """
    Get current user from JWT token

    Raises:
        HTTPException: If token is invalid or user not found
    """
    token = credentials.credentials
    token_data = verify_token(token)

    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = db.query(User).filter(User.id == token_data.user_id).first()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    return user


@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint with database connectivity test"""
    try:
        from sqlalchemy import text
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        return HealthResponse(status="healthy", version="1.0.0")
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed",
        )


@app.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """
    Register a new user

    - **email**: User's email address (must be unique)
    - **username**: Username (must be unique, 3-50 characters)
    - **password**: Password (minimum 8 characters)
    - **full_name**: Optional full name
    """
    # Check if email already exists
    existing_user = db.query(User).filter(User.email == user_data.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    # Check if username already exists
    existing_username = db.query(User).filter(User.username == user_data.username).first()
    if existing_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already taken",
        )

    # Create new user
    hashed_pw = hash_password(user_data.password)
    new_user = User(
        email=user_data.email,
        username=user_data.username,
        hashed_password=hashed_pw,
        full_name=user_data.full_name,
        role="user",  # Default role
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    logger.info(f"New user registered: {new_user.email}")

    return UserResponse(**new_user.to_dict())


@app.post("/login", response_model=TokenResponse)
async def login(
    credentials: UserLogin,
    response: Response,
    request: Request,
    db: Session = Depends(get_db)
):
    """
    Login with email and password

    Returns user info and sets httpOnly cookies for tokens (SECURITY: Prevents XSS token theft)

    Account lockout policy:
    - Maximum 5 failed attempts before 30-minute lockout
    - Lockout duration: 30 minutes (configurable)
    - Failed attempts reset on successful login

    SECURITY FEATURES:
    - Constant-time user lookup (prevents timing attacks)
    - Generic error messages (prevents user enumeration)
    - IP-based logging for security monitoring

    - **email**: User's email address
    - **password**: User's password
    """
    # SECURITY: Generic error message for all authentication failures
    GENERIC_AUTH_ERROR = "Invalid credentials"

    # SECURITY: Dummy password hash for constant-time verification when user doesn't exist
    # This maintains consistent timing by always performing bcrypt verification
    DUMMY_PASSWORD_HASH = "$2b$12$E5edpEhx3geomhU4lspI/eXOfL5pGyLxzgylPoEDg2zq1R7cb9ROy"

    # Get lockout configuration from environment (with bounds checking)
    max_attempts = max(1, min(int(os.getenv("MAX_LOGIN_ATTEMPTS", "5")), 10))  # 1-10
    lockout_minutes = max(1, min(int(os.getenv("LOCKOUT_DURATION_MINUTES", "30")), 1440))  # 1-24hrs

    # Get client IP for logging (handle proxy headers)
    client_ip = request.client.host if request.client else "unknown"
    if "x-forwarded-for" in request.headers:
        client_ip = request.headers["x-forwarded-for"].split(",")[0].strip()

    # Find user by email
    user = db.query(User).filter(User.email == credentials.email).first()

    # SECURITY: Always perform password verification for constant-time response
    if not user:
        # Verify against dummy hash to maintain consistent timing
        verify_password(credentials.password, DUMMY_PASSWORD_HASH)

        # Log failed attempt (non-existent user)
        logger.warning(
            f"Failed login attempt: email={credentials.email}, "
            f"reason=user_not_found, ip={client_ip}"
        )

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=GENERIC_AUTH_ERROR,
            headers={"WWW-Authenticate": "Bearer"},
        )

    # SECURITY: Verify password BEFORE checking lockout status (constant-time)
    password_valid = verify_password(credentials.password, user.hashed_password)

    # SECURITY: Check if account is locked (check AFTER password verification)
    if user.is_locked():
        # Record failed attempt if password was wrong
        if not password_valid:
            user.record_failed_login(max_attempts=max_attempts, lockout_duration_minutes=lockout_minutes)
            db.commit()

        # Log locked account login attempt with details
        lockout_remaining = (user.locked_until - datetime.utcnow()).total_seconds() / 60
        logger.warning(
            f"Login attempt on locked account: email={user.email}, "
            f"lockout_remaining={int(lockout_remaining)}min, "
            f"password_valid={password_valid}, ip={client_ip}"
        )

        # SECURITY: Return generic error (don't reveal lockout status)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=GENERIC_AUTH_ERROR,
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check password validity
    if not password_valid:
        # SECURITY: Record failed login attempt
        user.record_failed_login(max_attempts=max_attempts, lockout_duration_minutes=lockout_minutes)
        db.commit()

        # Log detailed information internally (not exposed to user)
        logger.warning(
            f"Failed login attempt: email={user.email}, "
            f"reason=invalid_password, "
            f"attempts={user.failed_login_attempts}/{max_attempts}, "
            f"ip={client_ip}"
        )

        # SECURITY: Generic error message (don't reveal remaining attempts)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=GENERIC_AUTH_ERROR,
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if account is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    # SECURITY: Reset failed login attempts on successful login
    user.reset_failed_attempts()

    # Update last login
    user.last_login = datetime.utcnow()
    db.commit()

    # Create tokens
    access_token = create_access_token(
        data={
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
        }
    )

    refresh_token = create_refresh_token()

    # Store refresh token in database
    session = DBSession(
        user_id=user.id,
        refresh_token=refresh_token,
        expires_at=datetime.utcnow() + timedelta(days=7),
    )
    db.add(session)
    db.commit()

    # SECURITY: Set tokens in httpOnly cookies (prevents XSS attacks)
    # Access token cookie - short-lived (30 minutes)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,       # Cannot be accessed by JavaScript
        secure=SECURE_COOKIES,  # HTTPS only (auto-detected: dev=False, prod=True)
        samesite="lax",      # CSRF protection
        max_age=1800,        # 30 minutes (matches ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    # Refresh token cookie - long-lived (7 days)
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,       # Cannot be accessed by JavaScript
        secure=SECURE_COOKIES,  # HTTPS only (auto-detected: dev=False, prod=True)
        samesite="lax",      # CSRF protection
        max_age=604800,      # 7 days (matches REFRESH_TOKEN_EXPIRE_DAYS)
    )

    logger.info(f"User logged in: {user.email} (tokens set in httpOnly cookies)")

    # Still return tokens in response body for backwards compatibility
    # Frontend should migrate to cookie-based auth
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        user=UserResponse(**user.to_dict()),
    )


@app.post("/refresh", response_model=TokenResponse)
async def refresh_token(request: RefreshTokenRequest, db: Session = Depends(get_db)):
    """
    Refresh access token using refresh token

    - **refresh_token**: Valid refresh token
    """
    # Find session by refresh token
    session = (
        db.query(DBSession)
        .filter(DBSession.refresh_token == request.refresh_token)
        .first()
    )

    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )

    # Check if token is expired
    if session.expires_at < datetime.utcnow():
        db.delete(session)
        db.commit()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token expired",
        )

    # Get user
    user = db.query(User).filter(User.id == session.user_id).first()

    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    # Update session last used
    session.last_used = datetime.utcnow()
    db.commit()

    # Create new access token
    access_token = create_access_token(
        data={
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
        }
    )

    return TokenResponse(
        access_token=access_token,
        refresh_token=request.refresh_token,
        user=UserResponse(**user.to_dict()),
    )


@app.post("/logout")
async def logout(
    request: RefreshTokenRequest,
    response: Response,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Logout user by invalidating refresh token and clearing cookies

    - **refresh_token**: Refresh token to invalidate
    """
    # Delete session
    session = (
        db.query(DBSession)
        .filter(
            DBSession.refresh_token == request.refresh_token,
            DBSession.user_id == current_user.id,
        )
        .first()
    )

    if session:
        db.delete(session)
        db.commit()
        logger.info(f"User logged out: {current_user.email}")

    # SECURITY: Clear httpOnly cookies
    response.delete_cookie(key="access_token")
    response.delete_cookie(key="refresh_token")

    return {"message": "Successfully logged out"}


@app.get("/me", response_model=UserResponse)
async def get_current_user_profile(current_user: User = Depends(get_current_user)):
    """
    Get current user's profile

    Requires authentication (Bearer token)
    """
    return UserResponse(**current_user.to_dict())


@app.patch("/me", response_model=UserResponse)
async def update_profile(
    update_data: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Update current user's profile

    - **full_name**: New full name
    - **email**: New email address
    """
    if update_data.full_name is not None:
        current_user.full_name = update_data.full_name

    if update_data.email is not None:
        # Check if email is already taken
        existing_user = (
            db.query(User)
            .filter(User.email == update_data.email, User.id != current_user.id)
            .first()
        )
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use",
            )
        current_user.email = update_data.email

    current_user.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(current_user)

    logger.info(f"User profile updated: {current_user.email}")

    return UserResponse(**current_user.to_dict())


@app.post("/change-password")
async def change_password(
    request: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Change current user's password

    - **current_password**: Current password for verification
    - **new_password**: New password (minimum 8 characters)
    """
    # Verify current password
    if not verify_password(request.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect",
        )

    # Update password
    current_user.hashed_password = hash_password(request.new_password)
    current_user.updated_at = datetime.utcnow()
    db.commit()

    logger.info(f"Password changed for user: {current_user.email}")

    return {"message": "Password successfully changed"}


@app.get("/users", response_model=list[UserResponse])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    List all users (admin only)

    - **skip**: Number of users to skip
    - **limit**: Maximum number of users to return
    """
    # Check if user has admin role
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions",
        )

    users = db.query(User).offset(skip).limit(limit).all()
    return [UserResponse(**user.to_dict()) for user in users]


if __name__ == "__main__":
    import uvicorn

    # Bind to 0.0.0.0 for Docker container accessibility
    uvicorn.run(app, host="0.0.0.0", port=8003)  # nosec B104
