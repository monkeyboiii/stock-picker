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

from fastapi import Depends, FastAPI, HTTPException, status
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

# Database configuration (should be in environment variables)
DATABASE_URL = "postgresql://user:password@localhost:5432/auth_db"  # TODO: Move to env

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
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """
    Login with email and password

    Returns JWT access token and refresh token

    - **email**: User's email address
    - **password**: User's password
    """
    # Find user by email
    user = db.query(User).filter(User.email == credentials.email).first()

    if not user or not verify_password(credentials.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

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

    logger.info(f"User logged in: {user.email}")

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
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Logout user by invalidating refresh token

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

    uvicorn.run(app, host="0.0.0.0", port=8003)
