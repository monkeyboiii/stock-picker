"""Pydantic schemas for authentication endpoints"""

import hashlib
import re
from typing import Optional

import requests
from loguru import logger
from pydantic import BaseModel, EmailStr, Field, field_validator


def check_password_pwned(password: str) -> bool:
    """
    Check if password appears in HaveIBeenPwned database using k-anonymity.

    Uses the HaveIBeenPwned Passwords API v3 with k-anonymity:
    - Only sends first 5 characters of SHA-1 hash
    - Receives all hashes matching that prefix
    - Checks locally if full hash is in the list

    API: https://haveibeenpwned.com/API/v3#PwnedPasswords

    Args:
        password: Password to check

    Returns:
        True if password found in breach database, False otherwise
        False if API is unavailable (fail open for availability)
    """
    try:
        # SHA-1 hash of the password (uppercase)
        sha1_hash = hashlib.sha1(password.encode('utf-8')).hexdigest().upper()
        prefix, suffix = sha1_hash[:5], sha1_hash[5:]

        # Query HaveIBeenPwned API with k-anonymity (only first 5 chars)
        url = f"https://api.pwnedpasswords.com/range/{prefix}"
        response = requests.get(url, timeout=3)

        if response.status_code == 200:
            # Check if our suffix appears in the response
            for line in response.text.splitlines():
                hash_suffix, count = line.split(':')
                if hash_suffix == suffix:
                    logger.warning(f"Password found in HaveIBeenPwned database (seen {count} times)")
                    return True  # Password found in breach
        elif response.status_code == 429:
            # Rate limited - log but don't block (fail open)
            logger.warning("HaveIBeenPwned API rate limited, skipping breach check")
        else:
            logger.warning(f"HaveIBeenPwned API returned {response.status_code}, skipping breach check")

    except requests.RequestException as e:
        # Network error - log but don't block registration (fail open)
        logger.warning(f"HaveIBeenPwned API unavailable: {e}, skipping breach check")
    except Exception as e:
        # Unexpected error - log but don't block
        logger.error(f"Unexpected error checking HaveIBeenPwned: {e}")

    return False  # Not found or API unavailable


def validate_password_complexity(password: str) -> str:
    """
    Validate password complexity requirements.

    Requirements:
    - Minimum 8 characters
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one digit
    - At least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?)

    Args:
        password: Password to validate

    Returns:
        Password if valid

    Raises:
        ValueError: If password doesn't meet complexity requirements
    """
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters long")

    if len(password) > 128:
        raise ValueError("Password must not exceed 128 characters")

    if not re.search(r"[A-Z]", password):
        raise ValueError("Password must contain at least one uppercase letter")

    if not re.search(r"[a-z]", password):
        raise ValueError("Password must contain at least one lowercase letter")

    if not re.search(r"\d", password):
        raise ValueError("Password must contain at least one digit")

    if not re.search(r"[!@#$%^&*()_+\-=\[\]{}|;:,.<>?]", password):
        raise ValueError("Password must contain at least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?)")

    # Check for common weak passwords
    weak_passwords = {
        "password", "12345678", "qwerty", "abc123", "password1",
        "password!", "password123", "admin123", "welcome1", "changeme"
    }
    if password.lower() in weak_passwords:
        raise ValueError("Password is too common. Please choose a stronger password")

    # SECURITY: Check if password appears in HaveIBeenPwned breach database
    if check_password_pwned(password):
        raise ValueError(
            "This password has been exposed in a data breach and cannot be used. "
            "Please choose a different password"
        )

    return password


class UserCreate(BaseModel):
    """Schema for user registration"""

    email: EmailStr = Field(..., description="User's email address")
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    password: str = Field(
        ...,
        min_length=8,
        description="Password (min 8 chars, uppercase, lowercase, digit, special char)"
    )
    full_name: Optional[str] = Field(None, max_length=255, description="Full name")

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Validate password complexity"""
        return validate_password_complexity(v)


class UserLogin(BaseModel):
    """Schema for user login"""

    email: EmailStr = Field(..., description="User's email address")
    password: str = Field(..., description="Password")


class UserResponse(BaseModel):
    """Schema for user response"""

    id: int
    email: str
    username: str
    full_name: Optional[str]
    role: str
    is_active: bool
    is_verified: bool
    oauth_provider: Optional[str]
    created_at: str
    last_login: Optional[str]


class TokenResponse(BaseModel):
    """Schema for token response"""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: UserResponse


class RefreshTokenRequest(BaseModel):
    """Schema for refresh token request"""

    refresh_token: str = Field(..., description="Refresh token")


class PasswordResetRequest(BaseModel):
    """Schema for password reset request"""

    email: EmailStr = Field(..., description="User's email address")


class PasswordResetConfirm(BaseModel):
    """Schema for password reset confirmation"""

    token: str = Field(..., description="Reset token")
    new_password: str = Field(
        ...,
        min_length=8,
        description="New password (min 8 chars, uppercase, lowercase, digit, special char)"
    )

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        """Validate password complexity"""
        return validate_password_complexity(v)


class UserUpdate(BaseModel):
    """Schema for updating user profile"""

    full_name: Optional[str] = Field(None, max_length=255)
    email: Optional[EmailStr] = None


class ChangePasswordRequest(BaseModel):
    """Schema for changing password"""

    current_password: str = Field(..., description="Current password")
    new_password: str = Field(
        ...,
        min_length=8,
        description="New password (min 8 chars, uppercase, lowercase, digit, special char)"
    )

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        """Validate password complexity"""
        return validate_password_complexity(v)
