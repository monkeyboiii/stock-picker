"""
Secrets Management - Redis-based secure secrets storage

This module provides secure storage and retrieval of sensitive configuration:
- Database credentials
- API keys
- OAuth client secrets
- Encryption keys

Uses Redis with encryption for sensitive data storage.
"""

import os
from base64 import b64decode, b64encode
from typing import Optional

from cryptography.fernet import Fernet
from loguru import logger

from app.cache.redis_cache import get_cache


class SecretsManager:
    """
    Secure secrets management using Redis with encryption

    Secrets are encrypted at rest using Fernet (symmetric encryption).
    The encryption key should be stored securely (environment variable or secrets service).
    """

    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize secrets manager

        Args:
            encryption_key: Base64-encoded Fernet key. If not provided,
                          will attempt to load from SECRETS_ENCRYPTION_KEY env var.
                          Generate new key with: Fernet.generate_key()
        """
        self.cache = get_cache()
        self.key_prefix = "secrets:"

        # Initialize encryption
        key_str = encryption_key or os.getenv("SECRETS_ENCRYPTION_KEY")

        if key_str:
            try:
                self.cipher = Fernet(key_str.encode() if isinstance(key_str, str) else key_str)
                self.encryption_enabled = True
                logger.info("Secrets encryption enabled")
            except Exception as e:
                logger.error(f"Failed to initialize encryption: {e}")
                logger.warning("Secrets will be stored WITHOUT encryption")
                self.encryption_enabled = False
                self.cipher = None
        else:
            logger.warning(
                "No encryption key provided. Secrets will be stored WITHOUT encryption. "
                "Set SECRETS_ENCRYPTION_KEY environment variable for production use."
            )
            self.encryption_enabled = False
            self.cipher = None

    def _make_key(self, secret_name: str) -> str:
        """Create prefixed key for secret"""
        return f"{self.key_prefix}{secret_name}"

    def _encrypt(self, value: str) -> str:
        """Encrypt secret value"""
        if not self.encryption_enabled or not self.cipher:
            return value

        try:
            encrypted_bytes = self.cipher.encrypt(value.encode())
            return b64encode(encrypted_bytes).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def _decrypt(self, encrypted_value: str) -> str:
        """Decrypt secret value"""
        if not self.encryption_enabled or not self.cipher:
            return encrypted_value

        try:
            encrypted_bytes = b64decode(encrypted_value.encode())
            decrypted_bytes = self.cipher.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    def set_secret(self, name: str, value: str, ttl: Optional[int] = None) -> bool:
        """
        Store a secret securely

        Args:
            name: Secret name (e.g., "postgres_password", "logto_client_secret")
            value: Secret value (will be encrypted)
            ttl: Time to live in seconds (None = never expire)

        Returns:
            True if successful, False otherwise

        Example:
            secrets.set_secret("db_password", "super_secret_pwd")
            secrets.set_secret("api_key", "sk-1234567890", ttl=86400)  # 24 hours
        """
        try:
            encrypted_value = self._encrypt(value)

            # Store in Redis
            key = self._make_key(name)
            if ttl:
                success = self.cache.client.setex(key, ttl, encrypted_value)
            else:
                success = self.cache.client.set(key, encrypted_value)

            if success:
                logger.info(f"Secret '{name}' stored successfully")
            return bool(success)

        except Exception as e:
            logger.error(f"Failed to store secret '{name}': {e}")
            return False

    def get_secret(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Retrieve a secret

        Args:
            name: Secret name
            default: Default value if secret not found

        Returns:
            Decrypted secret value or default

        Example:
            db_pwd = secrets.get_secret("db_password")
            api_key = secrets.get_secret("api_key", default="fallback_key")
        """
        try:
            key = self._make_key(name)
            encrypted_value = self.cache.client.get(key)

            if encrypted_value is None:
                if default is not None:
                    logger.debug(f"Secret '{name}' not found, using default")
                return default

            decrypted_value = self._decrypt(encrypted_value)
            return decrypted_value

        except Exception as e:
            logger.error(f"Failed to retrieve secret '{name}': {e}")
            return default

    def delete_secret(self, name: str) -> bool:
        """
        Delete a secret

        Args:
            name: Secret name

        Returns:
            True if deleted, False otherwise
        """
        try:
            key = self._make_key(name)
            deleted = self.cache.client.delete(key)

            if deleted:
                logger.info(f"Secret '{name}' deleted")
            return bool(deleted)

        except Exception as e:
            logger.error(f"Failed to delete secret '{name}': {e}")
            return False

    def exists(self, name: str) -> bool:
        """
        Check if secret exists

        Args:
            name: Secret name

        Returns:
            True if exists, False otherwise
        """
        try:
            key = self._make_key(name)
            return bool(self.cache.client.exists(key))

        except Exception as e:
            logger.error(f"Failed to check secret existence '{name}': {e}")
            return False

    def list_secrets(self) -> list[str]:
        """
        List all secret names

        Returns:
            List of secret names (without prefix)
        """
        try:
            pattern = self._make_key("*")
            keys = self.cache.client.keys(pattern)

            # Remove prefix from keys
            prefix_len = len(self.key_prefix)
            return [key[prefix_len:] for key in keys]

        except Exception as e:
            logger.error(f"Failed to list secrets: {e}")
            return []

    def rotate_secret(self, name: str, new_value: str) -> bool:
        """
        Rotate a secret (delete old, set new)

        Args:
            name: Secret name
            new_value: New secret value

        Returns:
            True if successful, False otherwise
        """
        try:
            old_value = self.get_secret(name)

            if old_value is None:
                logger.warning(f"Secret '{name}' does not exist, creating new")

            success = self.set_secret(name, new_value)

            if success and old_value:
                logger.info(f"Secret '{name}' rotated successfully")

            return success

        except Exception as e:
            logger.error(f"Failed to rotate secret '{name}': {e}")
            return False


# Global secrets manager instance
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """
    Get global secrets manager instance

    Returns:
        SecretsManager instance
    """
    global _secrets_manager

    if _secrets_manager is None:
        _secrets_manager = SecretsManager()

    return _secrets_manager


def get_secret(name: str, default: Optional[str] = None, fallback_env: bool = True) -> Optional[str]:
    """
    Helper function to get secret with fallback to environment variable

    Args:
        name: Secret name
        default: Default value if not found
        fallback_env: If True, try environment variable if Redis secret not found

    Returns:
        Secret value

    Example:
        # Try Redis first, then env var, then default
        db_pwd = get_secret("POSTGRES_PASSWORD", default="postgres")

        # Redis only
        api_key = get_secret("API_KEY", fallback_env=False)
    """
    manager = get_secrets_manager()

    # Try Redis first
    value = manager.get_secret(name)

    if value is not None:
        return value

    # Fallback to environment variable
    if fallback_env:
        env_value = os.getenv(name)
        if env_value is not None:
            logger.debug(f"Using environment variable for '{name}'")
            return env_value

    # Return default
    return default


# Example usage for common secrets
def get_database_credentials() -> dict:
    """
    Get database credentials from secrets or env

    Returns:
        Dict with database connection parameters
    """
    return {
        "username": get_secret("POSTGRES_USERNAME", default="postgres"),
        "password": get_secret("POSTGRES_PASSWORD", default=""),
        "host": get_secret("POSTGRES_HOST", default="localhost"),
        "port": get_secret("POSTGRES_PORT", default="5432"),
        "database": get_secret("POSTGRES_DATABASE", default="stock_picker"),
    }


def get_logto_config() -> dict:
    """
    Get Logto OIDC configuration from secrets or env

    Returns:
        Dict with Logto configuration
    """
    return {
        "endpoint": get_secret("LOGTO_ENDPOINT", default=""),
        "app_id": get_secret("LOGTO_APP_ID", default=""),
        "app_secret": get_secret("LOGTO_APP_SECRET", default=""),
        "resource": get_secret("LOGTO_RESOURCE", default=""),
    }


def get_redis_config() -> dict:
    """
    Get Redis configuration from secrets or env

    Returns:
        Dict with Redis configuration
    """
    return {
        "host": get_secret("REDIS_HOST", default="localhost"),
        "port": get_secret("REDIS_PORT", default="6379"),
        "db": get_secret("REDIS_DB", default="0"),
        "password": get_secret("REDIS_PASSWORD", default=None),
    }
