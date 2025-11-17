"""
Tests for database engine creation in app/db/engine.py
"""

import pytest
from sqlalchemy.engine import Engine

from app.db.engine import engine_from_env


class TestEngineFromEnv:
    """Tests for engine_from_env function."""

    def test_engine_from_env_creates_engine(self, temp_env_vars):
        """Test that engine_from_env creates a valid SQLAlchemy engine."""
        temp_env_vars(
            POSTGRES_USERNAME="test_user",
            POSTGRES_PASSWORD="test_pass",
            POSTGRES_HOST="localhost",
            POSTGRES_PORT="5432",
            POSTGRES_DATABASE="test_db"
        )

        engine = engine_from_env()
        assert isinstance(engine, Engine)

    def test_engine_uses_postgresql_driver(self, temp_env_vars):
        """Test that engine uses PostgreSQL driver."""
        temp_env_vars(
            POSTGRES_USERNAME="test_user",
            POSTGRES_PASSWORD="test_pass",
            POSTGRES_HOST="localhost",
            POSTGRES_PORT="5432",
            POSTGRES_DATABASE="test_db"
        )

        engine = engine_from_env()
        assert engine.dialect.name == 'postgresql'

    def test_engine_respects_custom_port(self, temp_env_vars):
        """Test that engine uses custom port from environment."""
        temp_env_vars(
            POSTGRES_USERNAME="test_user",
            POSTGRES_PASSWORD="test_pass",
            POSTGRES_HOST="localhost",
            POSTGRES_PORT="5433",  # Custom port
            POSTGRES_DATABASE="test_db"
        )

        engine = engine_from_env()
        assert '5433' in str(engine.url)

    def test_engine_default_values(self, temp_env_vars, monkeypatch):
        """Test that engine uses default values when env vars are not set."""
        # Clear all postgres env vars
        for var in ['POSTGRES_USERNAME', 'POSTGRES_PASSWORD', 'POSTGRES_HOST',
                    'POSTGRES_PORT', 'POSTGRES_DATABASE']:
            monkeypatch.delenv(var, raising=False)

        # Set minimal required env
        temp_env_vars(POSTGRES_DATABASE="test_db")

        engine = engine_from_env()

        # Should use defaults
        url_str = str(engine.url)
        assert 'postgres' in url_str  # Default username
        assert 'localhost' in url_str  # Default host

    def test_engine_echo_parameter(self, temp_env_vars):
        """Test that echo parameter is passed through."""
        temp_env_vars(
            POSTGRES_USERNAME="test_user",
            POSTGRES_PASSWORD="test_pass",
            POSTGRES_HOST="localhost",
            POSTGRES_PORT="5432",
            POSTGRES_DATABASE="test_db"
        )

        # Create engine with echo=True
        engine = engine_from_env(echo=True)
        assert engine.echo is True

        # Create engine with echo=False
        engine = engine_from_env(echo=False)
        assert engine.echo is False

    def test_engine_rejects_non_postgresql(self, temp_env_vars):
        """Test that non-PostgreSQL drivers are rejected."""
        temp_env_vars(
            DB_DRIVER="sqlite",
            POSTGRES_USERNAME="test_user",
            POSTGRES_PASSWORD="test_pass",
            POSTGRES_HOST="localhost",
            POSTGRES_PORT="5432",
            POSTGRES_DATABASE="test_db"
        )

        with pytest.raises(Exception, match="Only support postgresql"):
            engine_from_env()

    def test_engine_url_format(self, temp_env_vars):
        """Test that engine URL is properly formatted."""
        temp_env_vars(
            POSTGRES_USERNAME="myuser",
            POSTGRES_PASSWORD="mypass",
            POSTGRES_HOST="myhost",
            POSTGRES_PORT="5432",
            POSTGRES_DATABASE="mydb"
        )

        engine = engine_from_env()
        url_str = str(engine.url)

        # Check that URL contains expected components
        assert 'myuser' in url_str
        assert 'myhost' in url_str
        assert 'mydb' in url_str
        assert '5432' in url_str
