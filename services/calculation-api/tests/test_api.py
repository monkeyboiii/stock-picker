"""API endpoint tests for Calculation API"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


class TestHealthEndpoints:
    """Test health check endpoints"""

    def test_root_endpoint(self):
        """Test the root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["version"] == "1.0.0"
        assert data["service"] == "calculation-api"

    def test_health_endpoint(self):
        """Test the health endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_openapi_schema(self):
        """Test that OpenAPI schema is accessible"""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        assert "openapi" in schema
        assert "info" in schema
        assert schema["info"]["title"] == "Calculation API"

    def test_docs_accessible(self):
        """Test that docs are accessible"""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_redoc_accessible(self):
        """Test that redoc is accessible"""
        response = client.get("/redoc")
        assert response.status_code == 200


class TestIndicatorEndpoints:
    """Test technical indicator endpoints"""

    def test_moving_average(self):
        """Test MA calculation"""
        response = client.post(
            "/api/v1/indicators/ma", json={"prices": [100, 102, 101, 103, 105], "period": 3}
        )
        assert response.status_code == 200
        data = response.json()
        assert "values" in data
        assert len(data["values"]) == 5

    def test_exponential_moving_average(self):
        """Test EMA calculation"""
        response = client.post(
            "/api/v1/indicators/ema", json={"prices": [100, 102, 101, 103, 105], "period": 3}
        )
        assert response.status_code == 200
        data = response.json()
        assert "values" in data

    def test_rsi(self):
        """Test RSI calculation"""
        prices = [44, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28]
        response = client.post("/api/v1/indicators/rsi", json={"prices": prices, "period": 14})
        assert response.status_code == 200
        data = response.json()
        assert "values" in data

    def test_macd(self):
        """Test MACD calculation"""
        prices = list(range(100, 150))
        response = client.post(
            "/api/v1/indicators/macd",
            json={"prices": prices, "fast_period": 12, "slow_period": 26, "signal_period": 9},
        )
        assert response.status_code == 200
        data = response.json()
        assert "macd" in data
        assert "signal" in data
        assert "histogram" in data

    def test_bollinger_bands(self):
        """Test Bollinger Bands calculation"""
        prices = list(range(100, 130))
        response = client.post(
            "/api/v1/indicators/bollinger", json={"prices": prices, "period": 20, "std_dev": 2.0}
        )
        assert response.status_code == 200
        data = response.json()
        assert "upper" in data
        assert "middle" in data
        assert "lower" in data

    def test_stochastic(self):
        """Test Stochastic Oscillator calculation"""
        high = [46, 47, 48, 49, 50]
        low = [44, 45, 46, 47, 48]
        close = [45, 46, 47, 48, 49]
        response = client.post(
            "/api/v1/indicators/stochastic",
            json={"high": high, "low": low, "close": close, "k_period": 3, "d_period": 2},
        )
        assert response.status_code == 200
        data = response.json()
        assert "k" in data
        assert "d" in data

    def test_atr(self):
        """Test ATR calculation"""
        high = [46, 47, 48, 49, 50]
        low = [44, 45, 46, 47, 48]
        close = [45, 46, 47, 48, 49]
        response = client.post(
            "/api/v1/indicators/atr",
            json={"high": high, "low": low, "close": close, "period": 3},
        )
        assert response.status_code == 200
        data = response.json()
        assert "values" in data


class TestRiskEndpoints:
    """Test risk analytics endpoints"""

    def test_sharpe_ratio(self):
        """Test Sharpe ratio calculation"""
        returns = [0.01, 0.02, -0.01, 0.03, 0.02]
        response = client.post(
            "/api/v1/risk/sharpe", json={"returns": returns, "risk_free_rate": 0.02}
        )
        assert response.status_code == 200
        data = response.json()
        assert "sharpe_ratio" in data

    def test_sortino_ratio(self):
        """Test Sortino ratio calculation"""
        returns = [0.01, 0.02, -0.01, 0.03, 0.02]
        response = client.post(
            "/api/v1/risk/sortino",
            json={"returns": returns, "risk_free_rate": 0.02, "target_return": 0.0},
        )
        assert response.status_code == 200
        data = response.json()
        assert "sortino_ratio" in data

    def test_max_drawdown(self):
        """Test max drawdown calculation"""
        equity_curve = [100, 110, 105, 115, 120, 110, 125]
        response = client.post("/api/v1/risk/max-drawdown", json={"equity_curve": equity_curve})
        assert response.status_code == 200
        data = response.json()
        assert "max_drawdown" in data

    def test_calmar_ratio(self):
        """Test Calmar ratio calculation"""
        returns = [0.01, 0.02, -0.01, 0.03, 0.02]
        equity_curve = [100, 101, 103, 102, 105, 107]
        response = client.post(
            "/api/v1/risk/calmar", json={"returns": returns, "equity_curve": equity_curve}
        )
        assert response.status_code == 200
        data = response.json()
        assert "calmar_ratio" in data

    def test_var(self):
        """Test VaR calculation"""
        returns = [0.01, 0.02, -0.01, 0.03, 0.02, -0.02, 0.01]
        response = client.post(
            "/api/v1/risk/var",
            json={"returns": returns, "confidence_level": 0.95, "method": "historical"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "var" in data

    def test_cvar(self):
        """Test CVaR calculation"""
        returns = [0.01, 0.02, -0.01, 0.03, 0.02, -0.02, 0.01]
        response = client.post(
            "/api/v1/risk/cvar",
            json={"returns": returns, "confidence_level": 0.95, "method": "historical"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "cvar" in data

    def test_monte_carlo(self):
        """Test Monte Carlo simulation"""
        response = client.post(
            "/api/v1/risk/monte-carlo",
            json={
                "initial_value": 10000,
                "mean_return": 0.001,
                "std_return": 0.02,
                "periods": 252,
                "simulations": 1000,
                "random_seed": 42,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "final_values" in data
        assert "percentiles" in data
        assert "mean_final_value" in data
        assert len(data["final_values"]) == 1000


class TestOptimizationEndpoints:
    """Test portfolio optimization endpoints"""

    def test_portfolio_metrics(self):
        """Test portfolio metrics calculation"""
        weights = [0.5, 0.5]
        returns = [[0.01, 0.02, -0.01], [0.02, -0.01, 0.03]]
        response = client.post(
            "/api/v1/optimization/metrics", json={"weights": weights, "returns": returns}
        )
        assert response.status_code == 200
        data = response.json()
        assert "return" in data
        assert "volatility" in data
        assert "sharpe_ratio" in data

    def test_portfolio_metrics_invalid_weights(self):
        """Test portfolio metrics with invalid weights"""
        weights = [0.6, 0.5]  # Sum to 1.1
        returns = [[0.01, 0.02], [0.02, -0.01]]
        response = client.post(
            "/api/v1/optimization/metrics", json={"weights": weights, "returns": returns}
        )
        assert response.status_code == 400

    def test_optimize_max_sharpe(self):
        """Test max Sharpe portfolio optimization"""
        returns = [
            [0.01, 0.02, -0.01, 0.03, 0.02],
            [0.02, -0.01, 0.03, 0.01, 0.02],
            [-0.01, 0.03, 0.02, 0.01, -0.01],
        ]
        response = client.post(
            "/api/v1/optimization/optimize",
            json={"returns": returns, "method": "max_sharpe", "allow_short": False},
        )
        assert response.status_code == 200
        data = response.json()
        assert "weights" in data
        assert "metrics" in data
        # Weights should sum to 1
        assert abs(sum(data["weights"]) - 1.0) < 1e-6

    def test_optimize_min_volatility(self):
        """Test min volatility portfolio optimization"""
        returns = [
            [0.01, 0.02, -0.01, 0.03, 0.02],
            [0.02, -0.01, 0.03, 0.01, 0.02],
        ]
        response = client.post(
            "/api/v1/optimization/optimize",
            json={"returns": returns, "method": "min_volatility", "allow_short": False},
        )
        assert response.status_code == 200
        data = response.json()
        assert "weights" in data

    def test_efficient_frontier(self):
        """Test efficient frontier calculation"""
        returns = [
            [0.01, 0.02, -0.01, 0.03, 0.02],
            [0.02, -0.01, 0.03, 0.01, 0.02],
            [-0.01, 0.03, 0.02, 0.01, -0.01],
        ]
        response = client.post(
            "/api/v1/optimization/efficient-frontier",
            json={"returns": returns, "num_points": 20, "allow_short": False},
        )
        assert response.status_code == 200
        data = response.json()
        assert "frontier" in data
        assert "max_sharpe_portfolio" in data
        assert len(data["frontier"]) > 0
