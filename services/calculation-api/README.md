# Calculation API

**Fast, accurate financial calculations service** - Technical indicators, risk analytics, and portfolio optimization using Python + pandas/numpy.

## Overview

The Calculation API provides REST endpoints for:

- **Technical Indicators**: MA, EMA, RSI, MACD, Bollinger Bands, Stochastic, ATR
- **Risk Analytics**: Sharpe, Sortino, Calmar, Max Drawdown, VaR, CVaR, Monte Carlo
- **Portfolio Optimization**: Mean-variance optimization, efficient frontier

Built with **FastAPI**, **pandas**, **numpy**, and **scipy** for high-performance calculations.

## Features

✅ **Comprehensive indicators** - 7 technical indicators covering trend, momentum, and volatility
✅ **Risk analytics** - 7 risk metrics including VaR and Monte Carlo simulation
✅ **Portfolio optimization** - Mean-variance optimization and efficient frontier
✅ **Fast & accurate** - Vectorized calculations with pandas/numpy
✅ **Type-safe** - Pydantic models and OpenAPI spec generation
✅ **Well-tested** - 30+ unit tests covering all endpoints
✅ **Production-ready** - Docker support, health checks, logging

## Quick Start

### Using Docker (Recommended)

```bash
# Build image
docker build -t calculation-api .

# Run container
docker run -p 8005:8005 calculation-api

# Visit API docs
open http://localhost:8005/docs
```

### Using uv (Local Development)

```bash
# Install dependencies
uv sync --all-extras

# Run service
uv run uvicorn app.main:app --reload --port 8005

# Or activate venv and run
source .venv/bin/activate
uvicorn app.main:app --reload --port 8005
```

### Direct Python

```bash
# Install dependencies
pip install -e .

# Run service
python -m app.main

# Or use uvicorn directly
uvicorn app.main:app --reload --port 8005
```

## API Endpoints

### Technical Indicators (`/api/v1/indicators`)

| Endpoint | Description | Parameters |
|----------|-------------|------------|
| `POST /ma` | Simple Moving Average | prices, period |
| `POST /ema` | Exponential Moving Average | prices, period |
| `POST /rsi` | Relative Strength Index | prices, period |
| `POST /macd` | MACD | prices, fast/slow/signal periods |
| `POST /bollinger` | Bollinger Bands | prices, period, std_dev |
| `POST /stochastic` | Stochastic Oscillator | high, low, close, k/d periods |
| `POST /atr` | Average True Range | high, low, close, period |

### Risk Analytics (`/api/v1/risk`)

| Endpoint | Description | Parameters |
|----------|-------------|------------|
| `POST /sharpe` | Sharpe Ratio | returns, risk_free_rate |
| `POST /sortino` | Sortino Ratio | returns, risk_free_rate, target_return |
| `POST /max-drawdown` | Maximum Drawdown | equity_curve |
| `POST /calmar` | Calmar Ratio | returns, equity_curve |
| `POST /var` | Value at Risk | returns, confidence, method |
| `POST /cvar` | Conditional VaR | returns, confidence, method |
| `POST /monte-carlo` | Monte Carlo Simulation | initial_value, mean_return, std_return, periods, simulations |

### Portfolio Optimization (`/api/v1/optimization`)

| Endpoint | Description | Parameters |
|----------|-------------|------------|
| `POST /metrics` | Portfolio Metrics | weights, returns |
| `POST /optimize` | Optimize Portfolio | returns, method, target_return, allow_short |
| `POST /efficient-frontier` | Efficient Frontier | returns, num_points, allow_short |

## Usage Examples

### Calculate RSI

```bash
curl -X POST "http://localhost:8005/api/v1/indicators/rsi" \
  -H "Content-Type: application/json" \
  -d '{
    "prices": [44, 44.34, 44.09, 43.61, 44.33, 44.83, 45.10, 45.42, 45.84, 46.08, 45.89, 46.03, 45.61, 46.28, 46.28],
    "period": 14
  }'
```

**Response:**
```json
{
  "values": [null, null, ..., 69.45, 66.25]
}
```

### Calculate Sharpe Ratio

```bash
curl -X POST "http://localhost:8005/api/v1/risk/sharpe" \
  -H "Content-Type: application/json" \
  -d '{
    "returns": [0.01, 0.02, -0.01, 0.03, 0.02],
    "risk_free_rate": 0.02
  }'
```

**Response:**
```json
{
  "sharpe_ratio": 1.234
}
```

### Optimize Portfolio (Max Sharpe)

```bash
curl -X POST "http://localhost:8005/api/v1/optimization/optimize" \
  -H "Content-Type: application/json" \
  -d '{
    "returns": [
      [0.01, 0.02, -0.01, 0.03],
      [0.02, -0.01, 0.03, 0.01],
      [-0.01, 0.03, 0.02, 0.01]
    ],
    "method": "max_sharpe",
    "allow_short": false
  }'
```

**Response:**
```json
{
  "weights": [0.45, 0.35, 0.20],
  "metrics": {
    "return": 0.12,
    "volatility": 0.08,
    "sharpe_ratio": 1.5
  }
}
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=html

# Run specific test file
uv run pytest tests/test_api.py -v

# Run specific test
uv run pytest tests/test_api.py::TestIndicatorEndpoints::test_rsi -v
```

## Architecture

```
calculation-api/
├── app/
│   ├── api/                    # API endpoint routers
│   │   ├── indicators.py       # Technical indicators endpoints
│   │   ├── risk.py             # Risk analytics endpoints
│   │   └── optimization.py     # Portfolio optimization endpoints
│   ├── core/                   # Core calculation logic
│   │   ├── indicators/         # Technical indicators (pandas/numpy)
│   │   │   ├── moving_average.py
│   │   │   ├── momentum.py
│   │   │   └── volatility.py
│   │   ├── risk/               # Risk analytics
│   │   │   ├── metrics.py
│   │   │   ├── var.py
│   │   │   └── monte_carlo.py
│   │   └── optimization/       # Portfolio optimization
│   │       └── portfolio.py
│   └── main.py                 # FastAPI application
├── tests/
│   └── test_api.py             # API endpoint tests (30+ tests)
├── Dockerfile
├── pyproject.toml
└── README.md
```

## Technology Stack

- **FastAPI** - High-performance async web framework
- **pandas** - Data manipulation and vectorized calculations
- **numpy** - Numerical computing
- **scipy** - Statistical functions and optimization
- **pydantic** - Data validation and OpenAPI generation

## Performance

- **Vectorized operations** with pandas/numpy for speed
- **O(n) time complexity** for most indicators
- **Handles large datasets** - tested with 10k+ data points
- **Monte Carlo** - 10,000 simulations in <1 second

## Configuration

Environment variables (optional):

```env
# Redis (for future caching)
REDIS_HOST=localhost
REDIS_PORT=6379

# Logging
LOG_LEVEL=INFO
```

## API Documentation

Visit these URLs when the service is running:

- **Swagger UI**: http://localhost:8005/docs
- **ReDoc**: http://localhost:8005/redoc
- **OpenAPI JSON**: http://localhost:8005/openapi.json

## Health Checks

```bash
# Health check endpoint
curl http://localhost:8005/health

# Response
{
  "status": "healthy",
  "version": "1.0.0",
  "service": "calculation-api"
}
```

## Future Enhancements

- [ ] Redis caching for computed results
- [ ] Batch calculation endpoints
- [ ] gRPC support for internal communication
- [ ] More indicators (ADX, CCI, Williams %R, etc.)
- [ ] Advanced risk models (CVaR with GARCH, etc.)
- [ ] Rust optimization for hot paths (Phase 11-12)

## Contributing

1. Write tests for new features
2. Follow existing code style (ruff formatting)
3. Update this README for new endpoints
4. Ensure all tests pass: `uv run pytest`

## License

MIT

---

**Version:** 1.0.0
**Port:** 8005
**Status:** Production-ready ✅
