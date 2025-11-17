

# Stock Picker REST API Documentation

## Overview

The Stock Picker REST API provides programmatic access to the backtesting framework. You can run backtests, manage strategies, and retrieve results through standard HTTP requests.

## Getting Started

### Starting the API Server

```bash
# Development mode with auto-reload
uvicorn app.api.main:app --reload

# Production mode
uvicorn app.api.main:app --host 0.0.0.0 --port 8000

# Using Python directly
python -m app.api.main
```

### API Documentation

Once the server is running, access the interactive documentation:

- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **OpenAPI JSON**: http://localhost:8000/api/openapi.json

### Base URL

```
http://localhost:8000/api/v1
```

## Authentication

**Current Version**: No authentication required

**Future**: JWT-based authentication will be added in a later phase.

## Endpoints

### Health Check

**GET** `/api/health`

Check API health and database connection.

```bash
curl http://localhost:8000/api/health
```

Response:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "database": "localhost:5432/stock_picker"
}
```

---

## Backtest Endpoints

### Create Backtest Run

**POST** `/api/v1/backtest/run`

Queue a new backtest for execution. The backtest runs asynchronously in the background.

**Request Body:**

```json
{
  "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
  "start_date": "2025-01-01",
  "end_date": "2025-03-31",
  "initial_capital": 1000000.0,
  "commission_rate": 0.0003,
  "slippage_rate": 0.001,
  "max_positions": 20
}
```

**Alternative**: Provide strategy inline:

```json
{
  "strategy_file_content": "strategy:\n  name: My Strategy\n  version: 1.0.0\n  ...",
  "start_date": "2025-01-01",
  "end_date": "2025-03-31"
}
```

**Response (202 Accepted):**

```json
{
  "id": "456e7890-e12b-34d5-a678-890123456789",
  "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
  "start_date": "2025-01-01",
  "end_date": "2025-03-31",
  "initial_capital": 1000000.0,
  "status": "pending",
  "created_at": "2025-11-17T10:30:00Z",
  "total_return": null,
  "sharpe_ratio": null,
  "max_drawdown": null,
  "total_trades": null
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/v1/backtest/run \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
    "start_date": "2025-01-01",
    "end_date": "2025-03-31",
    "initial_capital": 1000000.0,
    "max_positions": 20
  }'
```

---

### List Backtest Runs

**GET** `/api/v1/backtest/runs`

Retrieve paginated list of all backtest runs.

**Query Parameters:**

- `page` (int, default: 1): Page number
- `page_size` (int, default: 20): Items per page (max: 100)
- `status_filter` (string, optional): Filter by status (pending, running, completed, failed)

**Response (200 OK):**

```json
{
  "items": [
    {
      "id": "456e7890-e12b-34d5-a678-890123456789",
      "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
      "start_date": "2025-01-01",
      "end_date": "2025-03-31",
      "initial_capital": 1000000.0,
      "total_return": 25.67,
      "sharpe_ratio": 1.85,
      "max_drawdown": -8.45,
      "win_rate": 0.6533,
      "total_trades": 150,
      "status": "completed",
      "created_at": "2025-11-17T10:30:00Z",
      "completed_at": "2025-11-17T10:35:00Z"
    }
  ],
  "total": 45,
  "page": 1,
  "page_size": 20,
  "total_pages": 3
}
```

**Example:**

```bash
# Get first page
curl http://localhost:8000/api/v1/backtest/runs

# Filter by status
curl "http://localhost:8000/api/v1/backtest/runs?status_filter=completed&page=1&page_size=10"
```

---

### Get Backtest Run Details

**GET** `/api/v1/backtest/runs/{run_id}`

Get detailed information about a specific backtest run.

**Response (200 OK):**

```json
{
  "id": "456e7890-e12b-34d5-a678-890123456789",
  "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
  "start_date": "2025-01-01",
  "end_date": "2025-03-31",
  "initial_capital": 1000000.0,
  "commission_rate": 0.0003,
  "slippage_rate": 0.001,
  "total_return": 25.67,
  "sharpe_ratio": 1.85,
  "max_drawdown": -8.45,
  "win_rate": 0.6533,
  "profit_factor": 2.15,
  "total_trades": 150,
  "status": "completed",
  "created_at": "2025-11-17T10:30:00Z",
  "completed_at": "2025-11-17T10:35:00Z",
  "error_message": null
}
```

**Example:**

```bash
curl http://localhost:8000/api/v1/backtest/runs/456e7890-e12b-34d5-a678-890123456789
```

---

### Get Backtest Trades

**GET** `/api/v1/backtest/runs/{run_id}/trades`

Retrieve all trades from a backtest run.

**Query Parameters:**

- `page` (int, default: 1): Page number
- `page_size` (int, default: 50): Items per page (max: 500)

**Response (200 OK):**

```json
{
  "items": [
    {
      "id": 1,
      "backtest_run_id": "456e7890-e12b-34d5-a678-890123456789",
      "stock_code": "600000",
      "stock_name": "浦发银行",
      "entry_date": "2025-01-15",
      "entry_price": 10.50,
      "exit_date": "2025-01-25",
      "exit_price": 11.20,
      "exit_reason": "take_profit",
      "shares": 9500,
      "position_value": 99750.00,
      "gross_pnl": 6650.00,
      "commission": 59.85,
      "slippage": 199.50,
      "net_pnl": 6390.65,
      "return_pct": 6.41,
      "holding_days": 10,
      "collection_name": "银行"
    }
  ],
  "total": 150,
  "page": 1,
  "page_size": 50,
  "total_pages": 3
}
```

**Example:**

```bash
curl http://localhost:8000/api/v1/backtest/runs/456e7890-e12b-34d5-a678-890123456789/trades
```

---

### Get Equity Curve

**GET** `/api/v1/backtest/runs/{run_id}/equity-curve`

Retrieve daily portfolio snapshots (equity curve) for visualization.

**Response (200 OK):**

```json
[
  {
    "id": 1,
    "backtest_run_id": "456e7890-e12b-34d5-a678-890123456789",
    "snapshot_date": "2025-01-01",
    "cash": 1000000.00,
    "holdings_value": 0.00,
    "total_value": 1000000.00,
    "daily_return": 0.00,
    "cumulative_return": 0.00,
    "drawdown": 0.00,
    "open_positions": 0,
    "total_positions_closed": 0
  },
  {
    "id": 2,
    "backtest_run_id": "456e7890-e12b-34d5-a678-890123456789",
    "snapshot_date": "2025-01-02",
    "cash": 950000.00,
    "holdings_value": 52500.00,
    "total_value": 1002500.00,
    "daily_return": 0.25,
    "cumulative_return": 0.25,
    "drawdown": 0.00,
    "open_positions": 5,
    "total_positions_closed": 0
  }
]
```

**Example:**

```bash
curl http://localhost:8000/api/v1/backtest/runs/456e7890-e12b-34d5-a678-890123456789/equity-curve
```

---

### Delete Backtest Run

**DELETE** `/api/v1/backtest/runs/{run_id}`

Delete a backtest run and all associated data (trades, snapshots).

**Response (204 No Content)**

**Example:**

```bash
curl -X DELETE http://localhost:8000/api/v1/backtest/runs/456e7890-e12b-34d5-a678-890123456789
```

---

## Strategy Endpoints

### Create Strategy

**POST** `/api/v1/strategies`

Create a new strategy from JSON definition.

**Request Body:**

```json
{
  "name": "My Momentum Strategy",
  "version": "1.0.0",
  "author": "trader@example.com",
  "description": "Aggressive momentum with tight stops",
  "definition": {
    "parameters": {
      "take_profit_pct": 15.0,
      "stop_loss_pct": -5.0
    },
    "entry_conditions": {
      "operator": "AND",
      "conditions": [
        {
          "type": "indicator",
          "name": "quantity_relative_ratio",
          "comparison": ">=",
          "value": 1.5
        }
      ]
    },
    "exit_conditions": {
      "operator": "OR",
      "conditions": [
        {
          "type": "take_profit",
          "method": "percentage",
          "value": 15.0
        }
      ]
    }
  }
}
```

**Response (201 Created):**

```json
{
  "id": "789e0123-e45b-67d8-a901-234567890123",
  "name": "My Momentum Strategy",
  "version": "1.0.0",
  "author": "trader@example.com",
  "description": "Aggressive momentum with tight stops",
  "created_at": "2025-11-17T10:40:00Z",
  "updated_at": "2025-11-17T10:40:00Z",
  "is_active": true,
  "definition": {...}
}
```

**Example:**

```bash
curl -X POST http://localhost:8000/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d @strategy.json
```

---

### Upload Strategy File

**POST** `/api/v1/strategies/upload`

Upload a strategy from YAML or JSON file.

**Request (multipart/form-data):**

- `file`: YAML or JSON file

**Response (201 Created):** Same as Create Strategy

**Example:**

```bash
curl -X POST http://localhost:8000/api/v1/strategies/upload \
  -F "file=@strategies/tail_scraper.yaml"
```

---

### List Strategies

**GET** `/api/v1/strategies`

List all strategies with pagination.

**Query Parameters:**

- `page` (int, default: 1): Page number
- `page_size` (int, default: 20): Items per page (max: 100)
- `active_only` (bool, default: true): Only return active strategies
- `name` (string, optional): Filter by name (partial match)

**Response (200 OK):**

```json
{
  "items": [
    {
      "id": "789e0123-e45b-67d8-a901-234567890123",
      "name": "Tail Scraper",
      "version": "2.0.0",
      "author": "system",
      "description": "Original tail scraper strategy",
      "created_at": "2025-11-17T09:00:00Z",
      "is_active": true
    }
  ],
  "total": 5,
  "page": 1,
  "page_size": 20,
  "total_pages": 1
}
```

**Example:**

```bash
# List all active strategies
curl http://localhost:8000/api/v1/strategies

# Search by name
curl "http://localhost:8000/api/v1/strategies?name=momentum"

# Include inactive
curl "http://localhost:8000/api/v1/strategies?active_only=false"
```

---

### Get Strategy Details

**GET** `/api/v1/strategies/{strategy_id}`

Get complete strategy information including definition.

**Response (200 OK):**

```json
{
  "id": "789e0123-e45b-67d8-a901-234567890123",
  "name": "Tail Scraper",
  "version": "2.0.0",
  "author": "system",
  "description": "Original tail scraper strategy",
  "created_at": "2025-11-17T09:00:00Z",
  "updated_at": "2025-11-17T09:00:00Z",
  "is_active": true,
  "definition": {
    "parameters": {...},
    "entry_conditions": {...},
    "exit_conditions": {...}
  }
}
```

**Example:**

```bash
curl http://localhost:8000/api/v1/strategies/789e0123-e45b-67d8-a901-234567890123
```

---

### Deactivate Strategy

**PATCH** `/api/v1/strategies/{strategy_id}/deactivate`

Soft delete a strategy (remains in database but inactive).

**Response (200 OK):** Updated strategy

**Example:**

```bash
curl -X PATCH http://localhost:8000/api/v1/strategies/789e0123-e45b-67d8-a901-234567890123/deactivate
```

---

### Activate Strategy

**PATCH** `/api/v1/strategies/{strategy_id}/activate`

Reactivate a previously deactivated strategy.

**Response (200 OK):** Updated strategy

**Example:**

```bash
curl -X PATCH http://localhost:8000/api/v1/strategies/789e0123-e45b-67d8-a901-234567890123/activate
```

---

### Delete Strategy

**DELETE** `/api/v1/strategies/{strategy_id}`

Permanently delete a strategy. Fails if backtest runs exist.

**Response (204 No Content)**

**Example:**

```bash
curl -X DELETE http://localhost:8000/api/v1/strategies/789e0123-e45b-67d8-a901-234567890123
```

---

## Error Responses

All endpoints return consistent error responses:

```json
{
  "detail": "Error message"
}
```

Common HTTP status codes:

- `400 Bad Request`: Invalid input
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource conflict (e.g., cannot delete)
- `500 Internal Server Error`: Server error

---

## Python Client Example

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"

# Upload strategy
with open("strategies/tail_scraper.yaml", "rb") as f:
    response = requests.post(
        f"{BASE_URL}/strategies/upload",
        files={"file": f}
    )
    strategy = response.json()
    strategy_id = strategy["id"]

# Run backtest
backtest_request = {
    "strategy_id": strategy_id,
    "start_date": "2025-01-01",
    "end_date": "2025-03-31",
    "initial_capital": 1000000.0,
    "max_positions": 20
}

response = requests.post(
    f"{BASE_URL}/backtest/run",
    json=backtest_request
)
run = response.json()
run_id = run["id"]

# Poll for completion
import time
while True:
    response = requests.get(f"{BASE_URL}/backtest/runs/{run_id}")
    run = response.json()

    if run["status"] in ["completed", "failed"]:
        break

    print(f"Status: {run['status']}")
    time.sleep(5)

# Get results
print(f"Total Return: {run['total_return']}%")
print(f"Sharpe Ratio: {run['sharpe_ratio']}")
print(f"Max Drawdown: {run['max_drawdown']}%")
print(f"Total Trades: {run['total_trades']}")

# Get trades
response = requests.get(f"{BASE_URL}/backtest/runs/{run_id}/trades")
trades = response.json()
print(f"Found {trades['total']} trades")

# Get equity curve
response = requests.get(f"{BASE_URL}/backtest/runs/{run_id}/equity-curve")
equity_curve = response.json()
print(f"Equity curve has {len(equity_curve)} data points")
```

---

## Rate Limiting

**Current Version**: No rate limiting

**Future**: Rate limiting will be added based on API key tiers.

---

## Versioning

The API uses URL versioning (`/api/v1/...`). Breaking changes will result in a new version (`/api/v2/...`).

---

## Support

For issues and feature requests:
- GitHub: https://github.com/monkeyboiii/stock-picker/issues
- Documentation: See docs/backtest_framework_design.md

---

## See Also

- [Backtest Usage Guide](BACKTEST_USAGE.md)
- [Strategy DSL Guide](../strategies/README.md)
- [Main README](../README.md)
