# Frontend API Specification

Complete API specification for building frontend applications that interface with the Stock Picker Backtesting API.

## Table of Contents

- [WebSocket API](#websocket-api)
- [REST API Data Formats](#rest-api-data-formats)
- [Chart Data Structures](#chart-data-structures)
- [Authentication](#authentication)
- [Error Handling](#error-handling)

---

## WebSocket API

### Connection Endpoints

#### 1. Backtest Stream
**Endpoint**: `ws://localhost:8000/api/v1/ws/backtest/{run_id}?client_id={optional_client_id}`

**Description**: Subscribe to real-time updates for a specific backtest run.

**Connection Example**:
```javascript
const ws = new WebSocket(
  `ws://localhost:8000/api/v1/ws/backtest/${runId}?client_id=${clientId}`
);

ws.onopen = () => {
  console.log('Connected to backtest stream');
};

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  handleBacktestEvent(message);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('Disconnected from backtest stream');
};
```

#### 2. All Backtests Stream
**Endpoint**: `ws://localhost:8000/api/v1/ws/backtests?client_id={optional_client_id}`

**Description**: Subscribe to system-wide backtest events (started/completed/failed).

#### 3. Connection Stats Stream
**Endpoint**: `ws://localhost:8000/api/v1/ws/stats?client_id={optional_client_id}`

**Description**: Monitor WebSocket connection statistics (for debugging/monitoring).

---

### Message Types

#### Connection Acknowledged
```json
{
  "type": "connected",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "client_id": "client-123",
  "message": "Connected to backtest stream: 660e8400..."
}
```

#### Progress Update
```json
{
  "type": "progress",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "timestamp": "2025-11-17T10:30:45.123Z",
  "data": {
    "current": 125,
    "total": 250,
    "percentage": 50.0,
    "message": "Processing 2025-02-15"
  }
}
```

#### Trade Event
```json
{
  "type": "trade",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "timestamp": "2025-11-17T10:31:00.456Z",
  "data": {
    "trade_type": "entry",  // or "exit"
    "stock_code": "600000",
    "stock_name": "浦发银行",
    "price": 10.50,
    "shares": 10000,
    "date": "2025-01-15"
  }
}
```

**Exit Trade Example**:
```json
{
  "type": "trade",
  "run_id": "...",
  "timestamp": "2025-11-17T10:32:00.789Z",
  "data": {
    "trade_type": "exit",
    "stock_code": "600000",
    "exit_reason": "take_profit",
    "exit_price": 11.55,
    "entry_price": 10.50,
    "net_pnl": 9500.00,
    "return_pct": 10.0,
    "holding_days": 5
  }
}
```

#### Portfolio Snapshot
```json
{
  "type": "snapshot",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "timestamp": "2025-11-17T10:31:30.234Z",
  "data": {
    "date": "2025-01-15",
    "cash": 850000.00,
    "holdings_value": 200000.00,
    "total_value": 1050000.00,
    "daily_return": 0.5,
    "cumulative_return": 5.0,
    "open_positions": 2
  }
}
```

#### Status Change
```json
{
  "type": "status",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "timestamp": "2025-11-17T10:35:00.567Z",
  "data": {
    "status": "completed",  // or "running", "failed"
    "message": "Backtest completed successfully",
    "total_trades": 45,
    "total_return": 25.5,
    "sharpe_ratio": 1.85
  }
}
```

#### Error Event
```json
{
  "type": "error",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "timestamp": "2025-11-17T10:32:15.890Z",
  "data": {
    "error": "Data unavailable for date 2025-01-20",
    "details": {
      "date": "2025-01-20",
      "missing_stocks": ["600001", "600002"]
    }
  }
}
```

---

### Client Commands

Send commands to the server:

#### Ping/Pong (Keepalive)
```javascript
ws.send(JSON.stringify({
  command: "ping",
  timestamp: new Date().toISOString()
}));

// Server responds with:
// { "type": "pong", "timestamp": "..." }
```

#### Unsubscribe
```javascript
ws.send(JSON.stringify({
  command: "unsubscribe"
}));

// Server responds with:
// { "type": "unsubscribed", "run_id": "..." }
// Then closes connection
```

---

## REST API Data Formats

### Backtest Run Summary
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440001",
  "strategy_id": "770e8400-e29b-41d4-a716-446655440002",
  "strategy_name": "Tail Scraper V1",
  "status": "completed",
  "start_date": "2025-01-01",
  "end_date": "2025-03-31",
  "initial_capital": 1000000.00,
  "total_return": 25.50,
  "sharpe_ratio": 1.85,
  "max_drawdown": -8.50,
  "total_trades": 45,
  "created_at": "2025-11-17T09:00:00Z",
  "completed_at": "2025-11-17T09:05:30Z"
}
```

### Trade Record
```json
{
  "id": 12345,
  "backtest_run_id": "660e8400-e29b-41d4-a716-446655440001",
  "stock_code": "600000",
  "stock_name": "浦发银行",
  "entry_date": "2025-01-10",
  "entry_price": 10.50,
  "exit_date": "2025-01-15",
  "exit_price": 11.55,
  "exit_reason": "take_profit",
  "shares": 10000,
  "gross_pnl": 10500.00,
  "commission": 315.00,
  "slippage": 210.00,
  "net_pnl": 9975.00,
  "return_pct": 10.0,
  "holding_days": 5
}
```

### Portfolio Snapshot
```json
{
  "id": 67890,
  "backtest_run_id": "660e8400-e29b-41d4-a716-446655440001",
  "snapshot_date": "2025-01-15",
  "cash": 850000.00,
  "holdings_value": 200000.00,
  "total_value": 1050000.00,
  "daily_return": 0.5,
  "cumulative_return": 5.0,
  "drawdown": -2.5,
  "open_positions": 2
}
```

---

## Chart Data Structures

### Equity Curve
**Endpoint**: `GET /api/v1/backtest/runs/{run_id}/charts/equity-curve`

**Response**:
```json
{
  "chart_type": "equity_curve",
  "data": {
    "dates": [
      "2025-01-04",
      "2025-01-05",
      "2025-01-06",
      "..."
    ],
    "portfolio_value": [
      1000000.00,
      1002000.00,
      1005000.00,
      "..."
    ],
    "drawdown": [
      0.0,
      0.0,
      -1.2,
      "..."
    ],
    "benchmark": [
      1000000.00,
      1001000.00,
      1003000.00,
      "..."
    ]
  },
  "config": {
    "title": "Portfolio Equity Curve",
    "y_axis_label": "Portfolio Value (¥)",
    "show_drawdown": true
  }
}
```

**Frontend Rendering** (React + Recharts):
```jsx
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

function EquityCurveChart({ data }) {
  const chartData = data.dates.map((date, i) => ({
    date,
    value: data.portfolio_value[i],
    drawdown: data.drawdown[i],
    benchmark: data.benchmark?.[i]
  }));

  return (
    <LineChart width={800} height={400} data={chartData}>
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis dataKey="date" />
      <YAxis />
      <Tooltip />
      <Legend />
      <Line type="monotone" dataKey="value" stroke="#8884d8" name="Portfolio" />
      {data.benchmark && (
        <Line type="monotone" dataKey="benchmark" stroke="#82ca9d" name="Benchmark" />
      )}
    </LineChart>
  );
}
```

### Monthly Returns Heatmap
**Endpoint**: `GET /api/v1/backtest/runs/{run_id}/charts/monthly-returns`

**Response**:
```json
{
  "chart_type": "monthly_returns",
  "data": {
    "2025": [1.2, 2.5, -0.8, 3.1, 1.7, -1.2, 2.8, 0.5, -2.1, 1.9, 3.2, 0.8]
  },
  "config": {
    "title": "Monthly Returns (%)",
    "colormap": "RdYlGn"
  }
}
```

### Trade Distribution
**Endpoint**: `GET /api/v1/backtest/runs/{run_id}/charts/trade-distribution`

**Response**:
```json
{
  "chart_type": "trade_distribution",
  "data": {
    "bins": [-10.0, -8.0, -6.0, -4.0, -2.0, 0.0, 2.0, 4.0, 6.0, 8.0, 10.0],
    "frequencies": [2, 5, 8, 12, 15, 20, 18, 10, 6, 4]
  },
  "statistics": {
    "mean": 1.5,
    "median": 1.2,
    "std": 3.5,
    "min": -9.5,
    "max": 12.3
  },
  "config": {
    "title": "Distribution of Trade Returns",
    "x_axis_label": "Return (%)",
    "y_axis_label": "Frequency"
  }
}
```

---

## Authentication

Currently, the API does not require authentication. For production deployments:

1. **API Keys**: Add `X-API-Key` header
2. **JWT Tokens**: OAuth 2.0 / JWT bearer tokens
3. **CORS**: Configure `allow_origins` in `main.py`

**Example with API Key**:
```javascript
fetch('http://localhost:8000/api/v1/backtest/runs', {
  headers: {
    'X-API-Key': 'your-api-key-here'
  }
})
```

---

## Error Handling

### HTTP Error Response Format
```json
{
  "detail": "Backtest run not found: 660e8400-e29b-41d4-a716-446655440001"
}
```

### HTTP Status Codes
- `200 OK` - Successful request
- `201 Created` - Resource created
- `202 Accepted` - Async request accepted (backtest queued)
- `400 Bad Request` - Invalid input
- `404 Not Found` - Resource not found
- `422 Unprocessable Entity` - Validation failed
- `500 Internal Server Error` - Server error

### WebSocket Error Handling
```javascript
ws.onerror = (error) => {
  console.error('WebSocket error:', error);
  // Attempt reconnection with exponential backoff
  setTimeout(() => reconnect(), backoffDelay);
};

ws.onclose = (event) => {
  if (event.wasClean) {
    console.log(`Closed cleanly, code=${event.code}, reason=${event.reason}`);
  } else {
    console.error('Connection died, attempting reconnect...');
    setTimeout(() => reconnect(), 1000);
  }
};
```

---

## Rate Limiting

No rate limiting currently implemented. For production:
- Recommend: 100 requests/minute per IP
- WebSocket: 10 concurrent connections per IP
- Implement using `slowapi` or nginx

---

## CORS Configuration

**Current**: All origins allowed (`allow_origins=["*"]`)

**Production**: Specify allowed origins in `app/api/main.py`:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## Example: Complete Backtest Workflow

```javascript
// 1. Create and run backtest
const response = await fetch('http://localhost:8000/api/v1/backtest/run', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    strategy_id: '770e8400-e29b-41d4-a716-446655440002',
    start_date: '2025-01-01',
    end_date: '2025-03-31',
    initial_capital: 1000000
  })
});

const { id: runId } = await response.json();

// 2. Subscribe to WebSocket for real-time updates
const ws = new WebSocket(`ws://localhost:8000/api/v1/ws/backtest/${runId}`);

ws.onmessage = (event) => {
  const message = JSON.parse(event.data);

  switch (message.type) {
    case 'progress':
      updateProgressBar(message.data.percentage);
      break;
    case 'trade':
      addTradeToLog(message.data);
      break;
    case 'snapshot':
      updateEquityCurve(message.data);
      break;
    case 'status':
      if (message.data.status === 'completed') {
        fetchFinalResults(runId);
      }
      break;
  }
};

// 3. Fetch final results
async function fetchFinalResults(runId) {
  const [run, trades, equity] = await Promise.all([
    fetch(`/api/v1/backtest/runs/${runId}`).then(r => r.json()),
    fetch(`/api/v1/backtest/runs/${runId}/trades`).then(r => r.json()),
    fetch(`/api/v1/backtest/runs/${runId}/charts/equity-curve`).then(r => r.json())
  ]);

  displayResults({ run, trades, equity });
}
```

---

## Next Steps

See [FRONTEND_INTEGRATION_GUIDE.md](./FRONTEND_INTEGRATION_GUIDE.md) for:
- Complete React application setup
- Component examples
- State management patterns
- Real-time chart updates
- Playback controller implementation
