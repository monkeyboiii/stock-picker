# n8n Integration Guide - Stock Picker MCP Server

Complete guide for integrating the Stock Picker backtesting system with n8n workflow automation using the Model Context Protocol (MCP).

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [MCP Server Setup](#mcp-server-setup)
4. [n8n Workflow Examples](#n8n-workflow-examples)
5. [Available MCP Tools](#available-mcp-tools)
6. [Production Deployment](#production-deployment)
7. [Troubleshooting](#troubleshooting)

## Overview

The Stock Picker MCP server exposes backtesting functionality through HTTP Server-Sent Events (SSE), making it easy to integrate with n8n and other workflow automation tools.

### What You Can Do

- Create trading strategies from natural language descriptions
- Run backtests automatically on schedules
- Compare multiple strategies
- Generate performance reports
- Visualize results with chart data
- Integrate with other n8n nodes (Slack, Email, Webhooks, etc.)

### Architecture

```
n8n Workflow → HTTP Request (SSE) → MCP Server → Backtesting Engine → Database
                                  ↓
                            Tool Execution Results
```

## Prerequisites

### 1. Stock Picker MCP Server

The MCP server must be running:

```bash
# Start MCP server on port 8001
uvicorn app.mcp.server:app --host 0.0.0.0 --port 8001

# Or with Docker (see deployment section)
docker run -p 8001:8001 stock-picker-mcp
```

### 2. n8n Installation

Install n8n:

```bash
# Using npm
npm install -g n8n

# Using Docker
docker run -it --rm \
  --name n8n \
  -p 5678:5678 \
  -v ~/.n8n:/home/node/.n8n \
  n8nio/n8n
```

Access n8n at: http://localhost:5678

### 3. Database Setup

Ensure PostgreSQL is running with the stock_picker database:

```bash
# Check database connection
psql -U postgres -d stock_picker -c "SELECT version();"
```

## MCP Server Setup

### Server Endpoints

The MCP server exposes the following HTTP endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Server info and available tools |
| `/health` | GET | Health check |
| `/sse` | POST | MCP protocol SSE endpoint (main) |

### Testing the Server

```bash
# 1. Check server status
curl http://localhost:8001/

# 2. Health check
curl http://localhost:8001/health

# 3. List available tools
curl -X POST http://localhost:8001/sse \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/list",
    "id": 1
  }'
```

## n8n Workflow Examples

### Example 1: Create Strategy from Natural Language

**Workflow:** Trigger → HTTP Request → Set Variable → Send Notification

#### Step 1: Manual Trigger
- Node Type: **Manual Trigger**

#### Step 2: HTTP Request - Create Strategy
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Headers:
  ```json
  {
    "Content-Type": "application/json"
  }
  ```
- Body (JSON):
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "create_strategy",
      "arguments": {
        "description": "Buy stocks when they break above their 250-day moving average with high volume, hold for 30 days, take profit at 20% or stop loss at 10%",
        "name": "MA Breakout Strategy"
      }
    },
    "id": 1
  }
  ```

#### Step 3: Extract Strategy ID
- Node Type: **Set**
- Mode: `Manual Mapping`
- Set:
  - `strategy_id` = `{{ $json.result.strategy_id }}`
  - `strategy_name` = `{{ $json.result.name }}`

#### Step 4: Send Notification
- Node Type: **Slack** / **Email** / **Webhook**
- Message:
  ```
  ✅ Strategy Created!
  Name: {{ $node["Extract Strategy ID"].json["strategy_name"] }}
  ID: {{ $node["Extract Strategy ID"].json["strategy_id"] }}
  ```

### Example 2: Automated Backtest Scheduler

**Workflow:** Cron → HTTP Request (List Strategies) → Split In Batches → HTTP Request (Run Backtest) → HTTP Request (Get Results) → Email Report

#### Step 1: Cron Trigger
- Node Type: **Cron**
- Trigger Times: `0 2 * * 1` (Every Monday at 2 AM)

#### Step 2: List All Strategies
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Body:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "list_strategies",
      "arguments": {
        "limit": 100
      }
    },
    "id": 1
  }
  ```

#### Step 3: Split Strategies
- Node Type: **Split In Batches**
- Batch Size: `1`

#### Step 4: Run Backtest for Each Strategy
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Body:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "run_backtest",
      "arguments": {
        "strategy_id": "{{ $json.id }}",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "initial_capital": 100000
      }
    },
    "id": 1
  }
  ```

#### Step 5: Get Detailed Results
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Body:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "get_backtest_results",
      "arguments": {
        "backtest_id": "{{ $json.result.backtest_id }}"
      }
    },
    "id": 1
  }
  ```

#### Step 6: Email Weekly Report
- Node Type: **Email**
- Subject: `Weekly Backtest Report - {{ $now.format('YYYY-MM-DD') }}`
- Email Body:
  ```html
  <h2>Weekly Backtest Results</h2>

  <table border="1">
    <tr>
      <th>Strategy</th>
      <th>Total Return</th>
      <th>Sharpe Ratio</th>
      <th>Max Drawdown</th>
      <th>Win Rate</th>
    </tr>
    {{ $items.map(item => `
    <tr>
      <td>${item.json.strategy_name}</td>
      <td>${item.json.total_return}%</td>
      <td>${item.json.sharpe_ratio}</td>
      <td>${item.json.max_drawdown}%</td>
      <td>${item.json.win_rate}%</td>
    </tr>
    `).join('') }}
  </table>
  ```

### Example 3: Strategy Comparison Dashboard

**Workflow:** Webhook Trigger → HTTP Request (Compare) → HTTP Request (Get Charts) → Store in Database → Update Dashboard

#### Step 1: Webhook Trigger
- Node Type: **Webhook**
- Path: `/compare-strategies`
- Method: `POST`

#### Step 2: Compare Strategies
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Body:
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "compare_strategies",
      "arguments": {
        "backtest_ids": "{{ $json.body.backtest_ids }}",
        "metrics": ["total_return", "sharpe_ratio", "win_rate", "max_drawdown"]
      }
    },
    "id": 1
  }
  ```

#### Step 3: Get Equity Curve Charts
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `http://localhost:8001/sse`
- Body (loop through each backtest):
  ```json
  {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "get_chart_data",
      "arguments": {
        "backtest_id": "{{ $item }}",
        "chart_type": "equity_curve"
      }
    },
    "id": 1
  }
  ```

#### Step 4: Store Results
- Node Type: **MongoDB** / **PostgreSQL** / **MySQL**
- Operation: `Insert`
- Collection/Table: `comparison_results`

#### Step 5: Trigger Dashboard Refresh
- Node Type: **HTTP Request**
- Method: `POST`
- URL: `https://your-dashboard.com/api/refresh`

## Available MCP Tools

### 1. create_strategy

Create a new trading strategy from natural language.

**Input:**
```json
{
  "description": "string (required) - Natural language strategy description",
  "name": "string (optional) - Strategy name"
}
```

**Output:**
```json
{
  "strategy_id": "uuid",
  "name": "string",
  "description": "string",
  "config": { ... }
}
```

**Example Description Formats:**
- "Buy when price crosses above 50-day MA with volume above average"
- "RSI oversold strategy: enter at RSI < 30, exit at RSI > 70"
- "Momentum strategy with 20% take profit and 10% stop loss"

### 2. run_backtest

Run a backtest for a strategy.

**Input:**
```json
{
  "strategy_id": "uuid (required)",
  "start_date": "YYYY-MM-DD (required)",
  "end_date": "YYYY-MM-DD (required)",
  "initial_capital": "number (optional, default: 100000)"
}
```

**Output:**
```json
{
  "backtest_id": "uuid",
  "strategy_id": "uuid",
  "strategy_name": "string",
  "period": "string",
  "initial_capital": "number",
  "final_value": "number",
  "total_return": "number",
  "num_trades": "number",
  "sharpe_ratio": "number"
}
```

### 3. get_backtest_results

Get detailed backtest results.

**Input:**
```json
{
  "backtest_id": "uuid (required)"
}
```

**Output:**
```json
{
  "id": "uuid",
  "strategy_id": "uuid",
  "start_date": "date",
  "end_date": "date",
  "initial_capital": "number",
  "final_value": "number",
  "total_return": "number",
  "num_trades": "number",
  "sharpe_ratio": "number",
  "max_drawdown": "number",
  "win_rate": "number"
}
```

### 4. list_strategies

List all available strategies.

**Input:**
```json
{
  "limit": "number (optional, default: 10)"
}
```

**Output:**
```json
[
  {
    "id": "uuid",
    "name": "string",
    "description": "string",
    "created_at": "timestamp"
  }
]
```

### 5. compare_strategies

Compare multiple backtest runs.

**Input:**
```json
{
  "backtest_ids": ["uuid", "uuid", ...] (required, min: 2),
  "metrics": ["string", ...] (optional)
}
```

**Output:**
```json
{
  "comparison_id": "uuid",
  "num_strategies": "number",
  "metrics_compared": ["string"],
  "rankings": { ... }
}
```

### 6. get_chart_data

Get chart data for visualization.

**Input:**
```json
{
  "backtest_id": "uuid (required)",
  "chart_type": "equity_curve|monthly_returns|trade_distribution|drawdown (optional)"
}
```

**Output:**
```json
{
  "chart_type": "string",
  "data": [ ... ]
}
```

## Production Deployment

### Docker Deployment

#### 1. Create Dockerfile

```dockerfile
# app/mcp/Dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN pip install uv && uv pip install .

# Copy application
COPY app/ ./app/

# Expose MCP server port
EXPOSE 8001

# Start MCP server
CMD ["uvicorn", "app.mcp.server:app", "--host", "0.0.0.0", "--port", "8001"]
```

#### 2. Docker Compose Setup

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: stock_picker
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  mcp-server:
    build:
      context: .
      dockerfile: app/mcp/Dockerfile
    ports:
      - "8001:8001"
    environment:
      POSTGRES_HOST: postgres
      POSTGRES_PORT: 5432
      POSTGRES_DATABASE: stock_picker
      POSTGRES_USERNAME: postgres
      POSTGRES_PASSWORD: postgres
    depends_on:
      - postgres

  n8n:
    image: n8nio/n8n:latest
    ports:
      - "5678:5678"
    environment:
      - N8N_BASIC_AUTH_ACTIVE=true
      - N8N_BASIC_AUTH_USER=admin
      - N8N_BASIC_AUTH_PASSWORD=admin
    volumes:
      - n8n_data:/home/node/.n8n
    depends_on:
      - mcp-server

volumes:
  postgres_data:
  n8n_data:
```

#### 3. Start Services

```bash
# Build and start all services
docker-compose up -d

# Check logs
docker-compose logs -f mcp-server

# Access n8n
open http://localhost:5678
```

### Kubernetes Deployment

```yaml
# kubernetes/mcp-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mcp-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: mcp-server
  template:
    metadata:
      labels:
        app: mcp-server
    spec:
      containers:
      - name: mcp-server
        image: stock-picker-mcp:latest
        ports:
        - containerPort: 8001
        env:
        - name: POSTGRES_HOST
          value: "postgres-service"
        - name: POSTGRES_PORT
          value: "5432"
        livenessProbe:
          httpGet:
            path: /health
            port: 8001
          initialDelaySeconds: 10
          periodSeconds: 30
---
apiVersion: v1
kind: Service
metadata:
  name: mcp-server-service
spec:
  selector:
    app: mcp-server
  ports:
  - port: 8001
    targetPort: 8001
  type: LoadBalancer
```

### Environment Variables

Create `.env` file:

```env
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=stock_picker
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=your_password

# MCP Server
MCP_SERVER_HOST=0.0.0.0
MCP_SERVER_PORT=8001

# Logging
LOG_LEVEL=INFO
```

## Troubleshooting

### Common Issues

#### 1. Connection Refused

**Problem:** `Connection refused at localhost:8001`

**Solution:**
```bash
# Check if MCP server is running
curl http://localhost:8001/health

# Start MCP server
uvicorn app.mcp.server:app --host 0.0.0.0 --port 8001
```

#### 2. Database Connection Error

**Problem:** `could not connect to server: Connection refused`

**Solution:**
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Check connection string
psql postgresql://postgres:password@localhost:5432/stock_picker
```

#### 3. SSE Timeout

**Problem:** Server-Sent Events timing out

**Solution:**
- Increase n8n timeout: Settings → Executions → Timeout
- Check server logs for errors
- Verify network connectivity

#### 4. Tool Execution Fails

**Problem:** MCP tool returns error

**Solution:**
```bash
# Check server logs
docker-compose logs -f mcp-server

# Test tool directly
curl -X POST http://localhost:8001/sse \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/list", "id": 1}'
```

### Debugging Tips

1. **Enable Debug Logging:**
   ```python
   # In app/mcp/server.py
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Test Tools Individually:**
   Use the MCP client to test each tool before integrating with n8n.

3. **Monitor Database Queries:**
   ```sql
   -- Enable query logging in PostgreSQL
   ALTER SYSTEM SET log_statement = 'all';
   SELECT pg_reload_conf();
   ```

4. **Check n8n Execution Logs:**
   - Click on workflow execution
   - View detailed logs for each node
   - Check request/response data

## Best Practices

1. **Error Handling:** Always add error handling nodes in n8n workflows
2. **Retry Logic:** Use n8n's retry settings for network failures
3. **Rate Limiting:** Implement rate limiting if running many backtests
4. **Caching:** Cache strategy results to avoid redundant computations
5. **Monitoring:** Set up alerts for failed workflows
6. **Security:** Use authentication for production deployments
7. **Backups:** Regular database backups of backtest results

## Resources

- [MCP Protocol Specification](https://modelcontextprotocol.io/)
- [n8n Documentation](https://docs.n8n.io/)
- [Stock Picker API Documentation](./FRONTEND_API_SPEC.md)
- [Backtest Framework Design](./backtest_framework_design.md)

## Support

For issues and questions:
- GitHub Issues: https://github.com/monkeyboiii/stock-picker/issues
- MCP Server Logs: `docker-compose logs mcp-server`
- n8n Community: https://community.n8n.io/

---

**Last Updated:** 2025-11-17
**Version:** 1.0.0
