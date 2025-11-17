# Backtesting Framework Design Plan
## Stock Picker - Comprehensive Strategy Backtesting System

**Date:** 2025-11-17
**Status:** Design Phase
**Author:** Claude (AI Assistant)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Requirements Analysis](#2-requirements-analysis)
3. [Strategy Definition System](#3-strategy-definition-system)
4. [Database Architecture](#4-database-architecture)
5. [Execution Engine](#5-execution-engine)
6. [API Design](#6-api-design)
7. [Performance & Analytics](#7-performance--analytics)
8. [LLM Integration](#8-llm-integration)
9. [Frontend Integration](#9-frontend-integration)
10. [Implementation Roadmap](#10-implementation-roadmap)

---

## 1. Executive Summary

### Vision
Build a **production-grade backtesting framework** that enables users to:
- Define strategies using human language OR structured JSON/YAML
- Execute backtests against historical data (2023-2026+)
- Analyze performance metrics (P&L, Sharpe, drawdown, win rate)
- Compare multiple strategies side-by-side
- Visualize trades and equity curves (playback-able)
- Optimize strategy parameters programmatically

### Key Design Principles
1. **Separation of Concerns**: Strategy definition ≠ Execution engine ≠ Analytics
2. **Database Agnostic Core**: PostgreSQL for now, ClickHouse-ready architecture
3. **LLM-Friendly**: Strategy syntax designed for easy LLM translation
4. **API-First**: RESTful endpoints for all operations
5. **Performance-Oriented**: Batch processing, caching, pre-computation
6. **Extensible**: Plugin architecture for new indicators and conditions

---

## 2. Requirements Analysis

### Use Cases (From User Request)

| Use Case | Priority | Technical Requirement |
|----------|----------|----------------------|
| "View every point of sale in current strategy" | P0 | Trade log storage & retrieval API |
| "Compare profitability of two strategies" | P0 | Multi-strategy execution + comparison metrics |
| "Design strategy using LLM-friendly syntax" | P0 | DSL + LLM parser + validator |
| "Small tweaks: raise target price to x.xx" | P1 | Parametric strategy system |
| "Catch RSI sharp drop in 4h candles" | P1 | Technical indicator library + time frame support |
| Playback-able results | P0 | Time-series trade data + REST API |
| Front-end rendering support | P1 | JSON serialization + websockets (future) |

### Non-Functional Requirements

| Category | Requirement | Target Metric |
|----------|-------------|---------------|
| **Performance** | Backtest 3 years of data | < 10 seconds |
| **Scalability** | Support 5000+ stocks | Linear time complexity |
| **Accuracy** | Match real trading | 99.9% accuracy |
| **Availability** | API uptime | 99.5% |
| **Latency** | Strategy execution | < 100ms per query |
| **Data Volume** | Historical data | 10M+ rows |

---

## 3. Strategy Definition System

### 3.1 Strategy DSL (Domain-Specific Language)

#### Design Goals
1. **Human-readable**: `price > 10 AND volume > 1000000`
2. **LLM-parseable**: Structured enough for GPT-4/Claude to generate
3. **Type-safe**: Validate at parse time, not execution time
4. **Composable**: Combine conditions with AND/OR/NOT
5. **Extensible**: Add new indicators without DSL changes

#### Strategy JSON Schema

```json
{
  "strategy": {
    "name": "Momentum Breakout V1",
    "version": "1.0.0",
    "author": "user@example.com",
    "description": "Buy stocks breaking above MA250 with volume surge",

    "parameters": {
      "gain_min": 3.0,
      "gain_max": 5.0,
      "volume_ratio_min": 1.0,
      "turnover_rate_min": 5.0,
      "cap_min": 200000000,
      "cap_max": 20000000000,
      "ma_period": 250
    },

    "entry_conditions": {
      "operator": "AND",
      "conditions": [
        {
          "type": "price_change",
          "field": "close",
          "comparison": "between",
          "value": ["{{gain_min}}", "{{gain_max}}"],
          "unit": "percent",
          "lookback": 1
        },
        {
          "type": "indicator",
          "name": "quantity_relative_ratio",
          "comparison": ">=",
          "value": "{{volume_ratio_min}}"
        },
        {
          "type": "indicator",
          "name": "turnover_rate",
          "comparison": ">",
          "value": "{{turnover_rate_min}}"
        },
        {
          "type": "market_cap",
          "field": "circulation_capital",
          "comparison": "between",
          "value": ["{{cap_min}}", "{{cap_max}}"]
        },
        {
          "type": "technical",
          "indicator": "ma",
          "period": "{{ma_period}}",
          "comparison": ">",
          "target": "low"
        },
        {
          "type": "price_direction",
          "comparison": "close > open"
        },
        {
          "type": "risk_filter",
          "exclude": ["ST", "*"]
        }
      ]
    },

    "exit_conditions": {
      "operator": "OR",
      "conditions": [
        {
          "type": "take_profit",
          "method": "percentage",
          "value": 10.0
        },
        {
          "type": "stop_loss",
          "method": "percentage",
          "value": -5.0
        },
        {
          "type": "time_based",
          "max_holding_days": 30
        },
        {
          "type": "indicator",
          "name": "rsi",
          "period": 14,
          "comparison": ">",
          "value": 70
        }
      ]
    },

    "position_sizing": {
      "method": "equal_weight",
      "max_positions": 20,
      "capital_per_position": "1/max_positions"
    },

    "risk_management": {
      "max_portfolio_drawdown": 20.0,
      "max_position_size": 10.0,
      "stop_trading_on_drawdown": true
    }
  }
}
```

#### Alternative: YAML Format (More Human-Friendly)

```yaml
strategy:
  name: Momentum Breakout V1
  version: 1.0.0
  description: Buy stocks breaking above MA250 with volume surge

  parameters:
    gain_min: 3.0
    gain_max: 5.0
    volume_ratio_min: 1.0
    turnover_rate_min: 5.0
    cap_min: 200_000_000
    cap_max: 20_000_000_000
    ma_period: 250

  entry_conditions:
    - price_change: { close: between [3%, 5%], lookback: 1 }
    - quantity_relative_ratio >= 1.0
    - turnover_rate > 5.0
    - circulation_capital: between [200M, 20B]
    - low > ma(250)
    - close > open
    - exclude: [ST, "*"]

  exit_conditions:
    - take_profit: 10%
    - stop_loss: -5%
    - max_holding_days: 30
    - rsi(14) > 70

  position_sizing:
    max_positions: 20
    method: equal_weight
```

#### LLM-to-Strategy Translation Examples

**User Input:** "I want to buy stocks that gain 3-5% with high volume"

**LLM Output:**
```yaml
entry_conditions:
  - price_change: { close: between [3%, 5%], lookback: 1 }
  - volume > ma(5, volume)  # Volume above 5-day average
```

**User Input:** "Catch RSI sharp drop in 4h candles"

**LLM Output:**
```yaml
entry_conditions:
  - timeframe: 4h
  - rsi(14) < rsi(14, offset=1) - 10  # RSI dropped 10+ points
  - rsi(14, offset=1) > 50  # Was previously above 50
```

### 3.2 Strategy Condition Types

#### Built-in Condition Types

| Type | Fields | Example |
|------|--------|---------|
| `price_change` | field, comparison, value, unit, lookback | `close > prev_close * 1.03` |
| `indicator` | name, comparison, value | `quantity_relative_ratio >= 1.0` |
| `technical` | indicator, period, comparison, target | `low > ma(250)` |
| `market_cap` | field, comparison, value | `circulation_capital between [200M, 20B]` |
| `price_direction` | comparison | `close > open` |
| `risk_filter` | exclude | `exclude: ["ST", "*"]` |
| `volume` | comparison, value | `volume > ma(5, volume)` |
| `time_based` | max_holding_days | `max_holding_days: 30` |
| `take_profit` | method, value | `take_profit: 10%` |
| `stop_loss` | method, value | `stop_loss: -5%` |
| `rsi` | period, comparison, value | `rsi(14) > 70` |
| `macd` | fast, slow, signal, comparison | `macd_line > signal_line` |
| `bollinger` | period, std_dev, comparison | `close < bb_lower(20, 2)` |

#### Custom Indicators (Extensible)

```python
# app/strategy/indicators.py
from abc import ABC, abstractmethod

class Indicator(ABC):
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.Series:
        pass

class RSI(Indicator):
    def __init__(self, period: int = 14):
        self.period = period

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

# Register custom indicators
INDICATOR_REGISTRY = {
    'rsi': RSI,
    'ma': MovingAverage,
    'macd': MACD,
    'bollinger': BollingerBands,
    # ... extensible
}
```

### 3.3 Strategy Parser & Validator

```python
# app/strategy/parser.py
from typing import Dict, Any, List
from pydantic import BaseModel, validator
import yaml
import json

class StrategyCondition(BaseModel):
    type: str
    comparison: Optional[str]
    value: Optional[Union[float, List[float], str]]
    field: Optional[str]
    # ... other fields

class StrategyDefinition(BaseModel):
    name: str
    version: str
    description: Optional[str]
    parameters: Dict[str, Any]
    entry_conditions: Dict[str, Any]
    exit_conditions: Dict[str, Any]
    position_sizing: Dict[str, Any]
    risk_management: Optional[Dict[str, Any]]

    @validator('entry_conditions')
    def validate_entry_conditions(cls, v):
        # Ensure all referenced indicators exist
        # Validate comparison operators
        # Check parameter references {{param_name}}
        return v

    def substitute_parameters(self):
        """Replace {{param_name}} with actual values"""
        # Recursive substitution
        pass

class StrategyParser:
    @staticmethod
    def from_yaml(yaml_str: str) -> StrategyDefinition:
        data = yaml.safe_load(yaml_str)
        return StrategyDefinition(**data['strategy'])

    @staticmethod
    def from_json(json_str: str) -> StrategyDefinition:
        data = json.loads(json_str)
        return StrategyDefinition(**data['strategy'])

    @staticmethod
    def from_llm_response(text: str) -> StrategyDefinition:
        """Parse LLM-generated strategy"""
        # Try YAML first, fallback to JSON
        try:
            return StrategyParser.from_yaml(text)
        except:
            return StrategyParser.from_json(text)
```

---

## 4. Database Architecture

### 4.1 PostgreSQL vs ClickHouse Analysis

#### Current State: PostgreSQL 16+

**Strengths:**
- ✅ ACID compliance for transactional integrity
- ✅ Rich query support (LATERAL joins, CTEs, window functions)
- ✅ Mature ecosystem with SQLAlchemy support
- ✅ Already storing 10M+ rows efficiently
- ✅ Materialized views for pre-computation
- ✅ Good for moderate-scale analytics (< 100M rows)

**Limitations for Backtesting:**
- ⚠️ Time-series queries can be slow on large datasets
- ⚠️ Limited columnar storage benefits
- ⚠️ Aggregations across millions of rows less optimized
- ⚠️ No built-in time-series compression

#### ClickHouse Option

**Strengths:**
- ✅ 100-1000x faster for analytical queries
- ✅ Columnar storage perfect for backtesting (select specific columns)
- ✅ Time-series optimized (native Date/DateTime types)
- ✅ Horizontal scalability
- ✅ Compression (10-20x storage reduction)
- ✅ Aggregation functions optimized for finance (quantiles, percentiles)

**Limitations:**
- ❌ No ACID transactions (eventual consistency)
- ❌ Limited UPDATE/DELETE support (not for OLTP)
- ❌ Steeper learning curve
- ❌ Additional infrastructure complexity

#### **Recommendation: Hybrid Approach**

```
PostgreSQL (OLTP)                  ClickHouse (OLAP)
- Stock metadata                   - Historical price data (read-only)
- Daily data ingestion             - Backtest results (append-only)
- User sessions                    - Performance metrics (time-series)
- Strategy definitions             - Trade logs (analytical queries)
- Active positions                 - Aggregated statistics

        ↓                                  ↓
    Real-time operations          Historical analytics
    (Writes + Reads)              (Read-heavy, batch writes)
```

**Migration Path:**
1. **Phase 1** (Now): Keep PostgreSQL, add optimization
   - Partitioning on `trade_day` (monthly partitions)
   - Add indexes on common query patterns
   - Materialized views for common aggregations

2. **Phase 2** (Later): Add ClickHouse for hot queries
   - Sync `stock_daily` to ClickHouse (CDC pipeline)
   - Store backtest results in ClickHouse
   - Keep PostgreSQL as source of truth

3. **Phase 3** (Future): Full hybrid
   - PostgreSQL for writes + metadata
   - ClickHouse for all analytical queries
   - GraphQL layer abstracts both databases

### 4.2 New Tables for Backtesting

#### Table: `strategy`
```sql
CREATE TABLE strategy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    author VARCHAR(255),
    description TEXT,
    definition JSONB NOT NULL,  -- Strategy YAML/JSON
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(name, version)
);

CREATE INDEX idx_strategy_name ON strategy(name);
CREATE INDEX idx_strategy_created_at ON strategy(created_at DESC);
```

#### Table: `backtest_run`
```sql
CREATE TABLE backtest_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id UUID NOT NULL REFERENCES strategy(id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital NUMERIC(15, 2) DEFAULT 1000000.00,

    -- Configuration
    commission_rate NUMERIC(5, 4) DEFAULT 0.0003,  -- 0.03%
    slippage_rate NUMERIC(5, 4) DEFAULT 0.001,     -- 0.1%

    -- Results (computed after run)
    total_return NUMERIC(10, 4),          -- %
    sharpe_ratio NUMERIC(10, 4),
    max_drawdown NUMERIC(10, 4),          -- %
    win_rate NUMERIC(5, 4),               -- %
    profit_factor NUMERIC(10, 4),
    total_trades INTEGER,

    -- Status
    status VARCHAR(20) DEFAULT 'pending',  -- pending, running, completed, failed
    error_message TEXT,

    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT valid_date_range CHECK (end_date >= start_date)
);

CREATE INDEX idx_backtest_run_strategy ON backtest_run(strategy_id);
CREATE INDEX idx_backtest_run_status ON backtest_run(status);
CREATE INDEX idx_backtest_run_created ON backtest_run(created_at DESC);
```

#### Table: `trade` (Individual trades)
```sql
CREATE TABLE trade (
    id BIGSERIAL PRIMARY KEY,
    backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),

    -- Entry
    entry_date DATE NOT NULL,
    entry_price NUMERIC(10, 3) NOT NULL,
    entry_signal TEXT,  -- JSON of matched conditions

    -- Exit
    exit_date DATE,
    exit_price NUMERIC(10, 3),
    exit_reason VARCHAR(50),  -- take_profit, stop_loss, time_limit, signal

    -- Position
    shares INTEGER NOT NULL,
    position_value NUMERIC(15, 2),

    -- P&L
    gross_pnl NUMERIC(15, 2),              -- Before costs
    commission NUMERIC(15, 2),
    slippage NUMERIC(15, 2),
    net_pnl NUMERIC(15, 2),                -- After costs
    return_pct NUMERIC(10, 4),             -- %

    -- Metadata
    holding_days INTEGER,
    collection_name VARCHAR(100),          -- Industry/board

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_trade_backtest_run ON trade(backtest_run_id);
CREATE INDEX idx_trade_stock_code ON trade(stock_code);
CREATE INDEX idx_trade_entry_date ON trade(entry_date);
CREATE INDEX idx_trade_exit_date ON trade(exit_date);

-- Partition by entry_date (monthly)
CREATE TABLE trade_2025_01 PARTITION OF trade
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
-- ... more partitions
```

#### Table: `portfolio_snapshot` (Daily equity curve)
```sql
CREATE TABLE portfolio_snapshot (
    id BIGSERIAL PRIMARY KEY,
    backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,

    -- Portfolio value
    cash NUMERIC(15, 2) NOT NULL,
    holdings_value NUMERIC(15, 2) NOT NULL,
    total_value NUMERIC(15, 2) NOT NULL,

    -- Daily metrics
    daily_return NUMERIC(10, 4),           -- %
    cumulative_return NUMERIC(10, 4),      -- %
    drawdown NUMERIC(10, 4),               -- % from peak

    -- Positions
    open_positions INTEGER,
    total_positions_opened INTEGER,
    total_positions_closed INTEGER,

    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(backtest_run_id, snapshot_date)
);

CREATE INDEX idx_portfolio_snapshot_run ON portfolio_snapshot(backtest_run_id);
CREATE INDEX idx_portfolio_snapshot_date ON portfolio_snapshot(snapshot_date);
```

#### Table: `strategy_comparison`
```sql
CREATE TABLE strategy_comparison (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255),
    description TEXT,
    backtest_run_ids UUID[] NOT NULL,  -- Array of run IDs to compare

    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(255)
);
```

### 4.3 Database Optimization

#### Partitioning Strategy
```sql
-- Partition stock_daily by month (already large table)
ALTER TABLE stock_daily PARTITION BY RANGE (trade_day);

CREATE TABLE stock_daily_2023_01 PARTITION OF stock_daily
    FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
-- ... create partitions for each month

-- Partition trade table similarly
-- Future queries will only scan relevant partitions
```

#### Materialized Views for Backtesting
```sql
-- Pre-compute daily metrics for all stocks
CREATE MATERIALIZED VIEW mv_daily_metrics AS
SELECT
    sd.code,
    sd.trade_day,
    sd.close,
    sd.volume,
    sd.turnover_rate,
    sd.quantity_relative_ratio,
    LAG(sd.close, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day) AS prev_close,
    LAG(sd.volume, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day) AS prev_volume,
    AVG(sd.volume) OVER (PARTITION BY sd.code ORDER BY sd.trade_day ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5_volume,
    AVG(sd.close) OVER (PARTITION BY sd.code ORDER BY sd.trade_day ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS ma250,
    (sd.close / LAG(sd.close, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day) - 1) * 100 AS daily_return
FROM stock_daily sd
WHERE sd.trade_day >= '2023-01-01';

CREATE INDEX idx_mv_daily_metrics_code_date ON mv_daily_metrics(code, trade_day);

-- Refresh daily
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_metrics;
```

---

## 5. Execution Engine

### 5.1 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Backtest Orchestrator                     │
│  - Parse strategy                                            │
│  - Initialize portfolio                                      │
│  - Iterate through time                                      │
└──────────────────┬──────────────────────────────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌───────────────┐      ┌──────────────┐
│ Signal Engine │      │ Risk Manager │
│ - Eval entry  │      │ - Position   │
│ - Eval exit   │      │   sizing     │
│ - Filter      │      │ - Drawdown   │
│   stocks      │      │   limits     │
└───────┬───────┘      └──────┬───────┘
        │                     │
        └──────────┬──────────┘
                   │
                   ▼
        ┌──────────────────┐
        │ Position Manager │
        │ - Open positions │
        │ - Close positions│
        │ - Track P&L      │
        └─────────┬────────┘
                  │
                  ▼
        ┌──────────────────┐
        │ Results Recorder │
        │ - Log trades     │
        │ - Snapshots      │
        │ - Metrics        │
        └──────────────────┘
```

### 5.2 Core Classes

```python
# app/backtest/engine.py
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Dict, Optional
from decimal import Decimal

@dataclass
class Position:
    """Represents an open stock position"""
    stock_code: str
    stock_name: str
    entry_date: date
    entry_price: Decimal
    shares: int
    entry_signal: Dict[str, Any]

    @property
    def position_value(self) -> Decimal:
        return self.entry_price * self.shares

    def calculate_pnl(self, current_price: Decimal,
                      commission_rate: Decimal = Decimal('0.0003'),
                      slippage_rate: Decimal = Decimal('0.001')) -> Dict[str, Decimal]:
        """Calculate P&L with costs"""
        gross_pnl = (current_price - self.entry_price) * self.shares

        # Entry costs
        entry_commission = self.entry_price * self.shares * commission_rate
        entry_slippage = self.entry_price * self.shares * slippage_rate

        # Exit costs
        exit_commission = current_price * self.shares * commission_rate
        exit_slippage = current_price * self.shares * slippage_rate

        total_commission = entry_commission + exit_commission
        total_slippage = entry_slippage + exit_slippage

        net_pnl = gross_pnl - total_commission - total_slippage

        return {
            'gross_pnl': gross_pnl,
            'commission': total_commission,
            'slippage': total_slippage,
            'net_pnl': net_pnl,
            'return_pct': (net_pnl / self.position_value) * 100
        }


class Portfolio:
    """Manages portfolio state during backtest"""

    def __init__(self, initial_capital: Decimal):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.closed_positions: List[Dict] = []
        self.equity_curve: List[Dict] = []

    @property
    def holdings_value(self) -> Decimal:
        """Total value of open positions"""
        return sum(pos.position_value for pos in self.positions.values())

    @property
    def total_value(self) -> Decimal:
        """Cash + holdings"""
        return self.cash + self.holdings_value

    @property
    def total_return(self) -> Decimal:
        """Portfolio return %"""
        return ((self.total_value - self.initial_capital) / self.initial_capital) * 100

    def open_position(self, stock_code: str, stock_name: str,
                     entry_date: date, entry_price: Decimal,
                     shares: int, entry_signal: Dict) -> None:
        """Open a new position"""
        position = Position(
            stock_code=stock_code,
            stock_name=stock_name,
            entry_date=entry_date,
            entry_price=entry_price,
            shares=shares,
            entry_signal=entry_signal
        )

        cost = position.position_value
        if cost > self.cash:
            raise ValueError(f"Insufficient cash: {self.cash} < {cost}")

        self.cash -= cost
        self.positions[stock_code] = position

    def close_position(self, stock_code: str, exit_date: date,
                      exit_price: Decimal, exit_reason: str) -> Dict:
        """Close an existing position"""
        if stock_code not in self.positions:
            raise ValueError(f"No open position for {stock_code}")

        position = self.positions.pop(stock_code)

        # Calculate P&L
        pnl_data = position.calculate_pnl(exit_price)

        # Return cash
        proceeds = exit_price * position.shares - pnl_data['commission'] - pnl_data['slippage']
        self.cash += proceeds

        # Record closed trade
        trade = {
            'stock_code': stock_code,
            'stock_name': position.stock_name,
            'entry_date': position.entry_date,
            'entry_price': position.entry_price,
            'exit_date': exit_date,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'shares': position.shares,
            'holding_days': (exit_date - position.entry_date).days,
            **pnl_data
        }

        self.closed_positions.append(trade)
        return trade

    def snapshot(self, snapshot_date: date) -> Dict:
        """Take daily portfolio snapshot"""
        snapshot = {
            'snapshot_date': snapshot_date,
            'cash': self.cash,
            'holdings_value': self.holdings_value,
            'total_value': self.total_value,
            'cumulative_return': self.total_return,
            'open_positions': len(self.positions),
            'total_positions_closed': len(self.closed_positions)
        }

        self.equity_curve.append(snapshot)
        return snapshot


class BacktestEngine:
    """Main backtesting execution engine"""

    def __init__(self,
                 strategy: StrategyDefinition,
                 start_date: date,
                 end_date: date,
                 initial_capital: Decimal = Decimal('1000000'),
                 commission_rate: Decimal = Decimal('0.0003'),
                 slippage_rate: Decimal = Decimal('0.001')):

        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        self.portfolio = Portfolio(initial_capital)
        self.signal_engine = SignalEngine(strategy)
        self.risk_manager = RiskManager(strategy.risk_management)

    def run(self, engine: Engine) -> BacktestResult:
        """
        Main backtest loop

        Time-based iteration:
        1. For each trading day from start_date to end_date:
           a. Check exit conditions for open positions
           b. Close positions if exit triggered
           c. Scan for new entry signals
           d. Open new positions (if cash available)
           e. Take portfolio snapshot

        2. After loop completes:
           a. Close all remaining positions
           b. Calculate performance metrics
           c. Store results in database
        """

        from app.constant.schedule import next_trade_day, is_stock_market_open

        current_date = self.start_date

        while current_date <= self.end_date:
            if not is_stock_market_open(current_date):
                current_date = next_trade_day(current_date, inclusive=False)
                continue

            # Step 1: Check exits for open positions
            self._process_exits(current_date, engine)

            # Step 2: Find new entry signals
            self._process_entries(current_date, engine)

            # Step 3: Take daily snapshot
            self.portfolio.snapshot(current_date)

            # Move to next day
            current_date = next_trade_day(current_date, inclusive=False)

        # Final step: Close all remaining positions at end_date
        self._close_all_positions(self.end_date, engine)

        # Calculate metrics
        result = self._calculate_metrics()

        return result

    def _process_exits(self, current_date: date, engine: Engine) -> None:
        """Check and execute exit conditions"""

        for stock_code, position in list(self.portfolio.positions.items()):
            # Get current price
            current_price = self._get_price(stock_code, current_date, engine)
            if not current_price:
                continue

            # Evaluate exit conditions
            exit_reason = self.signal_engine.check_exit(
                position, current_price, current_date, engine
            )

            if exit_reason:
                self.portfolio.close_position(
                    stock_code, current_date, current_price, exit_reason
                )

    def _process_entries(self, current_date: date, engine: Engine) -> None:
        """Find and execute entry signals"""

        # Get candidate stocks (apply entry conditions filter)
        candidates = self.signal_engine.scan_entry_signals(current_date, engine)

        # Apply risk management
        allowed_positions = self.risk_manager.max_positions - len(self.portfolio.positions)

        if allowed_positions <= 0:
            return

        # Calculate position size
        capital_per_position = self.portfolio.cash / allowed_positions

        for candidate in candidates[:allowed_positions]:
            # Calculate shares to buy
            shares = int(capital_per_position / candidate['price'])

            if shares == 0:
                continue

            try:
                self.portfolio.open_position(
                    stock_code=candidate['code'],
                    stock_name=candidate['name'],
                    entry_date=current_date,
                    entry_price=candidate['price'],
                    shares=shares,
                    entry_signal=candidate['signal']
                )
            except ValueError:
                # Insufficient cash
                break

    def _close_all_positions(self, final_date: date, engine: Engine) -> None:
        """Close all remaining positions at backtest end"""

        for stock_code in list(self.portfolio.positions.keys()):
            price = self._get_price(stock_code, final_date, engine)
            if price:
                self.portfolio.close_position(
                    stock_code, final_date, price, 'backtest_end'
                )

    def _get_price(self, stock_code: str, trade_day: date, engine: Engine) -> Optional[Decimal]:
        """Fetch stock price for a given day"""
        from sqlalchemy import select
        from sqlalchemy.orm import Session
        from app.db.models import StockDaily

        with Session(engine) as session:
            stmt = select(StockDaily.close).where(
                StockDaily.code == stock_code,
                StockDaily.trade_day == trade_day
            )
            result = session.execute(stmt).scalar_one_or_none()
            return result

    def _calculate_metrics(self) -> 'BacktestResult':
        """Calculate comprehensive performance metrics"""

        trades = self.portfolio.closed_positions
        equity_curve = self.portfolio.equity_curve

        # Basic metrics
        total_trades = len(trades)
        winning_trades = [t for t in trades if t['net_pnl'] > 0]
        losing_trades = [t for t in trades if t['net_pnl'] < 0]

        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0

        total_return = self.portfolio.total_return

        # Sharpe ratio (simplified - assumes daily returns)
        daily_returns = [
            (equity_curve[i]['total_value'] / equity_curve[i-1]['total_value'] - 1)
            for i in range(1, len(equity_curve))
        ]

        if len(daily_returns) > 0:
            mean_return = np.mean(daily_returns)
            std_return = np.std(daily_returns)
            sharpe_ratio = (mean_return / std_return) * np.sqrt(252) if std_return > 0 else 0
        else:
            sharpe_ratio = 0

        # Max drawdown
        peak = equity_curve[0]['total_value']
        max_drawdown = 0

        for snapshot in equity_curve:
            if snapshot['total_value'] > peak:
                peak = snapshot['total_value']

            drawdown = (peak - snapshot['total_value']) / peak * 100
            max_drawdown = max(max_drawdown, drawdown)

        # Profit factor
        gross_profit = sum(t['gross_pnl'] for t in winning_trades) if winning_trades else 0
        gross_loss = abs(sum(t['gross_pnl'] for t in losing_trades)) if losing_trades else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        return BacktestResult(
            total_return=total_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate * 100,
            profit_factor=profit_factor,
            total_trades=total_trades,
            trades=trades,
            equity_curve=equity_curve
        )


@dataclass
class BacktestResult:
    """Container for backtest results"""
    total_return: Decimal
    sharpe_ratio: Decimal
    max_drawdown: Decimal
    win_rate: Decimal
    profit_factor: Decimal
    total_trades: int
    trades: List[Dict]
    equity_curve: List[Dict]

    def to_dict(self) -> Dict:
        return {
            'total_return': float(self.total_return),
            'sharpe_ratio': float(self.sharpe_ratio),
            'max_drawdown': float(self.max_drawdown),
            'win_rate': float(self.win_rate),
            'profit_factor': float(self.profit_factor),
            'total_trades': self.total_trades,
            'trades': self.trades,
            'equity_curve': self.equity_curve
        }
```

### 5.3 Signal Engine

```python
# app/backtest/signals.py
class SignalEngine:
    """Evaluates entry and exit conditions"""

    def __init__(self, strategy: StrategyDefinition):
        self.strategy = strategy
        self.entry_conditions = strategy.entry_conditions
        self.exit_conditions = strategy.exit_conditions

    def scan_entry_signals(self, trade_day: date, engine: Engine) -> List[Dict]:
        """
        Scan all stocks for entry signals

        Returns: List of dicts with:
        - code
        - name
        - price
        - signal (matched conditions)
        """

        # Convert strategy conditions to SQL
        sql_query = self._build_entry_query(trade_day)

        # Execute query
        with Session(engine) as session:
            results = session.execute(sql_query).fetchall()

        return [
            {
                'code': r.code,
                'name': r.name,
                'price': r.close,
                'signal': r.matched_conditions
            }
            for r in results
        ]

    def check_exit(self, position: Position, current_price: Decimal,
                   current_date: date, engine: Engine) -> Optional[str]:
        """
        Check if position should be exited

        Returns: Exit reason string or None
        """

        # Check all exit conditions
        for condition in self.exit_conditions['conditions']:
            if self._evaluate_exit_condition(condition, position, current_price, current_date):
                return condition['type']

        return None

    def _build_entry_query(self, trade_day: date) -> Select:
        """Build SQL query from strategy entry conditions"""
        # Similar to existing build_stmt_postgresql_lateral()
        # But dynamically constructed from strategy JSON
        pass

    def _evaluate_exit_condition(self, condition: Dict, position: Position,
                                 current_price: Decimal, current_date: date) -> bool:
        """Evaluate a single exit condition"""

        if condition['type'] == 'take_profit':
            pnl = position.calculate_pnl(current_price)
            return pnl['return_pct'] >= condition['value']

        elif condition['type'] == 'stop_loss':
            pnl = position.calculate_pnl(current_price)
            return pnl['return_pct'] <= condition['value']

        elif condition['type'] == 'time_based':
            holding_days = (current_date - position.entry_date).days
            return holding_days >= condition['max_holding_days']

        # Add more condition types...

        return False
```

---

**(Continued from Section 5...)**

---

## 6. API Design

### 6.1 RESTful Endpoints

#### Base URL: `/api/v1/backtest`

| Endpoint | Method | Description | Auth Required |
|----------|--------|-------------|---------------|
| `/strategies` | GET | List all strategies | No |
| `/strategies` | POST | Create new strategy | Yes |
| `/strategies/{id}` | GET | Get strategy details | No |
| `/strategies/{id}` | PUT | Update strategy | Yes |
| `/strategies/{id}` | DELETE | Delete strategy | Yes |
| `/strategies/generate` | POST | Generate strategy from natural language (LLM) | Yes |
| `/run` | POST | Execute backtest | Yes |
| `/runs` | GET | List backtest runs | No |
| `/runs/{id}` | GET | Get run results | No |
| `/runs/{id}/trades` | GET | Get trade log | No |
| `/runs/{id}/equity-curve` | GET | Get portfolio snapshots | No |
| `/runs/{id}/metrics` | GET | Get performance metrics | No |
| `/compare` | POST | Compare multiple strategies | Yes |
| `/compare/{id}` | GET | Get comparison results | No |

### 6.2 Request/Response Schemas

#### POST `/api/v1/backtest/strategies` - Create Strategy

**Request Body:**
```json
{
  "strategy": {
    "name": "Momentum Breakout V1",
    "version": "1.0.0",
    "description": "Buy stocks breaking above MA250 with volume surge",
    "definition": {
      "parameters": { "gain_min": 3.0, "gain_max": 5.0 },
      "entry_conditions": { "operator": "AND", "conditions": [...] },
      "exit_conditions": { "operator": "OR", "conditions": [...] },
      "position_sizing": { "max_positions": 20 },
      "risk_management": { "max_portfolio_drawdown": 20.0 }
    }
  }
}
```

**Response (201 Created):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Momentum Breakout V1",
  "version": "1.0.0",
  "created_at": "2025-11-17T10:30:00Z",
  "validation_status": "valid",
  "validation_errors": []
}
```

#### POST `/api/v1/backtest/run` - Execute Backtest

**Request Body:**
```json
{
  "strategy_id": "550e8400-e29b-41d4-a716-446655440000",
  "start_date": "2023-01-01",
  "end_date": "2025-11-17",
  "initial_capital": 1000000.00,
  "commission_rate": 0.0003,
  "slippage_rate": 0.001,
  "async": true  // If true, returns immediately with run_id
}
```

**Response (202 Accepted - Async):**
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "status": "running",
  "estimated_time_seconds": 8,
  "progress_url": "/api/v1/backtest/runs/660e8400-e29b-41d4-a716-446655440001/progress"
}
```

**Response (200 OK - Sync):**
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "status": "completed",
  "execution_time_seconds": 7.3,
  "results": {
    "total_return": 42.5,
    "sharpe_ratio": 1.8,
    "max_drawdown": -12.3,
    "win_rate": 65.2,
    "profit_factor": 2.1,
    "total_trades": 145
  },
  "trades_url": "/api/v1/backtest/runs/660e8400-e29b-41d4-a716-446655440001/trades",
  "equity_curve_url": "/api/v1/backtest/runs/660e8400-e29b-41d4-a716-446655440001/equity-curve"
}
```

#### GET `/api/v1/backtest/runs/{id}/trades` - Get Trade Log

**Query Parameters:**
- `limit` (default: 100) - Number of trades per page
- `offset` (default: 0) - Pagination offset
- `status` - Filter by status: `open`, `closed`, `all` (default: `all`)
- `sort_by` - Sort field: `entry_date`, `exit_date`, `net_pnl`, `return_pct` (default: `entry_date`)
- `order` - Sort order: `asc`, `desc` (default: `desc`)

**Response (200 OK):**
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "total_trades": 145,
  "trades": [
    {
      "id": 1001,
      "stock_code": "600000",
      "stock_name": "浦发银行",
      "entry_date": "2023-01-05",
      "entry_price": 10.50,
      "exit_date": "2023-01-15",
      "exit_price": 11.25,
      "exit_reason": "take_profit",
      "shares": 9500,
      "position_value": 99750.00,
      "gross_pnl": 7125.00,
      "commission": 60.00,
      "slippage": 100.00,
      "net_pnl": 6965.00,
      "return_pct": 6.98,
      "holding_days": 10,
      "collection_name": "银行"
    }
    // ... more trades
  ],
  "pagination": {
    "limit": 100,
    "offset": 0,
    "total": 145,
    "has_more": true,
    "next_url": "/api/v1/backtest/runs/660e8400-e29b-41d4-a716-446655440001/trades?limit=100&offset=100"
  }
}
```

#### GET `/api/v1/backtest/runs/{id}/equity-curve` - Get Portfolio Snapshots

**Response (200 OK):**
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "initial_capital": 1000000.00,
  "final_value": 1425000.00,
  "snapshots": [
    {
      "date": "2023-01-04",
      "cash": 900000.00,
      "holdings_value": 100000.00,
      "total_value": 1000000.00,
      "daily_return": 0.0,
      "cumulative_return": 0.0,
      "drawdown": 0.0,
      "open_positions": 1,
      "total_positions_opened": 1,
      "total_positions_closed": 0
    },
    {
      "date": "2023-01-05",
      "cash": 900000.00,
      "holdings_value": 102000.00,
      "total_value": 1002000.00,
      "daily_return": 0.2,
      "cumulative_return": 0.2,
      "drawdown": 0.0,
      "open_positions": 1,
      "total_positions_opened": 1,
      "total_positions_closed": 0
    }
    // ... daily snapshots
  ]
}
```

#### POST `/api/v1/backtest/strategies/generate` - LLM Strategy Generation

**Request Body:**
```json
{
  "prompt": "I want to catch stocks with RSI sharp drop in 4h candles, gaining 3-5% with high volume",
  "model": "gpt-4",  // or "claude-3-opus"
  "base_strategy_id": null,  // Optional: modify existing strategy
  "parameters": {
    "temperature": 0.7
  }
}
```

**Response (200 OK):**
```json
{
  "strategy": {
    "name": "RSI Sharp Drop Momentum (LLM Generated)",
    "version": "1.0.0",
    "description": "Generated strategy based on: 'catch stocks with RSI sharp drop...'",
    "definition": {
      "entry_conditions": {
        "operator": "AND",
        "conditions": [
          { "type": "price_change", "comparison": "between", "value": [3.0, 5.0], "unit": "percent" },
          { "type": "technical", "indicator": "rsi", "period": 14, "comparison": "<", "value": "{{rsi_prev}} - 10" },
          { "type": "volume", "comparison": ">", "value": "ma(5, volume)" }
        ]
      },
      "exit_conditions": { "operator": "OR", "conditions": [...] }
    }
  },
  "llm_reasoning": "The user wants to identify stocks experiencing a sharp RSI drop while maintaining positive price momentum (3-5% gain) with volume confirmation...",
  "confidence_score": 0.85,
  "warnings": [
    "4h candle data not available - using daily data instead",
    "Consider adding max_holding_days to prevent indefinite positions"
  ]
}
```

#### POST `/api/v1/backtest/compare` - Compare Strategies

**Request Body:**
```json
{
  "name": "Momentum Strategy Comparison",
  "backtest_run_ids": [
    "660e8400-e29b-41d4-a716-446655440001",
    "660e8400-e29b-41d4-a716-446655440002",
    "660e8400-e29b-41d4-a716-446655440003"
  ],
  "metrics": ["total_return", "sharpe_ratio", "max_drawdown", "win_rate"]
}
```

**Response (200 OK):**
```json
{
  "comparison_id": "770e8400-e29b-41d4-a716-446655440004",
  "name": "Momentum Strategy Comparison",
  "created_at": "2025-11-17T11:00:00Z",
  "runs": [
    {
      "run_id": "660e8400-e29b-41d4-a716-446655440001",
      "strategy_name": "Momentum Breakout V1",
      "total_return": 42.5,
      "sharpe_ratio": 1.8,
      "max_drawdown": -12.3,
      "win_rate": 65.2,
      "rank": 1
    },
    {
      "run_id": "660e8400-e29b-41d4-a716-446655440002",
      "strategy_name": "Momentum Breakout V2",
      "total_return": 38.2,
      "sharpe_ratio": 1.9,
      "max_drawdown": -10.5,
      "win_rate": 68.0,
      "rank": 2
    }
  ],
  "best_by_metric": {
    "total_return": "660e8400-e29b-41d4-a716-446655440001",
    "sharpe_ratio": "660e8400-e29b-41d4-a716-446655440002",
    "max_drawdown": "660e8400-e29b-41d4-a716-446655440002",
    "win_rate": "660e8400-e29b-41d4-a716-446655440002"
  },
  "visualization_url": "/api/v1/backtest/compare/770e8400-e29b-41d4-a716-446655440004/chart"
}
```

### 6.3 Error Handling

**Standard Error Response:**
```json
{
  "error": {
    "code": "INVALID_STRATEGY",
    "message": "Entry condition references unknown indicator: 'macd_custom'",
    "details": {
      "field": "entry_conditions.conditions[3].indicator",
      "allowed_indicators": ["rsi", "ma", "macd", "bollinger"]
    }
  }
}
```

**HTTP Status Codes:**
- `200 OK` - Successful request
- `201 Created` - Resource created
- `202 Accepted` - Async request accepted
- `400 Bad Request` - Invalid input
- `401 Unauthorized` - Authentication required
- `404 Not Found` - Resource not found
- `409 Conflict` - Resource conflict (e.g., duplicate strategy name)
- `422 Unprocessable Entity` - Validation failed
- `500 Internal Server Error` - Server error
- `503 Service Unavailable` - Service temporarily unavailable

### 6.4 WebSocket API (Future - Real-time Progress)

**Connect:** `ws://api.example.com/api/v1/backtest/runs/{run_id}/stream`

**Message Format:**
```json
{
  "type": "progress",
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "progress": 45.2,
  "current_date": "2024-06-15",
  "trades_completed": 67,
  "current_value": 1125000.00,
  "message": "Processing 2024-06-15..."
}
```

### 6.5 Implementation (FastAPI)

```python
# app/api/backtest.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from datetime import date
from decimal import Decimal

app = FastAPI(title="Stock Picker Backtest API", version="1.0.0")

class StrategyCreate(BaseModel):
    name: str
    version: str
    description: Optional[str]
    definition: dict

class BacktestRequest(BaseModel):
    strategy_id: str
    start_date: date
    end_date: date
    initial_capital: Decimal = Decimal('1000000')
    commission_rate: Decimal = Decimal('0.0003')
    slippage_rate: Decimal = Decimal('0.001')
    async_mode: bool = True

@app.post("/api/v1/backtest/strategies", status_code=201)
async def create_strategy(strategy: StrategyCreate):
    """Create a new trading strategy"""
    from app.strategy.parser import StrategyParser
    from app.db.engine import engine_from_env
    from app.db.models import Strategy
    from sqlalchemy.orm import Session

    # Validate strategy definition
    try:
        strategy_def = StrategyParser.from_dict(strategy.definition)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid strategy: {str(e)}")

    # Store in database
    engine = engine_from_env()
    with Session(engine) as session:
        db_strategy = Strategy(
            name=strategy.name,
            version=strategy.version,
            description=strategy.description,
            definition=strategy.definition
        )
        session.add(db_strategy)
        session.commit()
        session.refresh(db_strategy)

        return {
            "id": str(db_strategy.id),
            "name": db_strategy.name,
            "version": db_strategy.version,
            "created_at": db_strategy.created_at,
            "validation_status": "valid",
            "validation_errors": []
        }

@app.post("/api/v1/backtest/run")
async def run_backtest(request: BacktestRequest, background_tasks: BackgroundTasks):
    """Execute a backtest"""
    from app.backtest.engine import BacktestEngine
    from app.db.engine import engine_from_env
    from app.db.models import Strategy, BacktestRun
    from sqlalchemy.orm import Session
    import uuid

    engine = engine_from_env()

    # Load strategy
    with Session(engine) as session:
        strategy = session.get(Strategy, request.strategy_id)
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")

        # Create backtest run record
        run_id = uuid.uuid4()
        backtest_run = BacktestRun(
            id=run_id,
            strategy_id=strategy.id,
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=request.initial_capital,
            commission_rate=request.commission_rate,
            slippage_rate=request.slippage_rate,
            status='pending'
        )
        session.add(backtest_run)
        session.commit()

    if request.async_mode:
        # Run in background
        background_tasks.add_task(
            execute_backtest_task,
            run_id,
            strategy.definition,
            request.start_date,
            request.end_date,
            request.initial_capital,
            request.commission_rate,
            request.slippage_rate
        )

        return {
            "run_id": str(run_id),
            "status": "running",
            "estimated_time_seconds": 8,
            "progress_url": f"/api/v1/backtest/runs/{run_id}/progress"
        }
    else:
        # Run synchronously
        result = execute_backtest_task(
            run_id, strategy.definition, request.start_date, request.end_date,
            request.initial_capital, request.commission_rate, request.slippage_rate
        )

        return {
            "run_id": str(run_id),
            "status": "completed",
            "results": result.to_dict(),
            "trades_url": f"/api/v1/backtest/runs/{run_id}/trades",
            "equity_curve_url": f"/api/v1/backtest/runs/{run_id}/equity-curve"
        }

def execute_backtest_task(run_id, strategy_def, start_date, end_date,
                          initial_capital, commission_rate, slippage_rate):
    """Background task to execute backtest"""
    from app.backtest.engine import BacktestEngine
    from app.strategy.parser import StrategyParser
    from app.db.engine import engine_from_env
    from app.db.models import BacktestRun, Trade, PortfolioSnapshot
    from sqlalchemy.orm import Session
    from datetime import datetime

    engine = engine_from_env()

    try:
        # Update status to running
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            run.status = 'running'
            run.started_at = datetime.now()
            session.commit()

        # Parse strategy
        strategy = StrategyParser.from_dict(strategy_def)

        # Run backtest
        backtest = BacktestEngine(
            strategy=strategy,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            commission_rate=commission_rate,
            slippage_rate=slippage_rate
        )

        result = backtest.run(engine)

        # Store results
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            run.status = 'completed'
            run.completed_at = datetime.now()
            run.total_return = result.total_return
            run.sharpe_ratio = result.sharpe_ratio
            run.max_drawdown = result.max_drawdown
            run.win_rate = result.win_rate
            run.profit_factor = result.profit_factor
            run.total_trades = result.total_trades

            # Store trades
            for trade_data in result.trades:
                trade = Trade(
                    backtest_run_id=run_id,
                    **trade_data
                )
                session.add(trade)

            # Store portfolio snapshots
            for snapshot_data in result.equity_curve:
                snapshot = PortfolioSnapshot(
                    backtest_run_id=run_id,
                    **snapshot_data
                )
                session.add(snapshot)

            session.commit()

        return result

    except Exception as e:
        # Update status to failed
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            run.status = 'failed'
            run.error_message = str(e)
            run.completed_at = datetime.now()
            session.commit()

        raise

@app.get("/api/v1/backtest/runs/{run_id}/trades")
async def get_trades(run_id: str, limit: int = 100, offset: int = 0,
                    sort_by: str = "entry_date", order: str = "desc"):
    """Get trade log for a backtest run"""
    from app.db.engine import engine_from_env
    from app.db.models import Trade
    from sqlalchemy.orm import Session
    from sqlalchemy import select, func, desc, asc

    engine = engine_from_env()

    with Session(engine) as session:
        # Count total trades
        count_stmt = select(func.count(Trade.id)).where(Trade.backtest_run_id == run_id)
        total_trades = session.execute(count_stmt).scalar()

        # Get trades
        stmt = select(Trade).where(Trade.backtest_run_id == run_id)

        # Apply sorting
        sort_column = getattr(Trade, sort_by, Trade.entry_date)
        if order == "desc":
            stmt = stmt.order_by(desc(sort_column))
        else:
            stmt = stmt.order_by(asc(sort_column))

        # Apply pagination
        stmt = stmt.limit(limit).offset(offset)

        trades = session.execute(stmt).scalars().all()

        return {
            "run_id": run_id,
            "total_trades": total_trades,
            "trades": [trade.to_dict() for trade in trades],
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total_trades,
                "has_more": offset + limit < total_trades
            }
        }

@app.get("/api/v1/backtest/runs/{run_id}/equity-curve")
async def get_equity_curve(run_id: str):
    """Get portfolio snapshots (equity curve)"""
    from app.db.engine import engine_from_env
    from app.db.models import PortfolioSnapshot, BacktestRun
    from sqlalchemy.orm import Session
    from sqlalchemy import select

    engine = engine_from_env()

    with Session(engine) as session:
        # Get run info
        run = session.get(BacktestRun, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Backtest run not found")

        # Get snapshots
        stmt = select(PortfolioSnapshot).where(
            PortfolioSnapshot.backtest_run_id == run_id
        ).order_by(PortfolioSnapshot.snapshot_date)

        snapshots = session.execute(stmt).scalars().all()

        return {
            "run_id": run_id,
            "initial_capital": float(run.initial_capital),
            "final_value": float(snapshots[-1].total_value) if snapshots else 0,
            "snapshots": [s.to_dict() for s in snapshots]
        }
```

---

## 7. Performance & Analytics

### 7.1 Performance Metrics

#### Core Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Total Return** | `(final_value - initial_capital) / initial_capital * 100` | Overall profitability (%) |
| **Sharpe Ratio** | `(mean_return - risk_free_rate) / std_return * √252` | Risk-adjusted return |
| **Max Drawdown** | `max((peak - valley) / peak * 100)` | Worst peak-to-trough decline (%) |
| **Win Rate** | `winning_trades / total_trades * 100` | Percentage of profitable trades |
| **Profit Factor** | `gross_profit / gross_loss` | Ratio of gains to losses |
| **Average Win** | `sum(winning_pnl) / num_winning_trades` | Average profit per winning trade |
| **Average Loss** | `sum(losing_pnl) / num_losing_trades` | Average loss per losing trade |
| **Expectancy** | `(win_rate * avg_win) - (loss_rate * avg_loss)` | Expected value per trade |
| **Sortino Ratio** | `(mean_return - risk_free_rate) / downside_deviation * √252` | Risk-adjusted return (downside only) |
| **Calmar Ratio** | `total_return / abs(max_drawdown)` | Return relative to max drawdown |
| **Recovery Factor** | `total_return / abs(max_drawdown)` | Ability to recover from drawdowns |

#### Trade Analytics

| Metric | Description |
|--------|-------------|
| **Average Holding Period** | Mean days held per position |
| **Turnover Rate** | Number of trades per year |
| **Consecutive Wins** | Longest winning streak |
| **Consecutive Losses** | Longest losing streak |
| **Largest Win** | Best single trade (%) |
| **Largest Loss** | Worst single trade (%) |
| **R-Multiple Distribution** | Distribution of returns in units of risk |

### 7.2 Metrics Calculation Implementation

```python
# app/backtest/analytics.py
from typing import List, Dict
from decimal import Decimal
from dataclasses import dataclass
import numpy as np

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""

    # Returns
    total_return: Decimal
    annualized_return: Decimal
    cumulative_returns: List[Decimal]

    # Risk-adjusted
    sharpe_ratio: Decimal
    sortino_ratio: Decimal
    calmar_ratio: Decimal

    # Risk
    max_drawdown: Decimal
    avg_drawdown: Decimal
    max_drawdown_duration: int  # days
    volatility: Decimal

    # Trade statistics
    total_trades: int
    win_rate: Decimal
    profit_factor: Decimal
    avg_win: Decimal
    avg_loss: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    expectancy: Decimal

    # Streaks
    max_consecutive_wins: int
    max_consecutive_losses: int

    # Time-based
    avg_holding_period: Decimal  # days
    turnover_rate: Decimal  # trades per year

    def to_dict(self) -> Dict:
        return {
            'total_return': float(self.total_return),
            'annualized_return': float(self.annualized_return),
            'sharpe_ratio': float(self.sharpe_ratio),
            'sortino_ratio': float(self.sortino_ratio),
            'calmar_ratio': float(self.calmar_ratio),
            'max_drawdown': float(self.max_drawdown),
            'volatility': float(self.volatility),
            'win_rate': float(self.win_rate),
            'profit_factor': float(self.profit_factor),
            'total_trades': self.total_trades,
            # ... all fields
        }

class PerformanceAnalyzer:
    """Calculate comprehensive performance metrics"""

    @staticmethod
    def calculate(trades: List[Dict], equity_curve: List[Dict],
                  initial_capital: Decimal, trading_days: int) -> PerformanceMetrics:
        """Calculate all performance metrics"""

        # Basic trade statistics
        total_trades = len(trades)
        winning_trades = [t for t in trades if t['net_pnl'] > 0]
        losing_trades = [t for t in trades if t['net_pnl'] < 0]

        win_rate = Decimal(len(winning_trades) / total_trades) if total_trades > 0 else Decimal(0)

        # P&L statistics
        gross_profit = sum(t['net_pnl'] for t in winning_trades) if winning_trades else Decimal(0)
        gross_loss = abs(sum(t['net_pnl'] for t in losing_trades)) if losing_trades else Decimal(0)

        profit_factor = gross_profit / gross_loss if gross_loss > 0 else Decimal('inf')

        avg_win = gross_profit / len(winning_trades) if winning_trades else Decimal(0)
        avg_loss = gross_loss / len(losing_trades) if losing_trades else Decimal(0)

        largest_win = max([t['return_pct'] for t in winning_trades]) if winning_trades else Decimal(0)
        largest_loss = min([t['return_pct'] for t in losing_trades]) if losing_trades else Decimal(0)

        expectancy = (win_rate * avg_win) - ((Decimal(1) - win_rate) * avg_loss)

        # Returns
        final_value = equity_curve[-1]['total_value'] if equity_curve else initial_capital
        total_return = ((final_value - initial_capital) / initial_capital) * 100

        years = Decimal(trading_days) / Decimal(252)
        annualized_return = ((final_value / initial_capital) ** (Decimal(1) / years) - 1) * 100

        # Daily returns
        daily_returns = [
            (equity_curve[i]['total_value'] / equity_curve[i-1]['total_value'] - 1)
            for i in range(1, len(equity_curve))
        ]

        # Volatility (annualized)
        volatility = Decimal(np.std(daily_returns)) * Decimal(np.sqrt(252)) * 100 if daily_returns else Decimal(0)

        # Sharpe ratio (assumes 0 risk-free rate)
        mean_return = Decimal(np.mean(daily_returns)) if daily_returns else Decimal(0)
        std_return = Decimal(np.std(daily_returns)) if daily_returns else Decimal(0)
        sharpe_ratio = (mean_return / std_return) * Decimal(np.sqrt(252)) if std_return > 0 else Decimal(0)

        # Sortino ratio (downside deviation)
        downside_returns = [r for r in daily_returns if r < 0]
        downside_deviation = Decimal(np.std(downside_returns)) if downside_returns else Decimal(0)
        sortino_ratio = (mean_return / downside_deviation) * Decimal(np.sqrt(252)) if downside_deviation > 0 else Decimal(0)

        # Drawdown analysis
        peak = equity_curve[0]['total_value']
        max_drawdown = Decimal(0)
        max_dd_duration = 0
        current_dd_duration = 0
        total_drawdown = Decimal(0)
        num_drawdowns = 0

        for snapshot in equity_curve:
            if snapshot['total_value'] > peak:
                peak = snapshot['total_value']
                if current_dd_duration > 0:
                    num_drawdowns += 1
                current_dd_duration = 0
            else:
                current_dd_duration += 1

            drawdown = (peak - snapshot['total_value']) / peak * 100
            total_drawdown += drawdown

            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_dd_duration = max(max_dd_duration, current_dd_duration)

        avg_drawdown = total_drawdown / len(equity_curve) if equity_curve else Decimal(0)

        # Calmar ratio
        calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else Decimal(0)

        # Streaks
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_win_streak = 0
        current_loss_streak = 0

        for trade in trades:
            if trade['net_pnl'] > 0:
                current_win_streak += 1
                current_loss_streak = 0
                max_consecutive_wins = max(max_consecutive_wins, current_win_streak)
            else:
                current_loss_streak += 1
                current_win_streak = 0
                max_consecutive_losses = max(max_consecutive_losses, current_loss_streak)

        # Holding period
        avg_holding_period = Decimal(sum(t['holding_days'] for t in trades) / total_trades) if total_trades > 0 else Decimal(0)

        # Turnover rate (trades per year)
        turnover_rate = Decimal(total_trades) / years if years > 0 else Decimal(0)

        return PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            cumulative_returns=[Decimal(s['cumulative_return']) for s in equity_curve],
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            avg_drawdown=avg_drawdown,
            max_drawdown_duration=max_dd_duration,
            volatility=volatility,
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            expectancy=expectancy,
            max_consecutive_wins=max_consecutive_wins,
            max_consecutive_losses=max_consecutive_losses,
            avg_holding_period=avg_holding_period,
            turnover_rate=turnover_rate
        )
```

### 7.3 Visualization Data Structures

#### Equity Curve Chart
```json
{
  "chart_type": "equity_curve",
  "data": {
    "dates": ["2023-01-04", "2023-01-05", "..."],
    "portfolio_value": [1000000, 1002000, 1005000],
    "drawdown": [0, 0, -1.2],
    "benchmark": [1000000, 1001000, 1003000]  // Optional: market index
  },
  "config": {
    "title": "Portfolio Equity Curve",
    "y_axis_label": "Portfolio Value (¥)",
    "show_drawdown": true
  }
}
```

#### Monthly Returns Heatmap
```json
{
  "chart_type": "monthly_returns",
  "data": {
    "2023": [1.2, 2.5, -0.8, 3.1, 1.7, -1.2, 2.8, 0.5, -2.1, 1.9, 3.2, 0.8],
    "2024": [2.1, 1.8, 0.9, ...]
  },
  "config": {
    "title": "Monthly Returns (%)",
    "colormap": "RdYlGn"
  }
}
```

#### Trade Distribution
```json
{
  "chart_type": "trade_distribution",
  "data": {
    "bins": [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8, 10],
    "frequencies": [2, 5, 8, 12, 15, 20, 18, 10, 6, 4]
  },
  "config": {
    "title": "Distribution of Trade Returns",
    "x_axis_label": "Return (%)",
    "y_axis_label": "Frequency"
  }
}
```

---

## 8. LLM Integration

### 8.1 Architecture

```
┌──────────────┐
│ User Input   │  "I want to catch stocks with RSI sharp drop..."
└──────┬───────┘
       │
       ▼
┌──────────────────────┐
│ LLM Orchestrator     │
│ - Classify intent    │
│ - Route to handler   │
└──────┬───────────────┘
       │
       ├─────────────────┐
       │                 │
       ▼                 ▼
┌──────────────┐  ┌─────────────────┐
│ Strategy     │  │ Parameter       │
│ Generator    │  │ Tweaker         │
│ (GPT-4/      │  │ (Quick edits)   │
│  Claude-3)   │  │                 │
└──────┬───────┘  └────┬────────────┘
       │               │
       ▼               ▼
┌──────────────────────┐
│ Strategy Validator   │
│ - Check syntax       │
│ - Verify indicators  │
│ - Suggest fixes      │
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│ Backtest Executor    │
└──────────────────────┘
```

### 8.2 LLM Prompt Templates

#### Strategy Generation Prompt

```python
STRATEGY_GENERATION_PROMPT = """
You are an expert quantitative trading strategy designer. Your task is to convert natural language trading ideas into structured strategy definitions.

**User Request:**
{user_prompt}

**Available Indicators:**
- price_change: Compare price changes over time
- volume: Trading volume analysis
- ma (moving average): Simple moving average
- rsi (Relative Strength Index): Momentum oscillator (0-100)
- macd (Moving Average Convergence Divergence): Trend-following momentum
- bollinger_bands: Volatility bands
- quantity_relative_ratio: Volume relative to recent average
- turnover_rate: Trading activity as % of market cap
- circulation_capital: Market capitalization

**Available Condition Types:**
- Entry conditions: When to open a position
- Exit conditions: When to close a position (take_profit, stop_loss, time_based, indicator-based)

**Output Format:**
Return a JSON object following this schema:
```json
{
  "strategy": {
    "name": "Descriptive strategy name",
    "version": "1.0.0",
    "description": "What this strategy does and why",
    "parameters": {
      "param_name": default_value
    },
    "entry_conditions": {
      "operator": "AND",
      "conditions": [...]
    },
    "exit_conditions": {
      "operator": "OR",
      "conditions": [...]
    },
    "position_sizing": {
      "max_positions": 20,
      "method": "equal_weight"
    },
    "risk_management": {
      "max_portfolio_drawdown": 20.0
    }
  },
  "reasoning": "Explanation of design choices",
  "warnings": ["List any limitations or concerns"]
}
```

**Design Principles:**
1. **Risk Management First**: Always include stop_loss and take_profit
2. **Clear Entry/Exit**: Conditions should be unambiguous
3. **Parameter-driven**: Use {{parameters}} for easy tweaking
4. **Realistic Constraints**: Consider slippage, commissions, liquidity

**Example Output:**
For input: "Buy stocks gaining 3-5% with high volume"
```json
{
  "strategy": {
    "name": "Volume-Confirmed Momentum",
    "entry_conditions": {
      "operator": "AND",
      "conditions": [
        {"type": "price_change", "comparison": "between", "value": [3.0, 5.0], "unit": "percent"},
        {"type": "indicator", "name": "quantity_relative_ratio", "comparison": ">=", "value": 1.5}
      ]
    },
    "exit_conditions": {
      "operator": "OR",
      "conditions": [
        {"type": "take_profit", "value": 10.0},
        {"type": "stop_loss", "value": -5.0},
        {"type": "time_based", "max_holding_days": 30}
      ]
    }
  },
  "reasoning": "High volume confirms genuine interest...",
  "warnings": ["Consider adding MA filter to avoid choppy markets"]
}
```

Now process the user request and return a valid strategy JSON.
"""

#### Parameter Tweaking Prompt

```python
PARAMETER_TWEAK_PROMPT = """
You are modifying an existing trading strategy. The user wants to make a small change to a parameter.

**Current Strategy:**
{current_strategy_json}

**User Request:**
{user_request}

**Instructions:**
1. Identify which parameter the user wants to change
2. Update ONLY that parameter
3. Return the modified strategy JSON
4. Keep everything else identical

**Examples:**
- "Raise target price to 15%" → Update take_profit to 15.0
- "Lower stop loss to 3%" → Update stop_loss to -3.0
- "Increase holding period to 45 days" → Update max_holding_days to 45

Return the updated strategy JSON.
"""
```

### 8.3 Implementation

```python
# app/llm/strategy_generator.py
from typing import Dict, List, Optional
from pydantic import BaseModel
import json

class LLMConfig(BaseModel):
    provider: str = "openai"  # or "anthropic"
    model: str = "gpt-4"
    temperature: float = 0.7
    max_tokens: int = 2000

class StrategyGenerator:
    """Generate trading strategies using LLMs"""

    def __init__(self, config: LLMConfig):
        self.config = config

        if config.provider == "openai":
            from openai import OpenAI
            self.client = OpenAI()
        elif config.provider == "anthropic":
            from anthropic import Anthropic
            self.client = Anthropic()

    def generate_from_prompt(self, user_prompt: str,
                             base_strategy: Optional[Dict] = None) -> Dict:
        """Generate strategy from natural language"""

        if base_strategy:
            # Tweaking existing strategy
            prompt = PARAMETER_TWEAK_PROMPT.format(
                current_strategy_json=json.dumps(base_strategy, indent=2),
                user_request=user_prompt
            )
        else:
            # Creating new strategy
            prompt = STRATEGY_GENERATION_PROMPT.format(
                user_prompt=user_prompt
            )

        # Call LLM
        if self.config.provider == "openai":
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": "You are an expert quantitative trading strategist."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                response_format={"type": "json_object"}  # Ensure JSON output
            )

            strategy_json = json.loads(response.choices[0].message.content)

        elif self.config.provider == "anthropic":
            response = self.client.messages.create(
                model=self.config.model,
                max_tokens=self.config.max_tokens,
                temperature=self.config.temperature,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Extract JSON from response
            content = response.content[0].text
            strategy_json = json.loads(content)

        # Validate strategy
        from app.strategy.parser import StrategyParser
        try:
            StrategyParser.from_dict(strategy_json['strategy'])
            strategy_json['validation_status'] = 'valid'
            strategy_json['validation_errors'] = []
        except Exception as e:
            strategy_json['validation_status'] = 'invalid'
            strategy_json['validation_errors'] = [str(e)]

        return strategy_json

    def suggest_improvements(self, strategy: Dict, backtest_results: Dict) -> List[str]:
        """Suggest strategy improvements based on backtest results"""

        prompt = f"""
Analyze this trading strategy and its backtest results. Suggest specific improvements.

**Strategy:**
{json.dumps(strategy, indent=2)}

**Backtest Results:**
- Total Return: {backtest_results['total_return']}%
- Sharpe Ratio: {backtest_results['sharpe_ratio']}
- Max Drawdown: {backtest_results['max_drawdown']}%
- Win Rate: {backtest_results['win_rate']}%
- Total Trades: {backtest_results['total_trades']}

**Provide 3-5 actionable suggestions to improve risk-adjusted returns.**
"""

        if self.config.provider == "openai":
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.8
            )
            suggestions = response.choices[0].message.content

        return suggestions.split('\n')
```

### 8.4 Safety and Validation

```python
# app/llm/validator.py
class StrategyValidator:
    """Validate LLM-generated strategies"""

    @staticmethod
    def validate_conditions(strategy: Dict) -> List[str]:
        """Check for common mistakes"""
        warnings = []

        # Check for missing risk management
        exit_conditions = strategy.get('exit_conditions', {}).get('conditions', [])

        has_stop_loss = any(c.get('type') == 'stop_loss' for c in exit_conditions)
        has_take_profit = any(c.get('type') == 'take_profit' for c in exit_conditions)

        if not has_stop_loss:
            warnings.append("WARNING: No stop_loss defined - unlimited downside risk")

        if not has_take_profit:
            warnings.append("INFO: No take_profit defined - may miss profit opportunities")

        # Check for unrealistic parameters
        for condition in exit_conditions:
            if condition.get('type') == 'stop_loss':
                value = abs(condition.get('value', 0))
                if value > 50:
                    warnings.append(f"WARNING: Stop loss of {value}% is very large")

            if condition.get('type') == 'take_profit':
                value = condition.get('value', 0)
                if value > 100:
                    warnings.append(f"WARNING: Take profit of {value}% may be unrealistic")

        # Check for conflicting conditions
        # ... more validation logic

        return warnings
```

---

## 9. Frontend Integration

### 9.1 API Response Formats for Frontend

#### Equity Curve Data
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "metadata": {
    "strategy_name": "Momentum Breakout V1",
    "start_date": "2023-01-01",
    "end_date": "2025-11-17",
    "initial_capital": 1000000.00
  },
  "series": {
    "dates": ["2023-01-04", "2023-01-05", ...],
    "values": [1000000, 1002000, 1005000, ...],
    "drawdowns": [0, 0, -1.2, ...],
    "positions": [0, 1, 2, ...]
  },
  "annotations": [
    {
      "date": "2023-03-15",
      "type": "max_drawdown",
      "value": -12.3,
      "label": "Max Drawdown: -12.3%"
    },
    {
      "date": "2025-11-17",
      "type": "final_value",
      "value": 1425000,
      "label": "Final: ¥1,425,000"
    }
  ]
}
```

#### Trade Timeline (Playback)
```json
{
  "run_id": "660e8400-e29b-41d4-a716-446655440001",
  "playback_speed_options": [0.5, 1, 2, 5, 10],  // X multipliers
  "events": [
    {
      "timestamp": "2023-01-05T00:00:00Z",
      "type": "position_opened",
      "data": {
        "stock_code": "600000",
        "stock_name": "浦发银行",
        "entry_price": 10.50,
        "shares": 9500,
        "position_value": 99750.00,
        "reason": "Entry signal matched: price_change=4.2%, volume_ratio=1.8"
      }
    },
    {
      "timestamp": "2023-01-15T00:00:00Z",
      "type": "position_closed",
      "data": {
        "stock_code": "600000",
        "exit_price": 11.25,
        "exit_reason": "take_profit",
        "net_pnl": 6965.00,
        "return_pct": 6.98,
        "holding_days": 10
      }
    },
    {
      "timestamp": "2023-02-20T00:00:00Z",
      "type": "portfolio_snapshot",
      "data": {
        "total_value": 1025000,
        "cash": 800000,
        "holdings_value": 225000,
        "open_positions": 3,
        "cumulative_return": 2.5
      }
    }
  ]
}
```

#### Strategy Comparison Table
```json
{
  "comparison_id": "770e8400-e29b-41d4-a716-446655440004",
  "strategies": [
    {
      "name": "Momentum Breakout V1",
      "run_id": "660e8400-e29b-41d4-a716-446655440001"
    },
    {
      "name": "Momentum Breakout V2",
      "run_id": "660e8400-e29b-41d4-a716-446655440002"
    }
  ],
  "metrics": [
    {
      "name": "Total Return (%)",
      "values": [42.5, 38.2],
      "best_index": 0,
      "format": "percent"
    },
    {
      "name": "Sharpe Ratio",
      "values": [1.8, 1.9],
      "best_index": 1,
      "format": "decimal"
    },
    {
      "name": "Max Drawdown (%)",
      "values": [-12.3, -10.5],
      "best_index": 1,
      "format": "percent",
      "lower_is_better": true
    }
  ],
  "chart_data": {
    "equity_curves": {
      "dates": ["2023-01-04", ...],
      "series": [
        {"name": "V1", "values": [1000000, ...]},
        {"name": "V2", "values": [1000000, ...]}
      ]
    }
  }
}
```

### 9.2 WebSocket Event Stream

```python
# app/api/websocket.py
from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict
import asyncio
import json

class BacktestStreamer:
    """Stream backtest progress in real-time"""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, run_id: str):
        await websocket.accept()
        self.active_connections[run_id] = websocket

    def disconnect(self, run_id: str):
        if run_id in self.active_connections:
            del self.active_connections[run_id]

    async def send_progress(self, run_id: str, progress_data: Dict):
        """Send progress update to connected client"""
        if run_id in self.active_connections:
            await self.active_connections[run_id].send_json(progress_data)

    async def stream_backtest(self, run_id: str):
        """Stream backtest execution progress"""
        # This would be called from BacktestEngine
        # Example progress updates:

        # 1. Start
        await self.send_progress(run_id, {
            "type": "started",
            "run_id": run_id,
            "start_date": "2023-01-01",
            "end_date": "2025-11-17",
            "total_days": 730
        })

        # 2. Progress updates (every 10% or every 50 days)
        await self.send_progress(run_id, {
            "type": "progress",
            "progress": 25.0,
            "current_date": "2023-07-15",
            "trades_completed": 30,
            "current_value": 1050000.00
        })

        # 3. Trade events
        await self.send_progress(run_id, {
            "type": "trade_opened",
            "stock_code": "600000",
            "stock_name": "浦发银行",
            "entry_price": 10.50,
            "shares": 9500,
            "date": "2023-01-05"
        })

        await self.send_progress(run_id, {
            "type": "trade_closed",
            "stock_code": "600000",
            "exit_price": 11.25,
            "net_pnl": 6965.00,
            "return_pct": 6.98,
            "date": "2023-01-15"
        })

        # 4. Completion
        await self.send_progress(run_id, {
            "type": "completed",
            "final_value": 1425000.00,
            "total_return": 42.5,
            "total_trades": 145,
            "execution_time_seconds": 7.3
        })

streamer = BacktestStreamer()

@app.websocket("/api/v1/backtest/runs/{run_id}/stream")
async def websocket_endpoint(websocket: WebSocket, run_id: str):
    await streamer.connect(websocket, run_id)
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        streamer.disconnect(run_id)
```

### 9.3 Frontend Component Specifications

#### React Component Example (Equity Curve Chart)

```typescript
// EquityCurveChart.tsx
interface EquityCurveData {
  dates: string[];
  values: number[];
  drawdowns: number[];
  positions: number[];
}

interface EquityCurveChartProps {
  runId: string;
  data: EquityCurveData;
  annotations?: Annotation[];
}

const EquityCurveChart: React.FC<EquityCurveChartProps> = ({ runId, data, annotations }) => {
  // Using recharts or d3.js
  return (
    <LineChart width={800} height={400} data={chartData}>
      <XAxis dataKey="date" />
      <YAxis />
      <Tooltip />
      <Legend />
      <Line type="monotone" dataKey="value" stroke="#8884d8" name="Portfolio Value" />
      <Line type="monotone" dataKey="drawdown" stroke="#ff0000" name="Drawdown" />
      {annotations.map(ann => (
        <ReferenceLine key={ann.date} x={ann.date} stroke="green" label={ann.label} />
      ))}
    </LineChart>
  );
};
```

#### Playback Controller

```typescript
// BacktestPlayback.tsx
interface PlaybackState {
  currentIndex: number;
  isPlaying: boolean;
  speed: number;  // 1x, 2x, 5x, 10x
}

const BacktestPlayback: React.FC<{events: TradeEvent[]}> = ({ events }) => {
  const [state, setState] = useState<PlaybackState>({
    currentIndex: 0,
    isPlaying: false,
    speed: 1
  });

  useEffect(() => {
    if (state.isPlaying && state.currentIndex < events.length) {
      const timeout = setTimeout(() => {
        setState(s => ({ ...s, currentIndex: s.currentIndex + 1 }));
      }, 1000 / state.speed);

      return () => clearTimeout(timeout);
    }
  }, [state.isPlaying, state.currentIndex, state.speed]);

  const currentEvent = events[state.currentIndex];

  return (
    <div>
      <div className="playback-controls">
        <button onClick={() => setState(s => ({ ...s, isPlaying: !s.isPlaying }))}>
          {state.isPlaying ? 'Pause' : 'Play'}
        </button>
        <select value={state.speed} onChange={e => setState(s => ({ ...s, speed: Number(e.target.value) }))}>
          <option value={0.5}>0.5x</option>
          <option value={1}>1x</option>
          <option value={2}>2x</option>
          <option value={5}>5x</option>
          <option value={10}>10x</option>
        </select>
      </div>

      <div className="current-event">
        <h3>{currentEvent.type}</h3>
        <pre>{JSON.stringify(currentEvent.data, null, 2)}</pre>
      </div>

      <div className="progress-bar">
        <progress value={state.currentIndex} max={events.length} />
        <span>{state.currentIndex} / {events.length}</span>
      </div>
    </div>
  );
};
```

---

## 10. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-2)

#### Goals
- ✅ Database schema for backtest tables
- ✅ Basic execution engine (entry/exit logic)
- ✅ Position and portfolio management
- ✅ Trade logging

#### Deliverables
1. **Database Migration** (`app/sql/backtest_tables.sql`)
   - Create `strategy`, `backtest_run`, `trade`, `portfolio_snapshot` tables
   - Add indexes and partitions
   - Materialized view for daily metrics

2. **Core Engine** (`app/backtest/engine.py`)
   - `BacktestEngine` class
   - `Portfolio` class
   - `Position` class
   - `BacktestResult` dataclass

3. **Signal Engine** (`app/backtest/signals.py`)
   - `SignalEngine` class
   - SQL query builder for entry conditions
   - Exit condition evaluator

4. **Basic CLI** (extend `app/main.py`)
   ```bash
   stock-picker backtest --strategy TAIL_SCRAPER --start 2023-01-01 --end 2025-11-17
   ```

#### Acceptance Criteria
- ✅ Can run backtest for existing TAIL_SCRAPER strategy
- ✅ Trade log stored in database
- ✅ Basic P&L calculation working
- ✅ Unit tests for Position, Portfolio classes

---

### Phase 2: Strategy DSL (Weeks 3-4)

#### Goals
- ✅ JSON/YAML strategy definition
- ✅ Strategy parser and validator
- ✅ Dynamic SQL query builder
- ✅ Strategy storage

#### Deliverables
1. **Strategy Parser** (`app/strategy/parser.py`)
   - Pydantic models for strategy schema
   - YAML/JSON parser
   - Parameter substitution

2. **Strategy Validator** (`app/strategy/validator.py`)
   - Condition type validation
   - Indicator existence check
   - Parameter reference validation

3. **Query Builder** (`app/strategy/query_builder.py`)
   - Convert strategy conditions to SQL
   - Support all condition types
   - Optimize for PostgreSQL

4. **Strategy CRUD** (`app/db/strategy.py`)
   - Create/read/update/delete strategies
   - Version management
   - Template strategies

#### Acceptance Criteria
- ✅ Can define strategy in YAML
- ✅ Parser validates strategy structure
- ✅ Query builder generates correct SQL
- ✅ Can run backtest with custom strategy

---

### Phase 3: REST API (Weeks 5-6)

#### Goals
- ✅ RESTful endpoints for all operations
- ✅ Async backtest execution
- ✅ Pagination and filtering
- ✅ Error handling

#### Deliverables
1. **FastAPI App** (`app/api/main.py`)
   - API server setup
   - CORS configuration
   - Authentication (JWT)

2. **Backtest Endpoints** (`app/api/backtest.py`)
   - POST /api/v1/backtest/run
   - GET /api/v1/backtest/runs/{id}
   - GET /api/v1/backtest/runs/{id}/trades
   - GET /api/v1/backtest/runs/{id}/equity-curve

3. **Strategy Endpoints** (`app/api/strategies.py`)
   - GET /api/v1/backtest/strategies
   - POST /api/v1/backtest/strategies
   - GET /api/v1/backtest/strategies/{id}
   - PUT /api/v1/backtest/strategies/{id}

4. **Background Tasks** (`app/api/tasks.py`)
   - Celery/RQ for async execution
   - Progress tracking

#### Acceptance Criteria
- ✅ Can create strategy via API
- ✅ Can execute backtest via API
- ✅ Can fetch results via API
- ✅ API documented with OpenAPI/Swagger

---

### Phase 4: Analytics & Visualization (Weeks 7-8)

#### Goals
- ✅ Comprehensive performance metrics
- ✅ Strategy comparison
- ✅ Data structures for frontend charts

#### Deliverables
1. **Performance Analyzer** (`app/backtest/analytics.py`)
   - Sharpe ratio, Sortino, Calmar
   - Drawdown analysis
   - Trade statistics

2. **Comparison Engine** (`app/backtest/comparison.py`)
   - Multi-strategy comparison
   - Statistical significance tests
   - Ranking algorithms

3. **Chart Data Generators** (`app/api/charts.py`)
   - Equity curve data
   - Monthly returns heatmap
   - Trade distribution
   - Drawdown chart

4. **Benchmark Integration** (`app/backtest/benchmark.py`)
   - Index data (CSI 300, SSE 50)
   - Strategy vs. benchmark comparison
   - Alpha/Beta calculation

#### Acceptance Criteria
- ✅ All metrics calculated correctly
- ✅ Can compare 2+ strategies
- ✅ Frontend can render charts
- ✅ Benchmark comparison working

---

### Phase 5: LLM Integration (Weeks 9-10)

#### Goals
- ✅ Natural language strategy generation
- ✅ Parameter tweaking
- ✅ Strategy improvement suggestions

#### Deliverables
1. **LLM Strategy Generator** (`app/llm/generator.py`)
   - OpenAI GPT-4 integration
   - Anthropic Claude integration
   - Prompt templates

2. **LLM Endpoint** (`app/api/llm.py`)
   - POST /api/v1/backtest/strategies/generate
   - Input: natural language
   - Output: validated strategy JSON

3. **Safety & Validation** (`app/llm/validator.py`)
   - Check for unrealistic parameters
   - Warn about missing risk management
   - Suggest improvements

4. **LLM Advisor** (`app/llm/advisor.py`)
   - Analyze backtest results
   - Suggest optimizations
   - Explain strategy performance

#### Acceptance Criteria
- ✅ Can generate strategy from text
- ✅ LLM output validated
- ✅ Can tweak existing strategy parameters
- ✅ Safety checks prevent dangerous strategies

---

### Phase 6: Frontend Integration (Weeks 11-12)

#### Goals
- ✅ WebSocket streaming
- ✅ Playback-able results
- ✅ Interactive charts

#### Deliverables
1. **WebSocket Server** (`app/api/websocket.py`)
   - Real-time progress updates
   - Trade event streaming
   - Connection management

2. **Frontend API Specs** (`docs/frontend_api.md`)
   - JSON response formats
   - Chart data structures
   - WebSocket message formats

3. **Example Frontend** (`frontend/` - separate project)
   - React app (create-react-app)
   - Equity curve chart (recharts)
   - Playback controller
   - Strategy comparison table

4. **Documentation** (`docs/integration_guide.md`)
   - API usage examples
   - WebSocket client examples
   - Chart integration guide

#### Acceptance Criteria
- ✅ WebSocket streams backtest progress
- ✅ Frontend can render equity curve
- ✅ Playback controller works
- ✅ Strategy comparison visualized

---

### Phase 7: Optimization & Production (Weeks 13-14)

#### Goals
- ✅ Performance optimization
- ✅ ClickHouse migration (optional)
- ✅ Production deployment

#### Deliverables
1. **Database Optimization**
   - Partition existing tables
   - Add missing indexes
   - Refresh materialized views

2. **ClickHouse Integration** (Optional)
   - Setup ClickHouse instance
   - CDC pipeline from PostgreSQL
   - Migrate analytical queries

3. **Caching Layer** (`app/cache/`)
   - Redis for frequently accessed data
   - Cache invalidation strategy

4. **Production Deployment**
   - Docker containers
   - Kubernetes manifests
   - CI/CD pipeline (GitHub Actions)
   - Monitoring (Prometheus + Grafana)

5. **Documentation**
   - API reference (OpenAPI)
   - User guide
   - Deployment guide

#### Acceptance Criteria
- ✅ Backtest executes in < 10 seconds
- ✅ API handles 100+ concurrent requests
- ✅ Production-ready deployment
- ✅ Monitoring and alerting configured

---

### Timeline Summary

| Phase | Duration | Key Milestone |
|-------|----------|---------------|
| **Phase 1: Foundation** | Weeks 1-2 | ✅ Basic backtest engine working |
| **Phase 2: Strategy DSL** | Weeks 3-4 | ✅ Custom strategies supported |
| **Phase 3: REST API** | Weeks 5-6 | ✅ API endpoints functional |
| **Phase 4: Analytics** | Weeks 7-8 | ✅ Comprehensive metrics & comparison |
| **Phase 5: LLM Integration** | Weeks 9-10 | ✅ Natural language strategy creation |
| **Phase 6: Frontend** | Weeks 11-12 | ✅ Playback & visualization |
| **Phase 7: Production** | Weeks 13-14 | ✅ Production deployment |

**Total Duration:** ~14 weeks (3.5 months)

---

## 11. Success Criteria

### Functional Requirements
- ✅ Can define strategies in JSON/YAML
- ✅ Can execute backtests against 3 years of data
- ✅ Results match real trading (99.9% accuracy)
- ✅ Can compare multiple strategies
- ✅ Can generate strategies from natural language
- ✅ Can visualize equity curves and trade logs

### Non-Functional Requirements
- ✅ Backtest execution: < 10 seconds for 3 years
- ✅ API latency: < 100ms for queries
- ✅ Supports 5000+ stocks
- ✅ 99.5% uptime
- ✅ Comprehensive test coverage (>80%)

### User Experience
- ✅ Users can type human sentences and get strategies
- ✅ Users can view every trade in backtest
- ✅ Users can playback strategy execution
- ✅ Users can tweak parameters easily
- ✅ Users get clear error messages

---

**Design Document Complete**

**Status:** Ready for review and approval

**Next Steps:**
1. Review design with stakeholders
2. Refine based on feedback
3. Create detailed task breakdown for Phase 1
4. Begin implementation

**Contact:** Claude AI Assistant
**Date:** 2025-11-17
