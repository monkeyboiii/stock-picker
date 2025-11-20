# Backtest Framework Usage Guide

## Overview

The backtesting framework allows you to test trading strategies against historical data and evaluate their performance.

## Features

- **Position & Portfolio Management**: Realistic tracking of positions, cash, and costs
- **Exit Conditions**: Take profit, stop loss, time limits
- **Performance Metrics**: Total return, Sharpe ratio, max drawdown, win rate, profit factor
- **Database Integration**: Save and retrieve backtest results
- **CLI Interface**: Easy-to-use command-line interface

## Quick Start

### 1. Initialize Database

First, ensure your database has the backtest tables:

```bash
# Initialize database with stock data
stock-picker init -r -lll

# The backtest tables will be created automatically
```

### 2. Run a Backtest via CLI

```bash
# Basic backtest
stock-picker backtest --start 2025-01-01 --end 2025-03-31

# With custom parameters
stock-picker backtest \
  --start 2025-01-01 \
  --end 2025-03-31 \
  --capital 1000000 \
  --max-positions 20 \
  --take-profit 10.0 \
  --stop-loss -5.0 \
  --max-hold-days 30 \
  --save  # Save results to database
```

### 3. Run a Backtest via Python

```python
from datetime import date
from decimal import Decimal
from app.backtest.engine import BacktestEngine
from app.backtest.signals import StrategyBuilder
from app.db.engine import engine_from_env

# Create engine
engine = engine_from_env()

# Define strategy
strategy_def = StrategyBuilder.tail_scraper_strategy(
    take_profit_pct=10.0,
    stop_loss_pct=-5.0,
    max_holding_days=30,
)

# Initialize backtest
backtest = BacktestEngine(
    strategy_definition=strategy_def,
    start_date=date(2025, 1, 1),
    end_date=date(2025, 3, 31),
    initial_capital=Decimal("1000000"),
    max_positions=20,
)

# Run backtest
result = backtest.run(engine)

# View results
print(result)
```

## CLI Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--start` | Start date (YYYY-MM-DD) | **Required** |
| `--end` | End date (YYYY-MM-DD) | **Required** |
| `--capital` | Initial capital | 1,000,000 |
| `--max-positions` | Maximum concurrent positions | 20 |
| `--take-profit` | Take profit percentage | 10.0% |
| `--stop-loss` | Stop loss percentage | -5.0% |
| `--max-hold-days` | Maximum holding days | 30 |
| `--save` | Save results to database | False |
| `--strategy` | Strategy name | tail_scraper |

## Understanding Results

### Performance Metrics

- **Total Return**: Overall portfolio return (%)
- **Sharpe Ratio**: Risk-adjusted return (annualized)
- **Max Drawdown**: Maximum peak-to-trough decline (%)
- **Win Rate**: Percentage of profitable trades
- **Profit Factor**: Gross profit / Gross loss
- **Avg Win/Loss**: Average P&L per winning/losing trade

### Example Output

```
Backtest Results:
  Total Trades: 150
  Win Rate: 65.33%
  Total Return: 25.67%
  Sharpe Ratio: 1.85
  Max Drawdown: 8.45%
  Profit Factor: 2.15
  Avg Win: ¥8,450.23
  Avg Loss: ¥-3,920.18
  Avg Holding Days: 12.5
```

## Strategy Configuration

### Built-in Strategy: Tail Scraper

The Tail Scraper strategy uses the following entry conditions (T2-T8 filters):

- **T2**: Quantity relative ratio ≥ 1.0
- **T3**: Turnover rate > 5.0%
- **T4**: Circulation capital between 20M - 2B CNY
- **T6**: Exclude ST stocks and stocks with "*"
- **T7**: MA250 exists and low price > MA250
- **T8**: Close price > Open price (positive day)

### Exit Conditions

- **Take Profit**: Exit when return exceeds configured percentage
- **Stop Loss**: Exit when loss exceeds configured percentage
- **Time Limit**: Exit after maximum holding days
- **Backtest End**: Exit all positions at end of backtest period

### Position Sizing

- Equal weighting across all positions
- Maximum positions limit enforced
- Automatic position sizing based on available cash

## Database Schema

### Strategy Table

Stores strategy definitions in JSON format.

### BacktestRun Table

Stores backtest configuration and computed results.

### Trade Table

Individual trade records with P&L details.

### PortfolioSnapshot Table

Daily portfolio state for equity curve generation.

## Cost Model

The backtest includes realistic trading costs:

- **Commission**: 0.03% per trade (entry + exit)
- **Slippage**: 0.1% per trade (entry + exit)

Example for 10,000 CNY position:
- Entry commission: 3 CNY
- Entry slippage: 10 CNY
- Exit commission: 3 CNY
- Exit slippage: 10 CNY
- **Total cost**: 26 CNY (0.26%)

## Viewing Saved Results

### Query Backtest Runs

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.db.models import BacktestRun, Strategy
from app.db.engine import engine_from_env

engine = engine_from_env()

with Session(engine) as session:
    # Get all backtest runs
    stmt = select(BacktestRun).order_by(BacktestRun.created_at.desc())
    runs = session.execute(stmt).scalars().all()

    for run in runs:
        print(f"Run {run.id}: {run.start_date} to {run.end_date}")
        print(f"  Return: {run.total_return}%")
        print(f"  Trades: {run.total_trades}")
```

### Query Trades

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.db.models import Trade
from app.db.engine import engine_from_env

engine = engine_from_env()

with Session(engine) as session:
    # Get trades for a specific run
    stmt = select(Trade).where(Trade.backtest_run_id == run_id)
    trades = session.execute(stmt).scalars().all()

    for trade in trades:
        print(f"{trade.stock_code}: {trade.net_pnl} CNY ({trade.return_pct}%)")
```

### Query Equity Curve

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.db.models import PortfolioSnapshot
from app.db.engine import engine_from_env

engine = engine_from_env()

with Session(engine) as session:
    # Get equity curve for a specific run
    stmt = select(PortfolioSnapshot).where(
        PortfolioSnapshot.backtest_run_id == run_id
    ).order_by(PortfolioSnapshot.snapshot_date)

    snapshots = session.execute(stmt).scalars().all()

    for snapshot in snapshots:
        print(f"{snapshot.snapshot_date}: {snapshot.total_value} CNY")
```

## Advanced Usage

### Custom Strategy (Future)

In the future, you'll be able to define custom strategies using JSON/YAML:

```yaml
strategy:
  name: My Custom Strategy
  version: 1.0.0

  parameters:
    take_profit: 15.0
    stop_loss: -7.0

  entry_conditions:
    - rsi(14) < 30
    - close > ma(50)
    - volume > ma(20, volume)

  exit_conditions:
    - take_profit: 15%
    - stop_loss: -7%
    - rsi(14) > 70
```

## Troubleshooting

### No trades generated

- Check that your database has historical `stock_daily` data for the backtest period
- Verify that stocks match the entry conditions (T2-T8 filters)
- Try a longer date range

### Database errors

```bash
# Reinitialize database
stock-picker init -r -lll
```

### Import errors

Ensure all dependencies are installed:

```bash
uv sync
```

## Next Steps

- Experiment with different exit parameters
- Analyze winning vs losing trades
- Compare performance across different time periods
- Visualize equity curves and drawdowns

## See Also

- [Backtest Framework Design](backtest_framework_design.md)
- [CLAUDE.md](../CLAUDE.md) - Project overview
- [Main README](../README.md) - General usage
