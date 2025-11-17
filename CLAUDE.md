# Stock Picker - AI Assistant Guide

## Project Overview

**Stock Picker** is a Python-based automated stock screening and analysis system designed to:
1. Refresh stock data from Chinese markets (Shanghai, Shenzhen, Beijing, Hong Kong)
2. Calculate derived metrics (moving averages, rankings)
3. Filter stocks based on technical criteria
4. Display results via Google Sheets and TDX format

**Tech Stack:**
- Python 3.13+ with SQLAlchemy 2.0
- PostgreSQL 16+ (required)
- Data source: AKShare library (Chinese stock data)
- Display: Google Sheets API, TDX format

**Repository:** `monkeyboiii/stock-picker`
**Current Branch:** `claude/claude-md-mi2opmgciru6v2ng-01LHAUVBT63ecZ3KiYQ8CKzm`

## Architecture & Directory Structure

```
stock-picker/
├── app/
│   ├── main.py                 # CLI entry point with argparse
│   ├── backtest/              # Backtesting functionality
│   │   └── feed.py            # FeedDaily table refresh logic
│   ├── constant/              # Enums and constants
│   │   ├── collection.py      # CollectionType enum (analyst, board, concept, industry, index)
│   │   ├── confirm.py         # User confirmation utilities
│   │   ├── exchange.py        # Market identifiers (MARKET_SUPPORTED)
│   │   ├── misc.py            # Miscellaneous constants
│   │   ├── schedule.py        # Trade day calculations
│   │   └── version.py         # Version info
│   ├── data/                  # External data fetching
│   │   └── ak.py              # AKShare wrapper for pulling market data
│   ├── db/                    # Database layer
│   │   ├── engine.py          # SQLAlchemy engine creation from env
│   │   ├── ingest.py          # Data ingestion logic
│   │   ├── load.py            # Initial data loading (markets, stocks, collections)
│   │   ├── materialized_view.py  # PostgreSQL materialized view management
│   │   └── models.py          # SQLAlchemy ORM models (Market, Stock, Collection, etc.)
│   ├── display/               # Output formatting
│   │   ├── google_sheet.py    # Google Sheets integration
│   │   ├── tdx.py             # TDX format output
│   │   └── utils.py           # Display utilities (formatting)
│   ├── filter/                # Stock filtering logic
│   │   ├── misc.py            # Filter utilities and StockFilter class
│   │   ├── tail_scraper.py    # Main filtering logic (T1-T8 conditions)
│   │   └── sql/               # SQL query templates
│   ├── profile/               # Performance profiling
│   │   └── tracer.py          # Execution time tracing
│   ├── sql/                   # SQL scripts
│   │   ├── feed_daily.sql
│   │   ├── ingest.sql
│   │   ├── inspect.sql
│   │   ├── materialized_view.sql
│   │   ├── update.sql
│   │   └── values.sql
│   └── utils/                 # Utility scripts
│       ├── backtest.py        # Backtesting utilities
│       ├── ingest.py          # Auto-fill historical data
│       ├── reset.py           # Database reset utilities
│       └── update.py          # Calculate derived metrics (ma250)
├── tests/                     # Test files
│   └── ingest.py
├── docs/                      # Documentation
│   └── backtest_framework_design.md  # Backtest system design
├── reports/                   # Generated reports (gitignored)
├── pyproject.toml             # Python project configuration (PEP 621 - single source of truth)
├── example.env                # Environment template
└── README.md                  # User documentation
```

## Database Schema

### Core Tables (SQLAlchemy Models in `app/db/models.py`)

1. **Market** - Exchange information
   - Fields: id, name, name_short, country, open/close times, currency
   - Represents stock exchanges (Shanghai, Shenzhen, etc.)

2. **Stock** - Individual stocks
   - Primary key: code (e.g., "600000")
   - Fields: name, market_id (FK)
   - Relations: Many-to-many with Collection

3. **Collection** - Stock groupings (industry, concept, index, etc.)
   - Primary key: code
   - Fields: name, type (CollectionType enum)
   - Types: ANALYST, BOARD, CONCEPT_BOARD, INDUSTRY_BOARD, INDEX

4. **RelationCollectionStock** - Many-to-many join table
   - Links stocks to collections

5. **StockDaily** - Daily stock data
   - Composite PK: (code, trade_day)
   - Price fields: open, high, low, close
   - Volume fields: volume, turnover, capital, circulation_capital
   - Technical: quantity_relative_ratio, turnover_rate
   - Derived: ma_250 (250-day moving average)
   - Updated timestamp: last_updated

6. **CollectionDaily** - Daily collection metrics
   - Composite PK: (code, trade_day)
   - Fields: price, change, change_rate, capital, turnover_rate
   - Fields: gainer_count, loser_count, top_gainer, top_gain

7. **FeedDaily** - Filtered stocks (output)
   - Composite PK: (code, trade_day, filter_id)
   - Contains convenient denormalized data for display
   - Methods: to_dataframe(), convert_to_feed() for Google Sheets

### Database States

The application operates through well-defined states:

1. **State 1**: Not initialized (empty tables)
2. **State 2**: Initialized with basic market/stock/collection info
3. **State 3**: Partially filled stock_daily and collection_daily
4. **State 4**: Up-to-date daily data, but derived metrics missing
5. **State 5**: Fully up-to-date (ready for automation)

**Goal:** Reach State 5 for daily automation.

## Development Workflow

### Environment Setup

1. **Database**: PostgreSQL 16+ required
   ```sql
   CREATE DATABASE stock_picker;
   ```

2. **Environment Variables** (`.env` file):
   ```env
   POSTGRES_USERNAME=<username>
   POSTGRES_PASSWORD=<password>
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DATABASE=<database_name>
   GOOGLE_SHEET_ID=<sheet_id>
   ```

3. **Dependencies** (using uv):

   **Install uv:**
   ```bash
   # Install uv if not already installed
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   **Sync dependencies:**
   ```bash
   # Sync all dependencies including dev (recommended for development)
   uv sync

   # Or sync only production dependencies
   uv sync --no-dev
   ```

   **Note:** All dependencies are defined in `pyproject.toml` (PEP 621 compliant).
   This project uses `uv sync` exclusively for dependency management.

4. **Google Credentials**:
   - Place `credentials.json` in root (gitignored)
   - Required for Google Sheets integration

### Running the Application

**Using uv run (recommended - no activation needed):**
```bash
uv run stock-picker [command] [options]
```

**Or activate the virtual environment:**
```bash
source .venv/bin/activate  # On Unix
# .venv\Scripts\activate   # On Windows
stock-picker [command] [options]
```

**Direct module execution (alternative):**
```bash
export PYTHONPATH=.
python app/main.py [command] [options]
```

**Common Commands:**

```bash
# Initialize database (State 1 → State 2)
uv run stock-picker init -r -lll
# -r: reset (drop existing tables)
# -lll: load level 3 (market, stocks, collections)

# Full daily run (State 2/3/4 → State 5)
uv run stock-picker run
# Ingest data, calculate metrics, filter, display

# Ingest only (State 2 → State 3/4)
uv run stock-picker run -t ingest

# Update metrics (State 4 → State 5)
uv run stock-picker run -t update

# Filter stocks
uv run stock-picker run -t filter

# Display results
uv run stock-picker run -t display

# Reset database
uv run stock-picker reset
```

**Logging Options:**
- `-v`: Increase verbosity (SUCCESS → INFO → DEBUG → TRACE)
- `-q`: Quiet mode (WARNING+)
- `-s`: Suppress mode (ERROR only)
- `-S`: Store full logs to file
- `-t`: Enable tracing logs

## Key Conventions & Patterns

### Code Style

1. **Type Hints**: Use Python type hints extensively
   - SQLAlchemy models use `Mapped[Type]` annotations
   - Function signatures include return types

2. **Imports**: Module-level imports with relative paths
   ```python
   from app.db.models import Stock, StockDaily
   from app.constant.collection import CollectionType
   ```

3. **Logging**: Use `loguru` logger throughout
   ```python
   from loguru import logger
   logger.info("Message")
   logger.debug("Detailed info")
   logger.error("Error occurred")
   ```

4. **Database Sessions**: Context managers for database operations
   ```python
   from sqlalchemy.orm import Session
   with Session(engine) as session:
       # operations
       session.commit()
   ```

### Database Patterns

1. **Engine Creation**: Always use `engine_from_env()`
   ```python
   from app.db.engine import engine_from_env
   engine = engine_from_env(echo=True)  # echo=True for SQL logging
   ```

2. **Model Methods**:
   - `to_dict()`: Convert ORM instance to dictionary
   - `to_dataframe()`: Class method for batch conversion to pandas

3. **Materialized Views**: PostgreSQL materialized views for performance
   - Check existence before querying: `check_mv_exists()`
   - Create daily: `daily_create_mv()`

### Stock Filtering Logic

The main filtering happens in `app/filter/tail_scraper.py` with conditions T1-T8:

- **T2**: Quantity relative ratio ≥ 1.0
- **T3**: Turnover rate > 5.0%
- **T4**: Circulation capital between 20M - 2B (in units of 万元)
- **T6**: Exclude ST stocks and stocks with "*"
- **T7**: MA250 exists and low price > MA250
- **T8**: Close price > Open price (positive day)

Uses PostgreSQL LATERAL joins for efficient querying.

### Data Flow

```
1. AKShare API (app/data/ak.py)
   ↓
2. Database Ingestion (app/db/ingest.py)
   ↓
3. Metric Calculation (app/utils/update.py)
   ↓
4. Stock Filtering (app/filter/tail_scraper.py)
   ↓
5. Feed Generation (app/backtest/feed.py)
   ↓
6. Display Output (app/display/google_sheet.py, app/display/tdx.py)
```

## Testing

### Test Structure

The project uses **pytest** for testing with comprehensive test coverage:

```
tests/
├── conftest.py           # Shared fixtures and test configuration
├── test_models.py        # Database model tests
├── test_constants.py     # Constants and enums tests
├── test_filters.py       # Filter utility tests
├── test_display.py       # Display utility tests
├── test_engine.py        # Database engine tests
└── ingest.py             # Legacy ingest tests (to be migrated)
```

### Running Tests

**Run all tests:**
```bash
# Using pytest (recommended)
pytest

# With verbose output
pytest -v

# With coverage report
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_models.py

# Run specific test class
pytest tests/test_models.py::TestStockDaily

# Run specific test function
pytest tests/test_models.py::TestStockDaily::test_stock_daily_creation
```

**Run tests with markers:**
```bash
# Skip integration tests
pytest -m "not integration"

# Skip slow tests
pytest -m "not slow"

# Run only integration tests
pytest -m "integration"
```

**Using uv:**
```bash
uv run pytest
uv run pytest --cov=app
```

### Test Coverage

**Current test modules:**
- ✅ **Models** (`test_models.py`): Market, Stock, Collection, StockDaily, FeedDaily
- ✅ **Constants** (`test_constants.py`): CollectionType, trading schedules, holidays
- ✅ **Filters** (`test_filters.py`): StockFilter enum, filter utilities
- ✅ **Display** (`test_display.py`): Number formatting, color generation
- ✅ **Engine** (`test_engine.py`): Database engine creation
- ⚠️ **Ingestion** (`ingest.py`): Legacy tests for data ingestion

**Areas for future improvement:**
- Filter logic (tail_scraper.py)
- Google Sheets integration
- TDX format generation
- Backtesting utilities

### Writing New Tests

**Use shared fixtures from conftest.py:**
```python
def test_example(db_session, sample_stock):
    """Example test using fixtures."""
    db_session.add(sample_stock)
    db_session.commit()

    retrieved = db_session.query(Stock).first()
    assert retrieved.code == "600000"
```

**Available fixtures:**
- `in_memory_engine`: SQLite in-memory database
- `db_session`: Database session for testing
- `mock_engine`, `mock_session`: Mock objects
- `sample_market`, `sample_stock`, `sample_collection`: Sample model instances
- `sample_stock_daily`, `sample_feed_daily`: Sample daily data
- `sample_dataframe`, `stock_daily_dataframe`: Sample pandas DataFrames
- `temp_env_vars`: Set temporary environment variables

**Test organization:**
```python
class TestFeatureName:
    """Tests for FeatureName."""

    def test_basic_functionality(self):
        """Test description."""
        # Arrange
        input_data = ...

        # Act
        result = function_under_test(input_data)

        # Assert
        assert result == expected_value

    @pytest.mark.parametrize("input,expected", [
        (1, 2),
        (2, 4),
    ])
    def test_with_parameters(self, input, expected):
        """Test with multiple parameter sets."""
        assert function(input) == expected
```

### Continuous Integration

Tests are configured to run automatically via:
- pytest.ini in pyproject.toml
- Markers for integration and slow tests
- Coverage reporting

### Test Best Practices

1. **Use descriptive test names** - Test name should describe what is being tested
2. **One assertion per test** - Keep tests focused (exceptions for related assertions)
3. **Use fixtures for setup** - Avoid repetitive setup code
4. **Test edge cases** - Include boundary values, empty inputs, None values
5. **Mock external dependencies** - Don't call real APIs or databases in unit tests
6. **Keep tests fast** - Use in-memory databases and mocks
7. **Use parametrize for similar tests** - Avoid code duplication

## Git Workflow

**Branch Naming Convention:**
- Feature branches: `claude/claude-md-{session-id}`
- Always push to designated branch (never main directly)

**Recent Important Commits:**
- `68b5ff9`: Feat/backtest - Backtesting functionality
- `3b4285c`: fix ma250 calculation - Fixed moving average bug
- `ad0e8e8`: collection implemented - Collection system complete

**Commit Message Style:**
- Imperative mood: "fix bug" not "fixed bug"
- Reference issue/PR numbers when applicable
- Clear, concise descriptions

## Common AI Assistant Tasks

### Adding New Features

1. **New Stock Filter Criteria:**
   - Modify `app/filter/tail_scraper.py`
   - Update `build_stmt_postgresql_lateral()` function
   - Add new condition to WHERE clause
   - Test with `python app/main.py run -t filter --dryrun`

2. **New Data Source:**
   - Add fetching logic to `app/data/ak.py`
   - Create corresponding model in `app/db/models.py`
   - Add ingestion logic in `app/db/ingest.py`

3. **New Display Format:**
   - Create new module in `app/display/`
   - Add corresponding command in `app/main.py` run subcommand
   - Follow pattern from `google_sheet.py` or `tdx.py`

### Debugging

1. **Database Issues:**
   - Check connection: `.env` file configured correctly
   - Verify tables: `python app/main.py init --dryrun`
   - Inspect data: Use SQL scripts in `app/sql/inspect.sql`

2. **Data Ingestion Failures:**
   - Check AKShare availability (external API)
   - Verify date ranges (trade days only)
   - Review logs with `-v` flag

3. **Performance Issues:**
   - Enable materialized views: `--materialized` flag
   - Check `tracing.log` with `-t` flag
   - Review query performance in PostgreSQL logs

### Code Reading Entry Points

- **Start here:** `app/main.py` - Main CLI logic
- **Data models:** `app/db/models.py` - Database schema
- **Filtering logic:** `app/filter/tail_scraper.py` - Core algorithm
- **Data fetching:** `app/data/ak.py` - External API
- **Constants:** `app/constant/` - Enums and configuration

## Important Notes

1. **PostgreSQL Only**: SQLite not supported due to LATERAL join usage
2. **Chinese Markets**: Data source (AKShare) focuses on Chinese stock markets
3. **Trade Days**: Application uses `previous_trade_day()` to account for market holidays
4. **Materialized Views**: Stored procedure must exist for MV functionality
5. **Google API Limits**: Be mindful of API quotas for Sheets integration
6. **Timezone**: Market hours are in local exchange time

## TODOs (from README)

- [x] Redesign FeedDaily
- [ ] Backtests (partially implemented)
- [ ] Get state of database command
- [ ] Later insert of ma250 from materialized view
- [ ] Google sheet update automation
- [ ] Real-time data from 2:30 to 3:00 (akshare/openD)
- [ ] Async engine implementation

## Environment Variables Reference

```env
# Database
POSTGRES_USERNAME=<username>
POSTGRES_PASSWORD=<password>
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=<database_name>
DB_DRIVER=postgresql  # Optional, defaults to postgresql

# Google Sheets
GOOGLE_SHEET_ID=<your_sheet_id>

# Credentials file: credentials.json (in root, gitignored)
```

## Dependencies Summary

**All dependencies are now managed in `pyproject.toml` (PEP 621 compliant).**

**Core:**
- sqlalchemy==2.0.36 - ORM and database toolkit
- akshare==1.16.44 - Chinese stock data API
- pydantic==2.10.6 - Data validation
- psycopg2==2.9.10 - PostgreSQL adapter
- loguru==0.7.3 - Logging
- python-dotenv==1.0.1 - Environment configuration

**Display:**
- google-api-python-client==2.160.0
- gspread==6.1.4 - Google Sheets integration
- gspread-formatting==1.2.0

**Development (optional extras):**
- mypy==1.15.0 - Type checking
- ruff==0.9.10 - Linting and formatting
- matplotlib==3.10.0 - Plotting for backtests

**Installation:**
```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync all dependencies (including dev)
uv sync

# Or sync production only (no dev dependencies)
uv sync --no-dev
```

## Quick Reference Commands

```bash
# Install dependencies
uv sync

# First-time setup (using uv run - recommended)
uv run stock-picker init -r -lll

# Or activate venv and run directly
source .venv/bin/activate
stock-picker init -r -lll

# Or using direct module execution
python app/main.py init -r -lll

# Daily automation (once at State 5)
uv run stock-picker run

# Dry run filtering (no DB writes)
uv run stock-picker run -t filter --dryrun

# Verbose debugging
uv run stock-picker run -vvv

# Store logs
uv run stock-picker run -S -t

# Specific date
uv run stock-picker run --date 2025-11-17

# Linting and formatting
ruff check .
ruff format .

# Type checking
mypy app/
```

## Modern Python Tooling (uv Workflow)

This project uses **uv** exclusively for modern Python package management - an extremely fast Python package installer and resolver written in Rust.

### Why uv?

- **10-100x faster** than pip for dependency resolution and installation
- **Modern PEP 621 support** - reads from `pyproject.toml` directly
- **Better dependency resolution** with modern PEP standards
- **Built-in virtual environment management** - no need for separate venv tools
- **Lockfile-based** - ensures reproducible builds

### Primary Workflow: uv sync

**Quick start (recommended):**
```bash
# Run commands directly without syncing (fastest for one-off commands)
uv run stock-picker init -r -lll
uv run stock-picker run
```

**Development workflow:**
```bash
# Sync dependencies from pyproject.toml (creates/updates .venv)
uv sync              # All dependencies including dev

# Or sync production only
uv sync --no-dev     # Production dependencies only

# Run the installed command
source .venv/bin/activate
stock-picker run

# Or use uv run without activation
uv run stock-picker run
```

**Adding new dependencies:**
```bash
# 1. Add dependency to pyproject.toml manually
# 2. Sync to install it
uv sync

# Or use uv add (if available in your uv version)
uv add package-name==version
uv sync
```

**Updating dependencies:**
```bash
# Update all dependencies to latest compatible versions
uv sync --upgrade

# Lock current versions
uv lock
```

### Project Structure Notes

- **pyproject.toml**: Single source of truth for all dependencies (PEP 621)
- **uv.lock**: Lockfile for reproducible builds (auto-generated by uv sync)
- **.venv/**: Virtual environment (auto-created by uv sync)

### Ruff Configuration

Ruff is configured in `pyproject.toml` with:
- Line length: 120 characters
- Python 3.13+ target
- Enabled checks: pycodestyle, pyflakes, isort, flake8-bugbear, comprehensions, pyupgrade
- Auto-formatting with double quotes and 4-space indentation

**Usage:**
```bash
ruff check .           # Lint all files
ruff check --fix .     # Auto-fix issues
ruff format .          # Format code
```

### MyPy Configuration

MyPy is configured for gradual typing:
- Type checking with Pydantic plugin support
- Missing imports ignored for external libraries (akshare, gspread)
- Future goal: Enable strict mode (`disallow_untyped_defs = true`)

---

**Last Updated:** 2025-11-17
**Maintainer:** monkeyboiii
**License:** MIT
