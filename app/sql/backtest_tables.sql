-- Backtesting Framework Tables
-- SQLAlchemy will create these tables automatically, but this SQL is for documentation
-- and manual migration if needed.

-- Strategy table: Stores strategy definitions
CREATE TABLE IF NOT EXISTS strategy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    author VARCHAR(255),
    description TEXT,
    definition JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,

    UNIQUE(name, version)
);

CREATE INDEX IF NOT EXISTS idx_strategy_name ON strategy(name);
CREATE INDEX IF NOT EXISTS idx_strategy_created_at ON strategy(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_is_active ON strategy(is_active);

-- Backtest run table: Stores backtest execution configuration and results
CREATE TABLE IF NOT EXISTS backtest_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id UUID NOT NULL REFERENCES strategy(id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    initial_capital NUMERIC(15, 2) DEFAULT 1000000.00,

    -- Configuration
    commission_rate NUMERIC(5, 4) DEFAULT 0.0003,
    slippage_rate NUMERIC(5, 4) DEFAULT 0.001,

    -- Results (computed after run)
    total_return NUMERIC(10, 4),
    sharpe_ratio NUMERIC(10, 4),
    max_drawdown NUMERIC(10, 4),
    win_rate NUMERIC(5, 4),
    profit_factor NUMERIC(10, 4),
    total_trades INTEGER,

    -- Status
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,

    -- Timestamps
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT valid_date_range CHECK (end_date >= start_date)
);

CREATE INDEX IF NOT EXISTS idx_backtest_run_strategy ON backtest_run(strategy_id);
CREATE INDEX IF NOT EXISTS idx_backtest_run_status ON backtest_run(status);
CREATE INDEX IF NOT EXISTS idx_backtest_run_created ON backtest_run(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_run_dates ON backtest_run(start_date, end_date);

-- Trade table: Individual trades
CREATE TABLE IF NOT EXISTS trade (
    id BIGSERIAL PRIMARY KEY,
    backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    stock_code VARCHAR(10) NOT NULL,
    stock_name VARCHAR(50),

    -- Entry
    entry_date DATE NOT NULL,
    entry_price NUMERIC(10, 3) NOT NULL,
    entry_signal TEXT,

    -- Exit
    exit_date DATE,
    exit_price NUMERIC(10, 3),
    exit_reason VARCHAR(50),

    -- Position
    shares INTEGER NOT NULL,
    position_value NUMERIC(15, 2),

    -- P&L
    gross_pnl NUMERIC(15, 2),
    commission NUMERIC(15, 2),
    slippage NUMERIC(15, 2),
    net_pnl NUMERIC(15, 2),
    return_pct NUMERIC(10, 4),

    -- Metadata
    holding_days INTEGER,
    collection_name VARCHAR(100),

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_backtest_run ON trade(backtest_run_id);
CREATE INDEX IF NOT EXISTS idx_trade_stock_code ON trade(stock_code);
CREATE INDEX IF NOT EXISTS idx_trade_entry_date ON trade(entry_date);
CREATE INDEX IF NOT EXISTS idx_trade_exit_date ON trade(exit_date);
CREATE INDEX IF NOT EXISTS idx_trade_exit_reason ON trade(exit_reason);

-- Portfolio snapshot table: Daily equity curve
CREATE TABLE IF NOT EXISTS portfolio_snapshot (
    id BIGSERIAL PRIMARY KEY,
    backtest_run_id UUID NOT NULL REFERENCES backtest_run(id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,

    -- Portfolio value
    cash NUMERIC(15, 2) NOT NULL,
    holdings_value NUMERIC(15, 2) NOT NULL,
    total_value NUMERIC(15, 2) NOT NULL,

    -- Daily metrics
    daily_return NUMERIC(10, 4),
    cumulative_return NUMERIC(10, 4),
    drawdown NUMERIC(10, 4),

    -- Positions
    open_positions INTEGER,
    total_positions_opened INTEGER,
    total_positions_closed INTEGER,

    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(backtest_run_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_run ON portfolio_snapshot(backtest_run_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_date ON portfolio_snapshot(snapshot_date);

-- Strategy comparison table: Groups of backtest runs for comparison
CREATE TABLE IF NOT EXISTS strategy_comparison (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255),
    description TEXT,
    backtest_run_ids UUID[] NOT NULL,

    created_at TIMESTAMP DEFAULT NOW(),
    created_by VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_strategy_comparison_created ON strategy_comparison(created_at DESC);

-- Materialized view for daily metrics (for faster backtesting queries)
-- This is similar to the one in materialized_view.sql but optimized for backtesting
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_backtest_daily_metrics AS
SELECT
    sd.code,
    sd.trade_day,
    sd.close,
    sd.open,
    sd.high,
    sd.low,
    sd.volume,
    sd.turnover_rate,
    sd.quantity_relative_ratio,
    sd.circulation_capital,
    sd.ma_250,
    LAG(sd.close, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day) AS prev_close,
    LAG(sd.volume, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day) AS prev_volume,
    AVG(sd.volume) OVER (PARTITION BY sd.code ORDER BY sd.trade_day ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5_volume,
    (sd.close / NULLIF(LAG(sd.close, 1) OVER (PARTITION BY sd.code ORDER BY sd.trade_day), 0) - 1) * 100 AS daily_return,
    s.name AS stock_name
FROM stock_daily sd
JOIN stock s ON sd.code = s.code
WHERE sd.trade_day >= '2023-01-01';

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_backtest_daily_metrics_code_date ON mv_backtest_daily_metrics(code, trade_day);
CREATE INDEX IF NOT EXISTS idx_mv_backtest_daily_metrics_date ON mv_backtest_daily_metrics(trade_day);

-- Comments for documentation
COMMENT ON TABLE strategy IS 'Stores strategy definitions for backtesting in JSON format';
COMMENT ON TABLE backtest_run IS 'Stores backtest execution configuration and computed results';
COMMENT ON TABLE trade IS 'Individual trade records with complete P&L calculation';
COMMENT ON TABLE portfolio_snapshot IS 'Daily portfolio state for equity curve generation';
COMMENT ON TABLE strategy_comparison IS 'Groups of backtest runs for side-by-side comparison';
COMMENT ON MATERIALIZED VIEW mv_backtest_daily_metrics IS 'Pre-computed daily metrics for faster backtest execution';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stock_picker_user;
-- GRANT SELECT ON mv_backtest_daily_metrics TO stock_picker_user;
