-- Phase 4 Analytics Migration: Add enhanced metrics columns to backtest_run table

-- Add new metrics columns to backtest_run table
ALTER TABLE backtest_run
ADD COLUMN IF NOT EXISTS annualized_return NUMERIC(10, 4),
ADD COLUMN IF NOT EXISTS sortino_ratio NUMERIC(10, 4),
ADD COLUMN IF NOT EXISTS calmar_ratio NUMERIC(10, 4),
ADD COLUMN IF NOT EXISTS volatility NUMERIC(10, 4);

-- Add comments for documentation
COMMENT ON COLUMN backtest_run.annualized_return IS 'Annualized return percentage';
COMMENT ON COLUMN backtest_run.sortino_ratio IS 'Sortino ratio (downside deviation)';
COMMENT ON COLUMN backtest_run.calmar_ratio IS 'Calmar ratio (return / max drawdown)';
COMMENT ON COLUMN backtest_run.volatility IS 'Annualized volatility percentage';

-- Create strategy_comparison table for multi-strategy comparisons
CREATE TABLE IF NOT EXISTS strategy_comparison (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Comparison metadata
    num_strategies INTEGER NOT NULL,
    metrics_compared TEXT[], -- Array of metric names

    -- Results (stored as JSONB for flexibility)
    comparison_results JSONB NOT NULL,
    statistical_tests JSONB,

    CONSTRAINT strategy_comparison_num_strategies_check CHECK (num_strategies >= 2)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_strategy_comparison_created_at ON strategy_comparison(created_at DESC);

-- Junction table for comparison <-> backtest_run relationship
CREATE TABLE IF NOT EXISTS comparison_backtest_run (
    comparison_id UUID REFERENCES strategy_comparison(id) ON DELETE CASCADE,
    backtest_run_id UUID REFERENCES backtest_run(id) ON DELETE CASCADE,
    rank INTEGER,
    PRIMARY KEY (comparison_id, backtest_run_id)
);

CREATE INDEX IF NOT EXISTS idx_comparison_backtest_run_comparison ON comparison_backtest_run(comparison_id);
CREATE INDEX IF NOT EXISTS idx_comparison_backtest_run_backtest ON comparison_backtest_run(backtest_run_id);

-- Add comments
COMMENT ON TABLE strategy_comparison IS 'Stores multi-strategy comparison results';
COMMENT ON TABLE comparison_backtest_run IS 'Junction table linking comparisons to backtest runs';

-- Add permission grants if needed (adjust roles as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON strategy_comparison TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON comparison_backtest_run TO your_app_user;
