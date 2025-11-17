-- Database Optimization - Add Performance Indexes
-- Phase 7: Optimization & Production

-- This migration adds indexes to improve query performance for backtesting and analytics

-- ========================================
-- Stock Daily Table Indexes
-- ========================================

-- Index for date range queries (most common in backtests)
CREATE INDEX IF NOT EXISTS idx_stock_daily_trade_day
ON stock_daily(trade_day DESC);

-- Index for stock lookup with date
CREATE INDEX IF NOT EXISTS idx_stock_daily_code_date
ON stock_daily(code, trade_day DESC);

-- Index for filtering by price ranges
CREATE INDEX IF NOT EXISTS idx_stock_daily_close_price
ON stock_daily(close) WHERE close IS NOT NULL;

-- Index for MA250 filtering (tail scraper filter)
CREATE INDEX IF NOT EXISTS idx_stock_daily_ma250
ON stock_daily(ma_250) WHERE ma_250 IS NOT NULL;

-- Index for volume-based queries
CREATE INDEX IF NOT EXISTS idx_stock_daily_volume
ON stock_daily(volume) WHERE volume IS NOT NULL;

-- Index for turnover rate filtering
CREATE INDEX IF NOT EXISTS idx_stock_daily_turnover_rate
ON stock_daily(turnover_rate) WHERE turnover_rate IS NOT NULL;

-- Composite index for common filter combinations
CREATE INDEX IF NOT EXISTS idx_stock_daily_filter_combo
ON stock_daily(code, trade_day, close, ma_250, volume, turnover_rate)
WHERE ma_250 IS NOT NULL;

-- ========================================
-- Collection Daily Table Indexes
-- ========================================

-- Index for date-based queries
CREATE INDEX IF NOT EXISTS idx_collection_daily_trade_day
ON collection_daily(trade_day DESC);

-- Index for collection lookup with date
CREATE INDEX IF NOT EXISTS idx_collection_daily_code_date
ON collection_daily(code, trade_day DESC);

-- Index for change rate filtering
CREATE INDEX IF NOT EXISTS idx_collection_daily_change_rate
ON collection_daily(change_rate) WHERE change_rate IS NOT NULL;

-- ========================================
-- Backtest Tables Indexes
-- ========================================

-- Backtest Run indexes
CREATE INDEX IF NOT EXISTS idx_backtest_run_strategy_id
ON backtest_run(strategy_id);

CREATE INDEX IF NOT EXISTS idx_backtest_run_dates
ON backtest_run(start_date, end_date);

CREATE INDEX IF NOT EXISTS idx_backtest_run_created_at
ON backtest_run(created_at DESC);

-- Composite index for filtering by performance metrics
CREATE INDEX IF NOT EXISTS idx_backtest_run_performance
ON backtest_run(total_return DESC, sharpe_ratio DESC)
WHERE total_return IS NOT NULL AND sharpe_ratio IS NOT NULL;

-- Trade indexes
CREATE INDEX IF NOT EXISTS idx_trade_backtest_run_id
ON trade(backtest_run_id);

CREATE INDEX IF NOT EXISTS idx_trade_symbol_date
ON trade(symbol, entry_date);

CREATE INDEX IF NOT EXISTS idx_trade_entry_date
ON trade(entry_date DESC);

-- Portfolio Snapshot indexes
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_backtest_run
ON portfolio_snapshot(backtest_run_id);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshot_date
ON portfolio_snapshot(date DESC);

-- Strategy indexes
CREATE INDEX IF NOT EXISTS idx_strategy_created_at
ON strategy(created_at DESC);

-- Strategy Comparison indexes
CREATE INDEX IF NOT EXISTS idx_strategy_comparison_created_at
ON strategy_comparison(created_at DESC);

-- ========================================
-- Feed Daily Table Indexes
-- ========================================

-- Index for date-based queries
CREATE INDEX IF NOT EXISTS idx_feed_daily_trade_day
ON feed_daily(trade_day DESC);

-- Index for filter lookup
CREATE INDEX IF NOT EXISTS idx_feed_daily_filter_id
ON feed_daily(filter_id);

-- Composite index for feed queries
CREATE INDEX IF NOT EXISTS idx_feed_daily_filter_date
ON feed_daily(filter_id, trade_day DESC);

-- ========================================
-- Stock and Collection Relationship Indexes
-- ========================================

-- Index for stock-collection lookups
CREATE INDEX IF NOT EXISTS idx_relation_collection_stock_stock_code
ON relation_collection_stock(stock_code);

CREATE INDEX IF NOT EXISTS idx_relation_collection_stock_collection_code
ON relation_collection_stock(collection_code);

-- ========================================
-- Analyze Tables for Statistics
-- ========================================

-- Update statistics for query planner
ANALYZE stock_daily;
ANALYZE collection_daily;
ANALYZE backtest_run;
ANALYZE trade;
ANALYZE portfolio_snapshot;
ANALYZE strategy;
ANALYZE strategy_comparison;
ANALYZE feed_daily;
ANALYZE stock;
ANALYZE collection;
ANALYZE relation_collection_stock;

-- ========================================
-- Table Partitioning (for large datasets)
-- ========================================

-- Note: Partitioning requires table recreation, so we provide the DDL as comments
-- Uncomment and execute manually if needed for production optimization

/*
-- Example: Partition stock_daily by year
CREATE TABLE stock_daily_partitioned (
    LIKE stock_daily INCLUDING ALL
) PARTITION BY RANGE (trade_day);

-- Create partitions for each year
CREATE TABLE stock_daily_y2024 PARTITION OF stock_daily_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE stock_daily_y2025 PARTITION OF stock_daily_partitioned
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Migrate data (after testing)
-- INSERT INTO stock_daily_partitioned SELECT * FROM stock_daily;
-- DROP TABLE stock_daily;
-- ALTER TABLE stock_daily_partitioned RENAME TO stock_daily;
*/

-- ========================================
-- Performance Monitoring Views
-- ========================================

-- View for slow query monitoring
CREATE OR REPLACE VIEW v_table_stats AS
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) AS indexes_size,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- View for index usage statistics
CREATE OR REPLACE VIEW v_index_usage AS
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan AS index_scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- ========================================
-- Vacuum and Maintenance
-- ========================================

-- Regular maintenance commands (run periodically)
VACUUM ANALYZE stock_daily;
VACUUM ANALYZE collection_daily;
VACUUM ANALYZE backtest_run;
VACUUM ANALYZE trade;
VACUUM ANALYZE portfolio_snapshot;

-- ========================================
-- Index Creation Complete
-- ========================================

-- Verify indexes created
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
