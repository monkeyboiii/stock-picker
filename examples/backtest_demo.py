"""
Backtest Framework Demo

This script demonstrates how to use the backtesting framework.

Usage:
    python examples/backtest_demo.py

Prerequisites:
    - Database initialized with stock data
    - Historical stock_daily data loaded for the backtest period
"""

from datetime import date
from decimal import Decimal

from loguru import logger

from app.backtest.engine import BacktestEngine
from app.backtest.signals import StrategyBuilder
from app.backtest.strategy import create_strategy, get_or_create_tail_scraper_strategy
from app.db.engine import engine_from_env


def main():
    """Run a simple backtest demo"""

    # Initialize database engine
    engine = engine_from_env()
    logger.info("Connected to database")

    # Define backtest parameters
    start_date = date(2025, 1, 1)
    end_date = date(2025, 3, 31)
    initial_capital = Decimal("1000000")  # 1M CNY

    logger.info(
        f"Backtest configuration:\n"
        f"  Period: {start_date} to {end_date}\n"
        f"  Initial capital: ¥{initial_capital:,.2f}"
    )

    # Create strategy definition
    strategy_def = StrategyBuilder.tail_scraper_strategy(
        take_profit_pct=10.0,  # Exit at 10% profit
        stop_loss_pct=-5.0,  # Exit at -5% loss
        max_holding_days=30,  # Exit after 30 days
    )

    logger.info(
        f"Strategy: {strategy_def['name']} v{strategy_def['version']}\n"
        f"  Take profit: {strategy_def['take_profit_pct']}%\n"
        f"  Stop loss: {strategy_def['stop_loss_pct']}%\n"
        f"  Max hold: {strategy_def['max_holding_days']} days"
    )

    # Initialize backtest engine
    backtest_engine = BacktestEngine(
        strategy_definition=strategy_def,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        max_positions=20,  # Maximum 20 concurrent positions
    )

    # Run backtest
    logger.info("Starting backtest execution...")
    result = backtest_engine.run(engine)

    # Display results
    logger.success("Backtest completed!")
    print("\n" + "=" * 80)
    print(result)
    print("=" * 80)

    # Save to database
    logger.info("Saving results to database...")
    strategy_id = get_or_create_tail_scraper_strategy(engine)
    run_id = backtest_engine.save_to_database(engine, strategy_id, result)
    logger.success(f"Results saved with run_id: {run_id}")

    # Display sample trades
    if result.trades:
        logger.info("\nSample trades (first 5):")
        for i, trade in enumerate(result.trades[:5]):
            logger.info(
                f"  {i+1}. {trade['stock_code']} ({trade['stock_name']}): "
                f"¥{trade['net_pnl']:.2f} ({trade['return_pct']:.2f}%) "
                f"[{trade['holding_days']} days, {trade['exit_reason']}]"
            )

    # Display equity curve sample
    if result.equity_curve:
        logger.info("\nEquity curve (first 5 days):")
        for snapshot in result.equity_curve[:5]:
            logger.info(
                f"  {snapshot['snapshot_date']}: "
                f"¥{snapshot['total_value']:,.2f} "
                f"({snapshot['cumulative_return']:.2f}% cumulative return)"
            )


if __name__ == "__main__":
    main()
