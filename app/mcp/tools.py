"""
MCP Tool Definitions - Define available tools for LLM agents

This module defines the MCP tools that LLM agents can use to interact
with the backtesting system.
"""

from mcp.types import Tool


TOOL_CREATE_STRATEGY = Tool(
    name="create_strategy",
    description=(
        "Create a new trading strategy from a natural language description. "
        "The system will convert your description into a structured strategy configuration."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "description": {
                "type": "string",
                "description": (
                    "Natural language description of the trading strategy. "
                    "Example: 'Buy stocks when they break above their 250-day moving average "
                    "with high volume, hold for 30 days, take profit at 20% or stop loss at 10%'"
                ),
            },
            "name": {
                "type": "string",
                "description": "Optional name for the strategy. If not provided, a timestamped name will be generated.",
            },
        },
        "required": ["description"],
    },
)


TOOL_RUN_BACKTEST = Tool(
    name="run_backtest",
    description=(
        "Run a backtest for a specific strategy over a date range. "
        "Returns backtest ID and summary results."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "strategy_id": {
                "type": "string",
                "description": "UUID of the strategy to backtest",
            },
            "start_date": {
                "type": "string",
                "description": "Start date in YYYY-MM-DD format (e.g., '2024-01-01')",
            },
            "end_date": {
                "type": "string",
                "description": "End date in YYYY-MM-DD format (e.g., '2024-12-31')",
            },
            "initial_capital": {
                "type": "number",
                "description": "Initial capital in CNY. Default: 100000",
                "default": 100000,
            },
        },
        "required": ["strategy_id", "start_date", "end_date"],
    },
)


TOOL_GET_BACKTEST_RESULTS = Tool(
    name="get_backtest_results",
    description=(
        "Get detailed results for a completed backtest run. "
        "Includes performance metrics, trades, and equity curve."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "backtest_id": {
                "type": "string",
                "description": "UUID of the backtest run",
            },
        },
        "required": ["backtest_id"],
    },
)


TOOL_LIST_STRATEGIES = Tool(
    name="list_strategies",
    description="List all available trading strategies in the database.",
    inputSchema={
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "description": "Maximum number of strategies to return. Default: 10",
                "default": 10,
            },
        },
    },
)


TOOL_COMPARE_STRATEGIES = Tool(
    name="compare_strategies",
    description=(
        "Compare multiple backtest runs to find the best performing strategy. "
        "Provides statistical analysis and rankings."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "backtest_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of backtest run UUIDs to compare (minimum 2)",
            },
            "metrics": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Metrics to compare. Options: total_return, sharpe_ratio, sortino_ratio, "
                    "calmar_ratio, max_drawdown, win_rate, profit_factor. "
                    "Default: ['total_return', 'sharpe_ratio', 'win_rate']"
                ),
                "default": ["total_return", "sharpe_ratio", "win_rate"],
            },
        },
        "required": ["backtest_ids"],
    },
)


TOOL_GET_CHART_DATA = Tool(
    name="get_chart_data",
    description=(
        "Get chart data for visualizing backtest results. "
        "Supports equity curve, monthly returns, trade distribution, and drawdown charts."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "backtest_id": {
                "type": "string",
                "description": "UUID of the backtest run",
            },
            "chart_type": {
                "type": "string",
                "enum": ["equity_curve", "monthly_returns", "trade_distribution", "drawdown"],
                "description": (
                    "Type of chart data to retrieve:\n"
                    "- equity_curve: Portfolio value over time\n"
                    "- monthly_returns: Returns organized by month/year\n"
                    "- trade_distribution: Histogram of trade returns\n"
                    "- drawdown: Drawdown visualization over time"
                ),
                "default": "equity_curve",
            },
        },
        "required": ["backtest_id"],
    },
)
