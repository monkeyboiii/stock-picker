"""
MCP Server - Model Context Protocol Server for Stock Picker Backtesting

This module provides an MCP server that exposes backtesting functionality to LLM agents.
Uses HTTP Streamable transport (SSE) for production-ready streaming.

Run with:
    uvicorn app.mcp.server:app --host 0.0.0.0 --port 8001

Or integrate into n8n workflows using the HTTP endpoint.
"""

import asyncio
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request
from loguru import logger
from mcp.server import Server
from mcp.server.sse import SseServerTransport
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    TextContent,
    Tool,
)
from pydantic import AnyUrl
from sse_starlette import EventSourceResponse
from sqlalchemy.orm import Session

from app.api.dependencies import get_engine
from app.backtest.analytics import PerformanceAnalyzer
from app.backtest.comparison import ComparisonEngine
from app.backtest.engine import BacktestEngine
from app.constant.version import VERSION
from app.db.models import BacktestRun, Strategy
from app.mcp.nl_to_strategy import NLStrategyConverter
from app.mcp.tools import (
    TOOL_CREATE_STRATEGY,
    TOOL_GET_BACKTEST_RESULTS,
    TOOL_LIST_STRATEGIES,
    TOOL_RUN_BACKTEST,
    TOOL_COMPARE_STRATEGIES,
    TOOL_GET_CHART_DATA,
)


# Create MCP server
mcp_server = Server("stock-picker-backtest")


# FastAPI app for HTTP transport
app = FastAPI(
    title="Stock Picker MCP Server",
    description="Model Context Protocol server for backtesting trading strategies",
    version=VERSION,
)


@app.get("/")
async def root():
    """Root endpoint with server info"""
    return {
        "name": "stock-picker-backtest",
        "version": VERSION,
        "protocol": "mcp",
        "transport": "http-sse",
        "tools": [
            "create_strategy",
            "run_backtest",
            "get_backtest_results",
            "list_strategies",
            "compare_strategies",
            "get_chart_data",
        ],
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "version": VERSION}


@app.post("/sse")
async def handle_sse(request: Request):
    """
    SSE endpoint for MCP protocol

    This endpoint handles Server-Sent Events for streaming MCP messages.
    Compatible with n8n and other workflow automation tools.
    """
    async def event_generator():
        """Generate SSE events from MCP server"""
        transport = SseServerTransport("/messages")

        async with mcp_server.run(
            transport.read_stream,
            transport.write_stream,
            mcp_server.create_initialization_options(),
        ):
            # Read request body
            body = await request.json()

            # Process MCP message
            await transport.handle_post_message(body)

            # Stream response events
            async for event in transport.send_events():
                yield event

    return EventSourceResponse(event_generator())


# MCP Tool Handlers

@mcp_server.list_tools()
async def list_tools() -> List[Tool]:
    """List available MCP tools"""
    return [
        TOOL_CREATE_STRATEGY,
        TOOL_RUN_BACKTEST,
        TOOL_GET_BACKTEST_RESULTS,
        TOOL_LIST_STRATEGIES,
        TOOL_COMPARE_STRATEGIES,
        TOOL_GET_CHART_DATA,
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls from LLM"""
    logger.info(f"MCP tool called: {name} with arguments: {arguments}")

    try:
        if name == "create_strategy":
            return await handle_create_strategy(arguments)
        elif name == "run_backtest":
            return await handle_run_backtest(arguments)
        elif name == "get_backtest_results":
            return await handle_get_backtest_results(arguments)
        elif name == "list_strategies":
            return await handle_list_strategies(arguments)
        elif name == "compare_strategies":
            return await handle_compare_strategies(arguments)
        elif name == "get_chart_data":
            return await handle_get_chart_data(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as e:
        logger.error(f"Error in tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_create_strategy(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Create a new trading strategy from natural language description

    Args:
        arguments: {
            "description": "Natural language strategy description",
            "name": "Strategy name (optional)"
        }

    Returns:
        Strategy ID and confirmation
    """
    description = arguments.get("description", "")
    name = arguments.get("name", f"Strategy {datetime.now().strftime('%Y%m%d_%H%M%S')}")

    if not description:
        return [TextContent(type="text", text="Error: Strategy description is required")]

    # Convert natural language to strategy DSL
    converter = NLStrategyConverter()
    strategy_dict = converter.convert(description)
    strategy_dict["name"] = name

    # Save strategy to database
    engine = get_engine()
    with Session(engine) as session:
        strategy = Strategy(
            name=name,
            description=description,
            strategy_config=strategy_dict,
        )
        session.add(strategy)
        session.commit()
        session.refresh(strategy)

        result = {
            "strategy_id": str(strategy.id),
            "name": strategy.name,
            "description": strategy.description,
            "config": strategy.strategy_config,
        }

    return [TextContent(
        type="text",
        text=f"Strategy created successfully:\n{json.dumps(result, indent=2)}"
    )]


async def handle_run_backtest(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Run a backtest for a strategy

    Args:
        arguments: {
            "strategy_id": "UUID of strategy",
            "start_date": "YYYY-MM-DD",
            "end_date": "YYYY-MM-DD",
            "initial_capital": 100000 (optional)
        }

    Returns:
        Backtest run ID and basic results
    """
    strategy_id = arguments.get("strategy_id")
    start_date_str = arguments.get("start_date")
    end_date_str = arguments.get("end_date")
    initial_capital = arguments.get("initial_capital", 100000)

    if not all([strategy_id, start_date_str, end_date_str]):
        return [TextContent(
            type="text",
            text="Error: strategy_id, start_date, and end_date are required"
        )]

    # Parse dates
    try:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
    except ValueError as e:
        return [TextContent(type="text", text=f"Error: Invalid date format - {e}")]

    # Load strategy
    engine = get_engine()
    with Session(engine) as session:
        strategy = session.get(Strategy, strategy_id)
        if not strategy:
            return [TextContent(type="text", text=f"Error: Strategy {strategy_id} not found")]

        # Run backtest
        backtest_engine = BacktestEngine(session=session)
        result = backtest_engine.run(
            strategy_config=strategy.strategy_config,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
        )

        # Save backtest run
        backtest_run = BacktestRun(
            strategy_id=strategy.id,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            final_value=result.final_value,
            total_return=result.total_return,
            num_trades=result.num_trades,
            sharpe_ratio=result.sharpe_ratio if hasattr(result, 'sharpe_ratio') else None,
        )
        session.add(backtest_run)
        session.commit()
        session.refresh(backtest_run)

        response = {
            "backtest_id": str(backtest_run.id),
            "strategy_id": str(strategy.id),
            "strategy_name": strategy.name,
            "period": f"{start_date} to {end_date}",
            "initial_capital": initial_capital,
            "final_value": float(result.final_value),
            "total_return": float(result.total_return),
            "num_trades": result.num_trades,
            "sharpe_ratio": float(result.sharpe_ratio) if hasattr(result, 'sharpe_ratio') and result.sharpe_ratio else None,
        }

    return [TextContent(
        type="text",
        text=f"Backtest completed:\n{json.dumps(response, indent=2)}"
    )]


async def handle_get_backtest_results(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Get detailed results for a backtest run

    Args:
        arguments: {
            "backtest_id": "UUID of backtest run"
        }

    Returns:
        Detailed backtest results including trades and metrics
    """
    backtest_id = arguments.get("backtest_id")

    if not backtest_id:
        return [TextContent(type="text", text="Error: backtest_id is required")]

    engine = get_engine()
    with Session(engine) as session:
        backtest_run = session.get(BacktestRun, backtest_id)
        if not backtest_run:
            return [TextContent(type="text", text=f"Error: Backtest {backtest_id} not found")]

        # Build response
        result = {
            "id": str(backtest_run.id),
            "strategy_id": str(backtest_run.strategy_id),
            "start_date": backtest_run.start_date.isoformat(),
            "end_date": backtest_run.end_date.isoformat(),
            "initial_capital": float(backtest_run.initial_capital),
            "final_value": float(backtest_run.final_value),
            "total_return": float(backtest_run.total_return),
            "num_trades": backtest_run.num_trades,
            "sharpe_ratio": float(backtest_run.sharpe_ratio) if backtest_run.sharpe_ratio else None,
            "max_drawdown": float(backtest_run.max_drawdown) if backtest_run.max_drawdown else None,
            "win_rate": float(backtest_run.win_rate) if backtest_run.win_rate else None,
        }

    return [TextContent(
        type="text",
        text=f"Backtest results:\n{json.dumps(result, indent=2)}"
    )]


async def handle_list_strategies(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    List all available strategies

    Args:
        arguments: {
            "limit": 10 (optional)
        }

    Returns:
        List of strategies
    """
    limit = arguments.get("limit", 10)

    engine = get_engine()
    with Session(engine) as session:
        strategies = session.query(Strategy).limit(limit).all()

        result = [
            {
                "id": str(s.id),
                "name": s.name,
                "description": s.description,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in strategies
        ]

    return [TextContent(
        type="text",
        text=f"Found {len(result)} strategies:\n{json.dumps(result, indent=2)}"
    )]


async def handle_compare_strategies(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Compare multiple backtest runs

    Args:
        arguments: {
            "backtest_ids": ["uuid1", "uuid2", ...],
            "metrics": ["total_return", "sharpe_ratio", ...] (optional)
        }

    Returns:
        Comparison results with rankings
    """
    backtest_ids = arguments.get("backtest_ids", [])
    metrics = arguments.get("metrics", ["total_return", "sharpe_ratio", "win_rate"])

    if len(backtest_ids) < 2:
        return [TextContent(
            type="text",
            text="Error: At least 2 backtest IDs required for comparison"
        )]

    engine = get_engine()
    with Session(engine) as session:
        # Get comparison results
        comparison = ComparisonEngine.compare_strategies(
            backtest_run_ids=backtest_ids,
            metrics=metrics,
            name=f"Comparison {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            session=session,
        )

        result = {
            "comparison_id": str(comparison.id),
            "num_strategies": len(backtest_ids),
            "metrics_compared": metrics,
            "rankings": comparison.comparison_results.get("rankings", {}),
        }

    return [TextContent(
        type="text",
        text=f"Strategy comparison:\n{json.dumps(result, indent=2)}"
    )]


async def handle_get_chart_data(arguments: Dict[str, Any]) -> List[TextContent]:
    """
    Get chart data for visualization

    Args:
        arguments: {
            "backtest_id": "UUID of backtest run",
            "chart_type": "equity_curve|monthly_returns|trade_distribution|drawdown"
        }

    Returns:
        Chart data in JSON format
    """
    backtest_id = arguments.get("backtest_id")
    chart_type = arguments.get("chart_type", "equity_curve")

    if not backtest_id:
        return [TextContent(type="text", text="Error: backtest_id is required")]

    engine = get_engine()
    with Session(engine) as session:
        backtest_run = session.get(BacktestRun, backtest_id)
        if not backtest_run:
            return [TextContent(type="text", text=f"Error: Backtest {backtest_id} not found")]

        # Import chart data generator
        from app.api.charts import ChartDataGenerator

        chart_generator = ChartDataGenerator(session)

        if chart_type == "equity_curve":
            data = chart_generator.equity_curve(backtest_id)
        elif chart_type == "monthly_returns":
            data = chart_generator.monthly_returns(backtest_id)
        elif chart_type == "trade_distribution":
            data = chart_generator.trade_distribution(backtest_id)
        elif chart_type == "drawdown":
            data = chart_generator.drawdown_chart(backtest_id)
        else:
            return [TextContent(type="text", text=f"Error: Unknown chart type {chart_type}")]

    return [TextContent(
        type="text",
        text=f"Chart data ({chart_type}):\n{json.dumps(data, indent=2)}"
    )]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.mcp.server:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
