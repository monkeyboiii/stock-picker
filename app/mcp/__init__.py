"""
MCP (Model Context Protocol) Integration Module

This module provides an MCP server for exposing backtesting functionality
to LLM agents and workflow automation tools like n8n.

Components:
- server.py: MCP server with HTTP SSE transport
- tools.py: MCP tool definitions
- nl_to_strategy.py: Natural language to strategy DSL converter
"""

from app.mcp.nl_to_strategy import NLStrategyConverter
from app.mcp.server import app, mcp_server
from app.mcp.tools import (
    TOOL_COMPARE_STRATEGIES,
    TOOL_CREATE_STRATEGY,
    TOOL_GET_BACKTEST_RESULTS,
    TOOL_GET_CHART_DATA,
    TOOL_LIST_STRATEGIES,
    TOOL_RUN_BACKTEST,
)


__all__ = [
    "app",
    "mcp_server",
    "NLStrategyConverter",
    "TOOL_CREATE_STRATEGY",
    "TOOL_RUN_BACKTEST",
    "TOOL_GET_BACKTEST_RESULTS",
    "TOOL_LIST_STRATEGIES",
    "TOOL_COMPARE_STRATEGIES",
    "TOOL_GET_CHART_DATA",
]
