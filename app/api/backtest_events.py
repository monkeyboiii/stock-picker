"""
Backtest Event Emitter - Emit WebSocket events during backtest execution

This module provides utilities for sending real-time events during backtest execution:
- Progress updates
- Trade events
- Portfolio snapshots
- Status changes
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional

from loguru import logger

from app.api.websocket_manager import manager


class BacktestEventEmitter:
    """Emits WebSocket events during backtest execution"""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self.room = f"backtest:{run_id}"

    async def emit_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Emit progress update

        Args:
            current: Current progress value
            total: Total progress value
            message: Optional progress message
        """
        percentage = (current / total * 100) if total > 0 else 0

        event = {
            "type": "progress",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "current": current,
                "total": total,
                "percentage": round(percentage, 2),
                "message": message
            }
        }

        await manager.send_to_room(event, self.room)
        logger.debug(f"Progress event: {self.run_id} - {percentage:.1f}%")

    async def emit_trade(self, trade_data: Dict[str, Any], trade_type: str = "entry") -> None:
        """
        Emit trade event

        Args:
            trade_data: Trade information dict
            trade_type: Type of trade (entry/exit)
        """
        event = {
            "type": "trade",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "trade_type": trade_type,
                **trade_data
            }
        }

        await manager.send_to_room(event, self.room)

    async def emit_snapshot(self, snapshot_data: Dict[str, Any]) -> None:
        """
        Emit portfolio snapshot event

        Args:
            snapshot_data: Portfolio snapshot dict
        """
        event = {
            "type": "snapshot",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": snapshot_data
        }

        await manager.send_to_room(event, self.room)

    async def emit_status(self, status: str, message: str = "", data: Dict = None) -> None:
        """
        Emit status change event

        Args:
            status: New status (running/completed/failed)
            message: Status message
            data: Additional data
        """
        event = {
            "type": "status",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "status": status,
                "message": message,
                **(data or {})
            }
        }

        await manager.send_to_room(event, self.room)

        # Also emit to global room
        global_event = {
            "type": f"backtest_{status}",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "status": status,
                "message": message,
                **(data or {})
            }
        }
        await manager.send_to_room(global_event, "backtests:all")

    async def emit_error(self, error: str, details: Dict = None) -> None:
        """
        Emit error event

        Args:
            error: Error message
            details: Additional error details
        """
        event = {
            "type": "error",
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "error": error,
                "details": details or {}
            }
        }

        await manager.send_to_room(event, self.room)


def emit_backtest_event_sync(run_id: str, event_type: str, data: Dict[str, Any]) -> None:
    """
    Synchronous wrapper for emitting WebSocket events

    Args:
        run_id: Backtest run ID
        event_type: Type of event (progress/trade/snapshot/status/error)
        data: Event data

    This function can be called from synchronous code (like BacktestEngine)
    and will schedule the event emission on the event loop.
    """
    try:
        # Get or create event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop in this thread
            return

        # Create emitter
        emitter = BacktestEventEmitter(run_id)

        # Schedule appropriate emission based on type
        if event_type == "progress":
            coro = emitter.emit_progress(
                data.get("current", 0),
                data.get("total", 1),
                data.get("message", "")
            )
        elif event_type == "trade":
            coro = emitter.emit_trade(data.get("trade_data", {}), data.get("trade_type", "entry"))
        elif event_type == "snapshot":
            coro = emitter.emit_snapshot(data)
        elif event_type == "status":
            coro = emitter.emit_status(
                data.get("status", "unknown"),
                data.get("message", ""),
                data.get("data")
            )
        elif event_type == "error":
            coro = emitter.emit_error(data.get("error", ""), data.get("details"))
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return

        # Create task on the loop
        asyncio.create_task(coro)

    except Exception as e:
        logger.error(f"Failed to emit WebSocket event: {e}")
