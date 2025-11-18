"""
Tests for WebSocket functionality

Tests cover:
- ConnectionManager lifecycle and message broadcasting
- BacktestEventEmitter event types
- WebSocket endpoint connections (integration tests)
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import WebSocket

from app.api.backtest_events import BacktestEventEmitter, emit_backtest_event_sync
from app.api.websocket_manager import ConnectionManager, manager


class TestConnectionManager:
    """Tests for WebSocket ConnectionManager"""

    @pytest.fixture
    def connection_manager(self):
        """Create a fresh ConnectionManager instance"""
        return ConnectionManager()

    @pytest.fixture
    def mock_websocket(self):
        """Create a mock WebSocket"""
        ws = AsyncMock(spec=WebSocket)
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()
        return ws

    @pytest.mark.anyio
    async def test_connect(self, connection_manager, mock_websocket):
        """Test connecting a new WebSocket"""
        client_id = "test-client-1"

        await connection_manager.connect(mock_websocket, client_id)

        # Verify connection was accepted
        mock_websocket.accept.assert_awaited_once()

        # Verify client was registered
        assert client_id in connection_manager.active_connections
        assert connection_manager.active_connections[client_id] == mock_websocket

        # Verify metadata was created
        assert client_id in connection_manager.connection_metadata
        assert "connected_at" in connection_manager.connection_metadata[client_id]
        assert "rooms" in connection_manager.connection_metadata[client_id]

    @pytest.mark.anyio
    async def test_disconnect(self, connection_manager, mock_websocket):
        """Test disconnecting a WebSocket"""
        client_id = "test-client-1"

        # Manually add connection (bypass async connect)
        async with connection_manager._lock:
            connection_manager.active_connections[client_id] = mock_websocket
            connection_manager.connection_metadata[client_id] = {
                "connected_at": datetime.now().isoformat(),
                "rooms": set()
            }

        # Disconnect
        await connection_manager.disconnect(client_id)

        # Verify client was removed
        assert client_id not in connection_manager.active_connections
        assert client_id not in connection_manager.connection_metadata

    @pytest.mark.anyio
    async def test_send_personal_message(self, connection_manager, mock_websocket):
        """Test sending a message to a specific client"""
        client_id = "test-client-1"

        # Add connection
        connection_manager.active_connections[client_id] = mock_websocket

        # Send message
        message = {"type": "test", "data": "hello"}
        result = await connection_manager.send_personal_message(message, client_id)

        # Verify message was sent
        assert result is True
        mock_websocket.send_json.assert_awaited_once_with(message)

    @pytest.mark.anyio
    async def test_send_personal_message_nonexistent_client(self, connection_manager):
        """Test sending a message to a non-existent client"""
        result = await connection_manager.send_personal_message(
            {"type": "test"},
            "nonexistent-client"
        )

        # Should return False
        assert result is False

    @pytest.mark.anyio
    async def test_broadcast(self, connection_manager):
        """Test broadcasting a message to all clients"""
        # Add multiple connections
        ws1 = AsyncMock(spec=WebSocket)
        ws1.send_json = AsyncMock()
        ws2 = AsyncMock(spec=WebSocket)
        ws2.send_json = AsyncMock()

        connection_manager.active_connections["client-1"] = ws1
        connection_manager.active_connections["client-2"] = ws2

        # Broadcast message
        message = {"type": "broadcast", "data": "hello all"}
        await connection_manager.broadcast(message)

        # Verify all clients received message
        ws1.send_json.assert_awaited_once_with(message)
        ws2.send_json.assert_awaited_once_with(message)

    @pytest.mark.anyio
    async def test_join_room(self, connection_manager, mock_websocket):
        """Test joining a room"""
        client_id = "test-client-1"
        room = "backtest:12345"

        # Add connection
        async with connection_manager._lock:
            connection_manager.active_connections[client_id] = mock_websocket
            connection_manager.connection_metadata[client_id] = {
                "connected_at": datetime.now().isoformat(),
                "rooms": set()
            }

        # Join room
        result = await connection_manager.join_room(client_id, room)

        # Verify join was successful
        assert result is True
        assert room in connection_manager.rooms
        assert client_id in connection_manager.rooms[room]
        assert room in connection_manager.connection_metadata[client_id]["rooms"]

    @pytest.mark.anyio
    async def test_join_room_nonexistent_client(self, connection_manager):
        """Test joining a room with non-existent client"""
        result = await connection_manager.join_room("nonexistent-client", "test-room")
        assert result is False

    @pytest.mark.anyio
    async def test_leave_room(self, connection_manager, mock_websocket):
        """Test leaving a room"""
        client_id = "test-client-1"
        room = "backtest:12345"

        # Add connection and join room
        async with connection_manager._lock:
            connection_manager.active_connections[client_id] = mock_websocket
            connection_manager.connection_metadata[client_id] = {
                "connected_at": datetime.now().isoformat(),
                "rooms": {room}
            }
            connection_manager.rooms[room] = {client_id}

        # Leave room
        result = await connection_manager.leave_room(client_id, room)

        # Verify leave was successful
        assert result is True
        assert room not in connection_manager.rooms  # Room should be deleted when empty
        assert room not in connection_manager.connection_metadata[client_id]["rooms"]

    @pytest.mark.anyio
    async def test_send_to_room(self, connection_manager):
        """Test sending a message to all clients in a room"""
        room = "backtest:12345"

        # Add multiple connections to room
        ws1 = AsyncMock(spec=WebSocket)
        ws1.send_json = AsyncMock()
        ws2 = AsyncMock(spec=WebSocket)
        ws2.send_json = AsyncMock()
        ws3 = AsyncMock(spec=WebSocket)
        ws3.send_json = AsyncMock()

        connection_manager.active_connections["client-1"] = ws1
        connection_manager.active_connections["client-2"] = ws2
        connection_manager.active_connections["client-3"] = ws3

        connection_manager.rooms[room] = {"client-1", "client-2"}  # Only 2 clients in room

        # Send message to room
        message = {"type": "room_message", "data": "hello room"}
        await connection_manager.send_to_room(message, room)

        # Verify only room members received message
        ws1.send_json.assert_awaited_once_with(message)
        ws2.send_json.assert_awaited_once_with(message)
        ws3.send_json.assert_not_awaited()  # Not in room

    @pytest.mark.anyio
    async def test_get_room_members(self, connection_manager):
        """Test getting list of room members"""
        room = "backtest:12345"
        members = {"client-1", "client-2", "client-3"}

        async with connection_manager._lock:
            connection_manager.rooms[room] = members

        # Get members
        result = await connection_manager.get_room_members(room)

        # Verify all members are returned
        assert set(result) == members

    @pytest.mark.anyio
    async def test_get_room_members_nonexistent_room(self, connection_manager):
        """Test getting members of non-existent room"""
        result = await connection_manager.get_room_members("nonexistent-room")
        assert result == []

    @pytest.mark.anyio
    async def test_get_active_connections_count(self, connection_manager):
        """Test getting count of active connections"""
        # Add connections
        async with connection_manager._lock:
            connection_manager.active_connections["client-1"] = MagicMock()
            connection_manager.active_connections["client-2"] = MagicMock()
            connection_manager.active_connections["client-3"] = MagicMock()

        # Get count
        count = await connection_manager.get_active_connections_count()

        assert count == 3

    @pytest.mark.anyio
    async def test_get_rooms_count(self, connection_manager):
        """Test getting count of active rooms"""
        # Add rooms
        async with connection_manager._lock:
            connection_manager.rooms["room-1"] = {"client-1"}
            connection_manager.rooms["room-2"] = {"client-2"}

        # Get count
        count = await connection_manager.get_rooms_count()

        assert count == 2

    @pytest.mark.anyio
    async def test_get_connection_info(self, connection_manager):
        """Test getting connection metadata"""
        client_id = "test-client-1"

        # Add connection with metadata
        async with connection_manager._lock:
            connection_manager.connection_metadata[client_id] = {
                "connected_at": "2025-11-17T10:00:00",
                "rooms": {"room-1", "room-2"}
            }

        # Get info
        info = await connection_manager.get_connection_info(client_id)

        # Verify info
        assert info["connected_at"] == "2025-11-17T10:00:00"
        assert set(info["rooms"]) == {"room-1", "room-2"}

    @pytest.mark.anyio
    async def test_get_connection_info_nonexistent_client(self, connection_manager):
        """Test getting info for non-existent client"""
        info = await connection_manager.get_connection_info("nonexistent-client")
        assert info == {}


class TestBacktestEventEmitter:
    """Tests for BacktestEventEmitter"""

    @pytest.fixture
    def emitter(self):
        """Create a BacktestEventEmitter instance"""
        return BacktestEventEmitter(run_id="test-run-123")

    @pytest.fixture
    def mock_manager(self):
        """Mock the WebSocket manager"""
        with patch("app.api.backtest_events.manager") as mock:
            mock.send_to_room = AsyncMock()
            yield mock

    @pytest.mark.anyio
    async def test_emit_progress(self, emitter, mock_manager):
        """Test emitting progress event"""
        await emitter.emit_progress(current=50, total=100, message="Processing data")

        # Verify manager was called
        mock_manager.send_to_room.assert_awaited_once()

        # Verify event structure
        call_args = mock_manager.send_to_room.call_args
        event = call_args[0][0]  # First positional argument
        room = call_args[0][1]  # Second positional argument

        assert event["type"] == "progress"
        assert event["run_id"] == "test-run-123"
        assert event["data"]["current"] == 50
        assert event["data"]["total"] == 100
        assert event["data"]["percentage"] == 50.0
        assert event["data"]["message"] == "Processing data"
        assert room == "backtest:test-run-123"

    @pytest.mark.anyio
    async def test_emit_trade(self, emitter, mock_manager):
        """Test emitting trade event"""
        trade_data = {
            "symbol": "600000.SH",
            "price": 10.50,
            "quantity": 100,
            "timestamp": "2025-11-17T10:00:00"
        }

        await emitter.emit_trade(trade_data, trade_type="entry")

        # Verify event structure
        call_args = mock_manager.send_to_room.call_args
        event = call_args[0][0]

        assert event["type"] == "trade"
        assert event["run_id"] == "test-run-123"
        assert event["data"]["trade_type"] == "entry"
        assert event["data"]["symbol"] == "600000.SH"
        assert event["data"]["price"] == 10.50

    @pytest.mark.anyio
    async def test_emit_snapshot(self, emitter, mock_manager):
        """Test emitting portfolio snapshot event"""
        snapshot_data = {
            "total_value": 105000.00,
            "cash": 50000.00,
            "positions": 3,
            "timestamp": "2025-11-17T10:00:00"
        }

        await emitter.emit_snapshot(snapshot_data)

        # Verify event structure
        call_args = mock_manager.send_to_room.call_args
        event = call_args[0][0]

        assert event["type"] == "snapshot"
        assert event["run_id"] == "test-run-123"
        assert event["data"]["total_value"] == 105000.00
        assert event["data"]["cash"] == 50000.00

    @pytest.mark.anyio
    async def test_emit_status(self, emitter, mock_manager):
        """Test emitting status change event"""
        await emitter.emit_status(
            status="completed",
            message="Backtest completed successfully",
            data={"final_value": 120000}
        )

        # Verify event structure (should emit to both room and global)
        assert mock_manager.send_to_room.await_count == 2  # Room + global

        # Check first call (room-specific)
        call_args = mock_manager.send_to_room.call_args_list[0]
        event = call_args[0][0]
        room = call_args[0][1]

        assert event["type"] == "status"
        assert event["data"]["status"] == "completed"
        assert event["data"]["message"] == "Backtest completed successfully"
        assert event["data"]["final_value"] == 120000
        assert room == "backtest:test-run-123"

        # Check second call (global)
        call_args = mock_manager.send_to_room.call_args_list[1]
        event = call_args[0][0]
        room = call_args[0][1]

        assert event["type"] == "backtest_completed"
        assert room == "backtests:all"

    @pytest.mark.anyio
    async def test_emit_error(self, emitter, mock_manager):
        """Test emitting error event"""
        await emitter.emit_error(
            error="Data fetch failed",
            details={"source": "akshare", "code": 500}
        )

        # Verify event structure
        call_args = mock_manager.send_to_room.call_args
        event = call_args[0][0]

        assert event["type"] == "error"
        assert event["run_id"] == "test-run-123"
        assert event["data"]["error"] == "Data fetch failed"
        assert event["data"]["details"]["source"] == "akshare"


class TestEmitBacktestEventSync:
    """Tests for synchronous event emission wrapper"""

    @pytest.mark.anyio
    async def test_emit_progress_sync(self):
        """Test synchronous progress event emission"""
        with patch("app.api.backtest_events.manager") as mock_manager:
            mock_manager.send_to_room = AsyncMock()

            # Call synchronous wrapper
            emit_backtest_event_sync(
                run_id="test-run-123",
                event_type="progress",
                data={"current": 25, "total": 100, "message": "Loading data"}
            )

            # Allow event loop to process
            await asyncio.sleep(0.1)

            # Verify event was scheduled
            mock_manager.send_to_room.assert_awaited()

    @pytest.mark.anyio
    async def test_emit_trade_sync(self):
        """Test synchronous trade event emission"""
        with patch("app.api.backtest_events.manager") as mock_manager:
            mock_manager.send_to_room = AsyncMock()

            # Call synchronous wrapper
            emit_backtest_event_sync(
                run_id="test-run-123",
                event_type="trade",
                data={
                    "trade_data": {"symbol": "600000.SH", "price": 10.50},
                    "trade_type": "entry"
                }
            )

            # Allow event loop to process
            await asyncio.sleep(0.1)

            # Verify event was scheduled
            mock_manager.send_to_room.assert_awaited()

    def test_emit_sync_no_event_loop(self):
        """Test synchronous emission with no event loop (should not raise)"""
        # This should gracefully handle the case where there's no event loop
        emit_backtest_event_sync(
            run_id="test-run-123",
            event_type="progress",
            data={"current": 50, "total": 100}
        )
        # Should not raise any exception


# Integration tests (require running FastAPI server)
@pytest.mark.integration
class TestWebSocketEndpoints:
    """Integration tests for WebSocket endpoints"""

    @pytest.mark.anyio
    async def test_websocket_backtest_stream_connection(self):
        """Test connecting to backtest stream endpoint"""
        # This would require a running FastAPI server
        # Placeholder for integration test
        pass

    @pytest.mark.anyio
    async def test_websocket_all_backtests_connection(self):
        """Test connecting to all backtests stream endpoint"""
        # This would require a running FastAPI server
        # Placeholder for integration test
        pass

    @pytest.mark.anyio
    async def test_websocket_stats_connection(self):
        """Test connecting to stats endpoint"""
        # This would require a running FastAPI server
        # Placeholder for integration test
        pass
