"""
WebSocket Connection Manager - Manages WebSocket connections and broadcasting

This module provides:
- Connection lifecycle management
- Message broadcasting to specific connections
- Room/channel support for grouped messaging
- Connection state tracking
"""

import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set

from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger


class ConnectionManager:
    """
    Thread-safe WebSocket connection manager with asyncio locks

    Manages WebSocket connections and message broadcasting with proper
    concurrency control to prevent race conditions.
    """

    def __init__(self):
        # Active connections: {client_id: WebSocket}
        self.active_connections: Dict[str, WebSocket] = {}

        # Rooms/channels: {room_name: {client_id}}
        self.rooms: Dict[str, Set[str]] = defaultdict(set)

        # Connection metadata: {client_id: metadata_dict}
        self.connection_metadata: Dict[str, Dict] = {}

        # Asyncio lock for thread-safe operations
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        """
        Accept and register a new WebSocket connection (thread-safe)

        Args:
            websocket: FastAPI WebSocket instance
            client_id: Unique identifier for this connection
        """
        await websocket.accept()

        async with self._lock:
            self.active_connections[client_id] = websocket
            self.connection_metadata[client_id] = {
                "connected_at": datetime.now().isoformat(),
                "rooms": set()
            }

        logger.info(f"WebSocket connection established: {client_id}")

    async def disconnect(self, client_id: str) -> None:
        """
        Remove a WebSocket connection (thread-safe)

        Args:
            client_id: Unique identifier for the connection
        """
        async with self._lock:
            if client_id in self.active_connections:
                # Remove from all rooms
                if client_id in self.connection_metadata:
                    for room in self.connection_metadata[client_id]["rooms"]:
                        if room in self.rooms:
                            self.rooms[room].discard(client_id)
                            if not self.rooms[room]:
                                del self.rooms[room]

                # Remove connection
                del self.active_connections[client_id]
                if client_id in self.connection_metadata:
                    del self.connection_metadata[client_id]

                logger.info(f"WebSocket connection closed: {client_id}")

    async def send_personal_message(self, message: dict, client_id: str) -> bool:
        """
        Send a message to a specific connection

        Args:
            message: Dictionary to send as JSON
            client_id: Target connection ID

        Returns:
            True if sent successfully, False otherwise
        """
        if client_id in self.active_connections:
            try:
                await self.active_connections[client_id].send_json(message)
                return True
            except WebSocketDisconnect:
                await self.disconnect(client_id)
                return False
            except Exception as e:
                logger.error(f"Error sending message to {client_id}: {e}")
                return False
        return False

    async def broadcast(self, message: dict) -> None:
        """
        Broadcast a message to all active connections

        Args:
            message: Dictionary to send as JSON
        """
        disconnected_clients = []

        # Copy dict items to avoid RuntimeError during iteration
        async with self._lock:
            connections = list(self.active_connections.items())

        for client_id, connection in connections:
            try:
                await connection.send_json(message)
            except WebSocketDisconnect:
                disconnected_clients.append(client_id)
            except Exception as e:
                logger.error(f"Error broadcasting to {client_id}: {e}")
                disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            await self.disconnect(client_id)

    async def send_to_room(self, message: dict, room: str) -> None:
        """
        Send a message to all connections in a room

        Args:
            message: Dictionary to send as JSON
            room: Room/channel name
        """
        async with self._lock:
            if room not in self.rooms:
                return
            # Copy to avoid modification during iteration
            room_members = list(self.rooms[room])

        disconnected_clients = []

        for client_id in room_members:
            if client_id in self.active_connections:
                try:
                    await self.active_connections[client_id].send_json(message)
                except WebSocketDisconnect:
                    disconnected_clients.append(client_id)
                except Exception as e:
                    logger.error(f"Error sending to room {room}, client {client_id}: {e}")
                    disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            await self.disconnect(client_id)

    async def join_room(self, client_id: str, room: str) -> bool:
        """
        Add a connection to a room (thread-safe)

        Args:
            client_id: Connection ID
            room: Room/channel name

        Returns:
            True if joined successfully
        """
        async with self._lock:
            if client_id not in self.active_connections:
                return False

            self.rooms[room].add(client_id)

            if client_id in self.connection_metadata:
                self.connection_metadata[client_id]["rooms"].add(room)

        logger.debug(f"Client {client_id} joined room: {room}")
        return True

    async def leave_room(self, client_id: str, room: str) -> bool:
        """
        Remove a connection from a room (thread-safe)

        Args:
            client_id: Connection ID
            room: Room/channel name

        Returns:
            True if left successfully
        """
        async with self._lock:
            if room in self.rooms and client_id in self.rooms[room]:
                self.rooms[room].discard(client_id)

                if not self.rooms[room]:
                    del self.rooms[room]

                if client_id in self.connection_metadata:
                    self.connection_metadata[client_id]["rooms"].discard(room)

                logger.debug(f"Client {client_id} left room: {room}")
                return True

        return False

    async def get_room_members(self, room: str) -> List[str]:
        """
        Get list of client IDs in a room (thread-safe)

        Args:
            room: Room/channel name

        Returns:
            List of client IDs
        """
        async with self._lock:
            if room in self.rooms:
                return list(self.rooms[room])
            return []

    async def get_active_connections_count(self) -> int:
        """Get count of active WebSocket connections (thread-safe)"""
        async with self._lock:
            return len(self.active_connections)

    async def get_rooms_count(self) -> int:
        """Get count of active rooms (thread-safe)"""
        async with self._lock:
            return len(self.rooms)

    async def get_connection_info(self, client_id: str) -> dict:
        """
        Get metadata about a connection (thread-safe)

        Args:
            client_id: Connection ID

        Returns:
            Dictionary with connection metadata
        """
        async with self._lock:
            if client_id in self.connection_metadata:
                metadata = self.connection_metadata[client_id].copy()
                metadata["rooms"] = list(metadata["rooms"])
                return metadata
            return {}


# Global connection manager instance
manager = ConnectionManager()
