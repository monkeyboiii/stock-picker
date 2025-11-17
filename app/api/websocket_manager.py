"""
WebSocket Connection Manager - Manages WebSocket connections and broadcasting

This module provides:
- Connection lifecycle management
- Message broadcasting to specific connections
- Room/channel support for grouped messaging
- Connection state tracking
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Set

from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger


class ConnectionManager:
    """Manages WebSocket connections and message broadcasting"""

    def __init__(self):
        # Active connections: {client_id: WebSocket}
        self.active_connections: Dict[str, WebSocket] = {}

        # Rooms/channels: {room_name: {client_id}}
        self.rooms: Dict[str, Set[str]] = {}

        # Connection metadata: {client_id: metadata_dict}
        self.connection_metadata: Dict[str, Dict] = {}

    async def connect(self, websocket: WebSocket, client_id: str) -> None:
        """
        Accept and register a new WebSocket connection

        Args:
            websocket: FastAPI WebSocket instance
            client_id: Unique identifier for this connection
        """
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.connection_metadata[client_id] = {
            "connected_at": datetime.now().isoformat(),
            "rooms": set()
        }
        logger.info(f"WebSocket connection established: {client_id}")

    def disconnect(self, client_id: str) -> None:
        """
        Remove a WebSocket connection

        Args:
            client_id: Unique identifier for the connection
        """
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
                self.disconnect(client_id)
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

        for client_id, connection in self.active_connections.items():
            try:
                await connection.send_json(message)
            except WebSocketDisconnect:
                disconnected_clients.append(client_id)
            except Exception as e:
                logger.error(f"Error broadcasting to {client_id}: {e}")
                disconnected_clients.append(client_id)

        # Clean up disconnected clients
        for client_id in disconnected_clients:
            self.disconnect(client_id)

    async def send_to_room(self, message: dict, room: str) -> None:
        """
        Send a message to all connections in a room

        Args:
            message: Dictionary to send as JSON
            room: Room/channel name
        """
        if room not in self.rooms:
            return

        disconnected_clients = []

        for client_id in self.rooms[room]:
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
            self.disconnect(client_id)

    def join_room(self, client_id: str, room: str) -> bool:
        """
        Add a connection to a room

        Args:
            client_id: Connection ID
            room: Room/channel name

        Returns:
            True if joined successfully
        """
        if client_id not in self.active_connections:
            return False

        if room not in self.rooms:
            self.rooms[room] = set()

        self.rooms[room].add(client_id)

        if client_id in self.connection_metadata:
            self.connection_metadata[client_id]["rooms"].add(room)

        logger.debug(f"Client {client_id} joined room: {room}")
        return True

    def leave_room(self, client_id: str, room: str) -> bool:
        """
        Remove a connection from a room

        Args:
            client_id: Connection ID
            room: Room/channel name

        Returns:
            True if left successfully
        """
        if room in self.rooms and client_id in self.rooms[room]:
            self.rooms[room].discard(client_id)

            if not self.rooms[room]:
                del self.rooms[room]

            if client_id in self.connection_metadata:
                self.connection_metadata[client_id]["rooms"].discard(room)

            logger.debug(f"Client {client_id} left room: {room}")
            return True

        return False

    def get_room_members(self, room: str) -> List[str]:
        """
        Get list of client IDs in a room

        Args:
            room: Room/channel name

        Returns:
            List of client IDs
        """
        if room in self.rooms:
            return list(self.rooms[room])
        return []

    def get_active_connections_count(self) -> int:
        """Get count of active WebSocket connections"""
        return len(self.active_connections)

    def get_rooms_count(self) -> int:
        """Get count of active rooms"""
        return len(self.rooms)

    def get_connection_info(self, client_id: str) -> dict:
        """
        Get metadata about a connection

        Args:
            client_id: Connection ID

        Returns:
            Dictionary with connection metadata
        """
        if client_id in self.connection_metadata:
            metadata = self.connection_metadata[client_id].copy()
            metadata["rooms"] = list(metadata["rooms"])
            return metadata
        return {}


# Global connection manager instance
manager = ConnectionManager()
