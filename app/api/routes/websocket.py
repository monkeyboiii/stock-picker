"""
WebSocket API Routes - Real-time backtest streaming

Endpoints for streaming backtest progress, trades, and events in real-time.
"""

import uuid
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from loguru import logger

from app.api.auth import verify_token_userinfo
from app.api.websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/backtest/{run_id}")
async def websocket_backtest_stream(
    websocket: WebSocket,
    run_id: str,
    token: str = Query(..., description="JWT authentication token"),
    client_id: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for streaming backtest progress (authenticated)

    Streams real-time updates for a specific backtest run:
    - Progress updates (percentage complete)
    - Trade events (entry/exit)
    - Portfolio snapshots
    - Status changes (running -> completed)
    - Errors and warnings

    Args:
        run_id: Backtest run ID to stream
        token: JWT authentication token (required)
        client_id: Optional client identifier

    Message Format (JSON):
    {
        "type": "progress|trade|snapshot|status|error",
        "run_id": "...",
        "timestamp": "2025-11-17T10:30:00Z",
        "data": { ... }
    }
    """
    # Validate token before accepting connection
    try:
        user_info = await verify_token_userinfo(token)
        user_id = user_info.get("sub")

        if not user_id:
            logger.warning(f"WebSocket authentication failed: no user ID in token")
            await websocket.close(code=1008, reason="Authentication failed: invalid token")
            return

    except Exception as e:
        logger.warning(f"WebSocket authentication failed: {e}")
        await websocket.close(code=1008, reason="Authentication failed: invalid token")
        return

    # Generate client ID if not provided (associate with user ID)
    if not client_id:
        client_id = f"{user_id}:{uuid.uuid4()}"
    else:
        client_id = f"{user_id}:{client_id}"

    logger.info(f"WebSocket authenticated: user={user_id}, client={client_id}, run={run_id}")

    # Connect and join room for this backtest
    await manager.connect(websocket, client_id)
    room = f"backtest:{run_id}"
    manager.join_room(client_id, room)

    # Send connection acknowledgment
    await manager.send_personal_message(
        {
            "type": "connected",
            "run_id": run_id,
            "client_id": client_id,
            "user_id": user_id,
            "message": f"Connected to backtest stream: {run_id}"
        },
        client_id
    )

    try:
        # Keep connection alive and handle client messages
        while True:
            # Receive messages from client
            data = await websocket.receive_json()

            # Handle client commands
            command = data.get("command")

            if command == "ping":
                # Respond to ping
                await manager.send_personal_message(
                    {"type": "pong", "timestamp": data.get("timestamp")},
                    client_id
                )

            elif command == "unsubscribe":
                # Client wants to unsubscribe
                manager.leave_room(client_id, room)
                await manager.send_personal_message(
                    {"type": "unsubscribed", "run_id": run_id},
                    client_id
                )
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {client_id} from backtest {run_id}")
    except Exception as e:
        logger.error(f"WebSocket error for {client_id}: {e}")
    finally:
        manager.disconnect(client_id)


@router.websocket("/ws/backtests")
async def websocket_all_backtests(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token"),
    client_id: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for streaming all backtest activity (authenticated)

    Streams updates for all active backtests:
    - New backtest started
    - Backtest completed
    - Backtest failed

    Message Format (JSON):
    {
        "type": "backtest_started|backtest_completed|backtest_failed",
        "run_id": "...",
        "timestamp": "2025-11-17T10:30:00Z",
        "data": { ... }
    }
    """
    # Validate token before accepting connection
    try:
        user_info = await verify_token_userinfo(token)
        user_id = user_info.get("sub")

        if not user_id:
            logger.warning(f"WebSocket authentication failed: no user ID in token")
            await websocket.close(code=1008, reason="Authentication failed: invalid token")
            return

    except Exception as e:
        logger.warning(f"WebSocket authentication failed: {e}")
        await websocket.close(code=1008, reason="Authentication failed: invalid token")
        return

    # Generate client ID if not provided (associate with user ID)
    if not client_id:
        client_id = f"{user_id}:{uuid.uuid4()}"
    else:
        client_id = f"{user_id}:{client_id}"

    logger.info(f"WebSocket authenticated: user={user_id}, client={client_id}, stream=all")

    # Connect and join global backtest room
    await manager.connect(websocket, client_id)
    room = "backtests:all"
    manager.join_room(client_id, room)

    # Send connection acknowledgment
    await manager.send_personal_message(
        {
            "type": "connected",
            "client_id": client_id,
            "user_id": user_id,
            "message": "Connected to all backtests stream"
        },
        client_id
    )

    try:
        # Keep connection alive
        while True:
            data = await websocket.receive_json()

            command = data.get("command")

            if command == "ping":
                await manager.send_personal_message(
                    {"type": "pong", "timestamp": data.get("timestamp")},
                    client_id
                )

            elif command == "unsubscribe":
                manager.leave_room(client_id, room)
                await manager.send_personal_message(
                    {"type": "unsubscribed"},
                    client_id
                )
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {client_id} from all backtests stream")
    except Exception as e:
        logger.error(f"WebSocket error for {client_id}: {e}")
    finally:
        manager.disconnect(client_id)


@router.websocket("/ws/stats")
async def websocket_connection_stats(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token"),
    client_id: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for connection statistics (authenticated)

    Streams real-time statistics about WebSocket connections:
    - Active connections count
    - Active rooms count
    - Connected clients

    Useful for monitoring and debugging.
    Note: Consider adding admin-only scope check for production use.
    """
    # Validate token before accepting connection
    try:
        user_info = await verify_token_userinfo(token)
        user_id = user_info.get("sub")

        if not user_id:
            logger.warning(f"WebSocket authentication failed: no user ID in token")
            await websocket.close(code=1008, reason="Authentication failed: invalid token")
            return

        # TODO: Add admin scope check for production
        # if "admin" not in user_info.get("scopes", []):
        #     await websocket.close(code=1008, reason="Insufficient permissions")
        #     return

    except Exception as e:
        logger.warning(f"WebSocket authentication failed: {e}")
        await websocket.close(code=1008, reason="Authentication failed: invalid token")
        return

    # Generate client ID if not provided (associate with user ID)
    if not client_id:
        client_id = f"{user_id}:{uuid.uuid4()}"
    else:
        client_id = f"{user_id}:{client_id}"

    logger.info(f"WebSocket authenticated: user={user_id}, client={client_id}, stream=stats")

    await manager.connect(websocket, client_id)

    try:
        while True:
            # Wait for client request
            data = await websocket.receive_json()

            command = data.get("command")

            if command == "get_stats":
                # Send current stats
                stats = {
                    "type": "stats",
                    "data": {
                        "active_connections": manager.get_active_connections_count(),
                        "active_rooms": manager.get_rooms_count(),
                        "rooms": list(manager.rooms.keys()),
                    }
                }
                await manager.send_personal_message(stats, client_id)

            elif command == "ping":
                await manager.send_personal_message(
                    {"type": "pong", "timestamp": data.get("timestamp")},
                    client_id
                )

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {client_id} from stats stream")
    except Exception as e:
        logger.error(f"WebSocket error for {client_id}: {e}")
    finally:
        manager.disconnect(client_id)
