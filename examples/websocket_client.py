"""
WebSocket Client Example - Connect to backtest streaming

This example demonstrates how to connect to the Stock Picker backtest
WebSocket API and receive real-time updates.

Usage:
    python examples/websocket_client.py --run-id <backtest_run_id>
    python examples/websocket_client.py --all  # Listen to all backtests
    python examples/websocket_client.py --stats  # Connection stats

Requirements:
    pip install websockets asyncio
"""

import asyncio
import json
import argparse
from datetime import datetime
from typing import Optional

try:
    import websockets
except ImportError:
    print("Error: websockets library not installed")
    print("Install with: pip install websockets")
    exit(1)


class BacktestWebSocketClient:
    """WebSocket client for backtest streaming"""

    def __init__(self, base_url: str = "ws://localhost:8000"):
        self.base_url = base_url
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None

    async def connect_to_backtest(self, run_id: str, client_id: Optional[str] = None):
        """
        Connect to a specific backtest stream

        Args:
            run_id: Backtest run ID
            client_id: Optional client identifier
        """
        url = f"{self.base_url}/api/v1/ws/backtest/{run_id}"
        if client_id:
            url += f"?client_id={client_id}"

        print(f"Connecting to backtest stream: {run_id}")
        print(f"URL: {url}")
        print("-" * 60)

        async with websockets.connect(url) as websocket:
            self.websocket = websocket

            # Receive connection acknowledgment
            message = await websocket.recv()
            data = json.loads(message)
            print(f"✓ Connected: {data.get('message', 'N/A')}")
            print(f"  Client ID: {data.get('client_id', 'N/A')}")
            print("-" * 60)

            # Listen for events
            try:
                async for message in websocket:
                    data = json.loads(message)
                    self._handle_message(data)

            except websockets.exceptions.ConnectionClosed:
                print("\n✗ Connection closed")

    async def connect_to_all_backtests(self, client_id: Optional[str] = None):
        """
        Connect to all backtests stream

        Args:
            client_id: Optional client identifier
        """
        url = f"{self.base_url}/api/v1/ws/backtests"
        if client_id:
            url += f"?client_id={client_id}"

        print(f"Connecting to all backtests stream")
        print(f"URL: {url}")
        print("-" * 60)

        async with websockets.connect(url) as websocket:
            self.websocket = websocket

            # Receive connection acknowledgment
            message = await websocket.recv()
            data = json.loads(message)
            print(f"✓ Connected: {data.get('message', 'N/A')}")
            print("-" * 60)

            # Listen for events
            try:
                async for message in websocket:
                    data = json.loads(message)
                    self._handle_message(data)

            except websockets.exceptions.ConnectionClosed:
                print("\n✗ Connection closed")

    async def connect_to_stats(self, client_id: Optional[str] = None):
        """
        Connect to connection stats stream

        Args:
            client_id: Optional client identifier
        """
        url = f"{self.base_url}/api/v1/ws/stats"
        if client_id:
            url += f"?client_id={client_id}"

        print(f"Connecting to stats stream")
        print(f"URL: {url}")
        print("-" * 60)

        async with websockets.connect(url) as websocket:
            self.websocket = websocket

            # Request stats every 5 seconds
            try:
                while True:
                    # Send get_stats command
                    await websocket.send(json.dumps({
                        "command": "get_stats",
                        "timestamp": datetime.now().isoformat()
                    }))

                    # Receive response
                    message = await websocket.recv()
                    data = json.loads(message)
                    self._handle_message(data)

                    # Wait 5 seconds
                    await asyncio.sleep(5)

            except websockets.exceptions.ConnectionClosed:
                print("\n✗ Connection closed")

    def _handle_message(self, data: dict):
        """
        Handle incoming WebSocket message

        Args:
            data: Parsed JSON message
        """
        msg_type = data.get("type", "unknown")
        timestamp = data.get("timestamp", "N/A")
        run_id = data.get("run_id", "N/A")

        if msg_type == "progress":
            # Progress update
            progress_data = data.get("data", {})
            current = progress_data.get("current", 0)
            total = progress_data.get("total", 1)
            percentage = progress_data.get("percentage", 0)
            message = progress_data.get("message", "")

            print(f"📊 Progress: {percentage:.1f}% ({current}/{total})")
            if message:
                print(f"   {message}")

        elif msg_type == "trade":
            # Trade event
            trade_data = data.get("data", {})
            trade_type = trade_data.get("trade_type", "unknown")
            symbol = trade_data.get("symbol", "N/A")
            price = trade_data.get("price", 0)
            quantity = trade_data.get("quantity", 0)

            emoji = "🟢" if trade_type == "entry" else "🔴"
            print(f"{emoji} Trade {trade_type.upper()}: {symbol} @ {price} x {quantity}")

        elif msg_type == "snapshot":
            # Portfolio snapshot
            snapshot_data = data.get("data", {})
            total_value = snapshot_data.get("total_value", 0)
            cash = snapshot_data.get("cash", 0)
            positions = snapshot_data.get("positions", 0)

            print(f"💼 Portfolio: ${total_value:,.2f} (Cash: ${cash:,.2f}, Positions: {positions})")

        elif msg_type == "status":
            # Status change
            status_data = data.get("data", {})
            status = status_data.get("status", "unknown")
            message = status_data.get("message", "")

            emoji = "✓" if status == "completed" else "⚠" if status == "failed" else "▶"
            print(f"{emoji} Status: {status.upper()}")
            if message:
                print(f"   {message}")

        elif msg_type == "error":
            # Error event
            error_data = data.get("data", {})
            error = error_data.get("error", "Unknown error")
            details = error_data.get("details", {})

            print(f"❌ Error: {error}")
            if details:
                print(f"   Details: {details}")

        elif msg_type == "stats":
            # Connection stats
            stats_data = data.get("data", {})
            active_connections = stats_data.get("active_connections", 0)
            active_rooms = stats_data.get("active_rooms", 0)
            rooms = stats_data.get("rooms", [])

            print(f"📈 Connection Stats:")
            print(f"   Active Connections: {active_connections}")
            print(f"   Active Rooms: {active_rooms}")
            print(f"   Rooms: {', '.join(rooms) if rooms else 'None'}")

        elif msg_type in ["backtest_started", "backtest_completed", "backtest_failed"]:
            # Global backtest events
            status_data = data.get("data", {})
            message = status_data.get("message", "")

            emoji = "🚀" if msg_type == "backtest_started" else "✅" if msg_type == "backtest_completed" else "❌"
            print(f"{emoji} {msg_type.replace('_', ' ').title()}: {run_id}")
            if message:
                print(f"   {message}")

        else:
            # Unknown message type
            print(f"📨 Message ({msg_type}):")
            print(f"   {json.dumps(data, indent=2)}")

        print()  # Blank line for readability


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="WebSocket client for backtest streaming")
    parser.add_argument("--run-id", help="Backtest run ID to stream")
    parser.add_argument("--all", action="store_true", help="Listen to all backtests")
    parser.add_argument("--stats", action="store_true", help="Monitor connection stats")
    parser.add_argument("--url", default="ws://localhost:8000", help="WebSocket base URL")
    parser.add_argument("--client-id", help="Client identifier")

    args = parser.parse_args()

    client = BacktestWebSocketClient(base_url=args.url)

    try:
        if args.run_id:
            await client.connect_to_backtest(args.run_id, args.client_id)
        elif args.all:
            await client.connect_to_all_backtests(args.client_id)
        elif args.stats:
            await client.connect_to_stats(args.client_id)
        else:
            print("Error: Must specify --run-id, --all, or --stats")
            parser.print_help()
            return

    except KeyboardInterrupt:
        print("\n\n✓ Disconnected by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
