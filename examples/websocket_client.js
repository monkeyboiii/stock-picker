/**
 * WebSocket Client Example (JavaScript/Node.js)
 *
 * This example demonstrates how to connect to the Stock Picker backtest
 * WebSocket API using JavaScript/Node.js.
 *
 * Usage:
 *     node examples/websocket_client.js --run-id <backtest_run_id>
 *     node examples/websocket_client.js --all
 *     node examples/websocket_client.js --stats
 *
 * Requirements:
 *     npm install ws
 */

const WebSocket = require('ws');

class BacktestWebSocketClient {
    constructor(baseUrl = 'ws://localhost:8000') {
        this.baseUrl = baseUrl;
        this.ws = null;
    }

    /**
     * Connect to a specific backtest stream
     */
    connectToBacktest(runId, clientId = null) {
        let url = `${this.baseUrl}/api/v1/ws/backtest/${runId}`;
        if (clientId) {
            url += `?client_id=${clientId}`;
        }

        console.log(`Connecting to backtest stream: ${runId}`);
        console.log(`URL: ${url}`);
        console.log('-'.repeat(60));

        this.ws = new WebSocket(url);

        this.ws.on('open', () => {
            console.log('✓ WebSocket connection opened');
        });

        this.ws.on('message', (data) => {
            const message = JSON.parse(data);
            this._handleMessage(message);
        });

        this.ws.on('close', () => {
            console.log('\n✗ Connection closed');
        });

        this.ws.on('error', (error) => {
            console.error('❌ WebSocket error:', error.message);
        });
    }

    /**
     * Connect to all backtests stream
     */
    connectToAllBacktests(clientId = null) {
        let url = `${this.baseUrl}/api/v1/ws/backtests`;
        if (clientId) {
            url += `?client_id=${clientId}`;
        }

        console.log('Connecting to all backtests stream');
        console.log(`URL: ${url}`);
        console.log('-'.repeat(60));

        this.ws = new WebSocket(url);

        this.ws.on('open', () => {
            console.log('✓ WebSocket connection opened');
        });

        this.ws.on('message', (data) => {
            const message = JSON.parse(data);
            this._handleMessage(message);
        });

        this.ws.on('close', () => {
            console.log('\n✗ Connection closed');
        });

        this.ws.on('error', (error) => {
            console.error('❌ WebSocket error:', error.message);
        });
    }

    /**
     * Connect to connection stats stream
     */
    connectToStats(clientId = null) {
        let url = `${this.baseUrl}/api/v1/ws/stats`;
        if (clientId) {
            url += `?client_id=${clientId}`;
        }

        console.log('Connecting to stats stream');
        console.log(`URL: ${url}`);
        console.log('-'.repeat(60));

        this.ws = new WebSocket(url);

        this.ws.on('open', () => {
            console.log('✓ WebSocket connection opened');

            // Request stats every 5 seconds
            setInterval(() => {
                if (this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send(JSON.stringify({
                        command: 'get_stats',
                        timestamp: new Date().toISOString()
                    }));
                }
            }, 5000);
        });

        this.ws.on('message', (data) => {
            const message = JSON.parse(data);
            this._handleMessage(message);
        });

        this.ws.on('close', () => {
            console.log('\n✗ Connection closed');
        });

        this.ws.on('error', (error) => {
            console.error('❌ WebSocket error:', error.message);
        });
    }

    /**
     * Send ping command
     */
    sendPing() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                command: 'ping',
                timestamp: new Date().toISOString()
            }));
        }
    }

    /**
     * Unsubscribe from current stream
     */
    unsubscribe() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                command: 'unsubscribe'
            }));
        }
    }

    /**
     * Handle incoming WebSocket message
     */
    _handleMessage(data) {
        const msgType = data.type || 'unknown';
        const timestamp = data.timestamp || 'N/A';
        const runId = data.run_id || 'N/A';

        switch (msgType) {
            case 'connected':
                console.log(`✓ Connected: ${data.message || 'N/A'}`);
                console.log(`  Client ID: ${data.client_id || 'N/A'}`);
                console.log('-'.repeat(60));
                break;

            case 'progress':
                const progressData = data.data || {};
                const percentage = progressData.percentage || 0;
                const current = progressData.current || 0;
                const total = progressData.total || 1;
                const message = progressData.message || '';

                console.log(`📊 Progress: ${percentage.toFixed(1)}% (${current}/${total})`);
                if (message) {
                    console.log(`   ${message}`);
                }
                break;

            case 'trade':
                const tradeData = data.data || {};
                const tradeType = tradeData.trade_type || 'unknown';
                const symbol = tradeData.symbol || 'N/A';
                const price = tradeData.price || 0;
                const quantity = tradeData.quantity || 0;

                const emoji = tradeType === 'entry' ? '🟢' : '🔴';
                console.log(`${emoji} Trade ${tradeType.toUpperCase()}: ${symbol} @ ${price} x ${quantity}`);
                break;

            case 'snapshot':
                const snapshotData = data.data || {};
                const totalValue = snapshotData.total_value || 0;
                const cash = snapshotData.cash || 0;
                const positions = snapshotData.positions || 0;

                console.log(`💼 Portfolio: $${totalValue.toLocaleString()} (Cash: $${cash.toLocaleString()}, Positions: ${positions})`);
                break;

            case 'status':
                const statusData = data.data || {};
                const status = statusData.status || 'unknown';
                const statusMessage = statusData.message || '';

                const statusEmoji = status === 'completed' ? '✓' : status === 'failed' ? '⚠' : '▶';
                console.log(`${statusEmoji} Status: ${status.toUpperCase()}`);
                if (statusMessage) {
                    console.log(`   ${statusMessage}`);
                }
                break;

            case 'error':
                const errorData = data.data || {};
                const error = errorData.error || 'Unknown error';
                const details = errorData.details || {};

                console.log(`❌ Error: ${error}`);
                if (Object.keys(details).length > 0) {
                    console.log(`   Details: ${JSON.stringify(details)}`);
                }
                break;

            case 'stats':
                const statsData = data.data || {};
                const activeConnections = statsData.active_connections || 0;
                const activeRooms = statsData.active_rooms || 0;
                const rooms = statsData.rooms || [];

                console.log('📈 Connection Stats:');
                console.log(`   Active Connections: ${activeConnections}`);
                console.log(`   Active Rooms: ${activeRooms}`);
                console.log(`   Rooms: ${rooms.length > 0 ? rooms.join(', ') : 'None'}`);
                break;

            case 'pong':
                console.log('🏓 Pong received');
                break;

            case 'unsubscribed':
                console.log('✓ Unsubscribed from stream');
                break;

            case 'backtest_started':
            case 'backtest_completed':
            case 'backtest_failed':
                const backtestEmoji = msgType === 'backtest_started' ? '🚀' : msgType === 'backtest_completed' ? '✅' : '❌';
                const backtestData = data.data || {};
                const backtestMessage = backtestData.message || '';

                console.log(`${backtestEmoji} ${msgType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}: ${runId}`);
                if (backtestMessage) {
                    console.log(`   ${backtestMessage}`);
                }
                break;

            default:
                console.log(`📨 Message (${msgType}):`);
                console.log(JSON.stringify(data, null, 2));
        }

        console.log(); // Blank line for readability
    }

    /**
     * Close the WebSocket connection
     */
    close() {
        if (this.ws) {
            this.ws.close();
        }
    }
}

// Main entry point
function main() {
    const args = process.argv.slice(2);
    const client = new BacktestWebSocketClient();

    // Parse arguments
    let runId = null;
    let showAll = false;
    let showStats = false;
    let clientId = null;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--run-id' && i + 1 < args.length) {
            runId = args[i + 1];
            i++;
        } else if (args[i] === '--all') {
            showAll = true;
        } else if (args[i] === '--stats') {
            showStats = true;
        } else if (args[i] === '--client-id' && i + 1 < args.length) {
            clientId = args[i + 1];
            i++;
        }
    }

    // Connect based on arguments
    if (runId) {
        client.connectToBacktest(runId, clientId);
    } else if (showAll) {
        client.connectToAllBacktests(clientId);
    } else if (showStats) {
        client.connectToStats(clientId);
    } else {
        console.log('Usage:');
        console.log('  node examples/websocket_client.js --run-id <backtest_run_id>');
        console.log('  node examples/websocket_client.js --all');
        console.log('  node examples/websocket_client.js --stats');
        console.log('');
        console.log('Options:');
        console.log('  --client-id <id>    Optional client identifier');
        process.exit(1);
    }

    // Handle Ctrl+C
    process.on('SIGINT', () => {
        console.log('\n\n✓ Disconnected by user');
        client.close();
        process.exit(0);
    });
}

// Run if executed directly
if (require.main === module) {
    main();
}

module.exports = BacktestWebSocketClient;
