# Frontend Integration Guide

Complete guide for building a frontend application to visualize and interact with the Stock Picker Backtesting API.

## Table of Contents

- [Quick Start](#quick-start)
- [Technology Stack](#technology-stack)
- [Project Setup](#project-setup)
- [WebSocket Integration](#websocket-integration)
- [Component Examples](#component-examples)
- [State Management](#state-management)
- [Chart Integration](#chart-integration)
- [Playback Controller](#playback-controller)
- [Production Deployment](#production-deployment)

---

## Quick Start

### Prerequisites
- Node.js 18+ and npm/yarn
- Stock Picker API running on `http://localhost:8000`
- Basic knowledge of React and TypeScript

### Create New React App
```bash
# Using Vite (recommended - faster)
npm create vite@latest stock-picker-frontend -- --template react-ts
cd stock-picker-frontend
npm install

# Or using Create React App
npx create-react-app stock-picker-frontend --template typescript
cd stock-picker-frontend
```

### Install Dependencies
```bash
npm install \
  recharts \
  axios \
  @tanstack/react-query \
  zustand \
  date-fns \
  lucide-react \
  clsx \
  tailwind-merge

# Development dependencies
npm install -D \
  tailwindcss \
  postcss \
  autoprefixer \
  @types/node
```

---

## Technology Stack

### Recommended Stack
- **Framework**: React 18+ with TypeScript
- **Build Tool**: Vite (faster) or Create React App
- **State Management**: Zustand or React Query
- **Charts**: Recharts or Plotly.js
- **Styling**: Tailwind CSS
- **HTTP Client**: Axios
- **WebSocket**: Native WebSocket API
- **Date Handling**: date-fns

### Alternative Options
- **Charts**: D3.js, Chart.js, Apache ECharts
- **State**: Redux Toolkit, Jotai, Recoil
- **Styling**: Material-UI, Ant Design, Chakra UI

---

## Project Setup

### Directory Structure
```
stock-picker-frontend/
├── src/
│   ├── api/
│   │   ├── client.ts          # HTTP client setup
│   │   ├── websocket.ts       # WebSocket manager
│   │   └── types.ts           # TypeScript types
│   ├── components/
│   │   ├── BacktestList.tsx
│   │   ├── BacktestDetails.tsx
│   │   ├── EquityCurveChart.tsx
│   │   ├── TradeLog.tsx
│   │   ├── ProgressBar.tsx
│   │   └── PlaybackController.tsx
│   ├── hooks/
│   │   ├── useBacktest.ts
│   │   ├── useWebSocket.ts
│   │   └── usePlayback.ts
│   ├── stores/
│   │   └── backtestStore.ts
│   ├── utils/
│   │   └── formatters.ts
│   ├── App.tsx
│   └── main.tsx
├── package.json
└── tsconfig.json
```

### API Client Setup

**`src/api/client.ts`**:
```typescript
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export const apiClient = axios.create({
  baseURL: `${API_BASE_URL}/api/v1`,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor (for auth tokens, etc.)
apiClient.interceptors.request.use(
  (config) => {
    // Add auth token if available
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor (for error handling)
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      // Handle unauthorized
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);
```

**`src/api/types.ts`**:
```typescript
export interface BacktestRun {
  id: string;
  strategy_id: string;
  strategy_name: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  start_date: string;
  end_date: string;
  initial_capital: number;
  total_return?: number;
  sharpe_ratio?: number;
  max_drawdown?: number;
  total_trades?: number;
  created_at: string;
  completed_at?: string;
}

export interface Trade {
  id: number;
  stock_code: string;
  stock_name: string;
  entry_date: string;
  entry_price: number;
  exit_date: string;
  exit_price: number;
  exit_reason: string;
  shares: number;
  net_pnl: number;
  return_pct: number;
  holding_days: number;
}

export interface PortfolioSnapshot {
  snapshot_date: string;
  cash: number;
  holdings_value: number;
  total_value: number;
  daily_return: number;
  cumulative_return: number;
  drawdown: number;
  open_positions: number;
}

export interface ChartData {
  chart_type: string;
  data: any;
  config: any;
}

// WebSocket message types
export type WebSocketMessage =
  | ProgressMessage
  | TradeMessage
  | SnapshotMessage
  | StatusMessage
  | ErrorMessage;

export interface ProgressMessage {
  type: 'progress';
  run_id: string;
  timestamp: string;
  data: {
    current: number;
    total: number;
    percentage: number;
    message: string;
  };
}

export interface TradeMessage {
  type: 'trade';
  run_id: string;
  timestamp: string;
  data: {
    trade_type: 'entry' | 'exit';
    stock_code: string;
    stock_name?: string;
    price: number;
    shares: number;
    date: string;
    [key: string]: any;
  };
}

export interface SnapshotMessage {
  type: 'snapshot';
  run_id: string;
  timestamp: string;
  data: PortfolioSnapshot;
}

export interface StatusMessage {
  type: 'status';
  run_id: string;
  timestamp: string;
  data: {
    status: string;
    message: string;
    [key: string]: any;
  };
}

export interface ErrorMessage {
  type: 'error';
  run_id: string;
  timestamp: string;
  data: {
    error: string;
    details?: any;
  };
}
```

---

## WebSocket Integration

### WebSocket Manager

**`src/api/websocket.ts`**:
```typescript
import { WebSocketMessage } from './types';

export class BacktestWebSocket {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  private url: string;
  private clientId: string;
  private messageHandlers: ((message: WebSocketMessage) => void)[] = [];
  private isIntentionallyClosed = false;

  constructor(runId: string, clientId?: string) {
    this.clientId = clientId || `client-${Date.now()}`;
    const wsUrl = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';
    this.url = `${wsUrl}/api/v1/ws/backtest/${runId}?client_id=${this.clientId}`;
  }

  connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
          console.log('WebSocket connected');
          this.reconnectAttempts = 0;
          this.isIntentionallyClosed = false;
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const message: WebSocketMessage = JSON.parse(event.data);
            this.messageHandlers.forEach(handler => handler(message));
          } catch (error) {
            console.error('Failed to parse WebSocket message:', error);
          }
        };

        this.ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          reject(error);
        };

        this.ws.onclose = (event) => {
          console.log('WebSocket closed:', event.code, event.reason);

          if (!this.isIntentionallyClosed && this.reconnectAttempts < this.maxReconnectAttempts) {
            this.attemptReconnect();
          }
        };
      } catch (error) {
        reject(error);
      }
    });
  }

  private attemptReconnect() {
    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);

    console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(() => {
      this.connect().catch(error => {
        console.error('Reconnection failed:', error);
      });
    }, delay);
  }

  onMessage(handler: (message: WebSocketMessage) => void) {
    this.messageHandlers.push(handler);
    return () => {
      this.messageHandlers = this.messageHandlers.filter(h => h !== handler);
    };
  }

  send(data: any) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    } else {
      console.warn('WebSocket is not open, cannot send message');
    }
  }

  ping() {
    this.send({
      command: 'ping',
      timestamp: new Date().toISOString()
    });
  }

  unsubscribe() {
    this.send({
      command: 'unsubscribe'
    });
  }

  close() {
    this.isIntentionallyClosed = true;
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  getReadyState(): number | null {
    return this.ws?.readyState ?? null;
  }
}

// React Hook for WebSocket
export function useBacktestWebSocket(runId: string | null) {
  const [ws, setWs] = React.useState<BacktestWebSocket | null>(null);
  const [isConnected, setIsConnected] = React.useState(false);
  const [lastMessage, setLastMessage] = React.useState<WebSocketMessage | null>(null);

  React.useEffect(() => {
    if (!runId) return;

    const websocket = new BacktestWebSocket(runId);

    websocket.connect()
      .then(() => {
        setIsConnected(true);
        setWs(websocket);

        // Setup ping interval (every 30 seconds)
        const pingInterval = setInterval(() => {
          websocket.ping();
        }, 30000);

        return () => clearInterval(pingInterval);
      })
      .catch(error => {
        console.error('Failed to connect WebSocket:', error);
      });

    const unsubscribe = websocket.onMessage((message) => {
      setLastMessage(message);
    });

    return () => {
      unsubscribe();
      websocket.close();
      setIsConnected(false);
    };
  }, [runId]);

  return { ws, isConnected, lastMessage };
}
```

---

## Component Examples

### Backtest Progress Component

**`src/components/BacktestProgress.tsx`**:
```typescript
import React from 'react';
import { Progress } from './ui/progress';
import { useBacktestWebSocket } from '../api/websocket';

interface BacktestProgressProps {
  runId: string;
  onComplete?: () => void;
}

export function BacktestProgress({ runId, onComplete }: BacktestProgressProps) {
  const [progress, setProgress] = React.useState(0);
  const [status, setStatus] = React.useState<string>('Starting...');
  const [currentMessage, setCurrentMessage] = React.useState('');

  const { lastMessage } = useBacktestWebSocket(runId);

  React.useEffect(() => {
    if (!lastMessage) return;

    switch (lastMessage.type) {
      case 'progress':
        setProgress(lastMessage.data.percentage);
        setCurrentMessage(lastMessage.data.message);
        break;

      case 'status':
        setStatus(lastMessage.data.status);
        if (lastMessage.data.status === 'completed' && onComplete) {
          onComplete();
        }
        break;

      case 'error':
        setStatus('Error');
        setCurrentMessage(lastMessage.data.error);
        break;
    }
  }, [lastMessage, onComplete]);

  return (
    <div className="space-y-4">
      <div className="flex justify-between text-sm text-gray-600">
        <span>{status}</span>
        <span>{progress.toFixed(1)}%</span>
      </div>

      <Progress value={progress} className="w-full" />

      {currentMessage && (
        <p className="text-sm text-gray-500">{currentMessage}</p>
      )}
    </div>
  );
}
```

### Equity Curve Chart

**`src/components/EquityCurveChart.tsx`**:
```typescript
import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';
import { ChartData } from '../api/types';

interface EquityCurveChartProps {
  data: ChartData;
  showBenchmark?: boolean;
}

export function EquityCurveChart({ data, showBenchmark = true }: EquityCurveChartProps) {
  const chartData = React.useMemo(() => {
    if (!data?.data) return [];

    return data.data.dates.map((date: string, i: number) => ({
      date: new Date(date).toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' }),
      portfolio: data.data.portfolio_value[i],
      benchmark: showBenchmark ? data.data.benchmark?.[i] : undefined,
      drawdown: data.data.drawdown?.[i],
    }));
  }, [data, showBenchmark]);

  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis yAxisId="left" />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip />
        <Legend />
        <Line
          yAxisId="left"
          type="monotone"
          dataKey="portfolio"
          stroke="#8884d8"
          name="Portfolio Value"
          strokeWidth={2}
        />
        {showBenchmark && (
          <Line
            yAxisId="left"
            type="monotone"
            dataKey="benchmark"
            stroke="#82ca9d"
            name="Benchmark"
            strokeWidth={2}
            strokeDasharray="5 5"
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  );
}
```

### Trade Log

**`src/components/TradeLog.tsx`**:
```typescript
import React from 'react';
import { format } from 'date-fns';
import { Trade } from '../api/types';

interface TradeLogProps {
  trades: Trade[];
  maxHeight?: string;
}

export function TradeLog({ trades, maxHeight = '400px' }: TradeLogProps) {
  return (
    <div className="overflow-auto" style={{ maxHeight }}>
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50 sticky top-0">
          <tr>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
              Stock
            </th>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
              Entry
            </th>
            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
              Exit
            </th>
            <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">
              P&L
            </th>
            <th className="px-4 py-2 text-right text-xs font-medium text-gray-500 uppercase">
              Return %
            </th>
            <th className="px-4 py-2 text-center text-xs font-medium text-gray-500 uppercase">
              Days
            </th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {trades.map((trade) => (
            <tr key={trade.id} className="hover:bg-gray-50">
              <td className="px-4 py-2 text-sm">
                <div>{trade.stock_code}</div>
                <div className="text-gray-500 text-xs">{trade.stock_name}</div>
              </td>
              <td className="px-4 py-2 text-sm">
                <div>{format(new Date(trade.entry_date), 'yyyy-MM-dd')}</div>
                <div className="text-gray-500 text-xs">¥{trade.entry_price.toFixed(2)}</div>
              </td>
              <td className="px-4 py-2 text-sm">
                <div>{format(new Date(trade.exit_date), 'yyyy-MM-dd')}</div>
                <div className="text-gray-500 text-xs">¥{trade.exit_price.toFixed(2)}</div>
              </td>
              <td className={`px-4 py-2 text-sm text-right font-medium ${
                trade.net_pnl >= 0 ? 'text-green-600' : 'text-red-600'
              }`}>
                ¥{trade.net_pnl.toFixed(2)}
              </td>
              <td className={`px-4 py-2 text-sm text-right ${
                trade.return_pct >= 0 ? 'text-green-600' : 'text-red-600'
              }`}>
                {trade.return_pct.toFixed(2)}%
              </td>
              <td className="px-4 py-2 text-sm text-center text-gray-500">
                {trade.holding_days}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

---

## Playback Controller

### Playback Hook

**`src/hooks/usePlayback.ts`**:
```typescript
import React from 'react';
import { PortfolioSnapshot } from '../api/types';

export function usePlayback(snapshots: PortfolioSnapshot[]) {
  const [currentIndex, setCurrentIndex] = React.useState(0);
  const [isPlaying, setIsPlaying] = React.useState(false);
  const [playbackSpeed, setPlaybackSpeed] = React.useState(1);

  const currentSnapshot = snapshots[currentIndex];

  // Auto-play effect
  React.useEffect(() => {
    if (!isPlaying || currentIndex >= snapshots.length - 1) {
      return;
    }

    const delay = 1000 / playbackSpeed; // milliseconds per snapshot
    const timer = setTimeout(() => {
      setCurrentIndex(i => Math.min(i + 1, snapshots.length - 1));
    }, delay);

    return () => clearTimeout(timer);
  }, [isPlaying, currentIndex, snapshots.length, playbackSpeed]);

  const play = () => setIsPlaying(true);
  const pause = () => setIsPlaying(false);
  const reset = () => {
    setCurrentIndex(0);
    setIsPlaying(false);
  };
  const seekTo = (index: number) => {
    setCurrentIndex(Math.max(0, Math.min(index, snapshots.length - 1)));
  };

  return {
    currentSnapshot,
    currentIndex,
    totalSnapshots: snapshots.length,
    isPlaying,
    playbackSpeed,
    play,
    pause,
    reset,
    seekTo,
    setPlaybackSpeed,
  };
}
```

### Playback Controller Component

**`src/components/PlaybackController.tsx`**:
```typescript
import React from 'react';
import { Play, Pause, RotateCcw, ChevronLeft, ChevronRight } from 'lucide-react';

interface PlaybackControllerProps {
  currentIndex: number;
  totalSnapshots: number;
  isPlaying: boolean;
  playbackSpeed: number;
  onPlay: () => void;
  onPause: () => void;
  onReset: () => void;
  onSeek: (index: number) => void;
  onSpeedChange: (speed: number) => void;
}

export function PlaybackController({
  currentIndex,
  totalSnapshots,
  isPlaying,
  playbackSpeed,
  onPlay,
  onPause,
  onReset,
  onSeek,
  onSpeedChange,
}: PlaybackControllerProps) {
  return (
    <div className="space-y-4 p-4 bg-gray-50 rounded-lg">
      {/* Progress Bar */}
      <div className="space-y-2">
        <input
          type="range"
          min="0"
          max={totalSnapshots - 1}
          value={currentIndex}
          onChange={(e) => onSeek(parseInt(e.target.value))}
          className="w-full"
        />
        <div className="flex justify-between text-sm text-gray-600">
          <span>Day {currentIndex + 1}</span>
          <span>/ {totalSnapshots} days</span>
        </div>
      </div>

      {/* Controls */}
      <div className="flex items-center justify-center space-x-4">
        <button
          onClick={onReset}
          className="p-2 rounded-full hover:bg-gray-200"
          title="Reset"
        >
          <RotateCcw size={20} />
        </button>

        <button
          onClick={() => onSeek(currentIndex - 1)}
          disabled={currentIndex === 0}
          className="p-2 rounded-full hover:bg-gray-200 disabled:opacity-50"
          title="Previous Day"
        >
          <ChevronLeft size={20} />
        </button>

        {isPlaying ? (
          <button
            onClick={onPause}
            className="p-3 rounded-full bg-blue-500 text-white hover:bg-blue-600"
            title="Pause"
          >
            <Pause size={24} />
          </button>
        ) : (
          <button
            onClick={onPlay}
            className="p-3 rounded-full bg-blue-500 text-white hover:bg-blue-600"
            disabled={currentIndex >= totalSnapshots - 1}
            title="Play"
          >
            <Play size={24} />
          </button>
        )}

        <button
          onClick={() => onSeek(currentIndex + 1)}
          disabled={currentIndex >= totalSnapshots - 1}
          className="p-2 rounded-full hover:bg-gray-200 disabled:opacity-50"
          title="Next Day"
        >
          <ChevronRight size={20} />
        </button>

        {/* Speed Control */}
        <select
          value={playbackSpeed}
          onChange={(e) => onSpeedChange(parseFloat(e.target.value))}
          className="px-3 py-2 rounded border border-gray-300 text-sm"
        >
          <option value={0.5}>0.5x</option>
          <option value={1}>1x</option>
          <option value={2}>2x</option>
          <option value={5}>5x</option>
          <option value={10}>10x</option>
        </select>
      </div>
    </div>
  );
}
```

---

## State Management

### Zustand Store Example

**`src/stores/backtestStore.ts`**:
```typescript
import { create } from 'zustand';
import { BacktestRun, Trade, PortfolioSnapshot } from '../api/types';

interface BacktestStore {
  runs: BacktestRun[];
  currentRun: BacktestRun | null;
  trades: Trade[];
  snapshots: PortfolioSnapshot[];

  setRuns: (runs: BacktestRun[]) => void;
  setCurrentRun: (run: BacktestRun | null) => void;
  setTrades: (trades: Trade[]) => void;
  setSnapshots: (snapshots: PortfolioSnapshot[]) => void;
  addSnapshot: (snapshot: PortfolioSnapshot) => void;
}

export const useBacktestStore = create<BacktestStore>((set) => ({
  runs: [],
  currentRun: null,
  trades: [],
  snapshots: [],

  setRuns: (runs) => set({ runs }),
  setCurrentRun: (run) => set({ currentRun: run }),
  setTrades: (trades) => set({ trades }),
  setSnapshots: (snapshots) => set({ snapshots }),
  addSnapshot: (snapshot) => set((state) => ({
    snapshots: [...state.snapshots, snapshot]
  })),
}));
```

---

## Production Deployment

### Build Configuration

**`.env.production`**:
```
VITE_API_BASE_URL=https://api.yourdomain.com
VITE_WS_URL=wss://api.yourdomain.com
```

### Build and Deploy
```bash
# Build for production
npm run build

# Preview production build
npm run preview

# Deploy to Vercel/Netlify/etc
# dist/ folder contains static files
```

### Nginx Configuration (if self-hosting)
```nginx
server {
    listen 80;
    server_name yourdomain.com;

    root /var/www/stock-picker-frontend/dist;
    index index.html;

    # SPA routing
    location / {
        try_files $uri $uri/ /index.html;
    }

    # API proxy
    location /api/ {
        proxy_pass http://localhost:8000/api/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    # WebSocket proxy
    location /api/v1/ws/ {
        proxy_pass http://localhost:8000/api/v1/ws/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "Upgrade";
        proxy_set_header Host $host;
    }
}
```

---

## Next Steps

1. **Implement remaining components**: Metrics dashboard, strategy comparison, etc.
2. **Add authentication**: JWT tokens or OAuth
3. **Error boundaries**: React error boundaries for graceful error handling
4. **Testing**: Jest + React Testing Library
5. **Accessibility**: ARIA labels, keyboard navigation
6. **Performance**: Code splitting, lazy loading, memoization

For complete API reference, see [FRONTEND_API_SPEC.md](./FRONTEND_API_SPEC.md).
