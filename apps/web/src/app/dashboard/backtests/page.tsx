'use client';

import { useState } from 'react';
import { createBacktestClient } from '@repo/api/backtest';

export default function BacktestsPage() {
  const [selectedBacktest, setSelectedBacktest] = useState<string | null>(null);

  const backtestClient = createBacktestClient(
    process.env.NEXT_PUBLIC_BACKTEST_API_URL || 'http://localhost:8001'
  );

  // Placeholder backtest data
  const backtests = [
    {
      id: '1',
      name: 'MA Crossover Strategy',
      status: 'completed',
      return: 12.5,
      sharpe: 1.82,
      maxDrawdown: -8.3,
      trades: 47,
      winRate: 68.1,
      createdAt: '2025-11-15',
    },
    {
      id: '2',
      name: 'RSI Mean Reversion',
      status: 'completed',
      return: 8.7,
      sharpe: 1.45,
      maxDrawdown: -12.1,
      trades: 62,
      winRate: 58.1,
      createdAt: '2025-11-12',
    },
    {
      id: '3',
      name: 'Momentum Breakout',
      status: 'running',
      return: 0,
      sharpe: 0,
      maxDrawdown: 0,
      trades: 0,
      winRate: 0,
      createdAt: '2025-11-18',
    },
  ];

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold text-gray-900">Backtests</h1>
        <button className="px-4 py-2 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700">
          New Backtest
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Total Backtests</div>
          <div className="text-2xl font-bold">{backtests.length}</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Best Return</div>
          <div className="text-2xl font-bold text-success">+12.5%</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Best Sharpe</div>
          <div className="text-2xl font-bold">1.82</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Avg Win Rate</div>
          <div className="text-2xl font-bold">63.1%</div>
        </div>
      </div>

      {/* Backtests List */}
      <div className="bg-white shadow rounded-lg overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-medium text-gray-900">All Backtests</h2>
        </div>
        <div className="divide-y divide-gray-200">
          {backtests.map((backtest) => (
            <div
              key={backtest.id}
              className="px-6 py-4 hover:bg-gray-50 cursor-pointer"
              onClick={() => setSelectedBacktest(backtest.id)}
            >
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center">
                  <h3 className="text-lg font-medium text-gray-900">
                    {backtest.name}
                  </h3>
                  <span
                    className={`ml-3 px-2 py-1 text-xs font-medium rounded-full ${
                      backtest.status === 'completed'
                        ? 'bg-success/10 text-success'
                        : 'bg-warning/10 text-warning'
                    }`}
                  >
                    {backtest.status}
                  </span>
                </div>
                <span className="text-sm text-gray-500">{backtest.createdAt}</span>
              </div>

              {backtest.status === 'completed' && (
                <div className="grid grid-cols-5 gap-4">
                  <div>
                    <div className="text-xs text-gray-500">Return</div>
                    <div className={`text-sm font-medium ${
                      backtest.return >= 0 ? 'text-success' : 'text-bearish'
                    }`}>
                      {backtest.return >= 0 ? '+' : ''}{backtest.return}%
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Sharpe</div>
                    <div className="text-sm font-medium text-gray-900">
                      {backtest.sharpe}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Max DD</div>
                    <div className="text-sm font-medium text-bearish">
                      {backtest.maxDrawdown}%
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Trades</div>
                    <div className="text-sm font-medium text-gray-900">
                      {backtest.trades}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-gray-500">Win Rate</div>
                    <div className="text-sm font-medium text-gray-900">
                      {backtest.winRate}%
                    </div>
                  </div>
                </div>
              )}

              {backtest.status === 'running' && (
                <div className="flex items-center text-sm text-gray-500">
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-primary-600 mr-2"></div>
                  Backtest in progress...
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* API Integration Note */}
      <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-4">
        <p className="text-sm text-blue-800">
          <strong>API Integration:</strong> Using @repo/api/backtest client.
          Connect to backtest-api on port 8001 to run and manage backtests.
        </p>
      </div>
    </div>
  );
}
