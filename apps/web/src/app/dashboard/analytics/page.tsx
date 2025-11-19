'use client';

import { useState } from 'react';
import { createCalculationClient } from '@repo/api/calculation';

export default function AnalyticsPage() {
  const [calculating, setCalculating] = useState(false);

  const calculationClient = createCalculationClient(
    process.env.NEXT_PUBLIC_CALCULATION_API_URL || 'http://localhost:8005'
  );

  const handleCalculate = async (type: string) => {
    setCalculating(true);
    // TODO: Call calculation API
    // const { data, error } = await calculationClient.POST('/api/v1/indicators/rsi', {
    //   body: { prices: [...], period: 14 }
    // });
    setTimeout(() => setCalculating(false), 1000);
  };

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Analytics</h1>

      {/* Technical Indicators */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">
          📊 Technical Indicators
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <button
            onClick={() => handleCalculate('ma')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Moving Average</div>
            <div className="text-sm text-gray-500">SMA, EMA, WMA</div>
          </button>
          <button
            onClick={() => handleCalculate('rsi')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">RSI</div>
            <div className="text-sm text-gray-500">Relative Strength Index</div>
          </button>
          <button
            onClick={() => handleCalculate('macd')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">MACD</div>
            <div className="text-sm text-gray-500">Moving Average Convergence</div>
          </button>
          <button
            onClick={() => handleCalculate('bollinger')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Bollinger Bands</div>
            <div className="text-sm text-gray-500">Volatility bands</div>
          </button>
          <button
            onClick={() => handleCalculate('stochastic')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Stochastic</div>
            <div className="text-sm text-gray-500">%K and %D oscillator</div>
          </button>
          <button
            onClick={() => handleCalculate('atr')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">ATR</div>
            <div className="text-sm text-gray-500">Average True Range</div>
          </button>
        </div>
      </div>

      {/* Risk Analytics */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">
          📈 Risk Analytics
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <button
            onClick={() => handleCalculate('sharpe')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Sharpe Ratio</div>
            <div className="text-sm text-gray-500">Risk-adjusted return</div>
          </button>
          <button
            onClick={() => handleCalculate('sortino')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Sortino Ratio</div>
            <div className="text-sm text-gray-500">Downside risk-adjusted</div>
          </button>
          <button
            onClick={() => handleCalculate('var')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Value at Risk</div>
            <div className="text-sm text-gray-500">VaR & CVaR</div>
          </button>
          <button
            onClick={() => handleCalculate('drawdown')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Max Drawdown</div>
            <div className="text-sm text-gray-500">Peak to trough decline</div>
          </button>
          <button
            onClick={() => handleCalculate('monte-carlo')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Monte Carlo</div>
            <div className="text-sm text-gray-500">Simulation analysis</div>
          </button>
          <button
            onClick={() => handleCalculate('calmar')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">Calmar Ratio</div>
            <div className="text-sm text-gray-500">Return / Max DD</div>
          </button>
        </div>
      </div>

      {/* Portfolio Optimization */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">
          🎯 Portfolio Optimization
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <button
            onClick={() => handleCalculate('optimize-sharpe')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">
              Maximize Sharpe Ratio
            </div>
            <div className="text-sm text-gray-500">
              Find optimal portfolio weights for best risk-adjusted return
            </div>
          </button>
          <button
            onClick={() => handleCalculate('optimize-volatility')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left"
          >
            <div className="font-medium text-gray-900 mb-1">
              Minimize Volatility
            </div>
            <div className="text-sm text-gray-500">
              Find optimal weights for minimum portfolio variance
            </div>
          </button>
          <button
            onClick={() => handleCalculate('efficient-frontier')}
            className="p-4 border border-gray-200 rounded-lg hover:border-primary-500 hover:bg-primary-50 transition-colors text-left col-span-full"
          >
            <div className="font-medium text-gray-900 mb-1">
              Efficient Frontier
            </div>
            <div className="text-sm text-gray-500">
              Calculate all optimal risk-return combinations
            </div>
          </button>
        </div>
      </div>

      {calculating && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg p-6 flex items-center gap-3">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
            <span className="text-gray-900">Calculating...</span>
          </div>
        </div>
      )}

      {/* API Integration Note */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <p className="text-sm text-blue-800">
          <strong>API Integration:</strong> Using @repo/api/calculation client.
          Connect to calculation-api on port 8005 for technical indicators,
          risk analytics, and portfolio optimization.
        </p>
      </div>
    </div>
  );
}
