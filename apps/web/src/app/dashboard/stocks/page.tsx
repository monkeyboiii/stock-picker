'use client';

import { useState, useEffect } from 'react';
import { createTradingClient } from '@repo/api/trading';
import { colors } from '@repo/ui';

export default function StocksPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const tradingClient = createTradingClient(
    process.env.NEXT_PUBLIC_TRADING_API_URL || 'http://localhost:8000'
  );

  useEffect(() => {
    // TODO: Fetch stocks from trading API
    // fetchStocks();

    // Placeholder: Simulate API call
    setTimeout(() => {
      setLoading(false);
    }, 500);
  }, []);

  // Placeholder stock data
  const stocks = [
    { code: '600000', name: '浦发银行', price: 8.52, change: 0.35, changePercent: 4.28 },
    { code: '600036', name: '招商银行', price: 38.42, change: -0.58, changePercent: -1.49 },
    { code: '600519', name: '贵州茅台', price: 1678.50, change: 12.30, changePercent: 0.74 },
    { code: '000001', name: '平安银行', price: 12.45, change: 0.15, changePercent: 1.22 },
    { code: '000002', name: '万科A', price: 8.92, change: -0.12, changePercent: -1.33 },
  ];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">Loading stocks...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-error/10 border border-error rounded-lg p-4">
        <p className="text-error">{error}</p>
      </div>
    );
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold text-gray-900">Stocks</h1>
        <div className="flex gap-2">
          <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50">
            Filter
          </button>
          <button className="px-4 py-2 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700">
            Refresh Data
          </button>
        </div>
      </div>

      {/* Market Summary */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Shanghai Index</div>
          <div className="text-2xl font-bold">3,247.52</div>
          <div className="text-success text-sm">+0.82%</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Shenzhen Index</div>
          <div className="text-2xl font-bold">10,924.18</div>
          <div className="text-bearish text-sm">-0.34%</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Total Stocks</div>
          <div className="text-2xl font-bold">5,247</div>
          <div className="text-gray-500 text-sm">4 markets</div>
        </div>
        <div className="bg-white shadow rounded-lg p-4">
          <div className="text-sm text-gray-500 mb-1">Filtered</div>
          <div className="text-2xl font-bold">{stocks.length}</div>
          <div className="text-gray-500 text-sm">matching criteria</div>
        </div>
      </div>

      {/* Stocks Table */}
      <div className="bg-white shadow rounded-lg overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Code
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Name
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Price
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Change
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Change %
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {stocks.map((stock) => (
              <tr key={stock.code} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {stock.code}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                  {stock.name}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-right text-gray-900">
                  ¥{stock.price.toFixed(2)}
                </td>
                <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${
                  stock.change >= 0 ? 'text-success' : 'text-bearish'
                }`}>
                  {stock.change >= 0 ? '+' : ''}{stock.change.toFixed(2)}
                </td>
                <td className={`px-6 py-4 whitespace-nowrap text-sm text-right ${
                  stock.changePercent >= 0 ? 'text-success' : 'text-bearish'
                }`}>
                  {stock.changePercent >= 0 ? '+' : ''}{stock.changePercent.toFixed(2)}%
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <button className="text-primary-600 hover:text-primary-900 mr-4">
                    Details
                  </button>
                  <button className="text-gray-600 hover:text-gray-900">
                    Chart
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* API Integration Note */}
      <div className="mt-6 bg-blue-50 border border-blue-200 rounded-lg p-4">
        <p className="text-sm text-blue-800">
          <strong>API Integration:</strong> Using @repo/api/trading client.
          Connect to trading-api on port 8000 to fetch live stock data.
        </p>
      </div>
    </div>
  );
}
