export default function DashboardPage() {
  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 mb-8">Dashboard Overview</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        {/* Stats Cards */}
        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <span className="text-3xl">💹</span>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Total Stocks
                  </dt>
                  <dd className="text-3xl font-semibold text-gray-900">5,247</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <span className="text-3xl">🔬</span>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Active Backtests
                  </dt>
                  <dd className="text-3xl font-semibold text-gray-900">12</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <span className="text-3xl">📈</span>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Best Sharpe
                  </dt>
                  <dd className="text-3xl font-semibold text-gray-900">2.34</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>

        <div className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <span className="text-3xl">✅</span>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    Win Rate
                  </dt>
                  <dd className="text-3xl font-semibold text-gray-900">68%</dd>
                </dl>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Recent Activity */}
      <div className="bg-white shadow rounded-lg mb-8">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-medium text-gray-900">Recent Activity</h2>
        </div>
        <div className="px-6 py-4">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-900">
                  Backtest completed: MA Crossover Strategy
                </p>
                <p className="text-sm text-gray-500">2 hours ago</p>
              </div>
              <span className="text-success text-sm font-medium">+12.5%</span>
            </div>
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-900">
                  Stock filter updated: High Volume Stocks
                </p>
                <p className="text-sm text-gray-500">5 hours ago</p>
              </div>
              <span className="text-info text-sm">142 stocks</span>
            </div>
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-900">
                  Portfolio optimization completed
                </p>
                <p className="text-sm text-gray-500">1 day ago</p>
              </div>
              <span className="text-success text-sm font-medium">Sharpe: 2.1</span>
            </div>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            Filter Stocks
          </h3>
          <p className="text-sm text-gray-500 mb-4">
            Apply technical filters to find trading opportunities
          </p>
          <a
            href="/dashboard/stocks"
            className="text-primary-600 hover:text-primary-700 text-sm font-medium"
          >
            Go to Stocks →
          </a>
        </div>

        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            Run Backtest
          </h3>
          <p className="text-sm text-gray-500 mb-4">
            Test your strategy with historical data
          </p>
          <a
            href="/dashboard/backtests"
            className="text-primary-600 hover:text-primary-700 text-sm font-medium"
          >
            Start Backtest →
          </a>
        </div>

        <div className="bg-white shadow rounded-lg p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-2">
            Calculate Metrics
          </h3>
          <p className="text-sm text-gray-500 mb-4">
            Compute technical indicators and risk metrics
          </p>
          <a
            href="/dashboard/analytics"
            className="text-primary-600 hover:text-primary-700 text-sm font-medium"
          >
            View Analytics →
          </a>
        </div>
      </div>
    </div>
  );
}
