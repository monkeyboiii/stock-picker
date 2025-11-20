import Link from 'next/link';

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center p-24">
      <div className="z-10 max-w-5xl w-full items-center justify-between font-mono text-sm">
        <h1 className="text-4xl font-bold mb-8 text-center">
          Stock Picker Platform
        </h1>

        <p className="text-xl text-center mb-12 text-gray-600">
          Automated stock screening and backtesting for Chinese markets
        </p>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-12">
          <div className="border border-gray-200 rounded-lg p-6 hover:border-primary-500 transition-colors">
            <h2 className="text-2xl font-semibold mb-2">📊 Trading</h2>
            <p className="text-gray-600 mb-4">
              Screen stocks from Shanghai, Shenzhen, Beijing, and Hong Kong markets with technical filters
            </p>
            <Link
              href="/dashboard/stocks"
              className="text-primary-600 hover:text-primary-700 font-medium"
            >
              View Stocks →
            </Link>
          </div>

          <div className="border border-gray-200 rounded-lg p-6 hover:border-primary-500 transition-colors">
            <h2 className="text-2xl font-semibold mb-2">🔬 Backtesting</h2>
            <p className="text-gray-600 mb-4">
              Test your trading strategies with historical data and comprehensive performance metrics
            </p>
            <Link
              href="/dashboard/backtests"
              className="text-primary-600 hover:text-primary-700 font-medium"
            >
              Run Backtest →
            </Link>
          </div>

          <div className="border border-gray-200 rounded-lg p-6 hover:border-primary-500 transition-colors">
            <h2 className="text-2xl font-semibold mb-2">📈 Analytics</h2>
            <p className="text-gray-600 mb-4">
              Calculate technical indicators, risk metrics, and portfolio optimization
            </p>
            <Link
              href="/dashboard/analytics"
              className="text-primary-600 hover:text-primary-700 font-medium"
            >
              View Analytics →
            </Link>
          </div>
        </div>

        <div className="flex justify-center gap-4">
          <Link
            href="/auth/login"
            className="px-6 py-3 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors font-medium"
          >
            Sign In
          </Link>
          <Link
            href="/auth/signup"
            className="px-6 py-3 border border-primary-600 text-primary-600 rounded-lg hover:bg-primary-50 transition-colors font-medium"
          >
            Sign Up
          </Link>
        </div>

        <div className="mt-16 text-center text-sm text-gray-500">
          <p className="mb-2">Powered by FastAPI, Next.js, and PostgreSQL</p>
          <p>
            <span className="text-success">5 Backend Services</span> •{' '}
            <span className="text-info">Type-Safe APIs</span> •{' '}
            <span className="text-warning">Real-Time Data</span>
          </p>
        </div>
      </div>
    </main>
  );
}
