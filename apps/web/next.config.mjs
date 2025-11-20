/** @type {import('next').NextConfig} */
const nextConfig = {
  transpilePackages: ['@repo/api', '@repo/auth', '@repo/ui'],
  reactStrictMode: true,
  output: 'standalone', // Enable for Docker deployment
  env: {
    NEXT_PUBLIC_TRADING_API_URL: process.env.NEXT_PUBLIC_TRADING_API_URL || 'http://localhost:8000',
    NEXT_PUBLIC_BACKTEST_API_URL: process.env.NEXT_PUBLIC_BACKTEST_API_URL || 'http://localhost:8001',
    NEXT_PUBLIC_AUTH_API_URL: process.env.NEXT_PUBLIC_AUTH_API_URL || 'http://localhost:8003',
    NEXT_PUBLIC_NOTIFICATION_API_URL: process.env.NEXT_PUBLIC_NOTIFICATION_API_URL || 'http://localhost:8004',
    NEXT_PUBLIC_CALCULATION_API_URL: process.env.NEXT_PUBLIC_CALCULATION_API_URL || 'http://localhost:8005',
  },
};

export default nextConfig;
