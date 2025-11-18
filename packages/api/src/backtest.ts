/**
 * Backtest API Client
 * Type-safe client for strategy backtesting and performance analysis
 */

import createClient from 'openapi-fetch';
import type { paths } from './generated/backtest';

export type BacktestPaths = paths;

/**
 * Create a Backtest API client
 * @param baseUrl - Base URL for the Backtest API (default: http://localhost:8001)
 * @param options - Additional fetch options
 */
export function createBacktestClient(
  baseUrl: string = 'http://localhost:8001',
  options?: RequestInit
) {
  return createClient<paths>({
    baseUrl,
    ...options,
  });
}

/**
 * Default Backtest API client instance
 */
export const backtestClient = createBacktestClient();

// Re-export types for convenience
export type {
  components as BacktestComponents,
} from './generated/backtest';
