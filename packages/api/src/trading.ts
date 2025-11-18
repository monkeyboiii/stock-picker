/**
 * Trading API Client
 * Type-safe client for stock data, filtering, and portfolio management
 */

import createClient from 'openapi-fetch';
import type { paths } from './generated/trading';

export type TradingPaths = paths;

/**
 * Create a Trading API client
 * @param baseUrl - Base URL for the Trading API (default: http://localhost:8000)
 * @param options - Additional fetch options
 */
export function createTradingClient(
  baseUrl: string = 'http://localhost:8000',
  options?: RequestInit
) {
  return createClient<paths>({
    baseUrl,
    ...options,
  });
}

/**
 * Default Trading API client instance
 */
export const tradingClient = createTradingClient();

// Re-export types for convenience
export type {
  components as TradingComponents,
} from './generated/trading';
