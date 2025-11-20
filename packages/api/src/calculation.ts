/**
 * Calculation API Client
 * Type-safe client for technical indicators, risk analytics, and portfolio optimization
 */

import createClient from 'openapi-fetch';
import type { paths } from './generated/calculation';

export type CalculationPaths = paths;

/**
 * Create a Calculation API client
 * @param baseUrl - Base URL for the Calculation API (default: http://localhost:8005)
 * @param options - Additional fetch options
 */
export function createCalculationClient(
  baseUrl: string = 'http://localhost:8005',
  options?: RequestInit
) {
  return createClient<paths>({
    baseUrl,
    ...options,
  });
}

/**
 * Default Calculation API client instance
 */
export const calculationClient = createCalculationClient();

// Re-export types for convenience
export type {
  components as CalculationComponents,
} from './generated/calculation';
