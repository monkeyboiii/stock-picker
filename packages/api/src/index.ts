/**
 * @repo/api - Type-safe API Clients
 *
 * Auto-generated from OpenAPI specs using openapi-typescript
 *
 * Usage:
 * ```typescript
 * import { tradingClient } from '@repo/api/trading';
 *
 * const { data, error } = await tradingClient.GET('/api/v1/stocks/{code}', {
 *   params: { path: { code: '600000' } }
 * });
 * ```
 */

// Export all API clients
export * from './trading';
export * from './backtest';
export * from './auth';
export * from './calculation';
export * from './notification';

// Export a combined client factory
export {
  createTradingClient,
  createBacktestClient,
  createAuthClient,
  createCalculationClient,
  createNotificationClient,
} from './trading';
export { createBacktestClient as backtestFactory } from './backtest';
export { createAuthClient as authFactory } from './auth';
export { createCalculationClient as calculationFactory } from './calculation';
export { createNotificationClient as notificationFactory } from './notification';
