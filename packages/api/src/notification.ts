/**
 * Notification API Client
 * Type-safe client for email, SMS, and push notifications
 */

import createClient from 'openapi-fetch';
import type { paths } from './generated/notification';

export type NotificationPaths = paths;

/**
 * Create a Notification API client
 * @param baseUrl - Base URL for the Notification API (default: http://localhost:8004)
 * @param options - Additional fetch options
 */
export function createNotificationClient(
  baseUrl: string = 'http://localhost:8004',
  options?: RequestInit
) {
  return createClient<paths>({
    baseUrl,
    ...options,
  });
}

/**
 * Default Notification API client instance
 */
export const notificationClient = createNotificationClient();

// Re-export types for convenience
export type {
  components as NotificationComponents,
} from './generated/notification';
