/**
 * Auth API Client
 * Type-safe client for authentication and authorization
 */

import createClient from 'openapi-fetch';
import type { paths } from './generated/auth';

export type AuthPaths = paths;

/**
 * Create an Auth API client
 * @param baseUrl - Base URL for the Auth API (default: http://localhost:8003)
 * @param options - Additional fetch options
 */
export function createAuthClient(
  baseUrl: string = 'http://localhost:8003',
  options?: RequestInit
) {
  return createClient<paths>({
    baseUrl,
    ...options,
  });
}

/**
 * Default Auth API client instance
 */
export const authClient = createAuthClient();

// Re-export types for convenience
export type {
  components as AuthComponents,
} from './generated/auth';
