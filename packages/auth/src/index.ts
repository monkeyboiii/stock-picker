/**
 * @repo/auth - Authentication Utilities
 *
 * Token storage, JWT decoding, and auth helpers
 *
 * Usage:
 * ```typescript
 * import { tokenStorage, decodeToken, isTokenExpired } from '@repo/auth';
 *
 * // Store tokens
 * await tokenStorage.setAccessToken(accessToken);
 *
 * // Check token expiration
 * if (isTokenExpired(token)) {
 *   // Refresh token
 * }
 *
 * // Get user info
 * const user = getUserFromToken(token);
 * ```
 */

// Export storage
export * from './storage';

// Export JWT utilities
export * from './jwt';

// Re-export types
export type { TokenStorage } from './storage';
export type { JWTPayload } from './jwt';
