/**
 * JWT Utilities
 * Decode and validate JWT tokens
 */

import { jwtDecode } from 'jwt-decode';

export interface JWTPayload {
  sub: string; // User ID
  email?: string;
  role?: string;
  exp: number; // Expiration timestamp
  iat: number; // Issued at timestamp
}

/**
 * Decode a JWT token without verifying signature
 * @param token - JWT token string
 * @returns Decoded payload or null if invalid
 */
export function decodeToken(token: string): JWTPayload | null {
  try {
    return jwtDecode<JWTPayload>(token);
  } catch (error) {
    console.error('Failed to decode token:', error);
    return null;
  }
}

/**
 * Check if a JWT token is expired
 * @param token - JWT token string
 * @returns true if expired, false if valid
 */
export function isTokenExpired(token: string): boolean {
  const payload = decodeToken(token);
  if (!payload) return true;

  const now = Date.now() / 1000; // Convert to seconds
  return payload.exp < now;
}

/**
 * Get remaining time until token expires
 * @param token - JWT token string
 * @returns Remaining time in seconds, or 0 if expired
 */
export function getTokenExpiresIn(token: string): number {
  const payload = decodeToken(token);
  if (!payload) return 0;

  const now = Date.now() / 1000;
  const remaining = payload.exp - now;
  return remaining > 0 ? remaining : 0;
}

/**
 * Extract user information from token
 * @param token - JWT token string
 * @returns User info or null if invalid
 */
export function getUserFromToken(token: string): {
  id: string;
  email?: string;
  role?: string;
} | null {
  const payload = decodeToken(token);
  if (!payload) return null;

  return {
    id: payload.sub,
    email: payload.email,
    role: payload.role,
  };
}
