/**
 * Token Storage
 *
 * SECURITY UPDATE: Auth tokens are now stored in httpOnly cookies (set by backend).
 * This prevents XSS attacks as JavaScript cannot access httpOnly cookies.
 *
 * The browser automatically sends cookies with each request, so manual storage
 * is not needed for web applications.
 *
 * This module is kept for:
 * 1. Mobile applications (which need AsyncStorage)
 * 2. Backwards compatibility
 * 3. Development/testing purposes
 *
 * For production web apps: Use cookie-based authentication (no manual storage needed)
 */

const ACCESS_TOKEN_KEY = 'stock_picker_access_token';
const REFRESH_TOKEN_KEY = 'stock_picker_refresh_token';

export interface TokenStorage {
  getAccessToken(): Promise<string | null>;
  setAccessToken(token: string): Promise<void>;
  removeAccessToken(): Promise<void>;
  getRefreshToken(): Promise<string | null>;
  setRefreshToken(token: string): Promise<void>;
  removeRefreshToken(): Promise<void>;
  clear(): Promise<void>;
}

/**
 * Web Cookie-based implementation (RECOMMENDED for production)
 *
 * Tokens are stored in httpOnly cookies by the backend.
 * This class does NOT access cookies (JavaScript cannot read httpOnly cookies).
 * It only provides compatibility methods.
 */
export class WebCookieStorage implements TokenStorage {
  async getAccessToken(): Promise<string | null> {
    // Cannot read httpOnly cookies from JavaScript (this is a security feature)
    // The browser automatically sends cookies with requests
    console.warn('Tokens are in httpOnly cookies and cannot be accessed by JavaScript');
    return null;
  }

  async setAccessToken(token: string): Promise<void> {
    // No-op: Backend sets httpOnly cookies
    console.warn('Tokens are managed by httpOnly cookies set by the backend');
  }

  async removeAccessToken(): Promise<void> {
    // No-op: Backend clears cookies via /logout endpoint
  }

  async getRefreshToken(): Promise<string | null> {
    // Cannot read httpOnly cookies from JavaScript
    return null;
  }

  async setRefreshToken(token: string): Promise<void> {
    // No-op: Backend sets httpOnly cookies
  }

  async removeRefreshToken(): Promise<void> {
    // No-op: Backend clears cookies via /logout endpoint
  }

  async clear(): Promise<void> {
    // Cookies are cleared by calling the /logout endpoint
  }
}

/**
 * DEPRECATED: Web localStorage implementation
 *
 * WARNING: Storing tokens in localStorage is vulnerable to XSS attacks.
 * Only use this for development/testing.
 *
 * For production, use WebCookieStorage (cookies are set by backend).
 */
export class WebLocalStorage implements TokenStorage {
  async getAccessToken(): Promise<string | null> {
    if (typeof window === 'undefined') return null;
    console.warn('SECURITY: Using localStorage for tokens is deprecated. Use httpOnly cookies instead.');
    return localStorage.getItem(ACCESS_TOKEN_KEY);
  }

  async setAccessToken(token: string): Promise<void> {
    if (typeof window === 'undefined') return;
    console.warn('SECURITY: Using localStorage for tokens is deprecated. Use httpOnly cookies instead.');
    localStorage.setItem(ACCESS_TOKEN_KEY, token);
  }

  async removeAccessToken(): Promise<void> {
    if (typeof window === 'undefined') return;
    localStorage.removeItem(ACCESS_TOKEN_KEY);
  }

  async getRefreshToken(): Promise<string | null> {
    if (typeof window === 'undefined') return null;
    return localStorage.getItem(REFRESH_TOKEN_KEY);
  }

  async setRefreshToken(token: string): Promise<void> {
    if (typeof window === 'undefined') return;
    localStorage.setItem(REFRESH_TOKEN_KEY, token);
  }

  async removeRefreshToken(): Promise<void> {
    if (typeof window === 'undefined') return;
    localStorage.removeItem(REFRESH_TOKEN_KEY);
  }

  async clear(): Promise<void> {
    await this.removeAccessToken();
    await this.removeRefreshToken();
  }
}

/**
 * Default token storage instance (uses secure httpOnly cookies)
 */
export const tokenStorage: TokenStorage = new WebCookieStorage();
