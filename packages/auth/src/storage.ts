/**
 * Token Storage
 * Cross-platform token storage (web: localStorage, mobile: AsyncStorage)
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
 * Web localStorage implementation
 */
export class WebTokenStorage implements TokenStorage {
  async getAccessToken(): Promise<string | null> {
    if (typeof window === 'undefined') return null;
    return localStorage.getItem(ACCESS_TOKEN_KEY);
  }

  async setAccessToken(token: string): Promise<void> {
    if (typeof window === 'undefined') return;
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
 * Default token storage instance
 */
export const tokenStorage: TokenStorage = new WebTokenStorage();
