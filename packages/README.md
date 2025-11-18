# Shared TypeScript Packages

This directory contains shared TypeScript packages used across frontend applications.

## Packages

### `@repo/api` (Coming Soon - Phase 6)
- **Purpose**: Type-safe API clients generated from OpenAPI specs
- **Tech**: openapi-typescript, openapi-fetch
- **Auto-generated**: From FastAPI OpenAPI specs

### `@repo/auth` (Coming Soon - Phase 6)
- **Purpose**: Authentication utilities and hooks
- **Features**:
  - `useAuth()` hook
  - `AuthProvider` context
  - Token storage (web: localStorage, mobile: AsyncStorage)
  - Session management

### `@repo/ui` (Coming Soon - Phase 6)
- **Purpose**: Universal UI components (web + React Native)
- **Features**:
  - Design tokens (colors, spacing, typography)
  - Button, Input, Card, etc.
  - Works on both web and mobile

### `@repo/charts` (Coming Soon - Phase 6)
- **Purpose**: Chart components for financial data
- **Features**:
  - EquityCurve chart
  - TradeLog table
  - PerformanceMetrics dashboard

### `@repo/config` (Coming Soon - Phase 6)
- **Purpose**: Shared configuration
- **Includes**: ESLint configs, TypeScript configs, Tailwind configs

## Development

```bash
# Build all packages
turbo build --filter="@repo/*"

# Watch mode for development
turbo dev --filter="@repo/*"
```

## Usage in Apps

```typescript
// In apps/web or apps/mobile
import { tradingApi } from '@repo/api'
import { useAuth } from '@repo/auth'
import { Button } from '@repo/ui'
import { EquityCurve } from '@repo/charts'
```
