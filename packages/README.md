# Shared TypeScript Packages

This directory contains shared TypeScript packages used across web, mobile, and desktop applications.

## Packages

### @repo/api ✅

Type-safe API clients generated from OpenAPI specs.

**Features:**
- Auto-generated from FastAPI OpenAPI specs
- Full TypeScript type safety
- Support for all backend services (trading, backtest, auth, calculation, notification)
- Built on `openapi-fetch` for lightweight, type-safe HTTP requests

**Usage:**
```typescript
import { tradingClient } from '@repo/api/trading';

const { data, error } = await tradingClient.GET('/api/v1/stocks/{code}', {
  params: { path: { code: '600000' } }
});
```

### @repo/auth ✅

Authentication utilities for JWT token management.

**Features:**
- Cross-platform token storage (web: localStorage)
- JWT decoding and validation
- Token expiration checking
- User info extraction from tokens

**Usage:**
```typescript
import { tokenStorage, isTokenExpired, getUserFromToken } from '@repo/auth';

await tokenStorage.setAccessToken(accessToken);
const user = getUserFromToken(token);
```

### @repo/ui ✅

Shared UI components and design tokens.

**Features:**
- Design tokens (colors, spacing)
- Trading-specific colors (bullish/bearish)
- Consistent styling across platforms

**Usage:**
```typescript
import { colors, spacing } from '@repo/ui';

const styles = {
  container: { padding: spacing[4], backgroundColor: colors.primary[500] }
};
```

### @repo/typescript-config ✅

Shared TypeScript configurations.

**Configurations:**
- `base.json` - Base config
- `nextjs.json` - Next.js config
- `react-library.json` - React library config

## Development

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Generate API types from OpenAPI
pnpm codegen
```

## Architecture

```
packages/
├── api/                    # OpenAPI-generated API clients
├── auth/                   # Authentication utilities
├── ui/                     # UI components and design tokens
└── config/                 # Shared configurations
    └── typescript/         # TypeScript configs
```

## Type Safety

All packages are fully typed with TypeScript. API types are auto-generated from OpenAPI specs to guarantee backend/frontend sync.

---

**Last Updated:** 2025-11-18
**Status:** Phase 6 Complete ✅
