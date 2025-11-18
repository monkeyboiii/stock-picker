# Frontend Applications

This directory contains all frontend applications for the stock analysis platform.

## Applications

### `web/` (Coming Soon - Phase 7)
- **Tech Stack**: Next.js 14 + React 18 + TypeScript
- **Purpose**: Main web application with SSR/SSG for SEO
- **Features**: Dashboard, stock screening, backtest viewer, marketing pages

### `mobile/` (Coming Soon - Phase 8)
- **Tech Stack**: React Native + Expo + TypeScript
- **Purpose**: Mobile app for iOS and Android
- **Features**: Reuses 80%+ code from web via shared packages

### `desktop/` (Coming Soon - Optional)
- **Tech Stack**: Electron + React Native Web
- **Purpose**: Desktop application for Windows, macOS, Linux
- **Features**: Reuses web app with desktop-specific features

## Development

```bash
# Start all frontend apps
pnpm dev --filter=web --filter=mobile

# Build all apps
pnpm build --filter=web --filter=mobile
```

## Shared Dependencies

All apps use shared packages from `packages/`:
- `@repo/api` - Type-safe API clients (OpenAPI-generated)
- `@repo/auth` - Authentication utilities
- `@repo/ui` - Universal UI components
- `@repo/charts` - Chart components
