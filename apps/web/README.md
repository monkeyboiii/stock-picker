# Stock Picker Web Application

Next.js 14 web application for the Stock Picker platform - trading, backtesting, and analytics for Chinese stock markets.

## Features

✅ **Next.js 14** with App Router
✅ **TypeScript** for type safety
✅ **Tailwind CSS** for styling
✅ **Type-Safe API Clients** from @repo/api
✅ **Authentication** with JWT tokens
✅ **Responsive Design** for desktop and mobile

## Architecture

```
src/
├── app/
│   ├── page.tsx                # Landing page
│   ├── layout.tsx              # Root layout
│   ├── globals.css             # Global styles
│   ├── auth/
│   │   ├── login/              # Login page
│   │   └── signup/             # Signup page
│   └── dashboard/
│       ├── layout.tsx          # Dashboard layout with sidebar
│       ├── page.tsx            # Dashboard overview
│       ├── stocks/             # Stock screening
│       ├── backtests/          # Backtest management
│       └── analytics/          # Technical & risk analytics
```

## Getting Started

### Prerequisites

- Node.js 20+
- PNPM 9+
- Backend services running (trading-api, auth-api, etc.)

### Installation

```bash
# From repository root
pnpm install

# Or from apps/web directory
pnpm install
```

### Development

```bash
# Run development server
pnpm dev

# Open browser
open http://localhost:3000
```

### Build

```bash
# Build for production
pnpm build

# Start production server
pnpm start
```

## Environment Variables

Create `.env.local` for local development:

```env
NEXT_PUBLIC_TRADING_API_URL=http://localhost:8000
NEXT_PUBLIC_BACKTEST_API_URL=http://localhost:8001
NEXT_PUBLIC_AUTH_API_URL=http://localhost:8003
NEXT_PUBLIC_NOTIFICATION_API_URL=http://localhost:8004
NEXT_PUBLIC_CALCULATION_API_URL=http://localhost:8005
```

## Pages

### Landing Page (`/`)
- Platform overview
- Feature highlights
- Links to login/signup

### Authentication
- **`/auth/login`** - User login
- **`/auth/signup`** - User registration

### Dashboard (`/dashboard`)
- Overview with stats and recent activity
- Quick actions for common tasks

### Stocks (`/dashboard/stocks`)
- Stock screening and filtering
- Market indices summary
- Real-time stock data table
- Integration with Trading API

### Backtests (`/dashboard/backtests`)
- Backtest management
- Performance metrics (return, Sharpe, drawdown)
- Backtest execution status
- Integration with Backtest API

### Analytics (`/dashboard/analytics`)
- Technical indicators (MA, RSI, MACD, etc.)
- Risk analytics (Sharpe, VaR, Monte Carlo)
- Portfolio optimization
- Integration with Calculation API

## API Integration

The app uses type-safe API clients from `@repo/api`:

```typescript
import { createTradingClient } from '@repo/api/trading';
import { createAuthClient } from '@repo/api/auth';
import { createCalculationClient } from '@repo/api/calculation';

// Fully typed requests and responses
const tradingClient = createTradingClient('http://localhost:8000');
const { data, error } = await tradingClient.GET('/api/v1/stocks/{code}', {
  params: { path: { code: '600000' } }
});
```

## Styling

Uses **Tailwind CSS** with custom configuration:

- Design tokens from `@repo/ui`
- Custom colors (primary, success, bullish, bearish)
- Responsive breakpoints
- Dark mode support (future)

### Color Usage

```typescript
import { colors } from '@repo/ui';

// Trading-specific colors
className="text-bullish"  // Green for profit
className="text-bearish"  // Red for loss
```

## Authentication

Uses `@repo/auth` for token management:

```typescript
import { tokenStorage, isTokenExpired } from '@repo/auth';

// Store tokens
await tokenStorage.setAccessToken(accessToken);

// Check expiration
if (isTokenExpired(token)) {
  // Refresh token logic
}
```

## Development Workflow

### Adding a New Page

1. Create directory: `src/app/your-page/`
2. Create `page.tsx` component
3. Add navigation link in dashboard layout
4. Integrate with appropriate API client

### Adding API Integration

1. Import client from `@repo/api`
2. Use typed methods (GET, POST, etc.)
3. Handle loading and error states
4. Display data in UI

### Styling Components

1. Use Tailwind utility classes
2. Reference design tokens from `@repo/ui`
3. Follow responsive design patterns
4. Use semantic color names

## Testing

```bash
# Type checking
pnpm type-check

# Linting
pnpm lint

# Build test (ensures no compilation errors)
pnpm build
```

## Deployment

### Vercel (Recommended)

```bash
# Deploy to Vercel
vercel

# Or via GitHub integration
git push origin main
```

### Docker

```bash
# Build Docker image
docker build -t stock-picker-web .

# Run container
docker run -p 3000:3000 stock-picker-web
```

### Self-Hosted

```bash
# Build for production
pnpm build

# Start with PM2
pm2 start npm --name "stock-picker-web" -- start
```

## Troubleshooting

**Problem**: API requests fail with CORS errors
**Solution**: Ensure backend services have CORS enabled for web origin

**Problem**: Types not found from @repo/* packages
**Solution**: Run `pnpm build` in packages directory first

**Problem**: Environment variables not working
**Solution**: Restart dev server after changing `.env.local`

**Problem**: Tailwind styles not applying
**Solution**: Check `content` paths in `tailwind.config.ts`

## Performance

- **Code Splitting**: Automatic with Next.js App Router
- **Image Optimization**: Use `next/image` component
- **Font Optimization**: Using `next/font/google`
- **Bundle Size**: Monitored with `next build`

## Future Enhancements

- [ ] Add charts for stock price history (Recharts)
- [ ] Real-time updates via WebSocket
- [ ] Dark mode support
- [ ] Mobile app (React Native) with shared @repo packages
- [ ] E2E tests with Playwright
- [ ] Performance monitoring (Sentry, Vercel Analytics)

---

**Version:** 1.0.0
**Next.js:** 14.2.21
**Status:** Phase 7 Complete ✅
