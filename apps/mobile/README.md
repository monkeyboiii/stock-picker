# Stock Picker Mobile Application

React Native mobile application for the Stock Picker platform - trading, backtesting, and analytics for Chinese stock markets on iOS and Android.

## Features

✅ **React Native + Expo** - Modern mobile development
✅ **Expo Router** - File-based navigation
✅ **TypeScript** for type safety
✅ **Type-Safe API Clients** from @repo/api
✅ **Authentication** with JWT tokens
✅ **Push Notifications** for alerts
✅ **Cross-Platform** - iOS, Android, and Web

## Architecture

```
app/
├── _layout.tsx                # Root layout
├── index.tsx                  # Landing page
├── auth/
│   ├── login.tsx              # Login screen
│   └── signup.tsx             # Signup screen
└── (dashboard)/
    ├── _layout.tsx            # Tab navigation layout
    ├── index.tsx              # Dashboard overview
    ├── stocks.tsx             # Stock screening
    ├── backtests.tsx          # Backtest management
    └── analytics.tsx          # Technical & risk analytics
```

## Getting Started

### Prerequisites

- Node.js 20+
- PNPM 9+
- Expo CLI
- iOS Simulator (Mac) or Android Emulator

### Installation

```bash
# From repository root
pnpm install

# Or from apps/mobile directory
pnpm install
```

### Development

```bash
# Start Expo dev server
pnpm start

# Run on iOS simulator
pnpm ios

# Run on Android emulator
pnpm android

# Run on web browser
pnpm web

# Scan QR code with Expo Go app for physical device testing
```

### Environment Variables

Create `.env.local` for local development:

```env
EXPO_PUBLIC_TRADING_API_URL=http://localhost:8000
EXPO_PUBLIC_BACKTEST_API_URL=http://localhost:8001
EXPO_PUBLIC_AUTH_API_URL=http://localhost:8003
EXPO_PUBLIC_NOTIFICATION_API_URL=http://localhost:8004
EXPO_PUBLIC_CALCULATION_API_URL=http://localhost:8005
```

**Note:** On iOS Simulator/Android Emulator, use `http://10.0.2.2:PORT` (Android) or `http://localhost:PORT` (iOS) to access host machine's localhost.

For physical devices on same network, use your machine's local IP (e.g., `http://192.168.1.100:8000`).

## Screens

### Landing Page (`/`)
- Platform overview
- Feature highlights
- Links to login/signup

### Authentication
- **`/auth/login`** - User login with JWT
- **`/auth/signup`** - User registration

### Dashboard (`/(dashboard)`)
- **Overview** - Stats cards and recent activity
- **Stocks** - Stock screening with filters
- **Backtests** - Backtest management and metrics
- **Analytics** - Technical indicators and risk analytics

## Navigation

Using **Expo Router** for file-based navigation:

- **Stack Navigation**: Authentication flows
- **Tabs Navigation**: Dashboard screens
- **Deep Linking**: Custom URL scheme `stock-picker://`

## API Integration

The app uses type-safe API clients from `@repo/api`:

```typescript
import { createTradingClient } from '@repo/api/trading';
import { createAuthClient } from '@repo/api/auth';
import { createCalculationClient } from '@repo/api/calculation';

// Fully typed requests and responses
const tradingClient = createTradingClient(process.env.EXPO_PUBLIC_TRADING_API_URL);
const { data, error } = await tradingClient.GET('/api/v1/stocks/{code}', {
  params: { path: { code: '600000' } }
});
```

## Styling

Uses **React Native StyleSheet** with design tokens from `@repo/ui`:

```typescript
import { colors } from '@repo/ui';

const styles = StyleSheet.create({
  button: {
    backgroundColor: colors.primary[600],
  },
  profit: {
    color: colors.bullish, // Green
  },
  loss: {
    color: colors.bearish, // Red
  },
});
```

## Authentication

Uses `@repo/auth` for token management:

```typescript
import { tokenStorage } from '@repo/auth';

// Store tokens
await tokenStorage.setAccessToken(accessToken);

// Get tokens
const token = await tokenStorage.getAccessToken();

// Clear tokens
await tokenStorage.clearTokens();
```

On React Native, tokens are stored using `expo-secure-store` for secure storage.

## Push Notifications

### Setup

1. **Configure EAS Project ID** in `app.json`:
   ```json
   {
     "extra": {
       "eas": {
         "projectId": "your-project-id"
       }
     }
   }
   ```

2. **Register for push tokens**:
   ```typescript
   import { registerForPushNotificationsAsync } from '@/utils/notifications';

   const pushToken = await registerForPushNotificationsAsync();
   // Send token to backend for storage
   ```

3. **Handle notifications**:
   ```typescript
   import * as Notifications from 'expo-notifications';

   Notifications.addNotificationReceivedListener(notification => {
     console.log('Notification received:', notification);
   });

   Notifications.addNotificationResponseReceivedListener(response => {
     console.log('Notification tapped:', response);
   });
   ```

### Notification Types

- **Price Alerts** - Stock price crosses threshold
- **Backtest Complete** - Backtest execution finished
- **Strategy Signals** - Trading strategy generates signal
- **System Alerts** - Maintenance or important updates

## Building for Production

### Development Build

```bash
# Install EAS CLI
npm install -g eas-cli

# Login to Expo
eas login

# Build for iOS Simulator
eas build --profile development --platform ios

# Build for Android Emulator
eas build --profile development --platform android
```

### Production Build

```bash
# Build for iOS
eas build --profile production --platform ios

# Build for Android
eas build --profile production --platform android

# Build for both
eas build --profile production --platform all
```

### App Store Submission

```bash
# iOS - Submit to App Store Connect
eas submit --platform ios

# Android - Submit to Google Play
eas submit --platform android
```

## Testing

### Type Checking

```bash
pnpm type-check
```

### Linting

```bash
pnpm lint
```

### Manual Testing

1. **iOS Simulator** (Mac only):
   ```bash
   pnpm ios
   ```

2. **Android Emulator**:
   ```bash
   pnpm android
   ```

3. **Expo Go App** (Physical Device):
   - Install Expo Go from App Store/Play Store
   - Scan QR code from `pnpm start`

### E2E Testing (Future)

- **Maestro** for mobile E2E tests
- **Detox** as alternative

## Performance

- **React Native Reanimated** for smooth animations
- **FlashList** for optimized lists (future enhancement)
- **Hermes** JavaScript engine on Android
- **JSC** on iOS (or Hermes if configured)

## Troubleshooting

**Problem**: API requests fail from physical device
**Solution**: Use your machine's local IP instead of `localhost` in environment variables

**Problem**: Push notifications not working
**Solution**:
- Must use physical device (not simulator)
- Check permissions in Settings
- Verify EAS project ID in `app.json`

**Problem**: Metro bundler errors
**Solution**: Clear cache with `npx expo start -c`

**Problem**: Types not found from @repo/* packages
**Solution**: Run `pnpm build` in packages directory first

**Problem**: iOS build fails
**Solution**:
- Update Xcode to latest version
- Run `pod install` in ios/ directory (if exists)
- Clear derived data

**Problem**: Android build fails
**Solution**:
- Update Android Studio
- Check JDK version (needs 17+)
- Clear gradle cache: `cd android && ./gradlew clean`

## Project Structure

```
apps/mobile/
├── app/                       # Expo Router screens
│   ├── _layout.tsx
│   ├── index.tsx
│   ├── auth/
│   └── (dashboard)/
├── src/
│   ├── components/           # Reusable components
│   ├── hooks/                # Custom React hooks
│   └── utils/                # Utilities (notifications, etc.)
├── assets/                   # Images, fonts, icons
├── app.json                  # Expo configuration
├── babel.config.js          # Babel configuration
├── tsconfig.json            # TypeScript configuration
└── package.json
```

## Development Workflow

### Adding a New Screen

1. Create file in `app/` directory
2. Use TypeScript for type safety
3. Import @repo packages for API/auth/styling
4. Add navigation link in appropriate layout

### Adding API Integration

1. Import client from `@repo/api`
2. Use typed methods (GET, POST, etc.)
3. Handle loading and error states
4. Display data in UI with proper formatting

### Styling Components

1. Use StyleSheet.create()
2. Import design tokens from `@repo/ui`
3. Follow responsive design patterns
4. Use semantic color names (bullish/bearish)

## Deployment Options

### Expo EAS (Recommended)

- Managed cloud builds
- OTA updates
- Automatic submission to stores
- Free tier available

### Custom Build

- Use `expo prebuild` to generate native projects
- Build with Xcode (iOS) or Android Studio (Android)
- More control over native code

## Future Enhancements

- [ ] Add charts for stock price history (react-native-chart-kit)
- [ ] Real-time updates via WebSocket
- [ ] Offline mode with local caching
- [ ] Dark mode support
- [ ] Biometric authentication
- [ ] Widgets (iOS 14+, Android)
- [ ] E2E tests with Maestro
- [ ] Performance monitoring (Sentry)

---

**Version:** 1.0.0
**Expo SDK:** ~52.0.11
**React Native:** 0.76.5
**Status:** Phase 8 Complete ✅
