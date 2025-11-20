# Mobile App Assets

This directory contains app icons and splash screens for the mobile application.

## Required Assets

To build the app, you need to provide the following assets:

### Icons

- **icon.png** (1024x1024 pixels)
  - Universal app icon
  - Should have a transparent or solid background

- **adaptive-icon.png** (1024x1024 pixels, Android)
  - Foreground layer for Android adaptive icons
  - The inner 512x512 circle is the visible area

- **notification-icon.png** (96x96 pixels, Android)
  - Android notification icon
  - Should be white icon on transparent background
  - Will be tinted by the system

### Splash Screens

- **splash.png** (1242x2436 pixels recommended)
  - Launch screen shown while app loads
  - Keep important content in center safe area

- **favicon.png** (48x48 pixels or larger)
  - Web favicon (when running on web)

## Generating Assets

You can use online tools like:
- https://www.appicon.co/
- https://icon.kitchen/

Or use Expo's asset generation:
```bash
npx expo-app-icon icon.png
npx expo-splash-screen
```

## Asset Guidelines

1. **Icon**: Should be simple, recognizable at small sizes
2. **Splash**: Should match your brand colors (currently white #ffffff)
3. **Notification icon**: Must be monochrome for Android
4. **Color scheme**: Primary blue #0ea5e9 (from design tokens)

## Temporary Placeholder

For development, you can create simple placeholder images:
- Solid color squares for icons
- Logo/text centered on white background for splash

These placeholders should be replaced before production builds.
