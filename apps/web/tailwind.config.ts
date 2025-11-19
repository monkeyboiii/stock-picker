import type { Config } from 'tailwindcss';
import { colors } from '@repo/ui';

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        primary: colors.primary,
        success: { DEFAULT: colors.success },
        warning: { DEFAULT: colors.warning },
        error: { DEFAULT: colors.error },
        info: { DEFAULT: colors.info },
        bullish: { DEFAULT: colors.bullish },
        bearish: { DEFAULT: colors.bearish },
      },
    },
  },
  plugins: [],
};

export default config;
