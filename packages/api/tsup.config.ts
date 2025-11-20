import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    trading: 'src/trading.ts',
    backtest: 'src/backtest.ts',
    auth: 'src/auth.ts',
    calculation: 'src/calculation.ts',
    notification: 'src/notification.ts',
  },
  format: ['cjs', 'esm'],
  dts: true,
  splitting: false,
  sourcemap: true,
  clean: true,
});
