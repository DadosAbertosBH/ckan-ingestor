import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './e2e', timeout: 120_000, workers: 1,
  use: { baseURL: 'http://127.0.0.1:5179', trace: 'retain-on-failure' },
  webServer: { command: 'pnpm run dev -- --port 5179', url: 'http://127.0.0.1:5179', reuseExistingServer: !process.env.CI },
});
