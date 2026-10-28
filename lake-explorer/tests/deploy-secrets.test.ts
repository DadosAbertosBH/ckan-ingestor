import { afterEach, expect, it } from 'vitest';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { createSecretsFile } from '../deploy-secrets.mjs';

const cleanups: (() => void)[] = [];
afterEach(() => { for (const cleanup of cleanups.splice(0)) cleanup(); });

it('writes only the R2 secrets to a private temporary file and removes it', () => {
  const result = createSecretsFile({ R2_ACCESS_KEY_ID: 'access', R2_SECRET_ACCESS_KEY: 'secret', OTHER: 'ignored' });
  cleanups.push(result.cleanup);
  expect(JSON.parse(readFileSync(result.path, 'utf8'))).toEqual({ R2_ACCESS_KEY_ID: 'access', R2_SECRET_ACCESS_KEY: 'secret' });
  expect(statSync(result.path).mode & 0o777).toBe(0o600);
  result.cleanup();
  expect(existsSync(result.path)).toBe(false);
});

it('requires both R2 credentials before creating a file', () => {
  expect(() => createSecretsFile({ R2_ACCESS_KEY_ID: 'access' })).toThrow('R2_SECRET_ACCESS_KEY');
});
