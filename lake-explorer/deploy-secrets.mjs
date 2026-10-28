import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export function createSecretsFile(env) {
  const names = ['R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY'];
  for (const name of names) {
    if (!env[name]) throw new Error(`Missing required environment variable: ${name}`);
  }
  const directory = mkdtempSync(join(tmpdir(), 'lake-explorer-secrets-'));
  const path = join(directory, 'secrets.json');
  try {
    writeFileSync(path, JSON.stringify(Object.fromEntries(names.map(name => [name, env[name]]))), { mode: 0o600 });
  } catch (error) {
    rmSync(directory, { recursive: true, force: true });
    throw error;
  }
  return { path, cleanup: () => rmSync(directory, { recursive: true, force: true }) };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const secrets = createSecretsFile(process.env);
  try {
    const result = spawnSync(process.execPath, ['node_modules/wrangler/bin/wrangler.js', 'deploy', '--secrets-file', secrets.path], { stdio: 'inherit' });
    if (result.error) throw result.error;
    process.exitCode = result.status ?? 1;
  } finally {
    secrets.cleanup();
  }
}
