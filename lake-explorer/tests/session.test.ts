import { expect, it, vi } from 'vitest';
import { jwtVerify } from 'jose';
import { handleRequest } from '../worker';

const publication = { id: 'one', createdAt: new Date().toISOString(), catalogKey: 'explorer/one/catalog.duckdb', protectedFilesKey: 'explorer/one/protected-files.json', bucket: 'public-datasets', tables: [] };
function environment() {
  return {
    BUCKET: { get: vi.fn(async (key: string) => ({ json: async () => key.endsWith('protected-files.json') ? ['s3://public-datasets/data/part.parquet'] : publication })) },
    ASSETS: { fetch: vi.fn(async () => new Response('compressed engine')) },
    SESSION_LIMITER: { limit: vi.fn(async () => ({ success: true })) },
    R2_ACCOUNT_ID: 'account', R2_ACCESS_KEY_ID: 'parent-key', R2_SECRET_ACCESS_KEY: 'parent-secret',
    R2_ENDPOINT: 'https://account.r2.cloudflarestorage.com', BUCKET_NAME: 'public-datasets',
  };
}
it('issues only temporary, read-only credentials scoped to publication objects', async () => {
  const env = environment();
  const response = await handleRequest(new Request('https://portal/api/session', { method: 'POST' }), env as never);
  expect(response.status).toBe(200);
  expect(response.headers.get('Cache-Control')).toBe('no-store');
  const data = await response.json() as any;
  expect(data.credentials.secretAccessKey).not.toBe('parent-secret');
  const jwt = atob(data.credentials.sessionToken).slice(4);
  const { payload } = await jwtVerify(jwt, new TextEncoder().encode('parent-secret'), { audience: 'account.r2.cloudflarestorage.com' });
  expect(payload.actions).toEqual(['GetObject', 'HeadObject']);
  expect(payload.paths).toEqual({ objectPaths: ['explorer/one/catalog.duckdb', 'data/part.parquet'] });
  expect(payload.exp! - payload.iat!).toBe(900);
});
it('serves the compressed engine within the static asset size limit', async () => {
  const env = environment();
  const response = await handleRequest(new Request('https://portal/engine/duckdb.wasm'), env as never);
  expect(response.headers.get('Content-Encoding')).toBe('gzip');
  expect(response.headers.get('Content-Type')).toBe('application/wasm');
});
it('rate limits issuance and rejects mismatched origins', async () => {
  const env = environment();
  env.SESSION_LIMITER.limit.mockResolvedValue({ success: false });
  expect((await handleRequest(new Request('https://portal/api/session', { method: 'POST' }), env as never)).status).toBe(429);
  expect((await handleRequest(new Request('https://portal/api/session', { method: 'POST', headers: { Origin: 'https://elsewhere' } }), env as never)).status).toBe(403);
});
it('accepts the Vite origin when the worker runs locally', async () => {
  const response = await handleRequest(new Request('http://127.0.0.1:8787/api/session', {
    method: 'POST', headers: { Origin: 'http://127.0.0.1:5173' },
  }), environment() as never);

  expect(response.status).toBe(200);
});
it('keeps renewals pinned to their original publication', async () => {
  const env = environment();
  await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: JSON.stringify({ publicationId: 'one' }) }), env as never);
  expect(env.BUCKET.get).toHaveBeenCalledWith('explorer/one/manifest.json');
});
it('reports stale publications as unhealthy', async () => {
  const env = environment();
  env.BUCKET.get.mockResolvedValue({ json: async () => ({ ...publication, createdAt: '2020-01-01T00:00:00Z' }) });
  expect((await handleRequest(new Request('https://portal/api/health'), env as never)).status).toBe(503);
});
it('scopes large lakes to tables referenced by the query', async () => {
  const env = environment();
  env.BUCKET.get.mockImplementation(async (key: string) => ({ json: async () => key.endsWith('protected-files.json')
    ? { files: ['s3://public-datasets/data/part.parquet', 's3://public-datasets/other/part.parquet'], tables: { 'main.people': ['s3://public-datasets/data/part.parquet'], 'main.other': ['s3://public-datasets/other/part.parquet'] } }
    : publication }));
  const response = await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: JSON.stringify({ tables: ['main.people'] }) }), env as never);
  expect(response.status).toBe(200);
  const data = await response.json() as any;
  const { payload } = await jwtVerify(atob(data.credentials.sessionToken).slice(4), new TextEncoder().encode('parent-secret'));
  expect(payload.paths).toEqual({ objectPaths: ['explorer/one/catalog.duckdb', 'data/part.parquet'] });
});
it('retires old publications while allowing the current version', async () => {
  const env = environment();
  const old = { ...publication, id: 'old', createdAt: '2020-01-01T00:00:00Z', catalogKey: 'explorer/old/catalog.duckdb' };
  env.BUCKET.get.mockImplementation(async (key: string) => ({ json: async () => key === 'explorer/old/manifest.json' ? old : publication }));
  const response = await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: JSON.stringify({ publicationId: 'old' }) }), env as never);
  expect(response.status).toBe(410);
});
it('validates request bodies before issuing credentials', async () => {
  const env = environment();
  for (const body of ['null', '[]', '{"tables":"all"}']) {
    expect((await handleRequest(new Request('https://portal/api/session', { method: 'POST', body }), env as never)).status).toBe(400);
  }
  expect((await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: ' '.repeat(5000) }), env as never)).status).toBe(413);
});
it('keeps credentials small for tables with many Parquet parts', async () => {
  const env = environment();
  env.BUCKET.get.mockImplementation(async (key: string) => ({ json: async () => key.endsWith('protected-files.json')
    ? { files: [], tables: { 'main.people': Array.from({ length: 2000 }, (_, i) => `s3://public-datasets/data/people/part-${i}.parquet`) } }
    : publication }));
  const response = await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: JSON.stringify({ tables: ['main.people'] }) }), env as never);
  const data = await response.json() as any;
  expect(data.credentials.sessionToken.length).toBeLessThan(8000);
  const { payload } = await jwtVerify(atob(data.credentials.sessionToken).slice(4), new TextEncoder().encode('parent-secret'));
  expect(payload.paths).toEqual({ objectPaths: ['explorer/one/catalog.duckdb'], prefixPaths: ['data/people/'] });
});
it('returns catalog metadata only during bootstrap', async () => {
  const env = environment();
  env.BUCKET.get.mockImplementation(async (key: string) => ({ json: async () => key.endsWith('protected-files.json') ? [] : { ...publication, tables: [{ name: 'people' }] } }));
  const response = await handleRequest(new Request('https://portal/api/session', { method: 'POST', body: JSON.stringify({ publicationId: 'one' }) }), env as never);
  const data = await response.json() as any;
  expect(data.publication.tables).toEqual([]);
});
