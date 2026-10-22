import { SignJWT } from 'jose';
import type { Publication } from '../shared/types';

const json = (value: unknown, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' } });

export async function handleRequest(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  try {
    if (url.pathname.startsWith('/engine/') && url.pathname.endsWith('.wasm')) {
      url.pathname += '.gz';
      const asset = await env.ASSETS.fetch(new Request(url, request));
      if (!asset.ok) return asset;
      const headers = new Headers(asset.headers);
      headers.set('Content-Encoding', 'gzip');
      headers.set('Content-Type', 'application/wasm');
      headers.set('Cache-Control', 'public, max-age=31536000, immutable');
      return new Response(asset.body, { headers, encodeBody: 'manual' });
    }
    if (!url.pathname.startsWith('/api/')) return env.ASSETS.fetch(request);
    if (!validOrigin(request.headers.get('Origin'), url)) return json({ error: 'Origem inválida' }, 403);
    if (url.pathname === '/api/health' || url.pathname === '/api/latest') {
      const object = await env.BUCKET.get('explorer/latest.json');
      if (!object) return json({ error: 'Aguardando publicação' }, 503);
      const publication = await object.json<Publication>();
      const stale = Date.now() - Date.parse(publication.createdAt) > 36 * 3600_000;
      return url.pathname.endsWith('health') ? json({ publicationId: publication.id, stale }, stale ? 503 : 200) : json({ id: publication.id, createdAt: publication.createdAt });
    }
    if (url.pathname !== '/api/session') return json({ error: 'Rota desconhecida' }, 404);
    if (request.method !== 'POST') return json({ error: 'Use POST' }, 405);
    const rate = await env.SESSION_LIMITER.limit({ key: request.headers.get('CF-Connecting-IP') ?? 'local' });
    if (!rate.success) return json({ error: 'Aguarde um minuto para iniciar outra sessão' }, 429);
    if (Number(request.headers.get('Content-Length') ?? 0) > 4096) return json({ error: 'Solicitação muito grande' }, 413);
    const chunks: Uint8Array[] = [];
    const reader = request.body?.getReader();
    let size = 0;
    if (reader) {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > 4096) { await reader.cancel(); return json({ error: 'Solicitação muito grande' }, 413); }
          chunks.push(value);
        }
      } finally { reader.releaseLock(); }
    }
    const bytes = new Uint8Array(size);
    let position = 0;
    for (const chunk of chunks) { bytes.set(chunk, position); position += chunk.byteLength; }
    const text = new TextDecoder().decode(bytes);
    const input: unknown = text ? JSON.parse(text) : {};
    if (!validInput(input)) return json({ error: 'Solicitação inválida' }, 400);
    if (input.publicationId && !/^[A-Za-z0-9_-]+$/.test(input.publicationId)) return json({ error: 'Publicação inválida' }, 400);
    const key = input.publicationId ? `explorer/${input.publicationId}/manifest.json` : 'explorer/latest.json';
    const object = await env.BUCKET.get(key);
    if (!object) return json({ error: 'Publicação indisponível. Atualize a página.' }, 410);
    const publication = await object.json<Publication>();
    if (input.publicationId && Date.now() - Date.parse(publication.createdAt) > 48 * 3600_000) {
      const latest = await env.BUCKET.get('explorer/latest.json');
      const current = latest ? await latest.json<Publication>() : undefined;
      if (publication.id !== current?.id && publication.id !== current?.previousId) return json({ error: 'Esta publicação foi arquivada. Abra o catálogo atualizado.' }, 410);
    }
    if (publication.bucket !== env.BUCKET_NAME || !publication.catalogKey.startsWith(`explorer/${publication.id}/`)) throw new Error('invalid publication');
    const files = await env.BUCKET.get(publication.protectedFilesKey);
    if (!files) throw new Error('missing protected files');
    const prefix = `s3://${env.BUCKET_NAME}/`;
    const index = await files.json<string[] | { files: string[]; tables: Record<string, string[]> }>();
    let selectedFiles: string[];
    if (Array.isArray(index)) selectedFiles = index;
    else {
      if (input.tables?.some(table => !Object.hasOwn(index.tables, table))) return json({ error: 'Tabela desconhecida' }, 400);
      selectedFiles = [...new Set((input.tables ?? []).flatMap(table => index.tables[table]))];
    }
    const paths = selectedFiles.map(path => {
      if (!path.startsWith(prefix)) throw new Error('invalid data path');
      return path.slice(prefix.length);
    });
    const now = Math.floor(Date.now() / 1000);
    const scope: { objectPaths: string[]; prefixPaths?: string[] } = { objectPaths: [publication.catalogKey, ...paths] };
    if (JSON.stringify(scope).length > 4096) {
      const directories = [...new Set(paths.map(path => path.slice(0, path.lastIndexOf('/') + 1)))];
      if (directories.includes('')) return json({ error: 'Reduza a quantidade de tabelas nesta consulta' }, 422);
      scope.objectPaths = [publication.catalogKey];
      scope.prefixPaths = directories;
      if (JSON.stringify(scope).length > 4096) return json({ error: 'Reduza a quantidade de tabelas nesta consulta' }, 422);
    }
    const jwt = await new SignJWT({ bucket: env.BUCKET_NAME, actions: ['GetObject', 'HeadObject'], paths: scope })
      .setProtectedHeader({ alg: 'HS256', typ: 'JWT' }).setSubject(env.R2_ACCOUNT_ID)
      .setIssuer(env.R2_ACCESS_KEY_ID).setAudience(new URL(env.R2_ENDPOINT).host)
      .setIssuedAt(now).setExpirationTime(now + 900)
      .sign(new TextEncoder().encode(env.R2_SECRET_ACCESS_KEY));
    const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(jwt));
    const secretAccessKey = Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('');
    return json({ publication: input.publicationId ? { ...publication, tables: [] } : publication, files: selectedFiles, endpoint: env.R2_ENDPOINT, expiresAt: (now + 900) * 1000,
      credentials: { accessKeyId: env.R2_ACCESS_KEY_ID, secretAccessKey, sessionToken: btoa(`jwt/${jwt}`) } });
  } catch (error) {
    if (error instanceof SyntaxError) return json({ error: 'Solicitação inválida' }, 400);
    console.error(JSON.stringify({ event: 'session_failed', error: error instanceof Error ? error.name : 'unknown' }));
    return json({ error: 'Serviço de dados temporariamente indisponível' }, 503);
  }
}
export default { fetch: handleRequest };

function validInput(input: unknown): input is { publicationId?: string; tables?: string[] } {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) return false;
  if ('publicationId' in input && typeof input.publicationId !== 'string') return false;
  if ('tables' in input && (!Array.isArray(input.tables) || input.tables.length > 50 || input.tables.some(t => typeof t !== 'string'))) return false;
  return true;
}

function validOrigin(origin: string | null, url: URL): boolean {
  if (!origin || origin === url.origin) return true;
  return ["127.0.0.1", "localhost"].includes(url.hostname)
    && ["http://127.0.0.1:5173", "http://localhost:5173"].includes(origin);
}
