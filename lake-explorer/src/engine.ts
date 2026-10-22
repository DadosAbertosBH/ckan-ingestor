import * as duckdb from '@duckdb/duckdb-wasm';
import workerURL from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import { AwsClient } from 'aws4fetch';
import { Table, type RecordBatch } from 'apache-arrow';
import type { QueryResult, Session } from '../shared/types';
import { readCatalogResponse, type DownloadProgress } from './catalog-download';

const string = (value: string) => "'" + value.replaceAll("'", "''") + "'";

export class LakeEngine {
  private db?: duckdb.AsyncDuckDB;
  private conn?: duckdb.AsyncDuckDBConnection;
  private worker?: Worker;
  private session?: Session;
  private exportTable?: Table;
  private scopedTables: string[] = [];
  private queryNumber = 0;
  private cancelled = false;
  onProgress: (message: string, fraction?: number) => void = () => {};

  async connect() {
    this.onProgress('Preparando sua sessão de leitura…');
    const response = await fetch('/api/session', { method: 'POST' });
    if (!response.ok) throw new Error((await response.json()).error ?? 'Falha ao iniciar sessão');
    this.session = await response.json() as Session;
    const worker = new Worker(workerURL);
    this.worker = worker;
    this.db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), worker);
    this.onProgress('Iniciando o mecanismo de consultas…');
    const wasm = import.meta.env.DEV ? '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm' : '/engine/duckdb-eh-1.33.1-dev57.0.wasm';
    await this.db.instantiate(wasm);
    await this.db.open({ filesystem: { reliableHeadRequests: true, allowFullHTTPReads: false, forceFullHTTPReads: false }, query: { castDecimalToDouble: true } });
    this.conn = await this.db.connect();
    await this.conn.query("SET memory_limit='768MB'; SET threads=1;");
    await this.setCredentials();
    this.onProgress('Carregando o catálogo do lake…');
    const { publication, endpoint, credentials } = this.session;
    const client = new AwsClient({ ...credentials, service: 's3', region: 'auto' });
    const object = await client.fetch(`${endpoint}/${publication.bucket}/${publication.catalogKey}`);
    if (!object.ok) throw new Error('Falha ao carregar o catálogo');
    const bytes = await readCatalogResponse(object, progress => this.reportCatalogProgress(progress));
    const digest = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes))), b => b.toString(16).padStart(2, '0')).join('');
    if (digest !== publication.sha256 || bytes.byteLength !== publication.size) throw new Error('O catálogo recebido falhou na verificação de integridade');
    await this.db.registerFileBuffer('catalog.duckdb', bytes);
    this.onProgress('Abrindo o catálogo DuckLake…');
    await this.conn.query('LOAD ducklake');
    const dataPath = publication.dataPath ? `, DATA_PATH ${string(publication.dataPath)}, OVERRIDE_DATA_PATH true` : '';
    await this.conn.query(`ATTACH 'ducklake:catalog.duckdb' AS lake (READ_ONLY${dataPath})`);
    return publication;
  }

  private reportCatalogProgress({ received, total }: DownloadProgress) {
    const megabytes = (bytes: number) => (bytes / 1024 / 1024).toLocaleString('pt-BR', { maximumFractionDigits: 1 });
    const detail = total ? `${megabytes(received)} de ${megabytes(total)} MB` : `${megabytes(received)} MB`;
    this.onProgress(`Baixando catálogo do lake… ${detail}`, total ? received / total : undefined);
  }

  private async setCredentials() {
    if (!this.conn || !this.session) return;
    const c = this.session.credentials;
    await this.conn.query(`SET s3_region='auto'; SET s3_endpoint=${string(this.session.endpoint)}; SET s3_access_key_id=${string(c.accessKeyId)}; SET s3_secret_access_key=${string(c.secretAccessKey)}; SET s3_session_token=${string(c.sessionToken)};`);
  }

  private async renew(tables = this.scopedTables) {
    if (!this.session) return;
    const same = JSON.stringify(tables) === JSON.stringify(this.scopedTables);
    if (same && Date.now() < this.session.expiresAt - 120_000) return;
    const response = await fetch('/api/session', { method: 'POST', body: JSON.stringify({ publicationId: this.session.publication.id, tables }) });
    if (!response.ok) throw new Error('A sessão expirou. Atualize o catálogo para continuar.');
    const renewed = await response.json() as Session;
    this.session = { ...renewed, publication: this.session.publication };
    this.scopedTables = tables;
    await this.setCredentials();
    for (const path of this.session.files ?? []) {
      await this.db!.registerFileURL(path, path, duckdb.DuckDBDataProtocol.S3, false);
    }
  }

  async query(sql: string, params: unknown[] = [], referencedTables?: string[]): Promise<QueryResult> {
    if (!this.conn) throw new Error('Abra o catálogo para consultar');
    this.cancelled = false;
    this.exportTable = undefined;
    const references = referencedTables ?? await this.conn.getTableNames(sql);
    const tables = this.session!.publication.tables.filter(t => references.some(name => name === t.name || name.endsWith(`.${t.name}`))).map(t => `${t.schema}.${t.name}`).sort();
    await this.renew(tables);
    if (this.cancelled) throw new Error('Consulta cancelada');
    // Wrapping accepts a single relational query and bounds browser result memory.
    const bounded = `SELECT * FROM (${sql.trim().replace(/;\s*$/, '')}) AS resultado LIMIT 10001`;
    const start = performance.now();
    const prepared = `explorer_query_${++this.queryNumber}`;
    await this.conn.query(`PREPARE ${prepared} AS ${bounded}`);
    try {
      if (this.cancelled) throw new Error('Consulta cancelada');
      const values = params.map(value => {
        if (value == null) return 'NULL';
        if (typeof value === 'string') return string(value);
        if (typeof value === 'boolean') return value ? 'true' : 'false';
        if (typeof value === 'number' && Number.isFinite(value)) return String(value);
        if (typeof value === 'bigint') return String(value);
        throw new Error('Tipo de parâmetro inválido');
      });
      // SQL EXECUTE preserves bound parameters while using the cooperative
      // pending-query API; sendPrepared blocks until its first batch is ready.
      const reader = await this.conn.send(`EXECUTE ${prepared}${values.length ? `(${values.join(', ')})` : ''}`);
      await reader.open();
      const columns = reader.schema.fields.map(f => ({ name: f.name, type: f.type.toString() }));
      const rows: Record<string, unknown>[] = [];
      const batches: RecordBatch[] = [];
      for await (const batch of reader) {
        batches.push(batch);
        for (const row of batch.toArray()) rows.push(row.toJSON());
      }
      this.exportTable = new Table(reader.schema, batches).slice(0, 10000);
      return { columns, rows: rows.slice(0, 10000), truncated: rows.length > 10000, elapsedMs: performance.now() - start };
    } finally { await this.conn.query(`DEALLOCATE ${prepared}`); }
  }

  async export(format: 'csv' | 'parquet') {
    if (!this.conn || !this.db || !this.exportTable) throw new Error('Execute uma consulta primeiro');
    const name = `resultado.${format}`;
    await this.db.dropFile(name);
    await this.conn.query('DROP TABLE IF EXISTS _explorer_export');
    try {
      await this.conn.insertArrowTable(this.exportTable, { name: '_explorer_export', create: true });
      await this.conn.query(`COPY _explorer_export TO '${name}' (FORMAT ${format.toUpperCase()}${format === 'csv' ? ', HEADER true' : ''})`);
      return await this.db.copyFileToBuffer(name);
    } finally { await this.conn.query('DROP TABLE IF EXISTS _explorer_export'); }
  }

  async cancel() { this.cancelled = true; return this.conn?.cancelSent(); }
  async close() {
    await this.conn?.close();
    await this.db?.terminate();
    this.worker?.terminate();
    this.session = undefined;
    this.scopedTables = [];
    this.exportTable = undefined;
  }
}
