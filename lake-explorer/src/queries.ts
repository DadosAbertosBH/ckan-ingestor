import type { LakeTable } from '../shared/types';
export interface Filter { column: string; operator: string; value: string }
export interface ExploreQuery {
  table: LakeTable; columns?: string[]; filters?: Filter[]; order?: string;
  descending?: boolean; offset?: number; group?: string; measure?: string; aggregate?: string;
}
export function buildQuery(q: ExploreQuery): { sql: string; params: unknown[] } {
  const column = (name: string) => {
    if (!q.table.columns.some(c => c.name === name)) throw new Error('Coluna desconhecida');
    return quoteIdentifier(name);
  };
  const offset = q.offset ?? 0;
  if (!Number.isSafeInteger(offset) || offset < 0) throw new Error('Página inválida');
  const params: unknown[] = [];
  const where = (q.filters ?? []).map(f => {
    const c = column(f.column);
    if (f.operator === 'null') return `${c} IS NULL`;
    if (f.operator === 'notNull') return `${c} IS NOT NULL`;
    const operators: Record<string, string> = { equals: '=', notEquals: '<>', greater: '>', less: '<', contains: 'contains' };
    if (!operators[f.operator]) throw new Error('Filtro inválido');
    params.push(f.value);
    return f.operator === 'contains' ? `contains(CAST(${c} AS VARCHAR), ?)` : `${c} ${operators[f.operator]} ?`;
  });
  let selection = q.columns?.length ? q.columns.map(column).join(', ') : '*';
  if (q.group) {
    const agg = (q.aggregate ?? 'count').toUpperCase();
    if (!['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(agg)) throw new Error('Resumo inválido');
    selection = `${column(q.group)}, ${agg}(${agg === 'COUNT' ? '*' : column(q.measure ?? '')}) AS "valor"`;
  }
  let sql = `SELECT ${selection} FROM lake.${quoteIdentifier(q.table.schema)}.${quoteIdentifier(q.table.name)}`;
  if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
  if (q.group) sql += ` GROUP BY ${column(q.group)}`;
  if (q.order) sql += ` ORDER BY ${q.group && q.order === 'valor' ? '"valor"' : column(q.order)} ${q.descending ? 'DESC' : 'ASC'} NULLS LAST`;
  sql += ` LIMIT 100 OFFSET ${offset}`;
  return { sql, params };
}
export const quoteIdentifier = (name: string) => '"' + name.replaceAll('"', '""') + '"';
