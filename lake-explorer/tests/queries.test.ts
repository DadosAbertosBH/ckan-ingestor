import { describe, expect, it } from 'vitest';
import { buildQuery } from '../src/queries';
const table = { schema: 'main', name: 'weird"table', title: 'Sample', rows: '1000', columns: [{ name: 'city', type: 'VARCHAR' }, { name: 'amount', type: 'DOUBLE' }] };
describe('visual exploration SQL', () => {
  it('quotes names and parameterizes values', () => {
    const q = buildQuery({ table, filters: [{ column: 'city', operator: 'equals', value: "x' OR true --" }], order: 'amount', descending: true, offset: 100 });
    expect(q.sql).toContain('"weird""table"');
    expect(q.sql).toContain('"city" = ?');
    expect(q.sql).toContain('ORDER BY "amount" DESC');
    expect(q.sql).toContain('LIMIT 100 OFFSET 100');
    expect(q.params).toEqual(["x' OR true --"]);
  });
  it('aggregates filtered data before pagination', () => {
    const q = buildQuery({ table, group: 'city', measure: 'amount', aggregate: 'sum' });
    expect(q.sql).toContain('SUM("amount") AS "valor"');
    expect(q.sql).toContain('GROUP BY "city"');
  });
  it('rejects unknown columns and invalid offsets', () => {
    expect(() => buildQuery({ table, columns: ['hidden'] })).toThrow('Coluna');
    expect(() => buildQuery({ table, offset: -1 })).toThrow('Página');
  });
});
