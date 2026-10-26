// @vitest-environment happy-dom
import { mount } from '@vue/test-utils';
import { expect, it } from 'vitest';
import CatalogBrowser from '../src/CatalogBrowser.vue';
import type { Publication } from '../shared/types';

const publication = {
  sources: [{ id: 'a', title: 'Prefeitura', type: 'ckan', url: 'https://example.test' }],
  resources: [
    { id: 'p', sourceId: 'a', kind: 'collection', title: 'Despesas', path: ['Despesas'], status: 'discovered', deleted: false },
    { id: 'r', sourceId: 'a', parentId: 'p', kind: 'file', title: 'Pagamentos', description: 'Despesas de 2026', path: ['Despesas', 'Pagamentos'], status: 'failed', deleted: false },
    { id: 'waiting', sourceId: 'a', parentId: 'p', kind: 'file', title: 'Empenhos', path: ['Despesas', 'Empenhos'], status: 'processing', deleted: false },
  ],
  tables: [
    { schema: 'main', name: 'uuid', title: 'Pagamentos', rows: '2', columns: [], source: 'Prefeitura', sourceId: 'a', resourceId: 'r', path: ['Despesas', 'Pagamentos'], category: 'data', status: 'failed' },
    { schema: 'main', name: 'old-uuid', title: 'old-uuid', rows: '2', columns: [], category: 'legacy' },
  ],
} as unknown as Publication;

it('searches human names and context while keeping metadata and legacy in their areas', async () => {
  const wrapper = mount(CatalogBrowser, { props: { publication } });
  expect(wrapper.text()).toContain('Pagamentos');
  expect(wrapper.text()).not.toContain('old-uuid');
  expect(wrapper.text()).not.toContain('Empenhos');
  await wrapper.get('input[aria-label="Buscar conjuntos"]').setValue('despesas');
  await wrapper.get('button[data-table="main.uuid"]').trigger('click');
  expect(wrapper.emitted('choose')?.[0]).toEqual([publication.tables[0]]);
});

it('browses source and collection and includes unavailable resources through the filter', async () => {
  const wrapper = mount(CatalogBrowser, { props: { publication } });
  await wrapper.get('select[aria-label="Fonte"]').setValue('a');
  await wrapper.get('button[data-resource="p"]').trigger('click');
  expect(wrapper.text()).toContain('Despesas');
  await wrapper.get('input[aria-label="Mostrar todos os recursos"]').setValue(true);
  expect(wrapper.text()).toContain('Empenhos');
  await wrapper.get('button[data-resource="waiting"]').trigger('click');
  expect(wrapper.emitted('resource')?.[0][0]).toMatchObject({ id: 'waiting' });
});

it('preserves table navigation for legacy publications', () => {
  const wrapper = mount(CatalogBrowser, { props: { publication: { tables: [{ schema: 'main', name: 'uuid', title: 'Pessoas', rows: '2', columns: [] }] } as unknown as Publication } });
  expect(wrapper.text()).toContain('Pessoas');
});
