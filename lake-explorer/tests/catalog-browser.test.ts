// @vitest-environment happy-dom
import { mount } from '@vue/test-utils';
import { expect, it } from 'vitest';
import CatalogBrowser from '../src/CatalogBrowser.vue';
import type { Publication } from '../shared/types';

const publication = {
  sources: [{ id: 'a', title: 'Prefeitura', type: 'ckan', url: 'https://example.test' }, { id: 'b', title: 'Minas Gerais', type: 'ckan', url: 'https://mg.example.test' }],
  resources: [
    { id: 'p', sourceId: 'a', kind: 'collection', title: 'Despesas', path: ['Despesas'], status: 'discovered', deleted: false },
    { id: 'r', sourceId: 'a', parentId: 'p', kind: 'file', title: 'Pagamentos', description: 'Despesas de 2026', path: ['Despesas', 'Pagamentos'], status: 'failed', deleted: false },
    { id: 'waiting', sourceId: 'a', parentId: 'p', kind: 'file', title: 'Empenhos', path: ['Despesas', 'Empenhos'], status: 'processing', deleted: false },
    { id: 'p', sourceId: 'b', kind: 'collection', title: 'Saúde', path: ['Saúde'], status: 'discovered', deleted: false },
    { id: 'r', sourceId: 'b', parentId: 'p', kind: 'file', title: 'Hospitais', path: ['Saúde', 'Hospitais'], status: 'completed', deleted: false },
  ],
  tables: [
    { schema: 'main', name: 'uuid', title: 'Pagamentos', rows: '2', columns: [], source: 'Prefeitura', sourceId: 'a', resourceId: 'r', path: ['Despesas', 'Pagamentos'], category: 'data', status: 'failed' },
    { schema: 'main', name: 'old-uuid', title: 'old-uuid', rows: '2', columns: [], category: 'legacy' },
    { schema: 'main', name: 'mg', title: 'Hospitais', rows: '1', columns: [], sourceId: 'b', resourceId: 'r', category: 'data' },
    { schema: 'main', name: 'metadata', title: 'Metadados secretos', rows: '1', columns: [], category: 'metadata' },
  ],
} as unknown as Publication;

it('searches data across sources without showing metadata or legacy', async () => {
  const wrapper = mount(CatalogBrowser, { props: { publication } });
  expect(wrapper.text()).toContain('Prefeitura');
  expect(wrapper.text()).toContain('Minas Gerais');
  expect(wrapper.get('.list-caption').text()).toBe('2 fontes');
  expect(wrapper.text()).not.toContain('Pagamentos');
  expect(wrapper.text()).not.toContain('old-uuid');
  expect(wrapper.text()).not.toContain('Metadados secretos');
  expect(wrapper.text()).not.toContain('Empenhos');
  expect(wrapper.find('select[aria-label="Fonte"]').exists()).toBe(false);
  expect(wrapper.find('select[aria-label="Área do catálogo"]').exists()).toBe(false);
  expect(wrapper.find('input[aria-label="Mostrar todos os recursos"]').exists()).toBe(false);
  await wrapper.get('input[aria-label="Buscar conjuntos"]').setValue('despesas');
  await wrapper.get('button[data-table="main.uuid"]').trigger('click');
  expect(wrapper.emitted('choose')?.[0]).toEqual([publication.tables[0]]);
});

it('browses source and collection and shows available data', async () => {
  const wrapper = mount(CatalogBrowser, { props: { publication } });
  await wrapper.get('button[data-source="a"]').trigger('click');
  await wrapper.get('button[data-resource="p"]').trigger('click');
  expect(wrapper.get('.list-caption').text()).toBe('1 tabela disponível');
  expect(wrapper.text()).toContain('Despesas');
  expect(wrapper.text()).toContain('Pagamentos');
  expect(wrapper.text()).not.toContain('Empenhos');
  await wrapper.get('button[aria-label="Voltar"]').trigger('click');
  await wrapper.get('button[aria-label="Voltar"]').trigger('click');
  await wrapper.get('button[data-source="b"]').trigger('click');
  await wrapper.get('button[data-resource="p"]').trigger('click');
  expect(wrapper.text()).toContain('Hospitais');
  expect(wrapper.text()).not.toContain('Pagamentos');
});

it('preserves table navigation for legacy publications', () => {
  const wrapper = mount(CatalogBrowser, { props: { publication: { tables: [{ schema: 'main', name: 'uuid', title: 'Pessoas', rows: '2', columns: [] }] } as unknown as Publication } });
  expect(wrapper.text()).toContain('Pessoas');
});
