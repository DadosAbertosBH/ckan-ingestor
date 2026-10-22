// @vitest-environment happy-dom
import { flushPromises, mount } from '@vue/test-utils';
import { expect, it, vi } from 'vitest';
import App from '../src/App.vue';

vi.mock('../src/engine', () => ({ LakeEngine: class {
  async connect() { return { createdAt: '2026-10-22T10:00:00Z', snapshotId: '1', tables: [], sources: [{ id: 's', title: 'Prefeitura', type: 'ckan', url: 'https://example.test' }], resources: [{ id: 'r', sourceId: 's', kind: 'file', title: 'Empenhos', description: 'Empenhos de 2026', status: 'processing', deleted: false, path: ['Empenhos'], pageUrl: 'https://example.test/empenhos' }] }; }
  async close() {}
} }));

it('opens the catalog detail of a resource awaiting data', async () => {
  const wrapper = mount(App);
  await wrapper.get('button.start').trigger('click');
  await flushPromises();
  await wrapper.get('input[aria-label="Mostrar todos os recursos"]').setValue(true);
  await wrapper.get('button[data-resource="r"]').trigger('click');
  expect(wrapper.get('main.content').text()).toContain('Empenhos de 2026');
  expect(wrapper.get('main.content').text()).toContain('Em processamento');
  expect(wrapper.get('main.content a').attributes('href')).toBe('https://example.test/empenhos');
  wrapper.unmount();
});
