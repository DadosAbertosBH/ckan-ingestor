<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import type { Publication, LakeTable, CatalogResource } from '../shared/types';
const props = defineProps<{ publication: Publication; busy?: boolean }>();
const emit = defineEmits<{ choose: [table: LakeTable]; resource: [resource: CatalogResource] }>();
const search = ref(''), source = ref(''), parent = ref('');
const limit = ref(60);
const normalize = (text: string) => text.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase('pt-BR');
const resources = computed(() => props.publication.resources ?? []);
const key = (sourceId?: string, resourceId?: string) => `${sourceId}\0${resourceId}`;
const resourceIndex = computed(() => new Map(resources.value.map(r => [key(r.sourceId, r.id), r])));
const sourceNames = computed(() => new Map((props.publication.sources ?? []).map(s => [s.id, s.title])));
const availableAncestors = computed(() => {
  const available = new Set<string>();
  for (const resourceKey of props.publication.tables.filter(t => (t.category ?? 'data') === 'data' && t.resourceId && !t.deleted).map(t => key(t.sourceId, t.resourceId))) {
    let resource = resourceIndex.value.get(resourceKey);
    while (resource && !resource.deleted) {
      const resourceKey = key(resource.sourceId, resource.id);
      if (available.has(resourceKey)) break;
      available.add(resourceKey);
      resource = resourceIndex.value.get(key(resource.sourceId, resource.parentId));
    }
  }
  return available;
});
const sourceName = (id: string) => sourceNames.value.get(id) ?? '';
const current = computed(() => resourceIndex.value.get(key(source.value, parent.value)));
const sourceFolders = computed(() => !source.value && !search.value ? (props.publication.sources ?? []) : []);
const matches = (text: string) => normalize(text).includes(normalize(search.value));
const visibleTables = computed(() => props.publication.tables.filter(table => {
  if ((table.category ?? 'data') !== 'data') return false;
  if (source.value && table.sourceId !== source.value) return false;
  if (table.deleted) return false;
  if (!source.value && !search.value && table.sourceId && sourceNames.value.has(table.sourceId)) return false;
  const resource = resourceIndex.value.get(key(table.sourceId, table.resourceId));
  if (parent.value && !search.value && resource?.parentId !== parent.value && resource?.id !== parent.value) return false;
  if (source.value && !parent.value && !search.value && resource?.parentId) return false;
  return matches(`${table.title} ${table.source ?? sourceName(table.sourceId ?? '')} ${table.description ?? ''} ${table.dataset ?? ''} ${(table.path ?? []).join(' ')}`);
}));
const hasAvailableDescendant = (root: CatalogResource) => availableAncestors.value.has(key(root.sourceId, root.id));
const folders = computed(() => !source.value || search.value ? [] : resources.value.filter(r => r.sourceId === source.value && (r.parentId ?? '') === parent.value && r.kind === 'collection' && hasAvailableDescendant(r)));
const state = (status?: string, deleted?: boolean) => deleted ? 'Removido na origem' : ({ failed: 'Atualização com falha', processing: 'Em processamento', discovered: 'Aguardando ingestão', completed: 'Disponível' }[status ?? 'completed'] ?? status);
function open(resource: CatalogResource) {
  if (resource.kind === 'collection') parent.value = resource.id;
  else emit('resource', resource);
}
function back() { if (parent.value) parent.value = current.value?.parentId ?? ''; else source.value = ''; }
watch([search, source, parent], () => { limit.value = 60; });
watch(source, () => { parent.value = ''; });
</script>
<template>
  <label class="search"><span>⌕</span><input v-model="search" aria-label="Buscar conjuntos" placeholder="Buscar por nome, tema ou fonte…"></label>
  <div v-if="source" class="catalog-location"><button class="text-button" aria-label="Voltar" @click="back">← Voltar</button><span>{{ sourceName(source) }}<template v-if="current"> / {{ current.path.join(' / ') }}</template></span></div>
  <p class="list-caption">{{ visibleTables.length }} tabelas disponíveis</p>
  <nav class="table-list" aria-label="Conjuntos de dados">
    <button v-for="item in sourceFolders" :key="item.id" :data-source="item.id" class="table-card" :disabled="busy" @click="source = item.id"><span class="table-icon">▱</span><span><strong>{{ item.title }}</strong><small>Fonte</small></span><span>→</span></button>
    <button v-for="folder in folders" :key="folder.id" :data-resource="folder.id" class="table-card" :disabled="busy" @click="open(folder)"><span class="table-icon">▱</span><span><strong>{{ folder.title }}</strong><small>Coleção{{ folder.deleted ? ' · Removida na origem' : '' }}</small></span><span>→</span></button>
    <button v-for="table in visibleTables.slice(0, limit)" :key="table.schema + '.' + table.name" :data-table="table.schema + '.' + table.name" class="table-card" :disabled="busy" @click="emit('choose', table)"><span class="table-icon">▦</span><span><strong>{{ table.title }}</strong><small>{{ table.source || 'Dados preservados' }} · {{ Number(table.rows).toLocaleString('pt-BR') }} linhas</small><small v-if="table.path?.length">{{ table.path.join(' / ') }}</small><small v-if="table.status === 'failed' || table.deleted">{{ state(table.status, table.deleted) }} · dados disponíveis</small></span><span class="table-arrow">↗</span></button>
    <button v-if="visibleTables.length > limit" class="secondary" @click="limit += 60">Ver mais conjuntos</button>
    <p v-if="!visibleTables.length && !sourceFolders.length && !folders.length" class="muted">Tente buscar outro nome ou tema.</p>
  </nav>
</template>
