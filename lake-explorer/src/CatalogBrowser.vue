<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import type { Publication, LakeTable, CatalogResource } from '../shared/types';
const props = defineProps<{ publication: Publication; busy?: boolean }>();
const emit = defineEmits<{ choose: [table: LakeTable]; resource: [resource: CatalogResource] }>();
const search = ref(''), source = ref(''), parent = ref(''), area = ref('data');
const all = ref(false), limit = ref(60);
const normalize = (text: string) => text.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase('pt-BR');
const resources = computed(() => props.publication.resources ?? []);
const key = (sourceId?: string, resourceId?: string) => `${sourceId}\0${resourceId}`;
const resourceIndex = computed(() => new Map(resources.value.map(r => [key(r.sourceId, r.id), r])));
const sourceNames = computed(() => new Map((props.publication.sources ?? []).map(s => [s.id, s.title])));
const tableResources = computed(() => new Set(props.publication.tables.filter(t => t.resourceId).map(t => key(t.sourceId, t.resourceId))));
const availableAncestors = computed(() => {
  const available = new Set<string>();
  for (const resourceKey of tableResources.value) {
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
const matches = (text: string) => normalize(text).includes(normalize(search.value));
const visibleTables = computed(() => props.publication.tables.filter(table => {
  if ((table.category ?? 'data') !== area.value) return false;
  if (source.value && table.sourceId !== source.value) return false;
  if (area.value === 'data' && !all.value && table.deleted) return false;
  const resource = resourceIndex.value.get(key(table.sourceId, table.resourceId));
  if (parent.value && !search.value && resource?.parentId !== parent.value && resource?.id !== parent.value) return false;
  if (source.value && !parent.value && !search.value && resource?.parentId) return false;
  return matches(`${table.title} ${table.source ?? ''} ${table.description ?? ''} ${table.dataset ?? ''} ${(table.path ?? []).join(' ')}`);
}));
const hasTable = (resource: CatalogResource) => tableResources.value.has(key(resource.sourceId, resource.id));
const hasAvailableDescendant = (root: CatalogResource) => availableAncestors.value.has(key(root.sourceId, root.id));
const folders = computed(() => area.value !== 'data' || !source.value || search.value ? [] : resources.value.filter(r => r.sourceId === source.value && (r.parentId ?? '') === parent.value && r.kind === 'collection' && (all.value || hasAvailableDescendant(r))));
const unavailable = computed(() => !all.value || area.value !== 'data' ? [] : resources.value.filter(r => r.kind !== 'collection' && !hasTable(r) && (!source.value || r.sourceId === source.value) && (!source.value || search.value || (r.parentId ?? '') === parent.value) && matches(`${r.title} ${r.description ?? ''} ${sourceName(r.sourceId)} ${r.path.join(' ')}`)));
const state = (status?: string, deleted?: boolean) => deleted ? 'Removido na origem' : ({ failed: 'Atualização com falha', processing: 'Em processamento', discovered: 'Aguardando ingestão', completed: 'Disponível' }[status ?? 'completed'] ?? status);
function open(resource: CatalogResource) {
  if (resource.kind === 'collection') parent.value = resource.id;
  else emit('resource', resource);
}
watch([search, source, parent, area, all], () => { limit.value = 60; });
watch(source, () => { parent.value = ''; });
watch(area, () => { search.value = ''; source.value = ''; parent.value = ''; });
</script>
<template>
  <label class="search"><span>⌕</span><input v-model="search" aria-label="Buscar conjuntos" placeholder="Buscar por nome, tema ou fonte…"></label>
  <div class="catalog-filters">
    <label>Explorar<select v-model="area" aria-label="Área do catálogo"><option value="data">Dados</option><option value="metadata">Metadados</option><option value="legacy">Legado</option></select></label>
    <label v-if="area === 'data' && publication.sources?.length">Fonte<select v-model="source" aria-label="Fonte"><option value="">Todas as fontes</option><option v-for="item in publication.sources" :key="item.id" :value="item.id">{{ item.title }}</option></select></label>
    <label v-if="area === 'data'" class="catalog-all"><input v-model="all" type="checkbox" aria-label="Mostrar todos os recursos"> Mostrar todos os recursos</label>
  </div>
  <div v-if="current" class="catalog-location"><button class="text-button" @click="parent = current?.parentId ?? ''">← Voltar</button><span>{{ current.path.join(' / ') }}</span></div>
  <p class="list-caption">{{ visibleTables.length }} tabelas disponíveis</p>
  <nav class="table-list" aria-label="Conjuntos de dados">
    <button v-for="folder in folders" :key="folder.id" :data-resource="folder.id" class="table-card" :disabled="busy" @click="open(folder)"><span class="table-icon">▱</span><span><strong>{{ folder.title }}</strong><small>Coleção{{ folder.deleted ? ' · Removida na origem' : '' }}</small></span><span>→</span></button>
    <button v-for="table in visibleTables.slice(0, limit)" :key="table.schema + '.' + table.name" :data-table="table.schema + '.' + table.name" class="table-card" :disabled="busy" @click="emit('choose', table)"><span class="table-icon">▦</span><span><strong>{{ table.title }}</strong><small>{{ table.source || (area === 'metadata' ? 'Metadados do catálogo' : 'Dados preservados') }} · {{ Number(table.rows).toLocaleString('pt-BR') }} linhas</small><small v-if="table.path?.length">{{ table.path.join(' / ') }}</small><small v-if="table.status === 'failed' || table.deleted">{{ state(table.status, table.deleted) }} · dados disponíveis</small></span><span class="table-arrow">↗</span></button>
    <button v-for="resource in unavailable.slice(0, limit)" :key="resource.sourceId + ':' + resource.id" :data-resource="resource.id" class="table-card" :disabled="busy" @click="open(resource)"><span class="table-icon">◷</span><span><strong>{{ resource.title }}</strong><small>{{ sourceName(resource.sourceId) }} · {{ state(resource.status, resource.deleted) }}</small><small>{{ resource.path.join(' / ') }}</small></span></button>
    <button v-if="visibleTables.length > limit || unavailable.length > limit" class="secondary" @click="limit += 60">Ver mais conjuntos</button>
    <p v-if="!visibleTables.length && !folders.length && !unavailable.length" class="muted">Tente buscar outro nome ou tema.</p>
  </nav>
</template>
