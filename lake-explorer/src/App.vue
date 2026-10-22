<script setup lang="ts">
import { computed, defineAsyncComponent, onBeforeUnmount, ref } from 'vue';
import CatalogBrowser from './CatalogBrowser.vue';
import { LakeEngine } from './engine';
import { buildQuery, quoteIdentifier, type Filter } from './queries';
import type { CatalogResource, LakeTable, Publication, QueryResult } from '../shared/types';
import './style.css';
import './progress.css';
import './catalog.css';

const Chart = defineAsyncComponent(() => import('./Chart.vue'));
const SqlEditor = defineAsyncComponent(() => import('./SqlEditor.vue'));
const engine = new LakeEngine();
const publication = ref<Publication>();
const selected = ref<LakeTable>();
const selectedResource = ref<CatalogResource>();
const tableResource = computed(() => selected.value?.resourceId ? publication.value?.resources?.find(r => r.sourceId === selected.value?.sourceId && r.id === selected.value?.resourceId) : undefined);
const resourceTables = computed(() => publication.value?.tables.filter(t => t.sourceId === selectedResource.value?.sourceId && t.resourceId === selectedResource.value?.id) ?? []);
const statusLabel = (status: string) => ({ failed: 'Atualização com falha', completed: 'Disponível', processing: 'Em processamento', discovered: 'Aguardando ingestão', deleted: 'Removido na origem' }[status] ?? status);
function sourceLink(url?: string): string | undefined { return url && /^https?:\/\//i.test(url) ? url : undefined; }
function showResource(resource: CatalogResource) { selected.value = undefined; result.value = undefined; selectedResource.value = resource; }
const result = ref<QueryResult>();
const busy = ref(false), connecting = ref(false), error = ref(''), progress = ref(''), progressFraction = ref<number>(), updated = ref(false);
const tab = ref('Tabela');
const sql = ref('');
const offset = ref(0), order = ref(''), descending = ref(false), visibleColumns = ref<string[]>([]);
const filters = ref<Filter[]>([]), filterColumn = ref(''), filterOperator = ref('contains'), filterValue = ref('');
const group = ref(''), measure = ref(''), aggregate = ref('count');
const chartX = ref(''), chartY = ref(''), chartKind = ref('bar');
const showColumns = ref(false);
const favorites = ref<string[]>(readFavorites());
function readFavorites(): string[] { try { return JSON.parse(localStorage.getItem('lake-queries') ?? '[]'); } catch { return []; } }
const numeric = computed(() => selected.value?.columns.filter(c => /INT|DOUBLE|FLOAT|DECIMAL|NUMERIC/i.test(c.type)) ?? []);
const count = (n: number | string) => Number(n).toLocaleString('pt-BR');
const date = (d: string) => new Date(d).toLocaleString('pt-BR', { dateStyle: 'medium', timeStyle: 'short' });
function cell(value: unknown): string {
  if (value == null) return '—';
  if (typeof value === 'bigint' || typeof value === 'number') return value.toLocaleString('pt-BR');
  if (value instanceof Date) return value.toLocaleString('pt-BR');
  return typeof value === 'object' ? JSON.stringify(value, (_, v) => typeof v === 'bigint' ? String(v) : v) : String(value);
}
engine.onProgress = (message, fraction) => { progress.value = message; progressFraction.value = fraction; };
async function connect() {
  connecting.value = true; error.value = ''; progressFraction.value = undefined;
  try { publication.value = await engine.connect(); } catch (e) { error.value = String(e instanceof Error ? e.message : e); await engine.close(); }
  finally { connecting.value = false; }
}
async function choose(table: LakeTable) {
  if (busy.value) return;
  selectedResource.value = undefined;
  selected.value = table; tab.value = 'Tabela'; offset.value = 0; order.value = ''; filters.value = [];
  visibleColumns.value = table.columns.map(c => c.name); filterColumn.value = table.columns[0]?.name ?? '';
  group.value = table.columns.find(c => !/INT|DOUBLE|FLOAT|DECIMAL/.test(c.type))?.name ?? table.columns[0]?.name ?? '';
  measure.value = numeric.value[0]?.name ?? ''; aggregate.value = 'count';
  sql.value = `SELECT *\nFROM lake.${quoteIdentifier(table.schema)}.${quoteIdentifier(table.name)}\nLIMIT 100`;
  await run();
}
async function run() {
  if (!selected.value || busy.value) return;
  busy.value = true; error.value = ''; progress.value = 'Consultando os arquivos do lake…';
  try {
    if (tab.value === 'SQL') result.value = await engine.query(sql.value);
    else {
      const q = buildQuery({ table: selected.value, columns: visibleColumns.value, filters: filters.value, offset: offset.value, order: order.value || undefined, descending: descending.value,
        ...(tab.value === 'Resumir' || tab.value === 'Gráfico' ? { group: group.value, aggregate: aggregate.value, measure: measure.value } : {}) });
      result.value = await engine.query(q.sql, q.params, [`${selected.value.schema}.${selected.value.name}`]);
      chartX.value = result.value.columns[0]?.name ?? ''; chartY.value = result.value.columns[1]?.name ?? '';
    }
  } catch (e) { error.value = e instanceof Error ? e.message : String(e); result.value = undefined; }
  finally { busy.value = false; }
}
async function changeTab(value: string) { if (busy.value) return; tab.value = value; offset.value = 0; order.value = ''; if (value !== 'SQL') await run(); }
async function filter() { if (!filterColumn.value) return; filters.value.push({ column: filterColumn.value, operator: filterOperator.value, value: filterValue.value }); offset.value = 0; await run(); }
async function removeFilter(index: number) { filters.value.splice(index, 1); offset.value = 0; await run(); }
async function sort(name: string) { if (busy.value) return; descending.value = order.value === name ? !descending.value : false; order.value = name; await run(); }
async function page(delta: number) { offset.value += delta; await run(); }
async function download(format: 'csv' | 'parquet') {
  busy.value = true; error.value = '';
  try {
    const bytes = await engine.export(format);
    const url = URL.createObjectURL(new Blob([new Uint8Array(bytes)], { type: format === 'csv' ? 'text/csv;charset=utf-8' : 'application/octet-stream' }));
    const a = document.createElement('a'); a.href = url; a.download = `${selected.value?.title ?? 'resultado'}.${format}`; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  } catch (e) { error.value = String(e); } finally { busy.value = false; }
}
function save() { if (!favorites.value.includes(sql.value)) favorites.value.unshift(sql.value); favorites.value = favorites.value.slice(0, 20); localStorage.setItem('lake-queries', JSON.stringify(favorites.value)); }
async function refresh() { await engine.close(); publication.value = undefined; selected.value = undefined; selectedResource.value = undefined; result.value = undefined; updated.value = false; await connect(); }
const timer = setInterval(async () => { if (!publication.value) return; try { const response = await fetch('/api/latest'); if (response.ok) updated.value = (await response.json()).id !== publication.value.id; } catch { /* next poll retries */ } }, 60_000);
onBeforeUnmount(() => { clearInterval(timer); void engine.close(); });
</script>

<template>
  <div class="portal">
    <header class="topbar">
      <a class="brand" href="/"><span class="brand-mark">◈</span><span>observatório<span class="brand-sub">DADOS PÚBLICOS</span></span></a>
      <div class="top-right"><span class="status-dot"></span><span>Conhecimento ao alcance de todos</span><a href="https://github.com/noctcloud" target="_blank" rel="noreferrer">Noctcloud ↗</a></div>
    </header>

    <main v-if="!publication" class="welcome">
      <div class="eyebrow">EXPLORE. COMPARE. DESCUBRA.</div>
      <h1>Dados públicos,<br><em>novas perspectivas.</em></h1>
      <p class="intro">Encontre respostas nos dados. Explore tabelas, faça comparações e transforme números em histórias — direto do seu navegador.</p>
      <button class="primary start" :disabled="connecting" @click="connect">{{ connecting ? 'Abrindo o observatório…' : 'Explorar dados' }} <span>↗</span></button>
      <div v-if="connecting" class="progress" role="status">
        <span>{{ progress }}</span>
        <div v-if="progressFraction != null" class="progress-track" role="progressbar" aria-label="Download do catálogo" :aria-valuenow="Math.round(progressFraction * 100)" aria-valuemin="0" aria-valuemax="100"><span :style="{ width: `${Math.max(1, progressFraction * 100)}%` }"></span></div>
      </div>
      <p v-if="error" class="error" role="alert">{{ error }}</p>
      <div class="welcome-grid"><article><span>01 / ENCONTRE</span><h2>Comece por uma pergunta</h2><p>Procure um conjunto por tema, nome ou fonte.</p></article><article><span>02 / EXPLORE</span><h2>Deixe os dados falarem</h2><p>Filtre, organize e resuma com poucos cliques.</p></article><article><span>03 / COMPARTILHE</span><h2>Leve sua descoberta</h2><p>Baixe os resultados e os gráficos da sua análise.</p></article></div>
      <div class="welcome-foot"><span>CONSULTAS NO SEU NAVEGADOR</span><span>DuckLake + DuckDB</span><span>Atualização diária</span></div>
    </main>

    <div v-else class="workspace">
      <aside class="sidebar">
        <div class="sidebar-title"><span class="eyebrow">SEU PONTO DE PARTIDA</span><h2>Catálogo <small>{{ count(publication.tables.length) }}</small></h2></div>
        <CatalogBrowser :publication="publication" :busy="busy" @choose="choose" @resource="showResource" />
        <div class="snapshot"><span class="status-dot"></span><div>Atualização dos dados<strong>{{ date(publication.createdAt) }}</strong><small>Snapshot {{ publication.snapshotId }}</small></div></div>
      </aside>

      <main class="content">
        <div v-if="updated" class="update-banner">Há uma nova atualização dos dados. <button :disabled="busy" @click="refresh">Abrir catálogo atualizado ↗</button></div>
        <section v-if="selectedResource" class="resource-detail">
          <div class="breadcrumb">{{ publication.sources?.find(s => s.id === selectedResource?.sourceId)?.title }} / {{ selectedResource.path.join(' / ') }}</div>
          <h1>{{ selectedResource.title }}</h1><p>{{ selectedResource.description }}</p>
          <p role="status">{{ statusLabel(selectedResource.deleted ? 'deleted' : selectedResource.status) }}</p>
          <a v-if="sourceLink(selectedResource.pageUrl)" :href="sourceLink(selectedResource.pageUrl)" target="_blank" rel="noreferrer">Abrir na fonte ↗</a>
          <h2 v-if="resourceTables.length">Tabelas disponíveis</h2>
          <button v-for="table in resourceTables" :key="table.schema + '.' + table.name" class="table-card" @click="choose(table)">{{ table.title }} · {{ count(table.rows) }} linhas ↗</button>
          <details><summary>Detalhes técnicos</summary><p>Fonte: {{ selectedResource.sourceId }} · Recurso: {{ selectedResource.id }}</p></details>
        </section>
        <div v-else-if="!selected" class="empty-state"><span class="empty-symbol">▦</span><div class="eyebrow">UM MUNDO DE POSSIBILIDADES</div><h1>Qual será sua<br>próxima descoberta?</h1><p>Escolha um conjunto no catálogo para começar.<br>A tabela, os filtros e os gráficos esperam por você.</p><div class="hint">← Explore o catálogo ao lado</div></div>
        <template v-else>
          <div class="breadcrumb">Catálogo <span>/</span> {{ selected.source || 'Dados' }} <span v-if="selected.path?.length"> / {{ selected.path.join(' / ') }}</span></div>
          <p v-if="selected.deleted || selected.status === 'failed'" class="catalog-notice">{{ selected.deleted ? 'Removido na origem' : 'Atualização com falha' }} · exibindo os últimos dados disponíveis.</p>
          <div class="dataset-heading"><div><div class="eyebrow">CONJUNTO DE DADOS</div><h1>{{ selected.title }}</h1><p v-if="selected.description" class="description">{{ selected.description }}</p><div class="dataset-meta"><span><i class="status-dot"></i> {{ count(selected.rows) }} linhas</span><span>{{ selected.columns.length }} colunas</span><span>Atualizado em {{ date(publication.createdAt) }}</span></div></div><span class="dataset-stamp">DADOS<br>ABERTOS ↗</span></div>
          <a v-if="sourceLink(selected.pageUrl)" :href="sourceLink(selected.pageUrl)" target="_blank" rel="noreferrer">Abrir na fonte ↗</a>
          <details class="catalog-technical"><summary>Detalhes técnicos</summary><p>Tabela SQL: lake.{{ selected.schema }}.{{ selected.name }}</p><p v-if="selected.resourceId">Fonte: {{ selected.sourceId }} · Recurso: {{ selected.resourceId }}</p><button v-if="tableResource" class="text-button" @click="showResource(tableResource)">Ver recurso e tabelas</button></details>
          <div class="tabs" role="tablist"><button v-for="name in ['Tabela', 'Resumir', 'Gráfico', 'SQL']" :key="name" role="tab" :aria-selected="tab === name" :class="{ selected: tab === name }" :disabled="busy" @click="changeTab(name)">{{ name }}</button><div class="export-actions"><button class="text-button" :disabled="busy || !result" @click="download('csv')">↓ CSV</button><button class="text-button" :disabled="busy || !result" @click="download('parquet')">↓ Parquet</button></div></div>

          <section v-if="tab === 'SQL'" class="sql-panel"><div class="panel-label">SUA CONSULTA <button class="text-button" @click="save">☆ Salvar consulta</button></div><SqlEditor v-model="sql" :tables="publication.tables" :selected="selected" @run="run" /><div class="sql-completions"><span>Inserir coluna:</span><button v-for="c in selected.columns.slice(0, 12)" :key="c.name" @click="sql += quoteIdentifier(c.name)">{{ c.name }}</button></div><div class="sql-actions"><button class="primary" :disabled="busy" @click="run">Executar consulta <span>↗</span></button><select v-if="favorites.length" aria-label="Consultas favoritas" @change="sql = ($event.target as HTMLSelectElement).value"><option value="">Consultas salvas neste navegador</option><option v-for="favorite in favorites" :key="favorite" :value="favorite">{{ favorite.slice(0, 80) }}</option></select></div></section>
          <section v-else class="controls"><div class="filter-row"><span class="control-caption">Filtrar por</span><select v-model="filterColumn" aria-label="Coluna do filtro"><option v-for="c in selected.columns" :key="c.name">{{ c.name }}</option></select><select v-model="filterOperator" aria-label="Operador do filtro"><option value="contains">contém</option><option value="equals">é igual a</option><option value="notEquals">é diferente de</option><option value="greater">é maior que</option><option value="less">é menor que</option><option value="null">está vazio</option><option value="notNull">está preenchido</option></select><input v-if="!['null', 'notNull'].includes(filterOperator)" v-model="filterValue" aria-label="Valor do filtro" placeholder="Digite um valor" @keyup.enter="filter"><button class="secondary" :disabled="busy" @click="filter">Aplicar filtro</button><button v-if="tab === 'Tabela'" class="text-button" @click="showColumns = !showColumns">▥ Colunas</button></div><div v-if="filters.length" class="filter-tags"><button v-for="(f, i) in filters" :key="i" :disabled="busy" @click="removeFilter(i)">{{ f.column }} · {{ f.value || f.operator }} <span>×</span></button></div><div v-if="showColumns && tab === 'Tabela'" class="column-chooser"><label v-for="c in selected.columns" :key="c.name"><input v-model="visibleColumns" type="checkbox" :value="c.name">{{ c.name }}</label><button class="secondary" :disabled="busy || !visibleColumns.length" @click="run">Atualizar colunas</button></div>
            <div v-if="tab === 'Resumir' || tab === 'Gráfico'" class="summary-row"><label>Agrupar por<select v-model="group"><option v-for="c in selected.columns" :key="c.name">{{ c.name }}</option></select></label><label>Calcular<select v-model="aggregate"><option value="count">Contagem de linhas</option><option value="sum">Soma</option><option value="avg">Média</option><option value="min">Menor valor</option><option value="max">Maior valor</option></select></label><label v-if="aggregate !== 'count'">Coluna numérica<select v-model="measure"><option v-for="c in numeric" :key="c.name">{{ c.name }}</option></select></label><button class="secondary" :disabled="busy" @click="run">Atualizar resumo</button><label v-if="tab === 'Gráfico'">Visualização<select v-model="chartKind"><option value="bar">Barras</option><option value="line">Linhas</option><option value="scatter">Dispersão</option></select></label></div>
          </section>

          <div v-if="busy" class="query-progress" role="status"><span class="spinner"></span>{{ progress || 'Preparando resultado…' }}<button class="text-button" @click="engine.cancel()">Cancelar consulta</button></div>
          <p v-if="error" class="error" role="alert">{{ error }}</p>
          <template v-if="result">
            <Chart v-if="tab === 'Gráfico'" :result="result" :x="chartX" :y="chartY" :kind="chartKind" />
            <div v-else class="data-grid"><table><thead><tr><th class="row-number">#</th><th v-for="column in result.columns" :key="column.name"><button :disabled="busy || tab === 'SQL'" @click="sort(column.name)"><span>{{ column.name }} <span class="sort-icon">{{ order === column.name ? (descending ? '↓' : '↑') : '↕' }}</span></span><small>{{ column.type }}</small></button></th></tr></thead><tbody><tr v-for="(row, index) in result.rows" :key="index"><td class="row-number">{{ offset + index + 1 }}</td><td v-for="column in result.columns" :key="column.name" :class="{ null: row[column.name] == null }" :title="cell(row[column.name])">{{ cell(row[column.name]) }}</td></tr></tbody></table><div v-if="!result.rows.length" class="no-rows">Nenhuma linha encontrada. Ajuste os filtros para explorar outros resultados.</div></div>
            <footer class="results-footer"><span>{{ count(result.rows.length) }} linhas no resultado <span class="dot-separator">·</span> {{ (result.elapsedMs / 1000).toFixed(2) }} s <span v-if="result.truncated">· Exibindo as primeiras 10.000 linhas</span></span><div v-if="tab !== 'SQL'"><button class="text-button" :disabled="busy || offset === 0" @click="page(-100)">← Anterior</button><span>Página {{ offset / 100 + 1 }}</span><button class="text-button" :disabled="busy || result.rows.length < 100" @click="page(100)">Próxima →</button></div></footer>
            <p class="export-note">A exportação inclui as linhas do resultado exibido, até 10.000 linhas.</p>
          </template>
        </template>
      </main>
    </div>
  </div>
</template>
