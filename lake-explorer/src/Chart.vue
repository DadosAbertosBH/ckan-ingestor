<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, watch } from 'vue';
import { assembleECharts } from 'flint-chart/echarts';
import { init, type ECharts } from 'echarts';
import type { QueryResult } from '../shared/types';
const props = defineProps<{ result: QueryResult; x: string; y: string; kind: string }>();
const host = ref<HTMLElement>();
const error = ref('');
let chart: ECharts | undefined;
let observer: ResizeObserver;
function draw() {
  if (!chart || !props.x || !props.y) return;
  try {
    const values = props.result.rows.map(row => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, typeof v === 'bigint' ? Number(v) : v])));
    const spec = assembleECharts({
      data: { values },
      semantic_types: { [props.x]: props.kind === 'scatter' ? 'Quantity' : 'Category', [props.y]: 'Quantity' },
      chart_spec: { chartType: props.kind === 'line' ? 'Line Chart' : props.kind === 'scatter' ? 'Scatter Plot' : 'Bar Chart', encodings: { x: props.x, y: props.y } },
      theme_spec: 'economist',
    });
    chart.setOption(spec, true);
    error.value = '';
  } catch { error.value = 'Escolha colunas compatíveis para visualizar o gráfico.'; }
}
watch(() => [props.result, props.x, props.y, props.kind], draw);
onMounted(() => { chart = init(host.value!); observer = new ResizeObserver(() => chart?.resize()); observer.observe(host.value!); draw(); });
onBeforeUnmount(() => { observer?.disconnect(); chart?.dispose(); });
function download() {
  if (!chart) return;
  const a = document.createElement('a'); a.href = chart.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#fff' }); a.download = 'grafico.png'; a.click();
}
</script>
<template><div class="chart-shell"><p v-if="error" role="alert">{{ error }}</p><div ref="host" style="height:420px;width:100%" /><button class="secondary" @click="download">Baixar gráfico PNG</button></div></template>
