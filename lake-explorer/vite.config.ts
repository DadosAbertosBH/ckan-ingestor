import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import { readFileSync } from 'node:fs';
import { gzipSync } from 'node:zlib';

export default defineConfig({
  plugins: [vue(), {
    name: 'compressed-duckdb-engine',
    apply: 'build',
    buildStart() {
      this.emitFile({ type: 'asset', fileName: 'engine/duckdb-eh-1.33.1-dev57.0.wasm.gz',
        source: gzipSync(readFileSync(new URL('./node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url)), { level: 9 }) });
    },
  }],
  server: { proxy: { '/api': {
    target: 'http://127.0.0.1:8787',
    configure: proxy => proxy.on('proxyReq', proxyReq => {
      proxyReq.removeHeader('origin');
    }),
  } } },
  optimizeDeps: { include: ['echarts', 'flint-chart/echarts', 'codemirror', '@codemirror/lang-sql', '@codemirror/view', '@codemirror/autocomplete'] },
  worker: { format: 'es' },
});
