<template>
    <div class="datasets-view">
        <div class="header-row">
            <h1 class="page-title">Datasets</h1>
        </div>

        <div class="filters">
            <select
                v-model="filterInstanceId"
                class="filter-select"
                @change="resetAndLoad"
            >
                <option value="">All instances</option>
                <option
                    v-for="inst in instances"
                    :key="inst.id"
                    :value="inst.id"
                >
                    {{ inst.name }}
                </option>
            </select>
            <input
                v-model="filterSearch"
                type="text"
                placeholder="Search by name..."
                class="filter-input"
                @input="debouncedLoad"
            />
        </div>

        <div v-if="error" class="error-banner">
            <span>Failed to load datasets: {{ error }}</span>
            <button class="btn-retry" @click="loadDatasets">Retry</button>
        </div>
        <div v-if="loading" class="loading">Loading...</div>
        <table v-else-if="!error" class="data-table">
            <thead>
                <tr>
                    <th>Instance</th>
                    <th>Dataset</th>
                    <th>Total</th>
                    <th>Pending</th>
                    <th>Running</th>
                    <th>Completed</th>
                    <th>Failed</th>
                    <th>Outdated</th>
                    <th>Success %</th>
                    <th>Sync</th>
                    <th>Updated</th>
                </tr>
            </thead>
            <tbody>
                <tr
                    v-for="dataset in datasets"
                    :key="dataset.instance_id + ':' + dataset.dataset_name"
                    class="data-row"
                >
                    <td>{{ dataset.instance_name || "—" }}</td>
                    <td>
                        <a
                            v-if="dataset.ckan_dataset_url"
                            :href="dataset.ckan_dataset_url"
                            target="_blank"
                            rel="noopener"
                            class="dataset-link"
                        >
                            {{ dataset.dataset_name }}
                            <span class="external-icon">↗</span>
                        </a>
                        <span v-else class="dataset-name">{{ dataset.dataset_name }}</span>
                    </td>
                    <td class="mono">{{ dataset.total_resources }}</td>
                    <td class="mono">{{ dataset.pending_resources }}</td>
                    <td class="mono">{{ dataset.processing_resources }}</td>
                    <td class="mono">{{ dataset.completed_resources }}</td>
                    <td class="mono">{{ dataset.failed_resources }}</td>
                    <td class="mono">{{ dataset.outdated_resources ?? 0 }}</td>
                    <td>
                        <div class="success-rate">
                            <div class="progress-bar">
                                <div
                                    class="progress-fill"
                                    :class="successRateClass(dataset)"
                                    :style="{ width: successRate(dataset) + '%' }"
                                ></div>
                            </div>
                            <span class="mono">{{ successRate(dataset) }}%</span>
                        </div>
                    </td>
                    <td>
                        <span
                            v-if="hasNewerDataThanSync(dataset)"
                            class="sync-indicator sync-behind"
                            title="Dataset has data newer than the last metadata sync"
                        >
                            ⚠️
                        </span>
                        <span
                            v-else-if="dataset.instance_last_synced_at"
                            class="sync-indicator sync-ok"
                            title="Dataset is in sync with metadata"
                        >
                            ✅
                        </span>
                        <span v-else class="sync-indicator sync-unknown" title="No sync data available">
                            —
                        </span>
                    </td>
                    <td>{{ formatTime(dataset.updated_at) }}</td>
                </tr>
                <tr v-if="datasets.length === 0">
                    <td colspan="11" class="empty-state">No datasets found</td>
                </tr>
            </tbody>
        </table>

        <div class="pagination">
            <button
                class="btn-secondary"
                :disabled="offset === 0"
                @click="prevPage"
            >
                ← Previous
            </button>
            <span class="page-info"
                >Showing {{ offset + 1 }}–{{
                    offset + datasets.length
                }}</span
            >
            <button
                class="btn-secondary"
                :disabled="datasets.length < limit"
                @click="nextPage"
            >
                Next →
            </button>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from "vue";
import { useRouter, useRoute } from "vue-router";
import { useApi } from "@/composables/useApi";
import type { CkanInstance, Dataset } from "@/types";

const router = useRouter();
const route = useRoute();
const { fetchDatasets, fetchInstances } = useApi();

const datasets = ref<Dataset[]>([]);
const instances = ref<CkanInstance[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const filterInstanceId = ref((route.query.instance_id as string) || "");
const filterSearch = ref((route.query.search as string) || "");
const limit = 50;
const offset = ref(Number(route.query.offset) || 0);
let debounceTimer: ReturnType<typeof setTimeout> | null = null;

function syncQueryParams() {
    const query: Record<string, string> = {};
    if (filterInstanceId.value) query.instance_id = filterInstanceId.value;
    if (filterSearch.value) query.search = filterSearch.value;
    if (offset.value) query.offset = String(offset.value);
    router.replace({ query });
}

watch(
    [filterInstanceId, filterSearch, offset],
    syncQueryParams,
);

async function loadDatasets() {
    loading.value = true;
    error.value = null;
    try {
        datasets.value = await fetchDatasets({
            instance_id: filterInstanceId.value || undefined,
            search: filterSearch.value || undefined,
            limit,
            offset: offset.value,
        });
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

function resetAndLoad() {
    offset.value = 0;
    loadDatasets();
}

function debouncedLoad() {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
        offset.value = 0;
        loadDatasets();
    }, 300);
}

function prevPage() {
    offset.value = Math.max(0, offset.value - limit);
    loadDatasets();
}

function nextPage() {
    offset.value += limit;
    loadDatasets();
}

function formatTime(iso: string | null): string {
    if (!iso) return "—";
    return new Date(iso).toLocaleString();
}

function successRate(dataset: Dataset): number {
    if (dataset.total_resources === 0) return 0;
    return Math.round((dataset.completed_resources / dataset.total_resources) * 100);
}

function successRateClass(dataset: Dataset): string {
    const rate = successRate(dataset);
    if (rate >= 80) return "fill-green";
    if (rate >= 50) return "fill-yellow";
    return "fill-red";
}

function hasNewerDataThanSync(dataset: Dataset): boolean {
    if (!dataset.updated_at || !dataset.instance_last_synced_at) return false;
    return new Date(dataset.updated_at) > new Date(dataset.instance_last_synced_at);
}

onMounted(async () => {
    try {
        instances.value = await fetchInstances();
    } catch {
        // Non-fatal
    }
    loadDatasets();
});
</script>

<style scoped>
.header-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 24px;
}

.page-title {
    font-size: 24px;
    font-weight: 700;
}

.filters {
    display: flex;
    gap: 12px;
    margin-bottom: 16px;
}

.filter-select,
.filter-input {
    background: #1a1d27;
    border: 1px solid #2a2d37;
    border-radius: 6px;
    padding: 8px 12px;
    color: #e4e4e7;
    font-size: 14px;
}

.filter-select {
    min-width: 200px;
}
.filter-input {
    flex: 1;
    max-width: 360px;
}

.filter-select:focus,
.filter-input:focus {
    outline: none;
    border-color: #3b82f6;
}

.loading {
    text-align: center;
    padding: 40px;
    color: #9ca3af;
}

.error-banner {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #451a1a;
    border: 1px solid #ef4444;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 16px;
    color: #fca5a5;
    font-size: 14px;
}

.btn-retry {
    background: #ef4444;
    color: #fff;
    border: none;
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 13px;
    cursor: pointer;
    flex-shrink: 0;
}

.btn-retry:hover {
    background: #dc2626;
}

.data-table {
    width: 100%;
    border-collapse: collapse;
    background: #1a1d27;
    border-radius: 8px;
    overflow: hidden;
}

.data-table th {
    background: #2a2d37;
    padding: 10px 12px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #9ca3af;
}

.data-table td {
    padding: 10px 12px;
    font-size: 14px;
    border-top: 1px solid #2a2d37;
}

.dataset-link {
    color: #60a5fa;
    text-decoration: none;
    font-weight: 600;
}

.dataset-link:hover {
    color: #93bbfd;
    text-decoration: underline;
}

.dataset-name {
    font-weight: 600;
}

.external-icon {
    font-size: 12px;
    margin-left: 2px;
    opacity: 0.7;
}

.mono {
    font-family: "SF Mono", monospace;
    font-size: 13px;
    color: #9ca3af;
}

.success-rate {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 100px;
}

.progress-bar {
    width: 56px;
    height: 6px;
    background: #2a2d37;
    border-radius: 3px;
    overflow: hidden;
    flex-shrink: 0;
}

.progress-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.3s ease;
}

.fill-green {
    background: #22c55e;
}

.fill-yellow {
    background: #eab308;
}

.fill-red {
    background: #ef4444;
}

.sync-indicator {
    font-size: 14px;
}

.sync-unknown {
    color: #6b7280;
}

.data-row {
    transition: background 0.1s;
}
.data-row:hover {
    background: #22252f;
}
.empty-state {
    text-align: center;
    color: #9ca3af;
    padding: 40px !important;
}

.pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 16px;
    margin-top: 16px;
}

.page-info {
    font-size: 13px;
    color: #9ca3af;
}

.btn-secondary {
    background: #2a2d37;
    color: #e4e4e7;
    border: 1px solid #3a3d47;
    padding: 8px 16px;
    border-radius: 6px;
    font-size: 14px;
    cursor: pointer;
}

.btn-secondary:hover {
    background: #3a3d47;
}
.btn-secondary:disabled {
    opacity: 0.4;
    cursor: not-allowed;
}
</style>
