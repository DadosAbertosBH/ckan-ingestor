<template>
    <div class="syncs-view">
        <div class="header-row">
            <h1 class="page-title">Syncs</h1>
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
        </div>

        <div v-if="error" class="error-banner">
            <span>Failed to load syncs: {{ error }}</span>
            <button class="btn-retry" @click="loadSyncs">Retry</button>
        </div>
        <div v-if="loading" class="loading">Loading...</div>
        <table v-else-if="!error" class="data-table">
            <thead>
                <tr>
                    <th>Instance</th>
                    <th>Started</th>
                    <th>Duration</th>
                    <th>Total Packages</th>
                    <th>New Datasets</th>
                    <th>New Resources</th>
                    <th>Updated Datasets</th>
                    <th>Updated Resources</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
                <tr v-for="sync in syncs" :key="sync.id">
                    <td><router-link :to="{ name: 'sync-detail', params: { id: sync.id } }" class="sync-link">{{ sync.instance_name || sync.instance_id }}</router-link></td>
                    <td>{{ formatTime(sync.start_time) }}</td>
                    <td class="mono">{{ formatSyncDuration(sync) }}</td>
                    <td>{{ sync.total_packages }}</td>
                    <td>{{ sync.new_datasets }}</td>
                    <td>{{ sync.new_resources }}</td>
                    <td>{{ sync.updated_datasets }}</td>
                    <td>{{ sync.updated_resources }}</td>
                    <td><span :class="['status', sync.status || 'pending']">{{ statusLabel(sync.status) }}</span></td>
                </tr>
                <tr v-if="syncs.length === 0">
                    <td colspan="9" class="empty-state">No syncs found</td>
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
                >Showing {{ offset + 1 }}–{{ offset + syncs.length }}</span
            >
            <button
                class="btn-secondary"
                :disabled="syncs.length < limit"
                @click="nextPage"
            >
                Next →
            </button>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useApi } from "@/composables/useApi";
import { formatDuration } from "@/utils/jobDuration";
import type { CkanInstance, MetadataSync } from "@/types";

const { fetchSyncs, fetchInstances } = useApi();

const syncs = ref<MetadataSync[]>([]);
const instances = ref<CkanInstance[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const filterInstanceId = ref("");
const limit = 50;
const offset = ref(0);

async function loadSyncs() {
    loading.value = true;
    error.value = null;
    try {
        syncs.value = await fetchSyncs({
            instance_id: filterInstanceId.value || undefined,
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
    loadSyncs();
}

function prevPage() {
    offset.value = Math.max(0, offset.value - limit);
    loadSyncs();
}

function nextPage() {
    offset.value += limit;
    loadSyncs();
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

function formatSyncDuration(sync: MetadataSync): string {
    if (!sync.end_time) return "…";
    const ms =
        new Date(sync.end_time).getTime() - new Date(sync.start_time).getTime();
    return formatDuration(Math.max(0, ms));
}

function statusLabel(status: MetadataSync["status"]): string {
    return status === "failure" ? "Failed" : status === "success" ? "Success" : "Pending";
}

onMounted(async () => {
    try {
        instances.value = await fetchInstances();
    } catch {
        // Non-fatal: instance filter will just show IDs
    }
    loadSyncs();
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
    min-width: 160px;
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
    padding: 12px 16px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #9ca3af;
}

.data-table td {
    padding: 12px 16px;
    font-size: 14px;
    border-top: 1px solid #2a2d37;
}

.mono {
    font-family: "SF Mono", monospace;
    font-size: 13px;
    color: #9ca3af;
}

.sync-link { color: #93c5fd; text-decoration: none; }
.sync-link:hover { text-decoration: underline; }
.status { border-radius: 999px; padding: 3px 8px; font-size: 12px; font-weight: 600; }
.status.success { background: #14532d; color: #86efac; }
.status.failure { background: #7f1d1d; color: #fca5a5; }
.status.pending { background: #3f3f46; color: #d4d4d8; }

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
