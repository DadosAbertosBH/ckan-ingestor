<template>
    <div class="resources-view">
        <div class="header-row">
            <h1 class="page-title">Resources</h1>
        </div>

        <div class="filters">
            <select
                v-model="filterStatus"
                class="filter-select"
                @change="resetAndLoad"
            >
                <option value="">All statuses</option>
                <option value="pending">Pending</option>
                <option value="processing">Processing</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed</option>
            </select>
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
            <span>Failed to load resources: {{ error }}</span>
            <button class="btn-retry" @click="loadResources">Retry</button>
        </div>
        <div v-if="loading" class="loading">Loading...</div>
        <table v-else-if="!error" class="data-table">
            <thead>
                <tr>
                    <th>Resource</th>
                    <th>Format</th>
                    <th>Labels</th>
                    <th>Status</th>
                    <th>Jobs</th>
                    <th>Updated</th>
                </tr>
            </thead>
            <tbody>
                <tr
                    v-for="resource in resources"
                    :key="resource.resource_id"
                    class="clickable-row"
                    @click="goToResource(resource.resource_id)"
                >
                    <td>
                        {{
                            [resource.dataset_name, resource.resource_name]
                                .filter(Boolean)
                                .join(" / ") || "—"
                        }}
                    </td>
                    <td>{{ resource.resource_format || "—" }}</td>
                    <td>
                        <ResourceLabelBadge :labels="resource.labels ?? []" />
                    </td>
                    <td><JobStatusBadge :status="resource.status" /></td>
                    <td class="mono">{{ resource.job_count }}</td>
                    <td>{{ formatTime(resource.updated_at) }}</td>
                </tr>
                <tr v-if="resources.length === 0">
                    <td colspan="6" class="empty-state">No resources found</td>
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
                    offset + resources.length
                }}</span
            >
            <button
                class="btn-secondary"
                :disabled="resources.length < limit"
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
import JobStatusBadge from "@/components/JobStatusBadge.vue";
import ResourceLabelBadge from "@/components/ResourceLabelBadge.vue";
import { useApi } from "@/composables/useApi";
import type { CkanInstance, JobStatus, Resource } from "@/types";

const router = useRouter();
const route = useRoute();
const { fetchResources, fetchInstances } = useApi();

const resources = ref<Resource[]>([]);
const instances = ref<CkanInstance[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const filterStatus = ref<JobStatus | "">(
    (route.query.status as JobStatus) || "",
);
const filterInstanceId = ref((route.query.instance_id as string) || "");
const filterSearch = ref((route.query.search as string) || "");
const limit = 50;
const offset = ref(Number(route.query.offset) || 0);
let debounceTimer: ReturnType<typeof setTimeout> | null = null;

function syncQueryParams() {
    const query: Record<string, string> = {};
    if (filterStatus.value) query.status = filterStatus.value;
    if (filterInstanceId.value) query.instance_id = filterInstanceId.value;
    if (filterSearch.value) query.search = filterSearch.value;
    if (offset.value) query.offset = String(offset.value);
    router.replace({ query });
}

watch(
    [filterStatus, filterInstanceId, filterSearch, offset],
    syncQueryParams,
);

async function loadResources() {
    loading.value = true;
    error.value = null;
    try {
        resources.value = await fetchResources({
            status: filterStatus.value || undefined,
            instance_id: filterInstanceId.value || undefined,
            search: filterSearch.value || undefined,
            limit,
            offset: offset.value,
        } as any);
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

function resetAndLoad() {
    offset.value = 0;
    loadResources();
}

function debouncedLoad() {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
        offset.value = 0;
        loadResources();
    }, 300);
}

function goToResource(resourceId: string) {
    router.push({ name: "resource-detail", params: { resourceId } });
}

function prevPage() {
    offset.value = Math.max(0, offset.value - limit);
    loadResources();
}

function nextPage() {
    offset.value += limit;
    loadResources();
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

onMounted(async () => {
    try {
        instances.value = await fetchInstances();
    } catch {
        // Non-fatal
    }
    loadResources();
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

.clickable-row {
    cursor: pointer;
    transition: background 0.1s;
}
.clickable-row:hover {
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
