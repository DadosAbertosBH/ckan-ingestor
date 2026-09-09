<template>
    <div class="jobs-view">
        <div class="header-row">
            <h1 class="page-title">Jobs</h1>
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
                v-model="filterResourceId"
                type="text"
                placeholder="Filter by resource ID..."
                class="filter-input"
                @input="debouncedLoad"
            />
            <input
                v-model="filterTags"
                type="text"
                placeholder="Filter by tags (comma-separated)..."
                class="filter-input"
                @input="debouncedLoad"
            />
        </div>

        <div v-if="error" class="error-banner">
            <span>Failed to load jobs: {{ error }}</span>
            <button class="btn-retry" @click="loadJobs">Retry</button>
        </div>
        <div v-if="loading" class="loading">Loading...</div>
        <table v-else-if="!error" class="data-table">
            <thead>
                <tr>
                    <th>Resource</th>
                    <th>Job ID</th>
                    <th>Format</th>
                    <th>Labels</th>
                    <th>Broker</th>
                    <th>Status</th>
                    <th
                        class="sortable-header sortable-header-duration"
                        @click="toggleSort('duration')"
                    >
                        Duration
                        <span
                            v-if="sortBy === 'duration'"
                            class="sort-indicator"
                            :class="sortDir"
                            >{{ sortDir === 'asc' ? '▲' : '▼' }}</span
                        >
                    </th>
                    <th
                        class="sortable-header sortable-header-created"
                        @click="toggleSort('created_at')"
                    >
                        Created
                        <span
                            v-if="sortBy === 'created_at'"
                            class="sort-indicator"
                            :class="sortDir"
                            >{{ sortDir === 'asc' ? '▲' : '▼' }}</span
                        >
                    </th>
                </tr>
            </thead>
            <tbody>
                <tr
                    v-for="job in jobs"
                    :key="job.id"
                    class="clickable-row"
                    @click="goToJob(job.id)"
                >
                    <td>
                        <div class="resource-path">
                            {{
                                [job.instance_name, job.dataset_name, job.resource_name]
                                    .filter(Boolean)
                                    .join(" / ") || "—"
                            }}
                        </div>
                        <router-link
                            :to="{
                                name: 'resource-detail',
                                params: { resourceId: job.resource_id },
                            }"
                            class="resource-link"
                            @click.stop
                            :title="job.resource_id"
                        >
                            {{ job.resource_id }}
                        </router-link>
                    </td>
                    <td class="mono">
                        <router-link
                            :to="{
                                name: 'job-detail',
                                params: { id: job.id },
                            }"
                            class="job-id-link"
                            @click.stop
                            :title="job.id"
                        >
                            {{ job.id.substring(0, 8) }}…
                        </router-link>
                    </td>
                    <td>{{ job.resource_format || "—" }}</td>
                    <td><ResourceLabelBadge :labels="job.labels ?? []" /></td>
                    <td class="mono broker-cell">
                        <span v-if="job.message_topic" class="broker-meta">
                            {{ formatBrokerMetadata(job) }}
                        </span>
                        <span v-else>\u2014</span>
                    </td>
                    <td><JobStatusBadge :status="job.status" /></td>
                    <td class="mono">{{ formatJobDuration(job) }}</td>
                    <td>{{ formatTime(job.created_at) }}</td>
                </tr>
                <tr v-if="jobs.length === 0">
                    <td colspan="8" class="empty-state">No jobs found</td>
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
                >Showing {{ offset + 1 }}–{{ offset + jobs.length }}</span
            >
            <button
                class="btn-secondary"
                :disabled="jobs.length < limit"
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
import { jobDuration, formatDuration } from "@/utils/jobDuration";
import type { CkanInstance, Job, JobStatus } from "@/types";

const router = useRouter();
const route = useRoute();
const { fetchJobs, fetchInstances } = useApi();

const jobs = ref<Job[]>([]);
const instances = ref<CkanInstance[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const filterStatus = ref<JobStatus | "">(
    (route.query.status as JobStatus) || "",
);
const filterInstanceId = ref((route.query.instance_id as string) || "");
const filterResourceId = ref((route.query.resource_id as string) || "");
const filterTags = ref((route.query.tags as string) || "");
const sortBy = ref<string>((route.query.sort_by as string) || "");
const sortDir = ref<string>((route.query.sort_dir as string) || "");
const limit = 50;
const offset = ref(Number(route.query.offset) || 0);
let debounceTimer: ReturnType<typeof setTimeout> | null = null;

function syncQueryParams() {
    const query: Record<string, string> = {};
    if (filterStatus.value) query.status = filterStatus.value;
    if (filterInstanceId.value) query.instance_id = filterInstanceId.value;
    if (filterResourceId.value) query.resource_id = filterResourceId.value;
    if (filterTags.value) query.tags = filterTags.value;
    if (offset.value) query.offset = String(offset.value);
    if (sortBy.value) query.sort_by = sortBy.value;
    if (sortDir.value) query.sort_dir = sortDir.value;
    router.replace({ query });
}

watch(
    [filterStatus, filterInstanceId, filterResourceId, filterTags, offset, sortBy, sortDir],
    syncQueryParams,
);

async function loadJobs() {
    loading.value = true;
    error.value = null;
    try {
        jobs.value = await fetchJobs({
            status: filterStatus.value || undefined,
            resource_id: filterResourceId.value || undefined,
            instance_id: filterInstanceId.value || undefined,
            tags: filterTags.value || undefined,
            order_by: sortBy.value || undefined,
            order_dir: sortDir.value || undefined,
            limit,
            offset: offset.value,
        } as any);
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

function toggleSort(field: string) {
    if (sortBy.value === field) {
        if (sortDir.value === "asc") {
            sortDir.value = "desc";
        } else if (sortDir.value === "desc") {
            sortBy.value = "";
            sortDir.value = "";
        }
    } else {
        sortBy.value = field;
        sortDir.value = "asc";
    }
    offset.value = 0;
    loadJobs();
}

function resetAndLoad() {
    offset.value = 0;
    loadJobs();
}

function debouncedLoad() {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
        offset.value = 0;
        loadJobs();
    }, 300);
}

function goToJob(id: string) {
    router.push({ name: "job-detail", params: { id } });
}

function prevPage() {
    offset.value = Math.max(0, offset.value - limit);
    loadJobs();
}

function nextPage() {
    offset.value += limit;
    loadJobs();
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

function formatJobDuration(job: Job): string {
    const d = jobDuration(job);
    if (!d) return "—";
    const label = formatDuration(d.ms);
    return d.finished ? label : `${label}…`;
}

function formatBrokerMetadata(job: Job): string {
    const partition = "[" + (job.message_partition ?? "") + "]";
    const offset =
        job.message_offset === null || job.message_offset === undefined
            ? ""
            : "@" + job.message_offset;
    return String(job.message_topic) + partition + offset;
}

onMounted(async () => {
    try {
        instances.value = await fetchInstances();
    } catch {
        // Non-fatal: instance filter will just show IDs
    }
    loadJobs();
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
    flex-wrap: wrap;
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
    max-width: 240px;
}

.filter-select:focus,
.filter-input:focus {
    outline: none;
    border-color: #3b82f6;
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

.broker-cell {
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
}

.broker-meta {
    color: #a78bfa;
    font-size: 12px;
}

.resource-path {
    margin-bottom: 4px;
    font-size: 14px;
}

.resource-link {
    color: #60a5fa;
    text-decoration: none;
    font-family: "SF Mono", monospace;
    font-size: 12px;
}

.resource-link:hover {
    color: #93bbfd;
    text-decoration: underline;
}

.job-id-link {
    color: #60a5fa;
    text-decoration: none;
    font-family: "SF Mono", monospace;
    font-size: 13px;
}

.job-id-link:hover {
    color: #93bbfd;
    text-decoration: underline;
}

.sortable-header {
    cursor: pointer;
    user-select: none;
    transition: color 0.15s;
}

.sortable-header:hover {
    color: #e4e4e7;
}

.sort-indicator {
    margin-left: 4px;
    font-size: 10px;
}

.sort-indicator.asc {
    color: #60a5fa;
}

.sort-indicator.desc {
    color: #f59e0b;
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

</style>
