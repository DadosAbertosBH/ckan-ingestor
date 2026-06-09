<template>
    <div class="jobs-view">
        <div class="header-row">
            <h1 class="page-title">Jobs</h1>
            <button class="btn-primary" @click="showModal = true">
                + Enqueue Job
            </button>
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
            <input
                v-model="filterResourceId"
                type="text"
                placeholder="Filter by resource ID..."
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
                    <th>Resource ID</th>
                    <th>Format</th>
                    <th>Labels</th>
                    <th>Status</th>
                    <th>Duration</th>
                    <th>Created</th>
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
                        {{
                            [job.dataset_name, job.resource_name]
                                .filter(Boolean)
                                .join(" / ") || "—"
                        }}
                    </td>
                    <td class="mono">{{ job.resource_id.substring(0, 8) }}…</td>
                    <td>{{ job.resource_format || "—" }}</td>
                    <td><ResourceLabelBadge :labels="job.labels ?? []" /></td>
                    <td><JobStatusBadge :status="job.status" /></td>
                    <td class="mono">{{ formatJobDuration(job) }}</td>
                    <td>{{ formatTime(job.created_at) }}</td>
                </tr>
                <tr v-if="jobs.length === 0">
                    <td colspan="7" class="empty-state">No jobs found</td>
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

        <!-- Enqueue Modal -->
        <div
            v-if="showModal"
            class="modal-overlay"
            @click.self="showModal = false"
        >
            <div class="modal">
                <h2 class="modal-title">Enqueue New Job</h2>
                <form @submit.prevent="submitJob">
                    <div class="form-group">
                        <label>Resource ID *</label>
                        <input
                            v-model="form.resource_id"
                            type="text"
                            required
                            placeholder="CKAN resource UUID"
                        />
                    </div>
                    <div class="form-group">
                        <label>Dataset Name *</label>
                        <input
                            v-model="form.dataset_name"
                            type="text"
                            required
                            placeholder="CKAN dataset slug"
                        />
                    </div>
                    <div class="form-group">
                        <label>Resource Name</label>
                        <input
                            v-model="form.resource_name"
                            type="text"
                            placeholder="Optional"
                        />
                    </div>
                    <div class="form-group">
                        <label>Resource URL</label>
                        <input
                            v-model="form.resource_url"
                            type="text"
                            placeholder="Optional"
                        />
                    </div>
                    <div class="form-group">
                        <label>Format</label>
                        <input
                            v-model="form.resource_format"
                            type="text"
                            placeholder="CSV, JSON, PDF, etc."
                        />
                    </div>
                    <div class="modal-actions">
                        <button
                            type="button"
                            class="btn-secondary"
                            @click="showModal = false"
                        >
                            Cancel
                        </button>
                        <button
                            type="submit"
                            class="btn-primary"
                            :disabled="submitting"
                        >
                            {{ submitting ? "Creating..." : "Enqueue" }}
                        </button>
                    </div>
                </form>
            </div>
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
import type { Job, JobStatus } from "@/types";

const router = useRouter();
const route = useRoute();
const { fetchJobs, createJob } = useApi();

const jobs = ref<Job[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const filterStatus = ref<JobStatus | "">(
    (route.query.status as JobStatus) || "",
);
const filterResourceId = ref((route.query.resource_id as string) || "");
const limit = 50;
const offset = ref(Number(route.query.offset) || 0);
const showModal = ref(false);
const submitting = ref(false);
const form = ref({
    resource_id: "",
    dataset_name: "",
    resource_name: "",
    resource_url: "",
    resource_format: "",
});
let debounceTimer: ReturnType<typeof setTimeout> | null = null;

function syncQueryParams() {
    const query: Record<string, string> = {};
    if (filterStatus.value) query.status = filterStatus.value;
    if (filterResourceId.value) query.resource_id = filterResourceId.value;
    if (offset.value) query.offset = String(offset.value);
    router.replace({ query });
}

watch([filterStatus, filterResourceId, offset], syncQueryParams);

async function loadJobs() {
    loading.value = true;
    error.value = null;
    try {
        jobs.value = await fetchJobs({
            status: filterStatus.value || undefined,
            resource_id: filterResourceId.value || undefined,
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

async function submitJob() {
    submitting.value = true;
    try {
        await createJob({
            resource_id: form.value.resource_id,
            dataset_name: form.value.dataset_name,
            resource_name: form.value.resource_name || undefined,
            resource_url: form.value.resource_url || undefined,
            resource_format: form.value.resource_format || undefined,
        });
        showModal.value = false;
        form.value = {
            resource_id: "",
            dataset_name: "",
            resource_name: "",
            resource_url: "",
            resource_format: "",
        };
        await loadJobs();
    } catch (e: any) {
        alert(e.message || "Failed to create job");
    } finally {
        submitting.value = false;
    }
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

onMounted(loadJobs);
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

.btn-primary {
    background: #3b82f6;
    color: #fff;
    border: none;
    padding: 8px 16px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.15s;
}

.btn-primary:hover {
    background: #2563eb;
}
.btn-primary:disabled {
    opacity: 0.5;
    cursor: not-allowed;
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

.modal-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.6);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 100;
}

.modal {
    background: #1a1d27;
    border-radius: 12px;
    padding: 24px;
    width: 100%;
    max-width: 480px;
    border: 1px solid #2a2d37;
}

.modal-title {
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 20px;
}

.form-group {
    margin-bottom: 14px;
}

.form-group label {
    display: block;
    font-size: 13px;
    font-weight: 500;
    color: #9ca3af;
    margin-bottom: 4px;
}

.form-group input {
    width: 100%;
    background: #0f1117;
    border: 1px solid #2a2d37;
    border-radius: 6px;
    padding: 8px 12px;
    color: #e4e4e7;
    font-size: 14px;
    box-sizing: border-box;
}

.form-group input:focus {
    outline: none;
    border-color: #3b82f6;
}

.modal-actions {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    margin-top: 20px;
}
</style>
