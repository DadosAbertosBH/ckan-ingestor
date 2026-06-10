<template>
    <div class="resource-detail">
        <div v-if="loading" class="loading">Loading...</div>
        <div v-else-if="error" class="error-banner">
            <span>Failed to load resource: {{ error }}</span>
            <button class="btn-retry" @click="loadResource">Retry</button>
        </div>
        <div v-else-if="!resource" class="error">Resource not found</div>
        <template v-else>
            <div class="back-row">
                <router-link to="/resources" class="back-link"
                    >← Back to Resources</router-link
                >
            </div>

            <div class="resource-header">
                <div>
                    <h1 class="page-title">
                        {{ resource.resource_name || "Unnamed Resource" }}
                    </h1>
                    <div class="meta-line mono">{{ resource.resource_id }}</div>
                </div>
            </div>

            <div class="info-grid">
                <div class="info-item">
                    <span class="info-label">Status</span>
                    <JobStatusBadge :status="resource.status" />
                </div>
                <div class="info-item">
                    <span class="info-label">Format</span>
                    <span class="info-value">{{
                        resource.resource_format || "—"
                    }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Labels</span>
                    <span class="info-value">
                        <ResourceLabelBadge :labels="resource.labels ?? []" />
                        <span
                            v-if="!resource.labels?.length"
                            style="color: #9ca3af"
                            >—</span
                        >
                    </span>
                </div>
                <div class="info-item">
                    <span class="info-label">Dataset</span>
                    <span class="info-value">{{ resource.dataset_name }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Jobs</span>
                    <span class="info-value">{{ resource.job_count }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">CKAN</span>
                    <a
                        :href="resource.ckan_resource_url"
                        target="_blank"
                        class="info-link"
                        >View on CKAN ↗</a
                    >
                </div>
                <div v-if="resource.resource_url" class="info-item">
                    <span class="info-label">URL</span>
                    <a
                        :href="resource.resource_url"
                        target="_blank"
                        class="info-link"
                        >{{ resource.resource_url }}</a
                    >
                </div>
                <div class="info-item">
                    <span class="info-label">First Seen</span>
                    <span class="info-value">{{
                        formatTime(resource.created_at)
                    }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Last Updated</span>
                    <span class="info-value">{{
                        formatTime(resource.updated_at)
                    }}</span>
                </div>
            </div>

            <div class="jobs-section">
                <h2 class="section-title">
                    Job History ({{ resource.jobs?.length ?? 0 }})
                </h2>
                <div v-if="resource.jobs && resource.jobs.length > 0">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Status</th>
                                <th>Created</th>
                                <th>Started</th>
                                <th>Completed</th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr
                                v-for="job in resource.jobs"
                                :key="job.id"
                                class="clickable-row"
                                @click="goToJob(job.id)"
                            >
                                <td>
                                    <JobStatusBadge :status="job.status" />
                                </td>
                                <td>{{ formatTime(job.created_at) }}</td>
                                <td>
                                    {{
                                        job.started_at
                                            ? formatTime(job.started_at)
                                            : "—"
                                    }}
                                </td>
                                <td>
                                    {{
                                        job.completed_at
                                            ? formatTime(job.completed_at)
                                            : "—"
                                    }}
                                </td>
                                <td>
                                    <router-link
                                        :to="`/jobs/${job.id}`"
                                        class="info-link"
                                        @click.stop
                                        >View →</router-link
                                    >
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div v-else class="empty-state">No jobs recorded</div>
            </div>

            <div
                v-if="resource.preview && resource.preview.length > 0"
                class="preview-section"
            >
                <h2 class="section-title">
                    Data Preview ({{ resource.preview.length }} rows)
                </h2>
                <div class="preview-table-wrapper">
                    <table class="preview-table">
                        <thead>
                            <tr>
                                <th
                                    v-for="key in Object.keys(
                                        resource.preview[0] ?? {},
                                    )"
                                    :key="key"
                                >
                                    {{ key }}
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr v-for="(row, i) in resource.preview" :key="i">
                                <td
                                    v-for="key in Object.keys(
                                        resource.preview[0] ?? {},
                                    )"
                                    :key="key"
                                >
                                    {{ truncate(String(row[key] ?? "")) }}
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </template>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import JobStatusBadge from "@/components/JobStatusBadge.vue";
import ResourceLabelBadge from "@/components/ResourceLabelBadge.vue";
import { useApi } from "@/composables/useApi";
import type { ResourceDetail } from "@/types";

const props = defineProps<{ resourceId: string }>();
const router = useRouter();
const { fetchResource } = useApi();

const resource = ref<ResourceDetail | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);

async function loadResource() {
    loading.value = true;
    error.value = null;
    try {
        resource.value = await fetchResource(props.resourceId);
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

function goToJob(jobId: string) {
    router.push({ name: "job-detail", params: { id: jobId } });
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

function truncate(val: string, max = 60): string {
    return val.length > max ? val.substring(0, max) + "…" : val;
}

onMounted(loadResource);
</script>

<style scoped>
.loading,
.error {
    text-align: center;
    padding: 60px;
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

.back-row {
    margin-bottom: 16px;
}

.back-link {
    color: #9ca3af;
    text-decoration: none;
    font-size: 14px;
    transition: color 0.15s;
}

.back-link:hover {
    color: #e4e4e7;
}

.resource-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 24px;
}

.page-title {
    font-size: 24px;
    font-weight: 700;
}

.meta-line {
    margin-top: 4px;
    font-size: 13px;
    color: #9ca3af;
}

.mono {
    font-family: "SF Mono", monospace;
}

.info-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    background: #1a1d27;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 32px;
}

.info-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
}

.info-label {
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: #9ca3af;
    font-weight: 600;
}

.info-value {
    font-size: 14px;
}

.info-link {
    color: #3b82f6;
    text-decoration: none;
    word-break: break-all;
    font-size: 13px;
}

.info-link:hover {
    text-decoration: underline;
}

.section-title {
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 16px;
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
    padding: 32px;
    background: #1a1d27;
    border-radius: 8px;
}

.preview-section {
    margin-top: 32px;
}

.preview-table-wrapper {
    overflow-x: auto;
    margin-top: 8px;
}

.preview-table {
    width: 100%;
    border-collapse: collapse;
    background: #1a1d27;
    border-radius: 8px;
    overflow: hidden;
    font-size: 12px;
}

.preview-table th {
    background: #2a2d37;
    padding: 6px 10px;
    text-align: left;
    font-weight: 600;
    white-space: nowrap;
}

.preview-table td {
    padding: 6px 10px;
    border-top: 1px solid #2a2d37;
    white-space: nowrap;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
}
</style>
