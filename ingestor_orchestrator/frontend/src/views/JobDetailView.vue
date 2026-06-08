<template>
    <div class="job-detail">
        <div v-if="loading" class="loading">Loading...</div>
        <div v-else-if="error" class="error-banner">
            <span>Failed to load job: {{ error }}</span>
            <button class="btn-retry" @click="loadJob">Retry</button>
        </div>
        <div v-else-if="!job" class="error">Job not found</div>
        <template v-else>
            <div class="back-row">
                <router-link to="/jobs" class="back-link"
                    >← Back to Jobs</router-link
                >
            </div>

            <div class="job-header">
                <div>
                    <h1 class="page-title">
                        {{ job.resource_name || "Unnamed Resource" }}
                    </h1>
                    <div class="meta-line mono">{{ job.resource_id }}</div>
                </div>
                <div class="actions">
                    <button
                        v-if="job.status === 'failed'"
                        class="btn-primary"
                        @click="handleRetry"
                    >
                        Retry
                    </button>
                    <button
                        v-if="job.status === 'pending'"
                        class="btn-danger"
                        @click="handleDelete"
                    >
                        Cancel
                    </button>
                </div>
            </div>

            <div class="info-grid">
                <div class="info-item">
                    <span class="info-label">Status</span>
                    <JobStatusBadge :status="job.status" />
                </div>
                <div class="info-item">
                    <span class="info-label">Format</span>
                    <span class="info-value">{{
                        job.resource_format || "—"
                    }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Created</span>
                    <span class="info-value">{{
                        formatTime(job.created_at)
                    }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Started</span>
                    <span class="info-value">{{
                        job.started_at ? formatTime(job.started_at) : "—"
                    }}</span>
                </div>
                <div class="info-item">
                    <span class="info-label">Completed</span>
                    <span class="info-value">{{
                        job.completed_at ? formatTime(job.completed_at) : "—"
                    }}</span>
                </div>
                <div v-if="job.resource_url" class="info-item">
                    <span class="info-label">URL</span>
                    <a
                        :href="job.resource_url"
                        target="_blank"
                        class="info-link"
                        >{{ job.resource_url }}</a
                    >
                </div>
            </div>

            <div class="results-section">
                <h2 class="section-title">
                    Results ({{ job.results?.length ?? 0 }})
                </h2>
                <div v-if="job.results && job.results.length > 0">
                    <JobResultPanel
                        v-for="result in job.results"
                        :key="result.id"
                        :result="result"
                    />
                </div>
                <div v-else class="empty-state">No results yet</div>
            </div>
        </template>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import JobStatusBadge from "@/components/JobStatusBadge.vue";
import JobResultPanel from "@/components/JobResultPanel.vue";
import { useApi } from "@/composables/useApi";
import type { Job } from "@/types";

const props = defineProps<{ id: string }>();
const router = useRouter();
const { fetchJob, retryJob, deleteJob } = useApi();

const job = ref<Job | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);

async function loadJob() {
    loading.value = true;
    error.value = null;
    try {
        job.value = await fetchJob(props.id);
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

async function handleRetry() {
    try {
        job.value = await retryJob(props.id);
    } catch (e: any) {
        alert(e.message || "Failed to retry");
    }
}

async function handleDelete() {
    if (!confirm("Cancel this job?")) return;
    try {
        await deleteJob(props.id);
        router.push("/jobs");
    } catch (e: any) {
        alert(e.message || "Failed to delete");
    }
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

onMounted(loadJob);
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

.job-header {
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

.actions {
    display: flex;
    gap: 8px;
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
}

.btn-primary:hover {
    background: #2563eb;
}

.btn-danger {
    background: #ef4444;
    color: #fff;
    border: none;
    padding: 8px 16px;
    border-radius: 6px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
}

.btn-danger:hover {
    background: #dc2626;
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

.empty-state {
    text-align: center;
    color: #9ca3af;
    padding: 32px;
    background: #1a1d27;
    border-radius: 8px;
}
</style>
