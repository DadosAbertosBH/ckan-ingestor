<template>
    <div class="dashboard">
        <h1 class="page-title">
            Dashboard
            <span
                class="refresh-timer"
                title="Auto-refresh in {{ countdown }}s"
            >
                <svg
                    class="timer-ring"
                    width="20"
                    height="20"
                    viewBox="0 0 24 24"
                >
                    <circle
                        class="timer-bg"
                        cx="12"
                        cy="12"
                        r="9"
                        fill="none"
                        stroke="#2a2d37"
                        stroke-width="2.5"
                    />
                    <circle
                        class="timer-fill"
                        cx="12"
                        cy="12"
                        r="9"
                        fill="none"
                        stroke="#3b82f6"
                        stroke-width="2.5"
                        stroke-dasharray="56.548"
                        :stroke-dashoffset="56.548 * (1 - countdown / 30)"
                        stroke-linecap="round"
                        transform="rotate(-90 12 12)"
                    />
                </svg>
                <span class="timer-text">{{ countdown }}s</span>
            </span>
        </h1>

        <div class="instances-grid">
            <InstanceCard
                v-for="s in instanceStats"
                :key="s.instance.id"
                :stats="s"
                :syncing="syncingInstanceId === s.instance.id"
                @sync="handleSync(s.instance.id)"
            />
        </div>

        <div class="section">
            <h2 class="section-title">Recent Jobs</h2>
            <div v-if="jobsError" class="error-banner">
                <span>Failed to load jobs: {{ jobsError }}</span>
                <button class="btn-retry" @click="loadRecentJobs">Retry</button>
            </div>
            <div v-if="statsError" class="error-banner">
                <span>Failed to load instances: {{ statsError }}</span>
                <button class="btn-retry" @click="loadInstanceStats">
                    Retry
                </button>
            </div>
            <div v-if="loading && recentJobs.length === 0" class="loading">
                Loading...
            </div>
            <table v-else class="data-table">
                <thead>
                    <tr>
                        <th>Resource</th>
                        <th>Format</th>
                        <th>Status</th>
                        <th>Created</th>
                    </tr>
                </thead>
                <tbody>
                    <tr
                        v-for="job in recentJobs"
                        :key="job.id"
                        class="clickable-row"
                        @click="goToJob(job.id)"
                    >
                        <td>{{ job.resource_name || job.resource_id }}</td>
                        <td>{{ job.resource_format || "—" }}</td>
                        <td><JobStatusBadge :status="job.status" /></td>
                        <td>{{ formatTime(job.created_at) }}</td>
                    </tr>
                    <tr v-if="recentJobs.length === 0">
                        <td colspan="4" class="empty-state">No jobs yet</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { useRouter } from "vue-router";
import InstanceCard from "@/components/InstanceCard.vue";
import JobStatusBadge from "@/components/JobStatusBadge.vue";
import { useApi } from "@/composables/useApi";
import type { InstanceStats, Job } from "@/types";

const router = useRouter();
const { fetchInstanceStats, fetchJobs, syncMetadata } = useApi();

const instanceStats = ref<InstanceStats[]>([]);
const recentJobs = ref<Job[]>([]);
const loading = ref(true);
const statsError = ref<string | null>(null);
const jobsError = ref<string | null>(null);
const syncingInstanceId = ref<string | null>(null);
const countdown = ref(30);
let interval: ReturnType<typeof setInterval> | null = null;
let countdownInterval: ReturnType<typeof setInterval> | null = null;

function startCountdown() {
    countdown.value = 30;
    if (countdownInterval) clearInterval(countdownInterval);
    countdownInterval = setInterval(() => {
        if (countdown.value > 0) {
            countdown.value--;
        }
    }, 1000);
}

async function loadInstanceStats() {
    statsError.value = null;
    try {
        instanceStats.value = await fetchInstanceStats();
    } catch (e: any) {
        statsError.value = e.message || "Unknown error";
    }
}

async function loadRecentJobs() {
    jobsError.value = null;
    try {
        recentJobs.value = await fetchJobs({ limit: 10 });
    } catch (e: any) {
        jobsError.value = e.message || "Unknown error";
    }
}

async function loadData() {
    loading.value = true;
    await Promise.all([loadInstanceStats(), loadRecentJobs()]);
    loading.value = false;
    startCountdown();
}

function goToJob(id: string) {
    router.push({ name: "job-detail", params: { id } });
}

async function handleSync(instanceId: string) {
    syncingInstanceId.value = instanceId;
    try {
        await syncMetadata(instanceId);
        await loadInstanceStats();
    } catch (e: any) {
        statsError.value = e.message || "Sync failed";
    } finally {
        syncingInstanceId.value = null;
    }
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

onMounted(() => {
    loadData();
    startCountdown();
    interval = setInterval(() => {
        loadInstanceStats();
        loadRecentJobs();
        startCountdown();
    }, 30000);
});

onUnmounted(() => {
    if (interval) clearInterval(interval);
    if (countdownInterval) clearInterval(countdownInterval);
});
</script>

<style scoped>
.page-title {
    font-size: 24px;
    font-weight: 700;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 12px;
}

.refresh-timer {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    font-weight: 500;
    color: #6b7280;
}

.timer-ring {
    flex-shrink: 0;
}

.timer-fill {
    transition: stroke-dashoffset 0.3s linear;
}

.timer-text {
    font-variant-numeric: tabular-nums;
    min-width: 28px;
}

.instances-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 16px;
    margin-bottom: 32px;
}

@media (max-width: 768px) {
    .instances-grid {
        grid-template-columns: 1fr;
    }
}

.section {
    margin-top: 8px;
}

.section-title {
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 16px;
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

.clickable-row {
    cursor: pointer;
    transition: background 0.1s ease;
}

.clickable-row:hover {
    background: #22252f;
}

.empty-state {
    text-align: center;
    color: #9ca3af;
    padding: 40px !important;
}
</style>
