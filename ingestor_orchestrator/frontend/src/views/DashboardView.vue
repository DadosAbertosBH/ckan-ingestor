<template>
    <div class="dashboard">
        <h1 class="page-title">Dashboard</h1>

        <div class="stats-grid">
            <StatsCard
                title="Pending"
                :value="stats?.pending ?? 0"
                color="#f59e0b"
            />
            <StatsCard
                title="Processing"
                :value="stats?.processing ?? 0"
                color="#3b82f6"
            />
            <StatsCard
                title="Completed"
                :value="stats?.completed ?? 0"
                color="#10b981"
            />
            <StatsCard
                title="Failed"
                :value="stats?.failed ?? 0"
                color="#ef4444"
            />
        </div>

        <div class="section">
            <h2 class="section-title">Recent Jobs</h2>
            <div v-if="error" class="error-banner">
                <span>Failed to load dashboard: {{ error }}</span>
                <button class="btn-retry" @click="loadData">Retry</button>
            </div>
            <div v-if="loading" class="loading">Loading...</div>
            <table v-else-if="!error" class="data-table">
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
import StatsCard from "@/components/StatsCard.vue";
import JobStatusBadge from "@/components/JobStatusBadge.vue";
import { useApi } from "@/composables/useApi";
import type { DashboardStats, Job } from "@/types";

const router = useRouter();
const { fetchStats, fetchJobs } = useApi();

const stats = ref<DashboardStats | null>(null);
const recentJobs = ref<Job[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
let interval: ReturnType<typeof setInterval> | null = null;

async function loadData() {
    error.value = null;
    try {
        const [s, j] = await Promise.all([
            fetchStats(),
            fetchJobs({ limit: 10 }),
        ]);
        stats.value = s;
        recentJobs.value = j;
    } catch (e: any) {
        error.value = e.message || "Unknown error";
    } finally {
        loading.value = false;
    }
}

function goToJob(id: string) {
    router.push({ name: "job-detail", params: { id } });
}

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

onMounted(() => {
    loadData();
    interval = setInterval(loadData, 30000); // Auto-refresh every 30s
});

onUnmounted(() => {
    if (interval) clearInterval(interval);
});
</script>

<style scoped>
.page-title {
    font-size: 24px;
    font-weight: 700;
    margin-bottom: 24px;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 32px;
}

@media (max-width: 768px) {
    .stats-grid {
        grid-template-columns: repeat(2, 1fr);
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
