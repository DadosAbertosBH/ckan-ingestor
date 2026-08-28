<template>
    <div class="instance-card">
        <div class="card-header">
            <span class="instance-name">{{ stats.instance.name }}</span>
            <div class="header-actions">
                <button
                    class="btn-sync"
                    :disabled="syncing"
                    @click="$emit('sync')"
                    title="Sync metadata"
                >
                    {{ syncing ? "Syncing…" : "Sync" }}
                </button>
                <a
                    :href="stats.instance.url"
                    target="_blank"
                    class="instance-link"
                    title="Open CKAN instance"
                >
                    <svg
                        width="14"
                        height="14"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2"
                        stroke-linecap="round"
                        stroke-linejoin="round"
                    >
                        <path
                            d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"
                        />
                        <polyline points="15 3 21 3 21 9" />
                        <line x1="10" y1="14" x2="21" y2="3" />
                    </svg>
                </a>
            </div>
        </div>

        <div class="meta-row">
            <div class="meta-item">
                <span class="meta-label">Last Synced</span>
                <span class="meta-value">{{ formattedLastSynced }}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Datasets</span>
                <span class="meta-value">{{
                    stats.instance.dataset_count
                }}</span>
            </div>
            <div class="meta-item">
                <span class="meta-label">Resources</span>
                <span class="meta-value">{{
                    stats.instance.resource_count
                }}</span>
            </div>
        </div>

        <div class="divider" />

        <div class="jobs-section">
            <span class="jobs-title">Resources</span>
            <div class="job-stats">
                <router-link
                    :to="{
                        name: 'resources',
                        query: {
                            instance_id: stats.instance.id,
                            status: 'outdated',
                        },
                    }"
                    class="stat-pill stat-link"
                    style="background: rgba(168, 85, 247, 0.15); color: #a855f7"
                >
                    Outdated {{ stats.outdated ?? 0 }}
                </router-link>
                <router-link
                    :to="{
                        name: 'resources',
                        query: {
                            instance_id: stats.instance.id,
                            status: 'pending',
                        },
                    }"
                    class="stat-pill stat-link"
                    style="background: rgba(245, 158, 11, 0.15); color: #f59e0b"
                >
                    Pending {{ stats.pending }}
                </router-link>
                <router-link
                    :to="{
                        name: 'resources',
                        query: {
                            instance_id: stats.instance.id,
                            status: 'processing',
                        },
                    }"
                    class="stat-pill stat-link"
                    style="background: rgba(59, 130, 246, 0.15); color: #3b82f6"
                >
                    Processing {{ stats.processing }}
                </router-link>
                <router-link
                    :to="{
                        name: 'resources',
                        query: {
                            instance_id: stats.instance.id,
                            status: 'completed',
                        },
                    }"
                    class="stat-pill stat-link"
                    style="background: rgba(16, 185, 129, 0.15); color: #10b981"
                >
                    Completed {{ stats.completed }}
                </router-link>
                <router-link
                    :to="{
                        name: 'resources',
                        query: {
                            instance_id: stats.instance.id,
                            status: 'failed',
                        },
                    }"
                    class="stat-pill stat-link"
                    style="background: rgba(239, 68, 68, 0.15); color: #ef4444"
                >
                    Failed {{ stats.failed }}
                </router-link>
            </div>
        </div>
    </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { InstanceStats } from "@/types";

const props = defineProps<{
    stats: InstanceStats;
    syncing?: boolean;
}>();

defineEmits<{
    sync: [];
}>();

const formattedLastSynced = computed(() => {
    if (!props.stats.instance.last_metadata_synced) return "Never";
    return new Date(props.stats.instance.last_metadata_synced).toLocaleString();
});
</script>

<style scoped>
.instance-card {
    background: #1a1d27;
    border-radius: 8px;
    padding: 20px 24px;
    transition:
        transform 0.15s ease,
        box-shadow 0.15s ease;
}

.instance-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

.card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
}

.instance-name {
    font-size: 18px;
    font-weight: 700;
    color: #f3f4f6;
}

.header-actions {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
}

.btn-sync {
    background: #3b82f6;
    color: #fff;
    border: none;
    padding: 4px 12px;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.15s;
    white-space: nowrap;
}

.btn-sync:hover {
    background: #2563eb;
}

.btn-sync:disabled {
    opacity: 0.5;
    cursor: not-allowed;
}

.instance-link {
    color: #6b7280;
    transition: color 0.15s ease;
    display: flex;
    align-items: center;
    flex-shrink: 0;
}

.instance-link:hover {
    color: #9ca3af;
}

.meta-row {
    display: flex;
    gap: 24px;
    margin-bottom: 16px;
}

.meta-item {
    display: flex;
    flex-direction: column;
    gap: 4px;
}

.meta-label {
    font-size: 11px;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.meta-value {
    font-size: 14px;
    color: #f3f4f6;
    font-weight: 500;
}

.divider {
    height: 1px;
    background: #2a2d37;
    margin: 0 0 14px 0;
}

.jobs-section {
    display: flex;
    flex-direction: column;
    gap: 8px;
}

.jobs-title {
    font-size: 11px;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.job-stats {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}

.stat-pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
}

.stat-link {
    text-decoration: none;
    cursor: pointer;
    transition: opacity 0.15s ease;
}

.stat-link:hover {
    opacity: 0.8;
}
</style>
