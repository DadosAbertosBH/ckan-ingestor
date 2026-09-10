<template>
    <span class="badge" :style="{ backgroundColor: colors[status] }">
        {{ status }}
    </span>
</template>

<script setup lang="ts">
import type { ResourceStatus } from "@/types";

defineProps<{ status: ResourceStatus }>();

const colors: Record<ResourceStatus, string> = {
    pending: "#f59e0b",
    processing: "#3b82f6",
    completed: "#10b981",
    failed: "#ef4444",
    deleted: "#6b7280",
    outdated: "#a855f7",
};
</script>

<style scoped>
.badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
    color: #fff;
    text-transform: capitalize;
}
</style>
