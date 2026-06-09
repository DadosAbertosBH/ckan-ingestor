<template>
    <span v-for="label in labels" :key="label" class="label-badge" :style="{ backgroundColor: colors[label] ?? '#6b7280' }">
        {{ label }}
    </span>
</template>

<script setup lang="ts">
defineProps<{ labels: string[] }>();

const colors: Record<string, string> = {
    empty: "#6b7280",
};
</script>

<style scoped>
.label-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    color: #fff;
    text-transform: capitalize;
    margin-right: 4px;
}
</style>
