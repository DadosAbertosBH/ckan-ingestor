<template>
    <div
        class="stats-card clickable"
        :style="{ borderLeftColor: color }"
        @click="$emit('click')"
    >
        <div class="card-title">{{ title }}</div>
        <div class="card-value">{{ value }}</div>
    </div>
</template>

<script setup lang="ts">
defineProps<{
    title: string;
    value: number;
    color: string;
}>();

defineEmits<{
    click: [];
}>();
</script>

<style scoped>
.stats-card {
    background: #1a1d27;
    border-radius: 8px;
    padding: 20px 24px;
    border-left: 4px solid;
    transition:
        transform 0.15s ease,
        box-shadow 0.15s ease;
}

.stats-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

.clickable {
    cursor: pointer;
}

.card-title {
    font-size: 13px;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
}

.card-value {
    font-size: 32px;
    font-weight: 700;
    letter-spacing: -1px;
}
</style>
