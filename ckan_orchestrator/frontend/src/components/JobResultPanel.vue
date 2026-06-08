<template>
    <div
        class="result-panel"
        :class="{ success: result.success, failure: !result.success }"
    >
        <div class="result-header">
            <span class="result-icon">{{ result.success ? "✓" : "✗" }}</span>
            <span class="result-title">{{
                result.success ? "Success" : "Failed"
            }}</span>
            <span class="result-time">{{ formatTime(result.created_at) }}</span>
        </div>

        <div v-if="result.success" class="result-body">
            <div v-if="result.rows_processed !== null" class="meta-row">
                <span class="meta-label">Rows processed:</span>
                <span class="meta-value">{{
                    result.rows_processed.toLocaleString()
                }}</span>
            </div>
            <div
                v-if="
                    result.dataset_preview && result.dataset_preview.length > 0
                "
                class="preview-section"
            >
                <div class="meta-label">
                    Data preview ({{ result.dataset_preview.length }} rows):
                </div>
                <div class="preview-table-wrapper">
                    <table class="preview-table">
                        <thead>
                            <tr>
                                <th
                                    v-for="key in Object.keys(
                                        result.dataset_preview[0],
                                    )"
                                    :key="key"
                                >
                                    {{ key }}
                                </th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr
                                v-for="(row, i) in result.dataset_preview"
                                :key="i"
                            >
                                <td
                                    v-for="key in Object.keys(
                                        result.dataset_preview[0],
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
        </div>

        <div v-else class="result-body">
            <div v-if="result.error_message" class="error-message">
                {{ result.error_message }}
            </div>
            <details v-if="result.error_trace" class="trace-details">
                <summary class="trace-toggle">Show stack trace</summary>
                <pre class="trace-content">{{ result.error_trace }}</pre>
            </details>
        </div>
    </div>
</template>

<script setup lang="ts">
import type { JobResult } from "@/types";

defineProps<{ result: JobResult }>();

function formatTime(iso: string): string {
    return new Date(iso).toLocaleString();
}

function truncate(val: string, max = 60): string {
    return val.length > max ? val.substring(0, max) + "…" : val;
}
</script>

<style scoped>
.result-panel {
    background: #1a1d27;
    border-radius: 8px;
    padding: 16px 20px;
    margin-top: 12px;
    border: 1px solid #2a2d37;
}

.result-panel.success {
    border-left: 4px solid #10b981;
}

.result-panel.failure {
    border-left: 4px solid #ef4444;
}

.result-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
}

.result-icon {
    font-size: 16px;
    font-weight: 700;
}

.success .result-icon {
    color: #10b981;
}
.failure .result-icon {
    color: #ef4444;
}

.result-title {
    font-weight: 600;
    font-size: 14px;
}

.result-time {
    margin-left: auto;
    font-size: 12px;
    color: #9ca3af;
}

.meta-row {
    display: flex;
    gap: 8px;
    margin-bottom: 8px;
}

.meta-label {
    font-size: 13px;
    color: #9ca3af;
}

.meta-value {
    font-size: 13px;
    font-weight: 600;
}

.preview-section {
    margin-top: 12px;
}

.preview-table-wrapper {
    overflow-x: auto;
    margin-top: 8px;
}

.preview-table {
    width: 100%;
    border-collapse: collapse;
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

.error-message {
    background: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 13px;
    color: #fca5a5;
    margin-bottom: 8px;
}

.trace-details {
    margin-top: 8px;
}

.trace-toggle {
    cursor: pointer;
    font-size: 13px;
    color: #9ca3af;
    user-select: none;
}

.trace-toggle:hover {
    color: #e4e4e7;
}

.trace-content {
    margin-top: 8px;
    background: #0f1117;
    border-radius: 6px;
    padding: 12px;
    font-size: 11px;
    font-family: "SF Mono", "Fira Code", monospace;
    overflow-x: auto;
    color: #fca5a5;
    max-height: 300px;
    overflow-y: auto;
}
</style>
