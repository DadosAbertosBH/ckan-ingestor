<template>
    <div class="sync-detail">
        <div v-if="loading" class="loading">Loading...</div>
        <div v-else-if="error" class="error-banner">Failed to load sync: {{ error }}</div>
        <template v-else-if="sync">
            <router-link to="/syncs" class="back-link">← Back to Syncs</router-link>
            <h1>Sync: {{ sync.instance_name || sync.instance_id }}</h1>
            <div class="details">
                <div><span>Status</span><strong :class="['status', sync.status || 'pending']">{{ statusLabel(sync.status) }}</strong></div>
                <div><span>Started</span>{{ formatTime(sync.start_time) }}</div>
                <div><span>Finished</span>{{ sync.end_time ? formatTime(sync.end_time) : "—" }}</div>
                <div><span>Total packages</span>{{ sync.total_packages }}</div>
            </div>
            <section v-if="sync.error_message" class="failure">
                <h2>Error</h2>
                <pre>{{ sync.error_message }}</pre>
            </section>
        </template>
        <div v-else class="error-banner">Sync not found</div>
    </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useApi } from "@/composables/useApi";
import type { MetadataSync } from "@/types";

const props = defineProps<{ id: string }>();
const { fetchSync } = useApi();
const sync = ref<MetadataSync | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);

function formatTime(value: string) { return new Date(value).toLocaleString(); }
function statusLabel(status: MetadataSync["status"]) { return status === "failure" ? "Failed" : status === "success" ? "Success" : "Pending"; }

onMounted(async () => {
    try { sync.value = await fetchSync(props.id); }
    catch (e: any) { error.value = e.message || "Unknown error"; }
    finally { loading.value = false; }
});
</script>

<style scoped>
.loading { padding: 48px; text-align: center; color: #9ca3af; }
.back-link { color: #9ca3af; text-decoration: none; }
h1 { margin: 20px 0; font-size: 24px; }
.details { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; background: #1a1d27; padding: 18px; border-radius: 8px; }
.details div { display: grid; gap: 5px; }.details span { color: #9ca3af; font-size: 12px; text-transform: uppercase; }
.failure { margin-top: 20px; background: #451a1a; border: 1px solid #ef4444; border-radius: 8px; padding: 16px; }.failure h2 { margin-top: 0; color: #fca5a5; }.failure pre { white-space: pre-wrap; word-break: break-word; margin: 0; color: #fecaca; }
.status { border-radius: 999px; padding: 3px 8px; width: fit-content; font-size: 12px; }.status.success { background: #14532d; color: #86efac; }.status.failure { background: #7f1d1d; color: #fca5a5; }.status.pending { background: #3f3f46; color: #d4d4d8; }.error-banner { padding: 16px; color: #fca5a5; background: #451a1a; border-radius: 8px; }
</style>
