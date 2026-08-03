import { ref } from "vue";
import type {
  CkanInstance,
  Dataset,
  InstanceStats,
  Job,
  JobCreateRequest,
  JobStatus,
  MetadataSync,
  Resource,
  ResourceDetail,
} from "@/types";

const API_BASE = "/api";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const error = await response
      .json()
      .catch(() => ({ detail: response.statusText }));
    throw new Error(error.detail || response.statusText);
  }
  if (response.status === 204) return null as T;
  return response.json();
}

export function useApi() {
  const loading = ref(false);
  const error = ref<string | null>(null);

  const fetchInstanceStats = () => request<InstanceStats[]>("/dashboard/stats");
  const fetchInstances = () => request<CkanInstance[]>("/instances/");
  const fetchJobs = (params?: {
    status?: JobStatus;
    resource_id?: string;
    instance_id?: string;
    limit?: number;
    offset?: number;
  }) => {
    const query = new URLSearchParams();
    if (params?.status) query.set("status", params.status);
    if (params?.resource_id) query.set("resource_id", params.resource_id);
    if (params?.instance_id) query.set("instance_id", params.instance_id);
    if (params?.limit) query.set("limit", String(params.limit));
    if (params?.offset) query.set("offset", String(params.offset));
    const qs = query.toString();
    return request<Job[]>(`/jobs/${qs ? "?" + qs : ""}`);
  };
  const fetchJob = (id: string) => request<Job>(`/jobs/${id}`);
  const createJob = (data: JobCreateRequest) =>
    request<Job>("/jobs/", { method: "POST", body: JSON.stringify(data) });
  const retryJob = (id: string) =>
    request<Job>(`/jobs/${id}/retry`, { method: "POST" });
  const deleteJob = (id: string) =>
    request<void>(`/jobs/${id}`, { method: "DELETE" });
  const syncMetadata = (instanceId?: string) => {
    const path = instanceId ? `/metadata/sync/${instanceId}` : "/metadata/sync";
    return request<Record<string, unknown>>(path, { method: "POST" });
  };
  const fetchResources = (params?: {
    status?: JobStatus;
    instance_id?: string;
    search?: string;
    limit?: number;
    offset?: number;
  }) => {
    const query = new URLSearchParams();
    if (params?.status) query.set("status", params.status);
    if (params?.instance_id) query.set("instance_id", params.instance_id);
    if (params?.search) query.set("search", params.search);
    if (params?.limit) query.set("limit", String(params.limit));
    if (params?.offset) query.set("offset", String(params.offset));
    const qs = query.toString();
    return request<Resource[]>(`/resources/${qs ? "?" + qs : ""}`);
  };
  const fetchResource = (resourceId: string) =>
    request<ResourceDetail>(`/resources/${resourceId}`);
  const fetchSyncs = (params?: {
    instance_id?: string;
    limit?: number;
    offset?: number;
  }) => {
    const query = new URLSearchParams();
    if (params?.instance_id) query.set("instance_id", params.instance_id);
    if (params?.limit) query.set("limit", String(params.limit));
    if (params?.offset) query.set("offset", String(params.offset));
    const qs = query.toString();
    return request<MetadataSync[]>(`/syncs/${qs ? "?" + qs : ""}`);
  };
  const fetchDatasets = (params?: {
    instance_id?: string;
    search?: string;
    limit?: number;
    offset?: number;
  }) => {
    const query = new URLSearchParams();
    if (params?.instance_id) query.set("instance_id", params.instance_id);
    if (params?.search) query.set("search", params.search);
    if (params?.limit) query.set("limit", String(params.limit));
    if (params?.offset) query.set("offset", String(params.offset));
    const qs = query.toString();
    return request<Dataset[]>(`/datasets/${qs ? "?" + qs : ""}`);
  };

  return {
    loading,
    error,
    fetchInstanceStats,
    fetchInstances,
    fetchJobs,
    fetchJob,
    createJob,
    retryJob,
    deleteJob,
    syncMetadata,
    fetchResources,
    fetchResource,
    fetchSyncs,
    fetchDatasets,
  };
}
