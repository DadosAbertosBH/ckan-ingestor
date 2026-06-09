export type JobStatus = "pending" | "processing" | "completed" | "failed";

export interface CkanInstance {
  id: string;
  name: string;
  url: string;
  last_metadata_synced: string | null;
  dataset_count: number;
  resource_count: number;
  created_at: string;
  updated_at: string;
}

export interface InstanceStats {
  instance: CkanInstance;
  pending: number;
  processing: number;
  completed: number;
  failed: number;
}

export interface Job {
  id: string;
  instance_id: string | null;
  resource_id: string;
  resource_name: string | null;
  resource_url: string | null;
  resource_format: string | null;
  dataset_name: string | null;
  status: JobStatus;
  idempotency_key: string;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  completed_at: string | null;
  ckan_resource_url: string;
  labels?: string[];
  results?: JobResult[];
}

export interface JobResult {
  id: string;
  job_id: string;
  success: boolean;
  error_message: string | null;
  error_trace: string | null;
  dataset_preview: Record<string, unknown>[] | null;
  rows_processed: number | null;
  created_at: string;
}

export interface JobCreateRequest {
  resource_id: string;
  dataset_name: string;
  resource_name?: string;
  resource_url?: string;
  resource_format?: string;
}
