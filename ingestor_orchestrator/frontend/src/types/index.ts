export type JobStatus = "pending" | "processing" | "completed" | "failed";

export interface Job {
  id: string;
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

export interface DashboardStats {
  total_jobs: number;
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  last_24h: number;
}

export interface JobCreateRequest {
  resource_id: string;
  resource_name?: string;
  resource_url?: string;
  resource_format?: string;
}
