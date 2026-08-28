export type JobStatus = "pending" | "processing" | "completed" | "failed";
export type ResourceStatus = JobStatus | "outdated";

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
  outdated?: number;
}

export interface Job {
  id: string;
  instance_id: string | null;
  instance_name: string | null;
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
  kafka_topic?: string | null;
  kafka_partition?: number | null;
  kafka_offset?: number | null;
}

export interface JobResult {
  id: string;
  job_id: string;
  success: boolean;
  error_message: string | null;
  error_trace: string | null;
  dataset_preview: Record<string, unknown>[] | null;
  rows_processed: number | null;
  expected_rows: number | null;
  resource_size: number | null;
  encoding: string | null;
  created_at: string;
}

export interface JobCreateRequest {
  resource_id: string;
  dataset_name: string;
  resource_name?: string;
  resource_url?: string;
  resource_format?: string;
}

export interface Resource {
  resource_id: string;
  resource_name: string | null;
  resource_url: string | null;
  resource_format: string | null;
  dataset_name: string;
  status: ResourceStatus;
  instance_id: string;
  ckan_resource_url: string;
  labels: string[];
  job_count: number;
  created_at: string;
  updated_at: string;
}

export interface ResourceDetail extends Resource {
  latest_job: Job | null;
  jobs: Job[];
  preview: Record<string, unknown>[];
}

export interface Dataset {
  instance_id: string;
  instance_name: string | null;
  dataset_name: string;
  ckan_dataset_url: string;
  total_resources: number;
  pending_resources: number;
  processing_resources: number;
  completed_resources: number;
  failed_resources: number;
  outdated_resources?: number;
  updated_at: string | null;
  instance_last_synced_at: string | null;
}

export interface MetadataSync {
  id: string;
  instance_id: string;
  instance_name: string | null;
  start_time: string;
  end_time: string | null;
  total_packages: number;
  new_datasets: number;
  new_resources: number;
  updated_datasets: number;
  updated_resources: number;
}
