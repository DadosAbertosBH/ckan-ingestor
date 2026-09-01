import { describe, it, expect } from "vitest";
import { jobDuration } from "@/utils/jobDuration";
import type { Job } from "@/types";

function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: "1",
    instance_id: null,
    instance_name: null,
    resource_id: "abc",
    resource_name: null,
    resource_url: null,
    resource_format: null,
    dataset_name: "test-dataset",
    ckan_resource_url:
      "https://dados.pbh.gov.br/dataset/test-dataset/resource/abc",
    status: "completed",
    idempotency_key: "abc",
    created_at: "2025-01-01T00:00:00Z",
    updated_at: "2025-01-01T00:00:00Z",
    started_at: null,
    completed_at: null,
    ...overrides,
  };
}

describe("jobDuration", () => {
  it("returns null for pending jobs (not started)", () => {
    const job = makeJob({ status: "pending", started_at: null });
    expect(jobDuration(job)).toBeNull();
  });

  it("returns null when started_at is null regardless of status", () => {
    const job = makeJob({ status: "processing", started_at: null });
    expect(jobDuration(job)).toBeNull();
  });

  it("returns elapsed duration for completed jobs", () => {
    const job = makeJob({
      status: "completed",
      started_at: "2025-01-01T10:00:00Z",
      completed_at: "2025-01-01T10:05:30Z",
    });
    expect(jobDuration(job)).toEqual({ ms: 330_000, finished: true });
  });

  it("returns elapsed duration for failed jobs", () => {
    const job = makeJob({
      status: "failed",
      started_at: "2025-01-01T10:00:00Z",
      completed_at: "2025-01-01T10:00:12Z",
    });
    expect(jobDuration(job)).toEqual({ ms: 12_000, finished: true });
  });

  it("returns time since started_at for processing jobs", () => {
    const startedAt = new Date(Date.now() - 60_000).toISOString();
    const job = makeJob({
      status: "processing",
      started_at: startedAt,
      completed_at: null,
    });

    const result = jobDuration(job);
    expect(result).not.toBeNull();
    expect(result!.finished).toBe(false);
    // Allow 2s tolerance since "now" is captured inside the function
    expect(result!.ms).toBeGreaterThanOrEqual(58_000);
    expect(result!.ms).toBeLessThanOrEqual(65_000);
  });

  it("handles sub-second durations", () => {
    const job = makeJob({
      status: "completed",
      started_at: "2025-01-01T10:00:00.000Z",
      completed_at: "2025-01-01T10:00:00.500Z",
    });
    expect(jobDuration(job)).toEqual({ ms: 500, finished: true });
  });

  it("handles multi-hour durations", () => {
    const job = makeJob({
      status: "completed",
      started_at: "2025-01-01T00:00:00Z",
      completed_at: "2025-01-01T03:25:45Z",
    });
    // 3h 25m 45s = 12345s
    expect(jobDuration(job)).toEqual({ ms: 12_345_000, finished: true });
  });
});
