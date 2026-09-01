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
    status: "processing",
    idempotency_key: "abc",
    created_at: "2025-01-01T00:00:00+00:00",
    updated_at: "2025-01-01T00:00:00+00:00",
    started_at: null,
    completed_at: null,
    ...overrides,
  };
}

describe("jobDuration — timezone handling", () => {
  it("computes positive duration when API returns datetime with timezone", () => {
    // API now returns ISO 8601 with +00:00 (e.g. "2025-06-08T19:50:00+00:00")
    const now = new Date("2025-06-08T19:55:00Z");

    const job = makeJob({
      started_at: "2025-06-08T19:50:00+00:00",
      completed_at: null,
    });

    const result = jobDuration(job, now);
    expect(result).not.toBeNull();
    expect(result!.ms).toBeGreaterThan(0);
  });

  it("computes correct duration for completed job with timezone", () => {
    const job = makeJob({
      status: "completed",
      started_at: "2025-06-08T19:50:00+00:00",
      completed_at: "2025-06-08T19:55:30+00:00",
    });

    const result = jobDuration(job);
    expect(result).not.toBeNull();
    expect(result!.ms).toBe(330_000);
    expect(result!.finished).toBe(true);
  });

  it("computes positive duration for failed job with timezone", () => {
    const job = makeJob({
      status: "failed",
      started_at: "2025-06-08T19:50:00+00:00",
      completed_at: "2025-06-08T19:50:12+00:00",
    });

    const result = jobDuration(job);
    expect(result).not.toBeNull();
    expect(result!.ms).toBe(12_000);
  });
});
