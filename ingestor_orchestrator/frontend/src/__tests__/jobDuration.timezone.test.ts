import { describe, it, expect } from "vitest";
import { jobDuration } from "@/utils/jobDuration";
import type { Job } from "@/types";

function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: "1",
    instance_id: null,
    resource_id: "abc",
    resource_name: null,
    resource_url: null,
    resource_format: null,
    dataset_name: "test-dataset",
    ckan_resource_url:
      "https://dados.pbh.gov.br/dataset/test-dataset/resource/abc",
    status: "processing",
    idempotency_key: "abc",
    created_at: "2025-01-01T00:00:00",
    updated_at: "2025-01-01T00:00:00",
    started_at: null,
    completed_at: null,
    ...overrides,
  };
}

describe("jobDuration — timezone bug", () => {
  it("computes positive duration when API returns datetime without timezone", () => {
    // Simulates: server is UTC, job started 5 minutes ago
    // API returns "2025-06-08T19:50:00" (no Z, no +00:00)
    // Browser's "now" is 2025-06-08T19:55:00 UTC
    const now = new Date("2025-06-08T19:55:00Z");

    // This is what the API actually returns — no timezone suffix
    const job = makeJob({
      started_at: "2025-06-08T19:50:00",
      completed_at: null,
    });

    const result = jobDuration(job, now);
    expect(result).not.toBeNull();
    expect(result!.ms).toBeGreaterThan(0);
  });

  it("computes correct duration for completed job without timezone", () => {
    const job = makeJob({
      status: "completed",
      started_at: "2025-06-08T19:50:00",
      completed_at: "2025-06-08T19:55:30",
    });

    const result = jobDuration(job);
    expect(result).not.toBeNull();
    expect(result!.ms).toBe(330_000);
    expect(result!.finished).toBe(true);
  });

  it("computes positive duration for failed job without timezone", () => {
    const job = makeJob({
      status: "failed",
      started_at: "2025-06-08T19:50:00",
      completed_at: "2025-06-08T19:50:12",
    });

    const result = jobDuration(job);
    expect(result).not.toBeNull();
    expect(result!.ms).toBe(12_000);
  });
});
