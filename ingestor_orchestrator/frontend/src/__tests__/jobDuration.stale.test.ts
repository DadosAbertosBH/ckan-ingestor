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

describe("jobDuration — stale completed_at bug", () => {
  it("ignores completed_at from a previous run when status is processing", () => {
    // Old completed_at from a prior run is still set
    const job = makeJob({
      status: "processing",
      started_at: "2026-06-08T21:19:06",
      completed_at: "2026-06-08T20:58:13", // from previous run
    });
    const now = new Date("2026-06-08T21:19:36Z");

    const result = jobDuration(job, now);
    expect(result).not.toBeNull();
    expect(result!.ms).toBeGreaterThan(0); // should use now, not completed_at
    expect(result!.finished).toBe(false); // still processing, not finished
  });
});
