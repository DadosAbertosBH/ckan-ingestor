import { describe, it, expect, vi, beforeEach } from "vitest";
import { useApi } from "../composables/useApi";

function mockFetch(response: {
  ok: boolean;
  status?: number;
  statusText?: string;
  body?: any;
}) {
  const fn = vi.fn().mockResolvedValue({
    ok: response.ok,
    status: response.status ?? (response.ok ? 200 : 500),
    statusText: response.statusText ?? (response.ok ? "OK" : "Internal Server Error"),
    json: () => Promise.resolve(response.body),
  });
  vi.stubGlobal("fetch", fn);
  return fn;
}

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("useApi — success cases", () => {
  it("fetchJobs returns jobs on success", async () => {
    const jobs = [
      {
        id: "1",
        resource_id: "abc",
        resource_name: "test",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: null,
        status: "completed",
        idempotency_key: "abc",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
      },
    ];
    mockFetch({ ok: true, body: jobs });

    const { fetchJobs } = useApi();
    const result = await fetchJobs({ limit: 10 });
    expect(result).toEqual(jobs);
  });

  it("fetchJob returns a single job", async () => {
    const job = {
      id: "1",
      resource_id: "abc",
      resource_name: "test",
      resource_url: null,
      resource_format: "CSV",
      dataset_name: null,
      status: "completed",
      idempotency_key: "abc",
      created_at: "2025-01-01T00:00:00Z",
      updated_at: "2025-01-01T00:00:00Z",
      started_at: null,
      completed_at: null,
      results: [],
    };
    mockFetch({ ok: true, body: job });

    const { fetchJob } = useApi();
    const result = await fetchJob("1");
    expect(result).toEqual(job);
  });

  it("fetchStats returns dashboard stats", async () => {
    const stats = {
      total_jobs: 10,
      pending: 2,
      processing: 1,
      completed: 6,
      failed: 1,
      last_24h: 5,
    };
    mockFetch({ ok: true, body: stats });

    const { fetchStats } = useApi();
    const result = await fetchStats();
    expect(result).toEqual(stats);
  });

  it("createJob posts and returns the created job", async () => {
    const job = {
      id: "1",
      resource_id: "abc",
      resource_name: "test",
      resource_url: null,
      resource_format: "CSV",
      dataset_name: "my-dataset",
      status: "pending",
      idempotency_key: "abc",
      created_at: "2025-01-01T00:00:00Z",
      updated_at: "2025-01-01T00:00:00Z",
      started_at: null,
      completed_at: null,
      results: [],
    };
    const fetchFn = mockFetch({ ok: true, status: 201, body: job });

    const { createJob } = useApi();
    const result = await createJob({
      resource_id: "abc",
      resource_name: "test",
      dataset_name: "my-dataset",
    });

    expect(result).toEqual(job);
    expect(fetchFn).toHaveBeenCalledWith("/api/jobs/", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        resource_id: "abc",
        resource_name: "test",
        dataset_name: "my-dataset",
      }),
    });
  });
});

describe("useApi — error handling", () => {
  it("throws with detail message on API error (JSON body)", async () => {
    mockFetch({
      ok: false,
      status: 422,
      statusText: "Unprocessable Entity",
      body: { detail: "Resource ID is required" },
    });

    const { fetchJobs } = useApi();
    await expect(fetchJobs()).rejects.toThrow("Resource ID is required");
  });

  it("throws with statusText when response body is not JSON", async () => {
    const fn = vi.fn().mockResolvedValue({
      ok: false,
      status: 502,
      statusText: "Bad Gateway",
      json: () => Promise.reject(new Error("not JSON")),
    });
    vi.stubGlobal("fetch", fn);

    const { fetchJobs } = useApi();
    await expect(fetchJobs()).rejects.toThrow("Bad Gateway");
  });

  it("throws on network error (fetch rejects)", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockRejectedValue(new TypeError("Failed to fetch"))
    );

    const { fetchJobs } = useApi();
    await expect(fetchJobs()).rejects.toThrow("Failed to fetch");
  });

  it("throws on 404 for single job", async () => {
    mockFetch({
      ok: false,
      status: 404,
      statusText: "Not Found",
      body: { detail: "Job not found" },
    });

    const { fetchJob } = useApi();
    await expect(fetchJob("nonexistent")).rejects.toThrow("Job not found");
  });

  it("throws on 500 internal server error", async () => {
    mockFetch({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      body: { detail: "Database connection failed" },
    });

    const { fetchStats } = useApi();
    await expect(fetchStats()).rejects.toThrow("Database connection failed");
  });

  it("deleteJob returns null on 204", async () => {
    const fn = vi.fn().mockResolvedValue({
      ok: true,
      status: 204,
      statusText: "No Content",
    });
    vi.stubGlobal("fetch", fn);

    const { deleteJob } = useApi();
    const result = await deleteJob("1");
    expect(result).toBeNull();
  });

  it("deleteJob throws on 409 conflict", async () => {
    mockFetch({
      ok: false,
      status: 409,
      statusText: "Conflict",
      body: { detail: "Can only delete pending or failed jobs" },
    });

    const { deleteJob } = useApi();
    await expect(deleteJob("1")).rejects.toThrow(
      "Can only delete pending or failed jobs"
    );
  });

  it("retryJob throws on error", async () => {
    mockFetch({
      ok: false,
      status: 409,
      statusText: "Conflict",
      body: { detail: "Can only retry failed jobs" },
    });

    const { retryJob } = useApi();
    await expect(retryJob("1")).rejects.toThrow("Can only retry failed jobs");
  });
});

describe("useApi — query params", () => {
  it("fetchJobs builds query string correctly", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { fetchJobs } = useApi();
    await fetchJobs({ status: "failed", resource_id: "abc", limit: 10, offset: 20 });

    const calledUrl = fn.mock.calls[0][0] as string;
    expect(calledUrl).toContain("status=failed");
    expect(calledUrl).toContain("resource_id=abc");
    expect(calledUrl).toContain("limit=10");
    expect(calledUrl).toContain("offset=20");
  });

  it("fetchJobs omits empty params", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { fetchJobs } = useApi();
    await fetchJobs({});

    const calledUrl = fn.mock.calls[0][0] as string;
    expect(calledUrl).toBe("/api/jobs/");
  });
});
