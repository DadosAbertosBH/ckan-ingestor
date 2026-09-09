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
    statusText:
      response.statusText ?? (response.ok ? "OK" : "Internal Server Error"),
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
        instance_id: null,
        resource_id: "abc",
        resource_name: "test",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: "test-dataset",
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/test-dataset/resource/abc",
        status: "completed",
        idempotency_key: "abc",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
        labels: ["empty"],
      },
    ];
    mockFetch({ ok: true, body: jobs });

    const { fetchJobs } = useApi();
    const result = await fetchJobs({ limit: 10 });
    expect(result).toEqual(jobs);
  });

  it("fetchJob returns a single job with labels", async () => {
    const job = {
      id: "1",
      instance_id: null,
      resource_id: "abc",
      resource_name: "test",
      resource_url: null,
      resource_format: "CSV",
      dataset_name: "test-dataset",
      ckan_resource_url:
        "https://dados.pbh.gov.br/dataset/test-dataset/resource/abc",
      status: "completed",
      idempotency_key: "abc",
      created_at: "2025-01-01T00:00:00Z",
      updated_at: "2025-01-01T00:00:00Z",
      started_at: null,
      completed_at: null,
      labels: ["empty"],
      results: [],
    };
    mockFetch({ ok: true, body: job });

    const { fetchJob } = useApi();
    const result = await fetchJob("1");
    expect(result).toEqual(job);
  });

  it("fetchInstanceStats returns per-instance stats", async () => {
    const instanceStats = [
      {
        instance: {
          id: "inst-1",
          name: "CKAN Prod",
          url: "https://dados.pbh.gov.br",
          last_metadata_synced: "2025-01-01T00:00:00Z",
          dataset_count: 50,
          resource_count: 200,
          created_at: "2025-01-01T00:00:00Z",
          updated_at: "2025-01-01T00:00:00Z",
        },
        pending: 2,
        processing: 1,
        completed: 6,
        failed: 1,
      },
    ];
    mockFetch({ ok: true, body: instanceStats });

    const { fetchInstanceStats } = useApi();
    const result = await fetchInstanceStats();
    expect(result).toEqual(instanceStats);
  });

  it("syncMetadata calls POST with instance_id", async () => {
    const result = {
      instance_id: "inst-1",
      dataset_count: 5,
      resource_count: 20,
    };
    const fn = mockFetch({ ok: true, body: result });

    const { syncMetadata } = useApi();
    const data = await syncMetadata("inst-1");
    expect(data).toEqual(result);
    expect(fn).toHaveBeenCalledWith("/api/metadata/sync/inst-1", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });
  });

  it("syncMetadata calls POST without instance_id to sync all", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { syncMetadata } = useApi();
    await syncMetadata();
    expect(fn).toHaveBeenCalledWith("/api/metadata/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
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
      vi.fn().mockRejectedValue(new TypeError("Failed to fetch")),
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

    const { fetchInstanceStats } = useApi();
    await expect(fetchInstanceStats()).rejects.toThrow(
      "Database connection failed",
    );
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
      "Can only delete pending or failed jobs",
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
    await fetchJobs({
      status: "failed",
      resource_id: "abc",
      limit: 10,
      offset: 20,
    });

    const calledUrl = fn.mock.calls[0]![0] as string;
    expect(calledUrl).toContain("status=failed");
    expect(calledUrl).toContain("resource_id=abc");
    expect(calledUrl).toContain("limit=10");
    expect(calledUrl).toContain("offset=20");
  });

  it("fetchJobs omits empty params", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { fetchJobs } = useApi();
    await fetchJobs({});

    const calledUrl = fn.mock.calls[0]![0] as string;
    expect(calledUrl).toBe("/api/jobs/");
  });
});
