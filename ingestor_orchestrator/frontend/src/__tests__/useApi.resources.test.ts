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

describe("useApi — fetchResources", () => {
  it("fetchResources returns resources on success", async () => {
    const resources = [
      {
        resource_id: "abc-123",
        resource_name: "Population Data",
        resource_url: "https://example.com/data.csv",
        resource_format: "CSV",
        dataset_name: "my-dataset",
        status: "completed",
        instance_id: "inst-1",
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
        labels: ["empty"],
        job_count: 2,
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-06-01T00:00:00Z",
      },
    ];
    const fn = mockFetch({ ok: true, body: resources });

    const { fetchResources } = useApi();
    const result = await fetchResources();
    expect(result).toEqual(resources);
    expect(fn).toHaveBeenCalledWith("/api/resources/", {
      headers: { "Content-Type": "application/json" },
    });
  });

  it("fetchResources passes query params correctly", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { fetchResources } = useApi();
    await fetchResources({
      status: "failed",
      instance_id: "inst-1",
      search: "population",
      limit: 20,
      offset: 10,
    });

    const calledUrl = fn.mock.calls[0]![0] as string;
    expect(calledUrl).toContain("status=failed");
    expect(calledUrl).toContain("instance_id=inst-1");
    expect(calledUrl).toContain("search=population");
    expect(calledUrl).toContain("limit=20");
    expect(calledUrl).toContain("offset=10");
  });

  it("fetchResources omits empty params", async () => {
    const fn = mockFetch({ ok: true, body: [] });

    const { fetchResources } = useApi();
    await fetchResources({});

    const calledUrl = fn.mock.calls[0]![0] as string;
    // URL should not contain any query params
    expect(calledUrl).toBe("/api/resources/");
  });
});

describe("useApi — fetchResource", () => {
  it("fetchResource returns a single resource detail", async () => {
    const resource = {
      resource_id: "abc-123",
      resource_name: "Population Data",
      resource_url: "https://example.com/data.csv",
      resource_format: "CSV",
      dataset_name: "my-dataset",
      status: "completed",
      instance_id: "inst-1",
      ckan_resource_url:
        "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
      labels: [],
      job_count: 3,
      created_at: "2025-01-01T00:00:00Z",
      updated_at: "2025-06-01T00:00:00Z",
      latest_job: {
        id: "job-3",
        resource_id: "abc-123",
        resource_name: "Population Data",
        resource_url: "https://example.com/data.csv",
        resource_format: "CSV",
        dataset_name: "my-dataset",
        status: "completed",
        idempotency_key: "abc-123",
        instance_id: "inst-1",
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
        created_at: "2025-06-01T00:00:00Z",
        updated_at: "2025-06-01T00:01:00Z",
        started_at: "2025-06-01T00:00:00Z",
        completed_at: "2025-06-01T00:01:00Z",
        labels: [],
        results: [],
      },
      jobs: [],
    };
    mockFetch({ ok: true, body: resource });

    const { fetchResource } = useApi();
    const result = await fetchResource("abc-123");
    expect(result).toEqual(resource);
  });

  it("fetchResource throws on 404", async () => {
    mockFetch({
      ok: false,
      status: 404,
      statusText: "Not Found",
      body: { detail: "Resource not found" },
    });

    const { fetchResource } = useApi();
    await expect(fetchResource("nonexistent")).rejects.toThrow(
      "Resource not found",
    );
  });
});
