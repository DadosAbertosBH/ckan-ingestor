import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import JobsView from "@/views/JobsView.vue";

const mockFetchJobs = vi.fn().mockResolvedValue([]);
const mockCreateJob = vi.fn();

// Mock useApi so we don't need real HTTP
vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchJobs: mockFetchJobs,
    createJob: mockCreateJob,
  }),
}));

function createTestRouter(_initialQuery: Record<string, string> = {}) {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/jobs", name: "jobs", component: { template: "<div/>" } },
      {
        path: "/jobs/:id",
        name: "job-detail",
        component: { template: "<div/>" },
      },
    ],
  });
}

async function mountWithRouter(query: Record<string, string> = {}) {
  const router = createTestRouter();
  router.push({ path: "/jobs", query });
  await router.isReady();

  const wrapper = mount(JobsView, {
    global: {
      plugins: [router],
      stubs: {
        JobStatusBadge: true,
      },
    },
  });

  return { wrapper, router };
}

describe("JobsView — filter URL persistence", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchJobs.mockResolvedValue([]);
  });

  it("initializes filters from URL query params", async () => {
    const { wrapper } = await mountWithRouter({
      status: "failed",
      resource_id: "abc-123",
    });

    const statusSelect = wrapper.find(".filter-select")
      .element as HTMLSelectElement;
    const resourceIdInput = wrapper.find(".filter-input")
      .element as HTMLInputElement;

    expect(statusSelect.value).toBe("failed");
    expect(resourceIdInput.value).toBe("abc-123");
  });

  it("defaults to empty filters when no query params", async () => {
    const { wrapper } = await mountWithRouter();

    const statusSelect = wrapper.find(".filter-select")
      .element as HTMLSelectElement;
    const resourceIdInput = wrapper.find(".filter-input")
      .element as HTMLInputElement;

    expect(statusSelect.value).toBe("");
    expect(resourceIdInput.value).toBe("");
  });

  it("syncs status filter to URL when changed", async () => {
    const { wrapper, router } = await mountWithRouter();

    await wrapper.find(".filter-select").setValue("completed");
    await flushPromises();

    expect(router.currentRoute.value.query.status).toBe("completed");
  });

  it("syncs resource_id filter to URL when changed", async () => {
    const { wrapper, router } = await mountWithRouter();

    await wrapper.find(".filter-input").setValue("my-resource");
    await flushPromises();

    expect(router.currentRoute.value.query.resource_id).toBe("my-resource");
  });

  it("removes query param when filter is cleared", async () => {
    const { wrapper, router } = await mountWithRouter({ status: "failed" });

    await wrapper.find(".filter-select").setValue("");
    await flushPromises();

    expect(router.currentRoute.value.query.status).toBeUndefined();
  });
});

describe("JobsView — labels column", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchJobs.mockResolvedValue([]);
  });

  it("renders Labels column header", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const ths = wrapper.findAll("th");
    const labelsHeader = ths.find((th) => th.text() === "Labels");
    expect(labelsHeader).toBeTruthy();
  });

  it("renders ResourceLabelBadge when jobs have labels", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "1",
        resource_id: "abc-123",
        resource_name: "Test Resource",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: "test-ds",
        status: "completed",
        idempotency_key: "abc-123",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/test-ds/resource/abc-123",
        labels: ["empty"],
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const badge = wrapper.find(".label-badge");
    expect(badge.exists()).toBe(true);
    expect(badge.text()).toBe("empty");
  });

  it("renders multiple labels in a row", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "1",
        resource_id: "abc-123",
        resource_name: "Test Resource",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: "test-ds",
        status: "completed",
        idempotency_key: "abc-123",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/test-ds/resource/abc-123",
        labels: ["empty", "stale"],
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const badges = wrapper.findAll(".label-badge");
    expect(badges).toHaveLength(2);
  });

  it("shows dataset_name / resource_name in Resource column", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "1",
        resource_id: "abc-123",
        resource_name: "My Resource",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: "my-dataset",
        status: "completed",
        idempotency_key: "abc-123",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
        labels: [],
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("my-dataset");
    expect(wrapper.text()).toContain("My Resource");
  });
});
