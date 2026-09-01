import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import JobsView from "@/views/JobsView.vue";

const mockFetchJobs = vi.fn().mockResolvedValue([]);
const mockCreateJob = vi.fn();
const mockFetchInstances = vi.fn().mockResolvedValue([]);

// Mock useApi so we don't need real HTTP
vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchJobs: mockFetchJobs,
    createJob: mockCreateJob,
    fetchInstances: mockFetchInstances,
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
      {
        path: "/resources/:resourceId",
        name: "resource-detail",
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
        ResourceLabelBadge: true,
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
    // The resource_id filter input is now used for tags, so we find .filter-input for resource_id
    const resourceIdInput = wrapper.find('.filter-input[placeholder*="resource ID"]')
      .element as HTMLInputElement;

    expect(statusSelect.value).toBe("failed");
    expect(resourceIdInput.value).toBe("abc-123");
  });

  it("defaults to empty filters when no query params", async () => {
    const { wrapper } = await mountWithRouter();

    const statusSelect = wrapper.find(".filter-select")
      .element as HTMLSelectElement;
    const resourceIdInput = wrapper.find('.filter-input[placeholder*="resource ID"]')
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

    await wrapper.find('.filter-input[placeholder*="resource ID"]').setValue("my-resource");
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
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // ResourceLabelBadge is stubbed, so we check for the stub component
    const badge = wrapper.findComponent({ name: "ResourceLabelBadge" });
    expect(badge.exists()).toBe(true);
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
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const badge = wrapper.findComponent({ name: "ResourceLabelBadge" });
    expect(badge.exists()).toBe(true);
  });

  it("shows instance_name / dataset_name / resource_name in Resource column", async () => {
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
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("PBH");
    expect(wrapper.text()).toContain("my-dataset");
    expect(wrapper.text()).toContain("My Resource");
  });
});

describe("JobsView — resource column shows resource_id link", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("renders resource_id as a link to resource-detail", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "job-1",
        resource_id: "abc-12345-xyz",
        resource_name: "Test Resource",
        resource_url: null,
        resource_format: "CSV",
        dataset_name: "my-dataset",
        status: "completed",
        idempotency_key: "abc-12345-xyz",
        created_at: "2025-01-01T00:00:00Z",
        updated_at: "2025-01-01T00:00:00Z",
        started_at: null,
        completed_at: null,
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-12345-xyz",
        labels: [],
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // The resource_id link should exist and point to resource-detail
    const resourceLink = wrapper.find(".resource-link");
    expect(resourceLink.exists()).toBe(true);
    expect(resourceLink.text()).toContain("abc-1234");
  });
});

describe("JobsView — Job ID column", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("renders Job ID column header instead of Resource ID", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "job-uuid-12345",
        resource_id: "abc-123",
        resource_name: "Test Resource",
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
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const ths = wrapper.findAll("th");
    const jobIdHeader = ths.find((th) => th.text() === "Job ID");
    const resourceIdHeader = ths.find((th) => th.text() === "Resource ID");

    expect(jobIdHeader).toBeTruthy();
    expect(resourceIdHeader).toBeFalsy();
  });

  it("renders job ID with link to job-detail", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "job-uuid-12345",
        resource_id: "abc-123",
        resource_name: "Test Resource",
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
        instance_name: "PBH",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // Check that job ID is truncated and rendered
    const jobIdLink = wrapper.find(".job-id-link");
    expect(jobIdLink.exists()).toBe(true);
    expect(jobIdLink.text()).toContain("job-uuid");
  });
});

describe("JobsView — sortable column headers", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchJobs.mockResolvedValue([]);
  });

  it("toggles sort order when clicking Duration header", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const durationHeader = wrapper.find(".sortable-header-duration");
    expect(durationHeader.exists()).toBe(true);

    // Click once to set asc
    await durationHeader.trigger("click");
    await flushPromises();
    expect(mockFetchJobs).toHaveBeenCalledWith(
      expect.objectContaining({ order_by: "duration", order_dir: "asc" }),
    );

    // Click again to toggle to desc
    await durationHeader.trigger("click");
    await flushPromises();
    expect(mockFetchJobs).toHaveBeenCalledWith(
      expect.objectContaining({ order_by: "duration", order_dir: "desc" }),
    );

    // Click again to remove sort
    await durationHeader.trigger("click");
    await flushPromises();
    // After third click, order params should NOT be in the call
    const lastCall =
      mockFetchJobs.mock.calls[mockFetchJobs.mock.calls.length - 1]![0]!;
    expect(lastCall.order_by).toBeUndefined();
  });

  it("toggles sort order when clicking Created header", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const createdHeader = wrapper.find(".sortable-header-created");
    expect(createdHeader.exists()).toBe(true);

    // Click once to set asc
    await createdHeader.trigger("click");
    await flushPromises();
    expect(mockFetchJobs).toHaveBeenCalledWith(
      expect.objectContaining({ order_by: "created_at", order_dir: "asc" }),
    );

    // Click again to toggle to desc
    await createdHeader.trigger("click");
    await flushPromises();
    expect(mockFetchJobs).toHaveBeenCalledWith(
      expect.objectContaining({ order_by: "created_at", order_dir: "desc" }),
    );
  });

  it("shows sort direction indicator on active header", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // Initially no indicators
    expect(wrapper.find(".sort-indicator.asc").exists()).toBe(false);
    expect(wrapper.find(".sort-indicator.desc").exists()).toBe(false);

    // Click Duration header to set asc
    await wrapper.find(".sortable-header-duration").trigger("click");
    await flushPromises();

    // Duration header should show asc indicator
    expect(wrapper.find(".sortable-header-duration .sort-indicator.asc").exists()).toBe(true);
  });
});

describe("JobsView — Broker column", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("renders Broker column header", async () => {
    mockFetchJobs.mockResolvedValue([]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const ths = wrapper.findAll("th");
    const brokerHeader = ths.find((th) => th.text() === "Broker");
    expect(brokerHeader).toBeTruthy();
  });

  it("renders generic Iggy metadata", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "job-1",
        resource_id: "abc-123",
        resource_name: "Test Resource",
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
        instance_name: "PBH",
        broker_type: "iggy",
        message_stream: "ckan-ingestor",
        message_topic: "jobs",
        message_partition: 2,
        message_offset: null,
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("iggy");
    expect(wrapper.text()).toContain("ckan-ingestor/jobs");
    expect(wrapper.text()).toContain("2");
  });

  it("shows — when job has no message topic", async () => {
    mockFetchJobs.mockResolvedValue([
      {
        id: "job-1",
        resource_id: "abc-123",
        resource_name: "Test Resource",
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
        instance_name: "PBH",
        broker_type: null,
        message_stream: null,
        message_topic: null,
        message_partition: null,
        message_offset: null,
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("\u2014");
  });
});

describe("JobsView — tags filter", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchJobs.mockResolvedValue([]);
  });

  it("renders tags filter input", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const tagsInput = wrapper.find('.filter-input[placeholder*="tags"]');
    expect(tagsInput.exists()).toBe(true);
  });

  it("syncs tags filter to URL query params", async () => {
    const { wrapper, router } = await mountWithRouter();
    await flushPromises();

    await wrapper.find('.filter-input[placeholder*="tags"]').setValue("empty,stale");
    // Need to wait for debounce
    await new Promise((r) => setTimeout(r, 350));
    await flushPromises();

    expect(router.currentRoute.value.query.tags).toBe("empty,stale");
  });

  it("initializes tags filter from URL query params", async () => {
    const { wrapper } = await mountWithRouter({ tags: "empty" });
    await flushPromises();

    const tagsInput = wrapper.find('.filter-input[placeholder*="tags"]')
      .element as HTMLInputElement;
    expect(tagsInput.value).toBe("empty");
  });
});
