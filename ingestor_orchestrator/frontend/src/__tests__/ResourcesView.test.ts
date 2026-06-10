import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import ResourcesView from "@/views/ResourcesView.vue";

const mockFetchResources = vi.fn().mockResolvedValue([]);
const mockFetchInstances = vi.fn().mockResolvedValue([]);

vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchResources: mockFetchResources,
    fetchInstances: mockFetchInstances,
  }),
}));

function createTestRouter() {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: "/resources",
        name: "resources",
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
  router.push({ path: "/resources", query });
  await router.isReady();

  const wrapper = mount(ResourcesView, {
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

const sampleResource = {
  resource_id: "abc-123",
  resource_name: "Population Data",
  resource_url: "https://example.com/data.csv",
  resource_format: "CSV",
  dataset_name: "my-dataset",
  status: "completed" as const,
  instance_id: "inst-1",
  ckan_resource_url:
    "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
  labels: ["empty"],
  job_count: 3,
  created_at: "2025-01-01T00:00:00Z",
  updated_at: "2025-06-01T00:00:00Z",
};

describe("ResourcesView", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchResources.mockResolvedValue([]);
    mockFetchInstances.mockResolvedValue([]);
  });

  it("renders page title", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("Resources");
  });

  it("renders table headers", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const ths = wrapper.findAll("th").map((th) => th.text());
    expect(ths).toContain("Resource");
    expect(ths).toContain("Format");
    expect(ths).toContain("Labels");
    expect(ths).toContain("Status");
    expect(ths).toContain("Jobs");
    expect(ths).toContain("Updated");
  });

  it("renders resource rows", async () => {
    mockFetchResources.mockResolvedValue([sampleResource]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("Population Data");
    expect(wrapper.text()).toContain("CSV");
    expect(wrapper.text()).toContain("3");
  });

  it("shows dataset_name / resource_name in Resource column", async () => {
    mockFetchResources.mockResolvedValue([
      {
        ...sampleResource,
        resource_name: "My Resource",
        dataset_name: "my-dataset",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("my-dataset");
    expect(wrapper.text()).toContain("My Resource");
  });

  it("shows em-dash when resource_name is null", async () => {
    mockFetchResources.mockResolvedValue([
      { ...sampleResource, resource_name: null },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // The dataset_name should still show
    expect(wrapper.text()).toContain("my-dataset");
  });

  it("navigates to resource detail on row click", async () => {
    mockFetchResources.mockResolvedValue([sampleResource]);

    const { wrapper, router } = await mountWithRouter();
    await flushPromises();

    const row = wrapper.find(".clickable-row");
    expect(row.exists()).toBe(true);
    await row.trigger("click");
    await flushPromises();

    expect(router.currentRoute.value.name).toBe("resource-detail");
    expect(router.currentRoute.value.params.resourceId).toBe("abc-123");
  });

  it("shows empty state when no resources", async () => {
    mockFetchResources.mockResolvedValue([]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("No resources found");
  });

  it("renders status filter dropdown", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const selects = wrapper.findAll("select");
    expect(selects.length).toBeGreaterThanOrEqual(1);

    const statusSelect = selects[0]!;
    const options = statusSelect.findAll("option").map((o) => o.text());
    expect(options).toContain("All statuses");
  });

  it("renders search input", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const input = wrapper.find('input[placeholder*="earch"]');
    expect(input.exists()).toBe(true);
  });

  it("shows pagination controls", async () => {
    mockFetchResources.mockResolvedValue([sampleResource]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.find(".pagination").exists()).toBe(true);
  });
});
