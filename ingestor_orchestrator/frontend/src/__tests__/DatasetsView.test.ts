import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import DatasetsView from "@/views/DatasetsView.vue";

const mockFetchDatasets = vi.fn().mockResolvedValue([]);
const mockFetchInstances = vi.fn().mockResolvedValue([]);

vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchDatasets: mockFetchDatasets,
    fetchInstances: mockFetchInstances,
  }),
}));

function createTestRouter() {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      {
        path: "/datasets",
        name: "datasets",
        component: { template: "<div/>" },
      },
    ],
  });
}

async function mountWithRouter(query: Record<string, string> = {}) {
  const router = createTestRouter();
  router.push({ path: "/datasets", query });
  await router.isReady();

  const wrapper = mount(DatasetsView, {
    global: {
      plugins: [router],
    },
  });

  return { wrapper, router };
}

const sampleDataset = {
  instance_id: "inst-1",
  instance_name: "Instance 1",
  dataset_name: "covid-data",
  ckan_dataset_url: "https://example.com/dataset/covid-data",
  total_resources: 10,
  pending_resources: 2,
  processing_resources: 1,
  completed_resources: 5,
  failed_resources: 2,
  updated_at: "2025-06-01T00:00:00Z",
  instance_last_synced_at: "2025-05-15T00:00:00Z",
};

describe("DatasetsView", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchDatasets.mockResolvedValue([]);
    mockFetchInstances.mockResolvedValue([]);
  });

  it("renders page title", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("Datasets");
  });

  it("renders table headers", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const ths = wrapper.findAll("th").map((th) => th.text());
    expect(ths).toContain("Instance");
    expect(ths).toContain("Dataset");
    expect(ths).toContain("Total");
    expect(ths).toContain("Pending");
    expect(ths).toContain("Running");
    expect(ths).toContain("Completed");
    expect(ths).toContain("Failed");
    expect(ths).toContain("Outdated");
    expect(ths).toContain("Success %");
    expect(ths).toContain("Sync");
    expect(ths).toContain("Updated");
  });

  it("renders dataset rows with counts", async () => {
    mockFetchDatasets.mockResolvedValue([sampleDataset]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("covid-data");
    expect(wrapper.text()).toContain("10"); // total
    expect(wrapper.text()).toContain("5"); // completed
    expect(wrapper.text()).toContain("2"); // pending
    expect(wrapper.text()).toContain("1"); // running (processing)
  });

  it("shows success rate as percentage", async () => {
    mockFetchDatasets.mockResolvedValue([sampleDataset]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // 5 completed out of 10 total = 50%
    expect(wrapper.text()).toContain("50%");
  });

  it("renders dataset name as link to CKAN", async () => {
    mockFetchDatasets.mockResolvedValue([sampleDataset]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const link = wrapper.find(".dataset-link");
    expect(link.exists()).toBe(true);
    expect(link.attributes("href")).toBe(
      "https://example.com/dataset/covid-data",
    );
    expect(link.attributes("target")).toBe("_blank");
  });

  it("shows sync indicator when dataset has newer data than last sync", async () => {
    // updated_at (June) > instance_last_synced_at (May) → needs sync
    mockFetchDatasets.mockResolvedValue([sampleDataset]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    // Should show the "needs sync" indicator
    expect(wrapper.find(".sync-behind").exists()).toBe(true);
  });

  it("shows synced indicator when dataset is up to date", async () => {
    mockFetchDatasets.mockResolvedValue([
      {
        ...sampleDataset,
        updated_at: "2025-05-01T00:00:00Z", // before last sync
        instance_last_synced_at: "2025-05-15T00:00:00Z",
      },
    ]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.find(".sync-ok").exists()).toBe(true);
  });

  it("shows empty state when no datasets", async () => {
    mockFetchDatasets.mockResolvedValue([]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.text()).toContain("No datasets found");
  });

  it("renders instance filter dropdown", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const selects = wrapper.findAll("select");
    expect(selects.length).toBeGreaterThanOrEqual(1);

    const instanceSelect = selects[0]!;
    const options = instanceSelect.findAll("option").map((o) => o.text());
    expect(options).toContain("All instances");
  });

  it("renders search input", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();

    const input = wrapper.find('input[placeholder*="earch"]');
    expect(input.exists()).toBe(true);
  });

  it("shows pagination controls", async () => {
    mockFetchDatasets.mockResolvedValue([sampleDataset]);

    const { wrapper } = await mountWithRouter();
    await flushPromises();

    expect(wrapper.find(".pagination").exists()).toBe(true);
  });
});
