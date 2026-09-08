import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import SyncsView from "@/views/SyncsView.vue";

const mockFetchSyncs = vi.fn().mockResolvedValue([]);
const mockFetchInstances = vi.fn().mockResolvedValue([]);

vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchSyncs: mockFetchSyncs,
    fetchInstances: mockFetchInstances,
  }),
}));

function createTestRouter() {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/syncs", name: "syncs", component: { template: "<div/>" } },
      { path: "/syncs/:id", name: "sync-detail", component: { template: "<div/>" } },
    ],
  });
}

async function mountWithRouter() {
  const router = createTestRouter();
  router.push("/syncs");
  await router.isReady();
  const wrapper = mount(SyncsView, { global: { plugins: [router] } });
  return { wrapper, router };
}

const sampleSync = {
  id: "sync-1",
  instance_id: "inst-1",
  instance_name: "CKAN Prod",
  start_time: "2026-07-31T10:00:00Z",
  end_time: "2026-07-31T10:05:00Z",
  total_packages: 100,
  new_datasets: 3,
  new_resources: 10,
  updated_datasets: 1,
  updated_resources: 4,
  status: "failure",
  error_message: "parsing error: true",
};

describe("SyncsView", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    mockFetchSyncs.mockResolvedValue([]);
    mockFetchInstances.mockResolvedValue([]);
  });

  it("renders page title", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    expect(wrapper.text()).toContain("Syncs");
  });

  it("renders table headers", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    const ths = wrapper.findAll("th").map((th) => th.text());
    expect(ths).toContain("Instance");
    expect(ths).toContain("Started");
    expect(ths).toContain("Duration");
    expect(ths).toContain("Total Packages");
    expect(ths).toContain("New Datasets");
    expect(ths).toContain("New Resources");
    expect(ths).toContain("Updated Datasets");
    expect(ths).toContain("Updated Resources");
    expect(ths).toContain("Status");
  });

  it("renders sync rows", async () => {
    mockFetchSyncs.mockResolvedValue([sampleSync]);
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    expect(wrapper.text()).toContain("CKAN Prod");
    expect(wrapper.text()).toContain("100");
    expect(wrapper.text()).toContain("3");
    expect(wrapper.text()).toContain("10");
    expect(wrapper.text()).toContain("1");
    expect(wrapper.text()).toContain("4");
    expect(wrapper.text()).toContain("Failed");
    expect(wrapper.find('a[href="/syncs/sync-1"]').exists()).toBe(true);
  });

  it("shows empty state when no syncs", async () => {
    mockFetchSyncs.mockResolvedValue([]);
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    expect(wrapper.text()).toContain("No syncs found");
  });

  it("renders instance filter dropdown", async () => {
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    const selects = wrapper.findAll("select");
    expect(selects.length).toBeGreaterThanOrEqual(1);
    const options = selects[0]!.findAll("option").map((o) => o.text());
    expect(options).toContain("All instances");
  });

  it("shows pagination controls", async () => {
    mockFetchSyncs.mockResolvedValue([sampleSync]);
    const { wrapper } = await mountWithRouter();
    await flushPromises();
    expect(wrapper.find(".pagination").exists()).toBe(true);
  });
});
