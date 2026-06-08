import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import JobsView from "@/views/JobsView.vue";

// Mock useApi so we don't need real HTTP
vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchJobs: vi.fn().mockResolvedValue([]),
    createJob: vi.fn(),
  }),
}));

function createTestRouter(initialQuery: Record<string, string> = {}) {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/jobs", name: "jobs", component: { template: "<div/>" } },
      { path: "/jobs/:id", name: "job-detail", component: { template: "<div/>" } },
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
  });

  it("initializes filters from URL query params", async () => {
    const { wrapper, router } = await mountWithRouter({
      status: "failed",
      resource_id: "abc-123",
    });

    const statusSelect = wrapper.find(".filter-select").element as HTMLSelectElement;
    const resourceIdInput = wrapper.find(".filter-input").element as HTMLInputElement;

    expect(statusSelect.value).toBe("failed");
    expect(resourceIdInput.value).toBe("abc-123");
  });

  it("defaults to empty filters when no query params", async () => {
    const { wrapper } = await mountWithRouter();

    const statusSelect = wrapper.find(".filter-select").element as HTMLSelectElement;
    const resourceIdInput = wrapper.find(".filter-input").element as HTMLInputElement;

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
