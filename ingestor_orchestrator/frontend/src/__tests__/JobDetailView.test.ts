import { describe, it, expect, vi, beforeEach } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { createRouter, createMemoryHistory } from "vue-router";
import JobDetailView from "@/views/JobDetailView.vue";
import type { Job } from "@/types";

const mockFetchJob = vi.fn();
const mockRetryJob = vi.fn();
const mockDeleteJob = vi.fn();

vi.mock("@/composables/useApi", () => ({
  useApi: () => ({
    fetchJob: mockFetchJob,
    retryJob: mockRetryJob,
    deleteJob: mockDeleteJob,
  }),
}));

function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: "job-1",
    instance_id: null,
    resource_id: "abc-123",
    resource_name: "Population Data",
    resource_url: "https://example.com/data.csv",
    resource_format: "CSV",
    dataset_name: "my-dataset",
    status: "completed",
    idempotency_key: "abc-123",
    created_at: "2025-01-01T00:00:00Z",
    updated_at: "2025-01-01T00:00:00Z",
    started_at: "2025-01-01T00:00:00Z",
    completed_at: "2025-01-01T00:01:00Z",
    ckan_resource_url:
      "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
    labels: [],
    results: [],
    ...overrides,
  };
}

function createTestRouter() {
  return createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/jobs", name: "jobs", component: { template: "<div/>" } },
      {
        path: "/jobs/:id",
        name: "job-detail",
        component: JobDetailView,
      },
    ],
  });
}

async function mountDetail(jobOverrides: Partial<Job> = {}) {
  const router = createTestRouter();
  router.push({ name: "job-detail", params: { id: "job-1" } });
  await router.isReady();

  const wrapper = mount(JobDetailView, {
    props: { id: "job-1" },
    global: {
      plugins: [router],
      stubs: {
        JobStatusBadge: true,
        JobResultPanel: true,
        ResourceLabelBadge: false,
      },
    },
  });

  return wrapper;
}

beforeEach(() => {
  vi.restoreAllMocks();
  mockFetchJob.mockResolvedValue(makeJob());
});

describe("JobDetailView — labels", () => {
  it("shows em-dash when no labels", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ labels: [] }));

    const wrapper = await mountDetail({ labels: [] });
    await flushPromises();

    // No label-badge should be rendered
    expect(wrapper.find(".label-badge").exists()).toBe(false);
    // The Labels section should still be present in the info grid
    expect(wrapper.text()).toContain("Labels");
  });

  it("renders ResourceLabelBadge when labels exist", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ labels: ["empty"] }));

    const wrapper = await mountDetail({ labels: ["empty"] });
    await flushPromises();

    const badge = wrapper.find(".label-badge");
    expect(badge.exists()).toBe(true);
    expect(badge.text()).toBe("empty");
  });

  it("shows label section with Labels header", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ labels: ["empty", "stale"] }));

    const wrapper = await mountDetail({ labels: ["empty", "stale"] });
    await flushPromises();

    expect(wrapper.text()).toContain("Labels");
    const badges = wrapper.findAll(".label-badge");
    expect(badges).toHaveLength(2);
  });

  it("shows CKAN link when ckan_resource_url is present", async () => {
    mockFetchJob.mockResolvedValue(
      makeJob({
        ckan_resource_url:
          "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
      }),
    );

    const wrapper = await mountDetail();
    await flushPromises();

    expect(wrapper.text()).toContain("CKAN");
    const ckanLink = wrapper.find('a[href*="dados.pbh.gov.br"]');
    expect(ckanLink.exists()).toBe(true);
    expect(ckanLink.text()).toContain("View on CKAN");
    expect(ckanLink.attributes("href")).toBe(
      "https://dados.pbh.gov.br/dataset/my-dataset/resource/abc-123",
    );
    expect(ckanLink.attributes("target")).toBe("_blank");
  });
});

describe("JobDetailView — loading and error states", () => {
  it("shows loading state initially", () => {
    mockFetchJob.mockReturnValue(new Promise(() => {})); // never resolves

    const wrapper = mount(JobDetailView, {
      props: { id: "job-1" },
      global: {
        plugins: [createTestRouter()],
        stubs: {
          JobStatusBadge: true,
          JobResultPanel: true,
          ResourceLabelBadge: true,
        },
      },
    });

    expect(wrapper.text()).toContain("Loading...");
  });

  it("shows error banner on fetch failure", async () => {
    mockFetchJob.mockRejectedValue(new Error("Network error"));

    const wrapper = await mountDetail();
    await flushPromises();

    expect(wrapper.text()).toContain("Network error");
    expect(wrapper.find(".btn-retry").exists()).toBe(true);
  });
});

describe("JobDetailView — retry and delete", () => {
  it("shows Retry button for failed job", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ status: "failed" }));

    const wrapper = await mountDetail({ status: "failed" });
    await flushPromises();

    expect(wrapper.text()).toContain("Retry");
  });

  it("shows Cancel button for pending job", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ status: "pending" }));

    const wrapper = await mountDetail({ status: "pending" });
    await flushPromises();

    expect(wrapper.text()).toContain("Cancel");
  });

  it("does not show Retry for completed job", async () => {
    mockFetchJob.mockResolvedValue(makeJob({ status: "completed" }));

    const wrapper = await mountDetail({ status: "completed" });
    await flushPromises();

    expect(wrapper.text()).not.toContain("Retry");
  });

  it("shows resource URL when present", async () => {
    mockFetchJob.mockResolvedValue(
      makeJob({ resource_url: "https://example.com/data.csv" }),
    );

    const wrapper = await mountDetail({
      resource_url: "https://example.com/data.csv",
    });
    await flushPromises();

    const urlLink = wrapper.find('a[href="https://example.com/data.csv"]');
    expect(urlLink.exists()).toBe(true);
  });
});
