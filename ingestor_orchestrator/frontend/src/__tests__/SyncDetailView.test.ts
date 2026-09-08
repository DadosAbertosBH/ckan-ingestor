import { describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import SyncDetailView from "@/views/SyncDetailView.vue";

const fetchSync = vi.fn().mockResolvedValue({
  id: "sync-1",
  instance_id: "inst-1",
  instance_name: "Minas Gerais",
  start_time: "2026-09-08T15:44:32Z",
  end_time: "2026-09-08T15:44:33Z",
  status: "failure",
  error_message: "parsing error: true",
  total_packages: 0,
  new_datasets: 0,
  new_resources: 0,
  updated_datasets: 0,
  updated_resources: 0,
});

vi.mock("@/composables/useApi", () => ({ useApi: () => ({ fetchSync }) }));

describe("SyncDetailView", () => {
  it("shows the failure status and error message", async () => {
    const wrapper = mount(SyncDetailView, {
      props: { id: "sync-1" },
      global: { stubs: { "router-link": { template: "<a><slot /></a>" } } },
    });
    await flushPromises();

    expect(wrapper.text()).toContain("Failed");
    expect(wrapper.text()).toContain("parsing error: true");
  });
});
