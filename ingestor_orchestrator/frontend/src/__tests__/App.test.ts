import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import { createMemoryHistory, createRouter } from "vue-router";
import App from "@/App.vue";

describe("App navigation", () => {
  it("opens Jobs with the processing filter", async () => {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: "/", name: "dashboard", component: { template: "<div />" } },
        { path: "/jobs", name: "jobs", component: { template: "<div />" } },
        { path: "/resources", name: "resources", component: { template: "<div />" } },
        { path: "/syncs", name: "syncs", component: { template: "<div />" } },
        { path: "/datasets", name: "datasets", component: { template: "<div />" } },
      ],
    });
    await router.push("/");
    await router.isReady();

    const wrapper = mount(App, { global: { plugins: [router] } });
    const jobsLink = wrapper.findAll("a").find((link) => link.text() === "Jobs");

    expect(jobsLink?.attributes("href")).toBe("/jobs?status=processing");
  });
});
