import { describe, it, expect } from "vitest";
import { mount } from "@vue/test-utils";
import InstanceCard from "../components/InstanceCard.vue";

describe("SICOM instance", () => {
  it("uses the same metadata sync button as every other source", () => {
    const card = mount(InstanceCard, {
      props: { stats: { instance: { id: "sicom", name: "SICOM", url: "https://dadosabertos.tce.mg.gov.br/", source_type: "sicom", dataset_count: 0, resource_count: 0, last_metadata_synced: null, created_at: "", updated_at: "" }, pending: 0, processing: 0, completed: 0, failed: 0, deleted: 0 } },
      global: { stubs: { RouterLink: true } },
    });
    expect(card.find(".btn-sync").exists()).toBe(true);
    expect(card.text()).not.toContain("Daily crawler");
  });
});
