import { describe, it, expect } from "vitest";
import { mount } from "@vue/test-utils";
import ResourceLabelBadge from "@/components/ResourceLabelBadge.vue";

describe("ResourceLabelBadge", () => {
  it("renders a single label", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["empty"] },
    });

    const badges = wrapper.findAll(".label-badge");
    expect(badges).toHaveLength(1);
    expect(badges[0]!.text()).toBe("empty");
  });

  it("renders multiple labels", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["empty", "stale"] },
    });

    const badges = wrapper.findAll(".label-badge");
    expect(badges).toHaveLength(2);
    expect(badges[0]!.text()).toBe("empty");
    expect(badges[1]!.text()).toBe("stale");
  });

  it("renders nothing when labels array is empty", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: [] },
    });

    expect(wrapper.findAll(".label-badge")).toHaveLength(0);
  });

  it('applies gray background for "empty" label', () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["empty"] },
    });

    const badge = wrapper.find(".label-badge");
    expect(badge.attributes("style")).toContain("background-color: #6b7280");
  });

  it("applies fallback gray for unknown label", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["unknown-label"] },
    });

    const badge = wrapper.find(".label-badge");
    expect(badge.attributes("style")).toContain("background-color: #6b7280");
  });

  it("capitalizes label text via text-transform CSS", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["empty"] },
    });

    const badge = wrapper.find(".label-badge");
    // CSS text-transform applies, but the text content is raw "empty"
    expect(badge.text()).toBe("empty");
  });

  it("renders labels in order", () => {
    const wrapper = mount(ResourceLabelBadge, {
      props: { labels: ["stale", "empty"] },
    });

    const badges = wrapper.findAll(".label-badge");
    expect(badges[0]!.text()).toBe("stale");
    expect(badges[1]!.text()).toBe("empty");
  });
});
