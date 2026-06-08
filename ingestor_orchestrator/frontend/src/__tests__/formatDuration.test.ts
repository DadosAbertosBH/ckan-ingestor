import { describe, it, expect } from "vitest";
import { formatDuration } from "@/utils/jobDuration";

describe("formatDuration", () => {
  it("formats seconds", () => {
    expect(formatDuration(12_000)).toBe("12s");
  });

  it("formats minutes and seconds", () => {
    expect(formatDuration(330_000)).toBe("5m 30s");
  });

  it("formats hours, minutes and seconds", () => {
    expect(formatDuration(12_345_000)).toBe("3h 25m 45s");
  });

  it("formats zero", () => {
    expect(formatDuration(0)).toBe("0s");
  });

  it("formats sub-second as 0s", () => {
    expect(formatDuration(500)).toBe("0s");
  });

  it("formats minutes only (no seconds)", () => {
    expect(formatDuration(300_000)).toBe("5m 0s");
  });
});
