import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  ClockSyncControlFrameSchema,
  ClockSyncReplyFrameSchema,
  ClockReportControlFrameSchema,
  EventTimingSchema,
  validateMonotonic,
} from "../src/index.js";

const fixturesPath = resolve(__dirname, "../fixtures/timing_fixtures.json");
const fixtures = JSON.parse(readFileSync(fixturesPath, "utf-8"));

describe("Q1 Timing and Clock Control Frames (TypeScript)", () => {
  it("validates valid clock_sync frames against ClockSyncControlFrameSchema", () => {
    for (const frame of fixtures.valid_clock_sync) {
      const parsed = ClockSyncControlFrameSchema.parse(frame);
      expect(parsed.action).toBe("clock_sync");
      expect(parsed.t1).toBeGreaterThan(0);
    }
  });

  it("rejects invalid clock_sync frames", () => {
    for (const item of fixtures.invalid_clock_sync) {
      expect(() => ClockSyncControlFrameSchema.parse(item.frame)).toThrow();
    }
  });

  it("validates valid clock_sync_reply frames against ClockSyncReplyFrameSchema", () => {
    for (const frame of fixtures.valid_clock_sync_reply) {
      const parsed = ClockSyncReplyFrameSchema.parse(frame);
      expect(parsed.action).toBe("clock_sync_reply");
      expect(parsed.t2).toBeGreaterThanOrEqual(parsed.t1);
    }
  });

  it("rejects invalid clock_sync_reply frames", () => {
    for (const item of fixtures.invalid_clock_sync_reply) {
      expect(() => ClockSyncReplyFrameSchema.parse(item.frame)).toThrow();
    }
  });

  it("validates valid clock_report frames against ClockReportControlFrameSchema", () => {
    for (const frame of fixtures.valid_clock_report) {
      const parsed = ClockReportControlFrameSchema.parse(frame);
      expect(parsed.action).toBe("clock_report");
      expect(parsed.offset_ms).toBeDefined();
      expect(parsed.rtt_ms).toBeGreaterThanOrEqual(0);
    }
  });

  it("rejects invalid clock_report frames", () => {
    for (const item of fixtures.invalid_clock_report) {
      expect(() => ClockReportControlFrameSchema.parse(item.frame)).toThrow();
    }
  });

  it("validates EventTiming strict schema on valid fixtures and confirms zero monotonic violations", () => {
    for (const timing of fixtures.valid_event_timing) {
      const parsed = EventTimingSchema.parse(timing);
      const violations = validateMonotonic(parsed);
      expect(violations).toHaveLength(0);
    }
  });

  it("detects monotonic violations on inverted timing fixtures", () => {
    for (const item of fixtures.invalid_event_timing_monotonic) {
      const parsed = EventTimingSchema.parse(item.timing);
      const violations = validateMonotonic(parsed);
      expect(violations.length).toBeGreaterThan(0);
      expect(violations[0]).toContain(item.violating_stage);
    }
  });

  it("rejects extra fields on EventTiming strict schema", () => {
    for (const item of fixtures.invalid_event_timing_extra_field) {
      expect(() => EventTimingSchema.parse(item.timing)).toThrow();
    }
  });
});
