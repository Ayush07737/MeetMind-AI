import { describe, it, expect } from "vitest";
import { LatencyBudgetSchema, parseLatencyBudget } from "../src/index.js";

const VALID_BUDGET_OBJECT = {
  version: "1.0.0",
  metadata: {
    title: "MeetMind AI End-to-End Latency & Stability Quality Budget",
    status: "Accepted Contract",
    enforcement: "Automated via scripts/budget_check.py and acceptance tests",
  },
  constraints: {
    classification: {
      max_p50_ms: 200,
      max_p95_ms: 300,
      technology_constraint: "Rule-based pattern matching or lightweight local embeddings (<50ms).",
    },
    memory: {
      tracemalloc_headroom_mb: 10.0,
      tracemalloc_reason: "Bounded collections with hard caps form cyclic allocation/deallocation.",
      private_bytes_headroom_mb: 40.0,
      private_bytes_reason: "Windows process commit charge tracking committed virtual pages.",
      soak_max_meeting_seconds: 3600,
    },
    overhead: {
      max_own_overhead_per_stage_ms: 25.0,
    },
  },
  stages: {
    interim_text_visible: {
      name: "Interim Transcript Text Visible",
      owner: "Component #2 (meetmind_asr) + Chrome Extension / Frontend",
      clock: "t0_capture -> ui_render (interim)",
      target_p50_ms: 1000,
      target_p95_ms: 2000,
      aspirational_goal_ms: 500,
      status: "Accepted with 500ms aspirational target",
      observed: {
        p50_ms: 585.38,
        p95_ms: 1596.37,
        date: "2026-11-01",
        log_path: "logs/bench30.log",
        note: "Measured on 30-minute Nova-3 stream via India cloud endpoint",
      },
    },
    stable_trigger_text: {
      name: "Stable Trigger Final Transcript Available",
      owner: "Component #2 (meetmind_asr)",
      clock: "t0_capture -> asr_pub",
      target_p50_ms: 1000,
      target_p95_ms: 2000,
      status: "Accepted",
      observed: {
        p50_ms: 433.57,
        p95_ms: 1929.22,
        service_p95_ms: 4.93,
        date: "2026-11-01",
        log_path: "logs/bench30.log",
        note: "Measured on 30-minute run",
      },
    },
  },
  simulated: {
    network_rtt_ms: 60.0,
    gateway_overhead_ms: 5.0,
    asr_provider_delay_ms: 200.0,
    asr_pipeline_overhead_ms: 10.0,
    classifier_delay_ms: 50.0,
    retrieval_delay_ms: 150.0,
    card_publisher_overhead_ms: 10.0,
  },
  history: [
    {
      date: "2026-11-01",
      source: "logs/bench30.log",
      interim_p50_ms: 585.38,
      e2e_p50_ms: 433.57,
      service_p95_ms: 4.93,
      ws_rtt_ms: 61.03,
      note: "Authoritative 30-minute continuous run on India cloud endpoint",
    },
  ],
};

describe("Budget Contract Tests (Zod)", () => {
  it("validates valid budget object against LatencyBudgetSchema", () => {
    const budget = parseLatencyBudget(VALID_BUDGET_OBJECT);
    expect(() => LatencyBudgetSchema.parse(VALID_BUDGET_OBJECT)).not.toThrow();

    expect(budget.version).toBe("1.0.0");
    expect(budget.constraints.memory.tracemalloc_headroom_mb).toBe(10.0);
    expect(budget.constraints.memory.private_bytes_headroom_mb).toBe(40.0);
    expect(budget.constraints.memory.tracemalloc_reason.length).toBeGreaterThan(0);
    expect(budget.constraints.overhead.max_own_overhead_per_stage_ms).toBe(25.0);

    // Verify all stages carry a valid clock definition
    for (const stage of Object.values(budget.stages)) {
      expect(stage.clock).toBeDefined();
      expect(stage.clock.length).toBeGreaterThan(0);
      expect(stage.name.length).toBeGreaterThan(0);
      expect(stage.owner.length).toBeGreaterThan(0);
    }

    // Verify interim text stage
    const interimStage = budget.stages["interim_text_visible"];
    expect(interimStage).toBeDefined();
    expect(interimStage.target_p50_ms).toBe(1000);
    expect(interimStage.target_p95_ms).toBe(2000);
    expect(interimStage.aspirational_goal_ms).toBe(500);

    // Verify stable trigger text stage
    const stableStage = budget.stages["stable_trigger_text"];
    expect(stableStage).toBeDefined();
    expect(stableStage.target_p50_ms).toBe(1000);
    expect(stableStage.target_p95_ms).toBe(2000);
  });

  it("rejects an invalid budget structure", () => {
    const invalidBudget = {
      version: 123, // must be string
      stages: {},
    };
    expect(() => parseLatencyBudget(invalidBudget)).toThrow();
  });
});
