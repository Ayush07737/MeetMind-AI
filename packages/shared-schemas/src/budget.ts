/**
 * MeetMind AI — Latency Budget Schema & Types (Q2).
 *
 * TypeScript types and Zod schemas mirroring Python budget models.
 */

import { z } from "zod";

export const ObservedMetricsSchema = z.object({
  p50_ms: z.number(),
  p95_ms: z.number().optional().nullable(),
  service_p95_ms: z.number().optional().nullable(),
  date: z.string(),
  log_path: z.string(),
  note: z.string().default(""),
});
export type ObservedMetrics = z.infer<typeof ObservedMetricsSchema>;

export const StageBudgetSchema = z.object({
  name: z.string(),
  owner: z.string(),
  clock: z.string(),
  target_p50_ms: z.number().int().optional().nullable(),
  target_p95_ms: z.number().int().optional().nullable(),
  aspirational_goal_ms: z.number().int().optional().nullable(),
  status: z.string().default("Accepted"),
  observed: ObservedMetricsSchema.optional().nullable(),
  description: z.string().default(""),
  technology_requirement: z.string().default(""),
});
export type StageBudget = z.infer<typeof StageBudgetSchema>;

export const ClassificationConstraintsSchema = z.object({
  max_p50_ms: z.number().default(200),
  max_p95_ms: z.number().default(300),
  technology_constraint: z.string().default(""),
});
export type ClassificationConstraints = z.infer<typeof ClassificationConstraintsSchema>;

export const MemoryConstraintsSchema = z.object({
  tracemalloc_headroom_mb: z.number().default(10.0),
  private_bytes_headroom_mb: z.number().default(40.0),
  soak_max_meeting_seconds: z.number().default(3600),
});
export type MemoryConstraints = z.infer<typeof MemoryConstraintsSchema>;

export const OverheadConstraintsSchema = z.object({
  max_own_overhead_per_stage_ms: z.number().default(25.0),
});
export type OverheadConstraints = z.infer<typeof OverheadConstraintsSchema>;

export const BudgetConstraintsSchema = z.object({
  classification: ClassificationConstraintsSchema.default({}),
  memory: MemoryConstraintsSchema.default({}),
  overhead: OverheadConstraintsSchema.default({}),
});
export type BudgetConstraints = z.infer<typeof BudgetConstraintsSchema>;

export const SimulatedDelaysSchema = z.object({
  network_rtt_ms: z.number().default(60.0),
  gateway_overhead_ms: z.number().default(5.0),
  asr_provider_delay_ms: z.number().default(200.0),
  asr_pipeline_overhead_ms: z.number().default(10.0),
  classifier_delay_ms: z.number().default(50.0),
  retrieval_delay_ms: z.number().default(150.0),
  card_publisher_overhead_ms: z.number().default(10.0),
});
export type SimulatedDelays = z.infer<typeof SimulatedDelaysSchema>;

export const HistoryEntrySchema = z.object({
  date: z.string(),
  source: z.string(),
  interim_p50_ms: z.number().optional().nullable(),
  e2e_p50_ms: z.number().optional().nullable(),
  service_p95_ms: z.number().optional().nullable(),
  ws_rtt_ms: z.number().optional().nullable(),
  note: z.string().default(""),
});
export type HistoryEntry = z.infer<typeof HistoryEntrySchema>;

export const LatencyBudgetSchema = z.object({
  version: z.string(),
  metadata: z.record(z.unknown()).default({}),
  constraints: BudgetConstraintsSchema.default({}),
  stages: z.record(StageBudgetSchema).default({}),
  simulated: SimulatedDelaysSchema.default({}),
  history: z.array(HistoryEntrySchema).default([]),
});
export type LatencyBudget = z.infer<typeof LatencyBudgetSchema>;

export function parseLatencyBudget(raw: unknown): LatencyBudget {
  return LatencyBudgetSchema.parse(raw);
}

