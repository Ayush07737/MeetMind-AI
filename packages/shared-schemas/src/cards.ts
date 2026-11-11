/**
 * MeetMind AI — InsightCard Quality Contract (Q3 / Q-0c)
 *
 * Zod schemas and TypeScript types for InsightCard, validation, and confidence bands.
 */

import { z } from "zod";

export const SourceTypeSchema = z.enum([
  "internal",
  "external",
  "mixed",
  "ticket",
  "pull_request",
  "commit",
  "incident",
  "adr",
  "doc",
]);
export type SourceType = z.infer<typeof SourceTypeSchema>;

export const ClaimTypeSchema = z.enum([
  "number",
  "entity",
  "date",
  "assertion",
  "identifier",
  "state",
]);
export type ClaimType = z.infer<typeof ClaimTypeSchema>;

export const CardKindSchema = z.enum(["fact", "suggestion", "action_recommendation", "alert"]);
export type CardKind = z.infer<typeof CardKindSchema>;

export const CardTierSchema = z.union([z.literal(1), z.literal(2), z.literal(3)]);
export type CardTier = z.infer<typeof CardTierSchema>;

export const ConfidenceBandSchema = z.enum(["high", "medium", "low"]);
export type ConfidenceBand = z.infer<typeof ConfidenceBandSchema>;

export const CardOriginSchema = z.enum(["internal", "external", "mixed"]);
export type CardOrigin = z.infer<typeof CardOriginSchema>;

export const CardStatusSchema = z.enum(["verified", "inferred"]);
export type CardStatus = z.infer<typeof CardStatusSchema>;

export const EvidenceSchema = z.object({
  source_type: SourceTypeSchema,
  source_id: z.string().min(1),
  uri: z.string().nullable().optional(),
  span: z.string().min(1),
  retrieved_at: z.union([z.number(), z.string()]),
  as_of: z.union([z.number(), z.string()]).nullable().optional(),
});
export type Evidence = z.infer<typeof EvidenceSchema>;

export const ClaimSchema = z.object({
  claim_id: z.string().min(1),
  text: z.string().min(1),
  type: ClaimTypeSchema,
  value: z.union([z.string(), z.number()]).nullable().optional(),
  unit: z.string().nullable().optional(),
  status: CardStatusSchema,
  evidence: z.array(EvidenceSchema).default([]),
});
export type Claim = z.infer<typeof ClaimSchema>;

export const TextBlockSchema = z.object({
  type: z.enum(["paragraph", "header", "bullet"]),
  content: z.string().min(1),
});
export type TextBlock = z.infer<typeof TextBlockSchema>;

export const CardTimingSchema = z.object({
  trigger_ts_ms: z.number().int(),
  published_ts_ms: z.number().int(),
  retrieval_ms: z.number().optional().nullable(),
  reasoning_ms: z.number().optional().nullable(),
});
export type CardTiming = z.infer<typeof CardTimingSchema>;

export const CardTriggerSchema = z.object({
  utterance_id: z.string().min(1),
  category: z.string().min(1),
  trigger_confidence: z.number(),
  asr_confidence: z.number(),
  text_stability: z.number(),
});
export type CardTrigger = z.infer<typeof CardTriggerSchema>;

export const InsightCardSchema = z.object({
  card_id: z.string().min(1),
  revision: z.number().int().min(1).default(1),
  supersedes: z.string().nullable().optional(),
  meeting_id: z.string().min(1),
  tenant_id: z.string().min(1),
  trace_id: z.string().min(1),
  kind: CardKindSchema,
  tier: CardTierSchema,
  title: z.string().min(1),
  blocks: z.array(TextBlockSchema).default([]),
  claims: z.array(ClaimSchema).default([]),
  confidence_band: ConfidenceBandSchema,
  confidence_basis: z.record(z.string(), z.number()).default({}),
  origin: CardOriginSchema,
  status: CardStatusSchema,
  restricted: z.boolean().default(false),
  visible_roles: z.array(z.string()).default([]),
  timing: CardTimingSchema,
  trigger: CardTriggerSchema,
  late: z.boolean().default(false),
  abstain_reason: z.string().nullable().optional(),
  inferred_label: z.string().nullable().optional(),
});
export type InsightCard = z.infer<typeof InsightCardSchema>;

export function computeConfidenceBand(components: Record<string, number>): ConfidenceBand {
  const retrieval = components.retrieval_score ?? 0;
  const freshness = components.source_freshness ?? 0;
  const asr = components.asr_confidence ?? 0;
  const verifier = components.verifier_result ?? 0;

  if (retrieval >= 0.8 && freshness >= 0.7 && asr >= 0.9 && verifier >= 1.0) {
    return "high";
  }

  let borderlineCount = 0;
  let severeFail = false;

  if (retrieval >= 0.8) {
    // ok
  } else if (retrieval >= 0.6) {
    borderlineCount += 1;
  } else {
    severeFail = true;
  }

  if (freshness >= 0.7) {
    // ok
  } else if (freshness >= 0.5) {
    borderlineCount += 1;
  } else {
    severeFail = true;
  }

  if (asr >= 0.9) {
    // ok
  } else if (asr >= 0.75) {
    borderlineCount += 1;
  } else {
    severeFail = true;
  }

  if (verifier >= 1.0) {
    // ok
  } else if (verifier >= 0.7) {
    borderlineCount += 1;
  } else {
    severeFail = true;
  }

  if (!severeFail && borderlineCount === 1) {
    return "medium";
  }

  return "low";
}

export function validateOutboundCard(card: InsightCard): { valid: boolean; reason?: string } {
  if (!card.card_id || !card.timing.published_ts_ms || !card.confidence_band) {
    return { valid: false, reason: "Missing mandatory card envelope fields" };
  }

  if (card.restricted && (!card.visible_roles || card.visible_roles.length === 0)) {
    return { valid: false, reason: "Restricted card must specify non-empty visible_roles" };
  }

  if (card.status === "verified") {
    if (!card.claims || card.claims.length === 0) {
      return { valid: false, reason: "Verified card must contain at least one claim" };
    }
    for (const claim of card.claims) {
      if (claim.status !== "verified") {
        return {
          valid: false,
          reason: `Verified card contains unverified claim '${claim.claim_id}'`,
        };
      }
      if (!claim.evidence || claim.evidence.length === 0) {
        return {
          valid: false,
          reason: `Verified claim '${claim.claim_id}' has no supporting evidence`,
        };
      }
    }
  }

  if (card.status === "inferred") {
    if (card.kind !== "suggestion" && card.kind !== "alert") {
      return {
        valid: false,
        reason: `Inferred card cannot have kind '${card.kind}' (only suggestion|alert)`,
      };
    }
    if (!card.inferred_label) {
      return { valid: false, reason: "Inferred card must carry visible inferred_label" };
    }
  }

  if (card.status !== "inferred") {
    for (const claim of card.claims) {
      if (claim.status === "inferred") {
        return {
          valid: false,
          reason: `Live display card rejected: claim '${claim.claim_id}' is unverified`,
        };
      }
    }
  }

  return { valid: true };
}
