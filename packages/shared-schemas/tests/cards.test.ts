import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, expect, it } from "vitest";
import {
  computeConfidenceBand,
  InsightCard,
  InsightCardSchema,
  validateOutboundCard,
} from "../src/cards.js";

const FIXTURES_DIR = resolve(__dirname, "../fixtures/cards");

describe("InsightCard Quality Contract (Q3)", () => {
  it("parses and validates valid_card.json", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "valid_card.json"), "utf-8");
    const parsed = InsightCardSchema.parse(JSON.parse(raw));
    expect(parsed.card_id).toBe("c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c");
    expect(parsed.confidence_band).toBe("high");
    expect(parsed.claims[0].type).toBe("identifier");
    expect(parsed.claims[0].evidence[0].source_type).toBe("pull_request");

    const validation = validateOutboundCard(parsed);
    expect(validation.valid).toBe(true);
    expect(validation.reason).toBeUndefined();
  });

  it("rejects unverified claim on verified card", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "invalid_unverified_claim.json"), "utf-8");
    const parsed = InsightCardSchema.parse(JSON.parse(raw));
    const validation = validateOutboundCard(parsed);
    expect(validation.valid).toBe(false);
    expect(validation.reason?.toLowerCase()).toContain("unverified");
  });

  it("rejects verified claim with missing evidence", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "invalid_missing_evidence.json"), "utf-8");
    const parsed = InsightCardSchema.parse(JSON.parse(raw));
    const validation = validateOutboundCard(parsed);
    expect(validation.valid).toBe(false);
    expect(validation.reason?.toLowerCase()).toContain("evidence");
  });

  it("rejects inferred card with invalid kind", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "invalid_inferred_bad_kind.json"), "utf-8");
    const parsed = InsightCardSchema.parse(JSON.parse(raw));
    const validation = validateOutboundCard(parsed);
    expect(validation.valid).toBe(false);
    expect(validation.reason?.toLowerCase()).toContain("kind");
  });

  it("rejects restricted card with no visible_roles", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "invalid_restricted_no_roles.json"), "utf-8");
    const parsed = InsightCardSchema.parse(JSON.parse(raw));
    const validation = validateOutboundCard(parsed);
    expect(validation.valid).toBe(false);
    expect(validation.reason?.toLowerCase()).toContain("visible_roles");
  });

  it("computes deterministic confidence bands", () => {
    expect(
      computeConfidenceBand({
        retrieval_score: 0.9,
        source_freshness: 0.8,
        asr_confidence: 0.95,
        verifier_result: 1.0,
      }),
    ).toBe("high");

    expect(
      computeConfidenceBand({
        retrieval_score: 0.65, // borderline
        source_freshness: 0.8,
        asr_confidence: 0.95,
        verifier_result: 1.0,
      }),
    ).toBe("medium");

    expect(
      computeConfidenceBand({
        retrieval_score: 0.4, // severe fail
        source_freshness: 0.8,
        asr_confidence: 0.95,
        verifier_result: 1.0,
      }),
    ).toBe("low");
  });

  it("enforces revision semantics (highest revision wins, out-of-order discarded)", () => {
    const store = new Map<string, InsightCard>();
    const applyCard = (c: InsightCard): boolean => {
      const existing = store.get(c.card_id);
      if (existing && existing.revision >= c.revision) {
        return false;
      }
      store.set(c.card_id, c);
      return true;
    };

    const raw = readFileSync(resolve(FIXTURES_DIR, "valid_card.json"), "utf-8");
    const c1 = InsightCardSchema.parse(JSON.parse(raw));
    const c2 = InsightCardSchema.parse({ ...JSON.parse(raw), revision: 2, tier: 2 });

    expect(applyCard(c1)).toBe(true);
    expect(store.get(c1.card_id)?.revision).toBe(1);

    expect(applyCard(c2)).toBe(true);
    expect(store.get(c1.card_id)?.revision).toBe(2);

    expect(applyCard(c2)).toBe(false); // duplicate
    expect(applyCard(c1)).toBe(false); // out of order delivery
    expect(store.get(c1.card_id)?.revision).toBe(2);
  });

  it("validates Amendment A1 words and stable_prefix_words", async () => {
    const { TranscriptFinalPayloadSchema, TranscriptInterimPayloadSchema } =
      await import("../src/index.js");

    const finalParsed = TranscriptFinalPayloadSchema.parse({
      utterance_id: "0123456789abcdef01234567",
      speaker_id: "spk_0",
      channel: 0,
      text: "Merge PR-482 today",
      start_ms: 1000,
      end_ms: 2200,
      start_epoch_ms: 1793000001000,
      confidence: 0.97,
      language: "en-IN",
      provider: "deepgram",
      model: "nova-3",
      words: [
        { w: "Merge", start_ms: 1000, end_ms: 1250, confidence: 0.98, speaker: 0 },
        { w: "PR-482", start_ms: 1300, end_ms: 1800, confidence: 0.96, speaker: 0 },
      ],
    });
    expect(finalParsed.words?.length).toBe(2);

    const interimParsed = TranscriptInterimPayloadSchema.parse({
      speaker_id: "spk_0",
      channel: 0,
      text: "Merge PR-482",
      start_ms: 1000,
      end_ms: 1800,
      start_epoch_ms: 1793000001000,
      confidence: 0.91,
      language: "en-IN",
      provider: "deepgram",
      model: "nova-3",
      is_stable: true,
      stable_prefix_words: 2,
    });
    expect(interimParsed.stable_prefix_words).toBe(2);
  });
});
