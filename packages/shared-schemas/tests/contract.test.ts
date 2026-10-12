import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  AuditEventSchema,
  ConsentRecordSchema,
  ControlFrameSchema,
  TranscriptChunkFrameSchema,
  AckFrameSchema,
  ErrorFrameSchema,
  IntelligenceEventFrameSchema,
  EventEnvelopeSchema,
  TranscriptFinalPayloadSchema,
  TranscriptInterimPayloadSchema,
  TranscriptServiceDegradedPayloadSchema,
  computeUtteranceId,
  decodeAudioFrame,
  encodeAudioFrame,
} from "../src/index.js";

const FIXTURES_DIR = path.resolve(__dirname, "../fixtures");
const VALID_DIR = path.join(FIXTURES_DIR, "valid");
const INVALID_DIR = path.join(FIXTURES_DIR, "invalid");

describe("Contract Tests (Zod & Binary)", () => {
  it("accepts all valid fixtures", () => {
    // 1. Audit event
    const auditJson = JSON.parse(
      fs.readFileSync(path.join(VALID_DIR, "audit_event_1.json"), "utf-8"),
    );
    expect(() => AuditEventSchema.parse(auditJson)).not.toThrow();

    // 2. Consent record
    const consentJson = JSON.parse(
      fs.readFileSync(path.join(VALID_DIR, "consent_record_1.json"), "utf-8"),
    );
    expect(() => ConsentRecordSchema.parse(consentJson)).not.toThrow();
  });

  it("rejects all invalid fixtures", () => {
    // 1. Invalid audit event
    const auditInvalid = JSON.parse(
      fs.readFileSync(path.join(INVALID_DIR, "audit_event_empty_tenant.json"), "utf-8"),
    );
    expect(() => AuditEventSchema.parse(auditInvalid)).toThrow();

    // 2. Invalid consent record
    const consentInvalid = JSON.parse(
      fs.readFileSync(path.join(INVALID_DIR, "consent_record_invalid_type.json"), "utf-8"),
    );
    expect(() => ConsentRecordSchema.parse(consentInvalid)).toThrow();
  });

  it("validates gateway frame fixtures in TypeScript", () => {
    const gf = JSON.parse(
      fs.readFileSync(path.join(FIXTURES_DIR, "gateway_frame_fixtures.json"), "utf-8"),
    );

    for (const frame of gf.valid_control) {
      expect(() => ControlFrameSchema.parse(frame)).not.toThrow();
    }

    for (const invalidCase of gf.invalid_control) {
      expect(() => ControlFrameSchema.parse(invalidCase.frame)).toThrow();
    }

    for (const frame of gf.valid_transcript) {
      expect(() => TranscriptChunkFrameSchema.parse(frame)).not.toThrow();
    }

    for (const frame of gf.valid_ack) {
      expect(() => AckFrameSchema.parse(frame)).not.toThrow();
    }

    for (const frame of gf.valid_error) {
      expect(() => ErrorFrameSchema.parse(frame)).not.toThrow();
    }

    for (const frame of gf.valid_intelligence) {
      expect(() => IntelligenceEventFrameSchema.parse(frame)).not.toThrow();
    }
  });

  it("encodes and decodes binary audio frame fixtures in TypeScript", () => {
    const af = JSON.parse(
      fs.readFileSync(path.join(FIXTURES_DIR, "audio_frame_fixtures.json"), "utf-8"),
    );

    for (const c of af.valid) {
      const raw = Buffer.from(c.hex, "hex");
      const { header, payload } = decodeAudioFrame(new Uint8Array(raw));

      if (c.header.magic) {
        expect(header.magic).toBe(c.header.magic);
        expect(header.clientSeq).toBe(c.header.client_seq);
        expect(header.captureTsMs).toBe(BigInt(c.header.capture_ts_ms));
        expect(header.sampleRate).toBe(c.header.sample_rate);
        expect(header.channels).toBe(c.header.channels);
        expect(header.codec).toBe(c.header.codec);

        const reEncoded = encodeAudioFrame(header.clientSeq, header.captureTsMs, payload, {
          sampleRate: header.sampleRate,
          channels: header.channels,
          codec: header.codec,
        });
        expect(Buffer.from(reEncoded).toString("hex")).toBe(c.hex);
      } else {
        expect(header.version).toBe(c.header.version);
        expect(header.flags).toBe(c.header.flags);
        expect(header.clientSeq).toBe(BigInt(c.header.client_seq));
        expect(header.captureTsMs).toBe(BigInt(c.header.capture_ts_ms));

        // Round trip encode
        const reEncoded = encodeAudioFrame(
          header.clientSeq,
          header.captureTsMs,
          payload,
          header.flags,
          header.version,
        );
        expect(Buffer.from(reEncoded).toString("hex")).toBe(c.hex);
      }
      expect(Buffer.from(payload).toString("hex")).toBe(c.payload_hex);
    }

    for (const invalidCase of af.invalid) {
      const raw = Buffer.from(invalidCase.hex, "hex");
      expect(() => decodeAudioFrame(new Uint8Array(raw))).toThrow();
    }
  });

  it("validates ASR contract fixtures and deterministic utterance_id in TypeScript", () => {
    const raw = JSON.parse(
      fs.readFileSync(path.join(FIXTURES_DIR, "asr_contract_fixtures.json"), "utf-8"),
    );

    // 1. Deterministic utterance_id
    const uCase = raw.deterministic_utterance_id;
    const calcUid = computeUtteranceId(
      uCase.meeting_id,
      uCase.channel,
      uCase.speaker_id,
      uCase.start_ms,
    );
    expect(calcUid).toBe(uCase.expected_utterance_id);

    // 2. Transcript final
    const tfData = raw.transcript_final;
    const tfEnv = EventEnvelopeSchema.parse(tfData);
    expect(tfEnv.event_type).toBe("transcript_final");
    const tfPayload = TranscriptFinalPayloadSchema.parse(tfEnv.payload);
    expect(tfPayload.utterance_id).toBe(uCase.expected_utterance_id);
    expect(tfPayload.channel).toBe(0);
    expect(tfPayload.speaker_id).toBe("spk_0");
    expect(tfPayload.text).toBe("Welcome everyone to the quarterly review.");
    expect(tfPayload.confidence).toBe(0.985);
    expect(tfPayload.provider).toBe("deepgram");
    expect(tfPayload.model).toBe("nova-3");

    // 3. Transcript interim
    const tiData = raw.transcript_interim;
    const tiEnv = EventEnvelopeSchema.parse(tiData);
    expect(tiEnv.event_type).toBe("transcript_interim");
    const tiPayload = TranscriptInterimPayloadSchema.parse(tiEnv.payload);
    expect(tiPayload.is_stable).toBe(false);
    expect(tiPayload.confidence).toBe(0.92);

    // 4. Transcript service degraded
    const tdData = raw.transcript_service_degraded;
    const tdEnv = EventEnvelopeSchema.parse(tdData);
    expect(tdEnv.event_type).toBe("transcript_service_degraded");
    const tdPayload = TranscriptServiceDegradedPayloadSchema.parse(tdEnv.payload);
    expect(tdPayload.state).toBe("degraded");
    expect(tdPayload.reason).toBe("provider_unreachable");
  });
});
