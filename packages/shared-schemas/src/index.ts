/**
 * MeetMind AI — Shared Schemas
 *
 * Zod schemas and TypeScript types shared across the extension, frontend,
 * and any other TypeScript consumers. These mirror the Pydantic models
 * defined in meetmind_schemas.events (Python).
 */

import { createHash } from "crypto";
import { z } from "zod";

// ── Tenant ──────────────────────────────────────────────────────────────────

export const TenantIdSchema = z.string().min(1).describe("Unique tenant identifier");
export type TenantId = z.infer<typeof TenantIdSchema>;

// ── Consent ─────────────────────────────────────────────────────────────────

export const ConsentTypeSchema = z.enum(["audio_capture", "transcript_storage", "ai_analysis"]);
export type ConsentType = z.infer<typeof ConsentTypeSchema>;

export const ConsentActionSchema = z.enum(["granted", "revoked"]);
export type ConsentAction = z.infer<typeof ConsentActionSchema>;

export const ConsentEventSchema = z.object({
  id: z.string().uuid().optional(),
  tenantId: TenantIdSchema,
  userId: z.string().min(1),
  meetingId: z.string().min(1),
  consentType: ConsentTypeSchema,
  action: ConsentActionSchema.default("granted"),
  externalParticipants: z.boolean().default(false),
  jurisdictionHint: z.string().nullable().optional(),
  consentTextVersion: z.string().default("v1.0"),
  clientVersion: z.string().default("1.0.0"),
  createdAt: z.string().datetime().optional(),
});
export type ConsentEvent = z.infer<typeof ConsentEventSchema>;

export const ConsentRecordSchema = z.object({
  tenantId: TenantIdSchema,
  userId: z.string().min(1),
  meetingId: z.string().min(1),
  consentType: ConsentTypeSchema,
  grantedAt: z.string().datetime(),
  revokedAt: z.string().datetime().nullable().optional(),
});
export type ConsentRecord = z.infer<typeof ConsentRecordSchema>;

// ── Audit Event ─────────────────────────────────────────────────────────────

export const AuditEventSchema = z.object({
  tenantId: TenantIdSchema,
  userId: z.string().min(1),
  eventType: z.string().min(1),
  payload: z.record(z.unknown()),
  createdAt: z.string().datetime(),
});
export type AuditEvent = z.infer<typeof AuditEventSchema>;

// ── Enums & Constants ───────────────────────────────────────────────────────

export enum WSCloseCode {
  NORMAL_CLOSURE = 1000,
  INTERNAL_ERROR = 1011,
  PROTOCOL_VIOLATION = 4400,
  UNAUTHORIZED = 4401,
  FORBIDDEN = 4403,
  IDLE_TIMEOUT = 4408,
  WRITER_CONFLICT = 4409,
  RATE_LIMITED = 4429,
  AUDIT_UNAVAILABLE = 4503,
}

export enum ErrorCode {
  CONSENT_REQUIRED = "CONSENT_REQUIRED",
  AUTH_FAILED = "AUTH_FAILED",
  INVALID_FRAME = "INVALID_FRAME",
  UNKNOWN_FRAME_TYPE = "UNKNOWN_FRAME_TYPE",
  MEETING_ENDED = "MEETING_ENDED",
  INTERNAL_ERROR = "INTERNAL_ERROR",
  EVENTS_UNAVAILABLE = "EVENTS_UNAVAILABLE",
  RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED",
}

export enum AdapterType {
  CHROME_EXTENSION = "chrome_extension",
  DESKTOP_APP = "desktop_app",
  MEETING_BOT = "meeting_bot",
}

export enum ControlAction {
  MEETING_START = "meeting_start",
  MEETING_END = "meeting_end",
  CONSENT_CONFIRMED = "consent_confirmed",
}

export enum MeetingStatus {
  PENDING_CONSENT = "pending_consent",
  ACTIVE = "active",
  ENDED = "ended",
}

export const OUTBOUND_EVENT_TYPES = [
  "intelligence_event",
  "pre_meeting_brief",
  "transcript_interim",
  "transcript_service_degraded",
] as const;

export type OutboundEventType = (typeof OUTBOUND_EVENT_TYPES)[number];

// ── Client -> Server Frames ─────────────────────────────────────────────────

export const AdapterTypeSchema = z.enum(["chrome_extension", "desktop_app", "meeting_bot"]);

export const MeetingStartControlFrameSchema = z
  .object({
    type: z.literal("control"),
    action: z.literal("meeting_start"),
    adapter_type: AdapterTypeSchema.optional(),
    consent_type: z.string().optional(),
  })
  .strict();

export const MeetingEndControlFrameSchema = z
  .object({
    type: z.literal("control"),
    action: z.literal("meeting_end"),
    adapter_type: AdapterTypeSchema.optional(),
    consent_type: z.string().optional(),
  })
  .strict();

export const ConsentConfirmedControlFrameSchema = z
  .object({
    type: z.literal("control"),
    action: z.literal("consent_confirmed"),
    adapter_type: AdapterTypeSchema.optional(),
    consent_type: z.string().optional(),
    external_participants: z.boolean(),
    jurisdiction_hint: z.string().nullable().optional(),
    consent_text_version: z.string().min(1),
    client_version: z.string().min(1),
  })
  .strict();

export const ControlFrameSchema = z.discriminatedUnion("action", [
  MeetingStartControlFrameSchema,
  MeetingEndControlFrameSchema,
  ConsentConfirmedControlFrameSchema,
]);
export type ControlFrame = z.infer<typeof ControlFrameSchema>;

export const TranscriptChunkFrameSchema = z
  .object({
    type: z.literal("transcript_chunk"),
    text: z.string().min(1),
    client_seq: z.number().int().optional(),
    timestamp_ms: z.number().int().optional(),
  })
  .strict();
export type TranscriptChunkFrame = z.infer<typeof TranscriptChunkFrameSchema>;

export const ClientFrameSchema = z.union([ControlFrameSchema, TranscriptChunkFrameSchema]);
export type ClientFrame = z.infer<typeof ClientFrameSchema>;

// ── Server -> Client Frames ─────────────────────────────────────────────────

export const IntelligenceEventFrameSchema = z
  .object({
    type: z.literal("intelligence_event"),
    event_type: z.string(),
    payload: z.record(z.unknown()).default({}),
    meeting_id: z.string().default(""),
    timestamp_ms: z.number().int().optional(),
  })
  .strict();
export type IntelligenceEventFrame = z.infer<typeof IntelligenceEventFrameSchema>;

export const PreMeetingBriefFrameSchema = z
  .object({
    type: z.literal("intelligence_event"),
    event_type: z.literal("pre_meeting_brief"),
    payload: z.record(z.unknown()).default({}),
    meeting_id: z.string().default(""),
    timestamp_ms: z.number().int().optional(),
  })
  .strict();
export type PreMeetingBriefFrame = z.infer<typeof PreMeetingBriefFrameSchema>;

export const TranscriptInterimFrameSchema = z
  .object({
    type: z.literal("intelligence_event"),
    event_type: z.literal("transcript_interim"),
    payload: z.record(z.unknown()).default({}),
    meeting_id: z.string().default(""),
    timestamp_ms: z.number().int().optional(),
  })
  .strict();
export type TranscriptInterimFrame = z.infer<typeof TranscriptInterimFrameSchema>;

export const TranscriptServiceDegradedFrameSchema = z
  .object({
    type: z.literal("intelligence_event"),
    event_type: z.literal("transcript_service_degraded"),
    payload: z.record(z.unknown()).default({}),
    meeting_id: z.string().default(""),
    timestamp_ms: z.number().int().optional(),
  })
  .strict();
export type TranscriptServiceDegradedFrame = z.infer<typeof TranscriptServiceDegradedFrameSchema>;

export const AckFrameSchema = z
  .object({
    type: z.literal("ack"),
    seq: z.number().int(),
    frame_type: z.string().optional(),
    status: z.string().default("acked"),
    timestamp: z.string().datetime().optional(),
  })
  .strict();
export type AckFrame = z.infer<typeof AckFrameSchema>;

export const ErrorFrameSchema = z
  .object({
    type: z.literal("error"),
    code: z.nativeEnum(ErrorCode),
    message: z.string(),
  })
  .strict();
export type ErrorFrame = z.infer<typeof ErrorFrameSchema>;

// ── Export Keys ─────────────────────────────────────────────────────────────

export * from "./keys.js";

// ── Binary Audio Header Codec (WP5 & §2.3 Canonical) ─────────────────────────

export const BINARY_HEADER_SIZE = 18;

export interface AudioHeader {
  clientSeq: bigint | number;
  captureTsMs: bigint | number;
  sampleRate: number;
  channels: number;
  codec: number;
  magic: string;
  version: number;
  flags: number;
}

export interface EncodeAudioFrameOptions {
  sampleRate?: number;
  channels?: number;
  codec?: number;
  flags?: number;
  version?: number;
  magic?: string;
}

export function encodeAudioFrame(
  clientSeq: bigint | number,
  captureTsMs: bigint | number,
  payload: Uint8Array,
  optionsOrFlags?: number | EncodeAudioFrameOptions,
  legacyVersion?: number,
): Uint8Array {
  const buf = new Uint8Array(BINARY_HEADER_SIZE + payload.byteLength);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  if (typeof optionsOrFlags === "number" || (legacyVersion !== undefined && legacyVersion !== 1)) {
    // Legacy WP5 packing (>BBQQ)
    const flags = typeof optionsOrFlags === "number" ? optionsOrFlags : 0;
    const version = legacyVersion !== undefined ? legacyVersion : 1;
    view.setUint8(0, version);
    view.setUint8(1, flags);
    view.setBigUint64(2, BigInt(clientSeq), false);
    view.setBigUint64(10, BigInt(captureTsMs), false);
  } else {
    const opts = (optionsOrFlags as EncodeAudioFrameOptions) || {};
    if (opts.flags !== undefined || (opts.version !== undefined && opts.version !== 1)) {
      view.setUint8(0, opts.version ?? 1);
      view.setUint8(1, opts.flags ?? 0);
      view.setBigUint64(2, BigInt(clientSeq), false);
      view.setBigUint64(10, BigInt(captureTsMs), false);
    } else {
      // Canonical packing (>2sIQHBB: magic "MM", client_seq u32, capture_ts_ms u64, sample_rate u16, channels u8, codec u8)
      view.setUint8(0, 0x4d); // 'M'
      view.setUint8(1, 0x4d); // 'M'
      view.setUint32(2, Number(clientSeq), false);
      view.setBigUint64(6, BigInt(captureTsMs), false);
      view.setUint16(14, opts.sampleRate ?? 16000, false);
      view.setUint8(16, opts.channels ?? 1);
      view.setUint8(17, opts.codec ?? 1);
    }
  }

  buf.set(payload, BINARY_HEADER_SIZE);
  return buf;
}

export function decodeAudioFrame(data: Uint8Array): { header: AudioHeader; payload: Uint8Array } {
  if (data.byteLength < BINARY_HEADER_SIZE) {
    throw new Error(
      `Binary audio frame too short: ${data.byteLength} bytes, expected at least ${BINARY_HEADER_SIZE}`,
    );
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const payload = data.subarray(BINARY_HEADER_SIZE);

  // Check for canonical magic 'MM' (0x4D, 0x4D)
  if (view.getUint8(0) === 0x4d && view.getUint8(1) === 0x4d) {
    const clientSeq = view.getUint32(2, false);
    const captureTsMs = view.getBigUint64(6, false);
    const sampleRate = view.getUint16(14, false);
    const channels = view.getUint8(16);
    const codec = view.getUint8(17);
    return {
      header: {
        clientSeq,
        captureTsMs,
        sampleRate,
        channels,
        codec,
        magic: "MM",
        version: 1,
        flags: 0,
      },
      payload,
    };
  }

  // Check for legacy WP5 version = 1
  if (view.getUint8(0) === 1) {
    const version = view.getUint8(0);
    const flags = view.getUint8(1);
    const clientSeq = view.getBigUint64(2, false);
    const captureTsMs = view.getBigUint64(10, false);
    return {
      header: {
        clientSeq,
        captureTsMs,
        sampleRate: 16000,
        channels: 1,
        codec: 1,
        magic: "",
        version,
        flags,
      },
      payload,
    };
  }

  throw new Error(
    `Unsupported audio frame version or magic: byte0=0x${view.getUint8(0).toString(16)}`,
  );
}

// ── Event Envelope & Payloads (WP-A) ─────────────────────────────────────────

export const DegradedStateSchema = z.enum(["degraded", "still_degraded", "recovered"]);
export type DegradedState = z.infer<typeof DegradedStateSchema>;

export const DegradedReasonSchema = z.enum([
  "provider_unreachable",
  "provider_error",
  "unsupported_codec",
  "buffer_overflow",
  "audio_gap",
  "residency_violation",
]);
export type DegradedReason = z.infer<typeof DegradedReasonSchema>;

export const TranscriptFinalPayloadSchema = z.object({
  utterance_id: z.string().length(24),
  speaker_id: z.string(),
  speaker_name: z.string().nullable().optional(),
  speaker_epoch: z.number().int().default(0),
  channel: z.number().int().nullable().optional(),
  text: z.string(),
  start_ms: z.number().int(),
  end_ms: z.number().int(),
  start_epoch_ms: z.number().int(),
  confidence: z.number(),
  language: z.string(),
  provider: z.string(),
  model: z.string(),
  chunk_seq: z.number().int().nullable().optional(),
  chunk_recv_wall: z.number().nullable().optional(),
  final_recv_perf: z.number().nullable().optional(),
  publish_wall: z.number().nullable().optional(),
  e2e_ms: z.number().nullable().optional(),
  late: z.boolean().default(false).optional(),
});
export type TranscriptFinalPayload = z.infer<typeof TranscriptFinalPayloadSchema>;

export const TranscriptInterimPayloadSchema = z.object({
  speaker_id: z.string(),
  speaker_name: z.string().nullable().optional(),
  channel: z.number().int().nullable().optional(),
  text: z.string(),
  start_ms: z.number().int(),
  end_ms: z.number().int(),
  start_epoch_ms: z.number().int(),
  confidence: z.number(),
  language: z.string(),
  provider: z.string(),
  model: z.string(),
  is_stable: z.boolean().default(false),
});
export type TranscriptInterimPayload = z.infer<typeof TranscriptInterimPayloadSchema>;

export const TranscriptServiceDegradedPayloadSchema = z.object({
  state: DegradedStateSchema,
  reason: DegradedReasonSchema,
  since_ms: z.number().int(),
  elapsed_s: z.number().optional(),
});
export type TranscriptServiceDegradedPayload = z.infer<
  typeof TranscriptServiceDegradedPayloadSchema
>;

export const AudioGapPayloadSchema = z.object({
  from_seq: z.number().int(),
  to_seq: z.number().int(),
  gap_chunks: z.number().int(),
  capture_start_ms: z.number().int(),
  capture_end_ms: z.number().int(),
});
export type AudioGapPayload = z.infer<typeof AudioGapPayloadSchema>;

export const EventEnvelopeSchema = z.object({
  event_type: z.string(),
  meeting_id: z.string(),
  tenant_id: z.string(),
  ts_ms: z.number().int(),
  trace_id: z.string().optional(),
  payload: z.record(z.unknown()),
});
export type EventEnvelope = z.infer<typeof EventEnvelopeSchema>;

export function computeUtteranceId(
  meetingId: string,
  channel: number | null | undefined,
  speakerId: string,
  startMs: number,
): string {
  const chStr = channel !== null && channel !== undefined ? String(channel) : "0";
  const spkStr = speakerId || "spk_0";
  const key = `${meetingId}|${chStr}|${spkStr}|${startMs}`;
  return createHash("sha256").update(key).digest("hex").slice(0, 24);
}

export const ACTIVE_MEETINGS_SET_KEY = "mm:system:active_meetings";

export function activeMeetingsSetKey(): string {
  return ACTIVE_MEETINGS_SET_KEY;
}
