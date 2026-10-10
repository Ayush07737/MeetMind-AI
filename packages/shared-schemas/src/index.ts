/**
 * MeetMind AI — Shared Schemas
 *
 * Zod schemas and TypeScript types shared across the extension, frontend,
 * and any other TypeScript consumers. These mirror the Pydantic models
 * defined in meetmind_schemas.events (Python).
 */

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
  "transcript_chunk",
  "intelligence_event",
  "action_required",
  "summary_update",
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

// ── Binary Audio Header Codec (WP5) ─────────────────────────────────────────

export const BINARY_HEADER_SIZE = 18;

export interface AudioHeader {
  version: number;
  flags: number;
  clientSeq: bigint;
  captureTsMs: bigint;
}

export function encodeAudioFrame(
  clientSeq: bigint | number,
  captureTsMs: bigint | number,
  payload: Uint8Array,
  flags: number = 0,
  version: number = 1,
): Uint8Array {
  const buf = new Uint8Array(BINARY_HEADER_SIZE + payload.byteLength);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  view.setUint8(0, version);
  view.setUint8(1, flags);
  view.setBigUint64(2, BigInt(clientSeq), false); // Big-endian
  view.setBigUint64(10, BigInt(captureTsMs), false); // Big-endian
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
  const version = view.getUint8(0);
  if (version !== 1) {
    throw new Error(`Unsupported audio frame version: ${version}, expected 1`);
  }
  const flags = view.getUint8(1);
  const clientSeq = view.getBigUint64(2, false);
  const captureTsMs = view.getBigUint64(10, false);
  const payload = data.subarray(BINARY_HEADER_SIZE);

  return {
    header: { version, flags, clientSeq, captureTsMs },
    payload,
  };
}
