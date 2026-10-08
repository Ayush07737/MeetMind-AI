/**
 * MeetMind AI — Shared Schemas
 *
 * Zod schemas and TypeScript types shared across the extension, frontend,
 * and any other TypeScript consumers. These mirror the Pydantic models
 * defined in the Python backend services.
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

// ── WebSocket Close Codes ───────────────────────────────────────────────────

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
