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

export const ConsentRecordSchema = z.object({
  tenantId: TenantIdSchema,
  userId: z.string().min(1),
  meetingId: z.string().min(1),
  consentType: z.enum(["audio_capture", "transcript_storage", "ai_analysis"]),
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
