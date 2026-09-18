import { describe, it, expect } from "vitest";
import { ConsentRecordSchema, AuditEventSchema, TenantIdSchema } from "../src/index";

describe("shared-schemas", () => {
  it("validates a TenantId", () => {
    expect(TenantIdSchema.parse("default")).toBe("default");
  });

  it("rejects an empty TenantId", () => {
    expect(() => TenantIdSchema.parse("")).toThrow();
  });

  it("validates a ConsentRecord", () => {
    const record = {
      tenantId: "default",
      userId: "user_123",
      meetingId: "meeting_456",
      consentType: "audio_capture" as const,
      grantedAt: new Date().toISOString(),
      revokedAt: null,
    };
    expect(ConsentRecordSchema.parse(record)).toEqual(record);
  });

  it("validates an AuditEvent", () => {
    const event = {
      tenantId: "default",
      userId: "user_123",
      eventType: "consent_confirmed",
      payload: { meetingId: "meeting_456" },
      createdAt: new Date().toISOString(),
    };
    expect(AuditEventSchema.parse(event)).toEqual(event);
  });
});
