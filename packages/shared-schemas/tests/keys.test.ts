import { describe, it, expect } from "vitest";
import {
  audioStreamKey,
  transcriptStreamKey,
  eventsStreamKey,
  outStreamKey,
  meetingSessionKey,
  writerLeaseKey,
  asrOwnerLeaseKey,
  lastSeqKey,
  listenersKey,
  metricsKey,
  wsTicketKey,
  userConnectionsKey,
  tenantConnectionsKey,
  lifecycleStreamKey,
  janitorLockKey,
  validateTenantSegment,
  validateKeySegment,
} from "../src/index.js";

describe("Redis Key Schemes", () => {
  const tenant = "tenant_test1";
  const meeting = "mtg_abc123";

  it("formats stream and hash keys correctly", () => {
    expect(audioStreamKey(tenant, meeting)).toBe("mm:tenant_test1:audio:mtg_abc123");
    expect(transcriptStreamKey(tenant, meeting)).toBe("mm:tenant_test1:transcript:mtg_abc123");
    expect(eventsStreamKey(tenant, meeting)).toBe("mm:tenant_test1:events:mtg_abc123");
    expect(outStreamKey(tenant, meeting)).toBe("mm:tenant_test1:out:mtg_abc123");
    expect(meetingSessionKey(tenant, meeting)).toBe("mm:tenant_test1:meeting:mtg_abc123");
    expect(writerLeaseKey(tenant, meeting)).toBe("mm:tenant_test1:writer:mtg_abc123");
    expect(asrOwnerLeaseKey(tenant, meeting)).toBe("mm:tenant_test1:asr_owner:mtg_abc123");
    expect(lastSeqKey(tenant, meeting)).toBe("mm:tenant_test1:seq:mtg_abc123");
    expect(listenersKey(tenant, meeting)).toBe("mm:tenant_test1:listeners:mtg_abc123");
    expect(metricsKey(tenant, meeting)).toBe("mm:tenant_test1:metrics:mtg_abc123");
  });

  it("formats ticket and connection keys correctly", () => {
    expect(wsTicketKey("tkt_12345")).toBe("mm:ticket:tkt_12345");
    expect(userConnectionsKey(tenant, "user_123")).toBe("mm:tenant_test1:userconns:user_123");
    expect(tenantConnectionsKey(tenant)).toBe("mm:tenant_test1:conns");
    expect(lifecycleStreamKey()).toBe("mm:system:lifecycle");
    expect(janitorLockKey()).toBe("mm:global:janitor:lease");
  });

  it("normalizes and validates tenant segments", () => {
    expect(validateTenantSegment("  TENANT_A  ")).toBe("tenant_a");
    expect(() => validateTenantSegment("")).toThrow();
    expect(() => validateTenantSegment("bad tenant with spaces")).toThrow();
  });

  it("validates key segments", () => {
    expect(validateKeySegment("valid-id_123")).toBe("valid-id_123");
    expect(() => validateKeySegment("")).toThrow();
    expect(() => validateKeySegment("bad/id")).toThrow();
  });
});
