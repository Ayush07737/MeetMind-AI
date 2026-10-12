/**
 * MeetMind AI — Canonical Redis Key Schemes.
 *
 * All tenant-scoped keys follow the pattern:
 *   mm:{tenant}:{resource}:{meeting}
 *
 * Matches Python implementation in meetmind_schemas.keys.
 */

export const TENANT_SEGMENT_PATTERN = /^[a-z0-9_]{1,40}$/;
export const KEY_SEGMENT_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

export const LIFECYCLE_STREAM_KEY = "mm:system:lifecycle";
export const JANITOR_LEASE_KEY = "mm:global:janitor:lease";

export function validateTenantSegment(tenant: string): string {
  if (!tenant || !tenant.trim()) {
    throw new Error("Invalid tenant: must not be empty");
  }
  const norm = tenant.trim().toLowerCase();
  if (!TENANT_SEGMENT_PATTERN.test(norm)) {
    throw new Error(
      `Invalid tenant: '${tenant}'. Normalized '${norm}' must match ^[a-z0-9_]{1,40}$`,
    );
  }
  return norm;
}

export function validateKeySegment(val: string, name: string = "identifier"): string {
  if (!val || !KEY_SEGMENT_PATTERN.test(val)) {
    throw new Error(`Invalid ${name}: '${val}'. Must match ^[A-Za-z0-9_-]{1,64}$`);
  }
  return val;
}

export function audioStreamKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:audio:${m}`;
}

export function transcriptStreamKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:transcript:${m}`;
}

export function eventsStreamKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:events:${m}`;
}

export function outStreamKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:out:${m}`;
}

export function meetingSessionKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:meeting:${m}`;
}

export function writerLeaseKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:writer:${m}`;
}

export function asrOwnerLeaseKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:asr_owner:${m}`;
}

export const asrOwnerKey = asrOwnerLeaseKey;

export function lastSeqKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:seq:${m}`;
}

export function listenersKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:listeners:${m}`;
}

export function metricsKey(tenant: string, meetingId: string): string {
  const t = validateTenantSegment(tenant);
  const m = validateKeySegment(meetingId, "meetingId");
  return `mm:${t}:metrics:${m}`;
}

export function wsTicketKey(ticket: string): string {
  const t = validateKeySegment(ticket, "ticket");
  return `mm:ticket:${t}`;
}

export function userConnectionsKey(tenant: string, userId: string): string {
  const t = validateTenantSegment(tenant);
  const u = validateKeySegment(userId, "userId");
  return `mm:${t}:userconns:${u}`;
}

export function tenantConnectionsKey(tenant: string): string {
  const t = validateTenantSegment(tenant);
  return `mm:${t}:conns`;
}

export function lifecycleStreamKey(): string {
  return LIFECYCLE_STREAM_KEY;
}

export function janitorLockKey(): string {
  return JANITOR_LEASE_KEY;
}
