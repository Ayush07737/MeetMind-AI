import { describe, it, expect } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { AuditEventSchema, ConsentRecordSchema } from "../src/index.js";

const FIXTURES_DIR = path.resolve(__dirname, "../fixtures");
const VALID_DIR = path.join(FIXTURES_DIR, "valid");
const INVALID_DIR = path.join(FIXTURES_DIR, "invalid");

describe("Contract Tests (Zod)", () => {
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
});
