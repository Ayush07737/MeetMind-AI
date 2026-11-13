import { readFileSync } from "fs";
import { resolve } from "path";
import { describe, expect, it } from "vitest";
import {
  DecisionLifecycleGraphSchema,
  ENTITY_SUBTYPES,
  NODE_TYPES,
  RELATIONSHIP_TYPES,
  tenantNodeLabel,
  validateGraphLifecycle,
} from "../src/graph.js";

const FIXTURES_DIR = resolve(__dirname, "../fixtures/graph");
const GRAPH_SRC_PATH = resolve(__dirname, "../src/graph.ts");

describe("Decision Genome Graph Schemas (Q8)", () => {
  it("validates all 13 node types, 5 Entity subtypes, and 10 relationship types", () => {
    expect(NODE_TYPES).toHaveLength(13);
    expect(ENTITY_SUBTYPES).toHaveLength(5);
    expect(RELATIONSHIP_TYPES).toHaveLength(10);

    const raw = readFileSync(resolve(FIXTURES_DIR, "all_nodes_and_edges.json"), "utf-8");
    const parsed = DecisionLifecycleGraphSchema.parse(JSON.parse(raw));
    const check = validateGraphLifecycle(parsed);
    expect(check.valid).toBe(true);
    expect(check.errors).toEqual([]);

    const nodeTypes = new Set(parsed.nodes.map((n) => n.node_type));
    expect(nodeTypes).toEqual(new Set(NODE_TYPES));

    const entitySubtypes = new Set(
      parsed.nodes.filter((n) => n.node_type === "Entity").map((n) => n.subtype),
    );
    expect(entitySubtypes).toEqual(new Set(ENTITY_SUBTYPES));

    const relTypes = new Set(parsed.edges.map((e) => e.relationship_type));
    expect(relTypes).toEqual(new Set(RELATIONSHIP_TYPES));
  });

  it("validates engineering decision lifecycle fixture end to end", () => {
    const raw = readFileSync(resolve(FIXTURES_DIR, "engineering_decision_lifecycle.json"), "utf-8");
    const parsed = DecisionLifecycleGraphSchema.parse(JSON.parse(raw));
    const check = validateGraphLifecycle(parsed);
    expect(check.valid).toBe(true);

    const roundTrip = DecisionLifecycleGraphSchema.parse(JSON.parse(JSON.stringify(parsed)));
    expect(roundTrip).toEqual(parsed);
  });

  it("enforces tenant isolation and label namespacing", () => {
    expect(tenantNodeLabel("FinOps_Eng", "Decision")).toBe("finops_eng_Decision");
    expect(() => tenantNodeLabel("bad-tenant!", "Decision")).toThrow();

    const raw = JSON.parse(
      readFileSync(resolve(FIXTURES_DIR, "engineering_decision_lifecycle.json"), "utf-8"),
    );
    raw.nodes[0].tenant_id = "other_tenant";
    const parsed = DecisionLifecycleGraphSchema.parse(raw);
    const check = validateGraphLifecycle(parsed);
    expect(check.valid).toBe(false);
    expect(check.errors.some((e) => e.toLowerCase().includes("tenant mismatch"))).toBe(true);
  });

  it("contains zero Neo4j driver imports or Cypher code", () => {
    const src = readFileSync(GRAPH_SRC_PATH, "utf-8").toLowerCase();
    expect(src).not.toContain("neo4j-driver");
    expect(src).not.toContain("match (");
    expect(src).not.toContain("merge (");
  });
});
