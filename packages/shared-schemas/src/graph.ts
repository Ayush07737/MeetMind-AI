/**
 * MeetMind AI — Decision Genome Graph Schemas (Q8)
 *
 * Pure Zod schemas and TypeScript types for Decision Genome nodes,
 * engineering Entity subtypes, edges, and end-to-end lifecycle validation.
 * Zero Neo4j driver imports or Cypher query strings.
 */

import { z } from "zod";
import { ClaimTypeSchema, ConfidenceBandSchema, SourceTypeSchema } from "./cards.js";

export const NODE_TYPES = [
  "Statement",
  "Claim",
  "Evidence",
  "Assumption",
  "Decision",
  "Approval",
  "Action",
  "Outcome",
  "Meeting",
  "Participant",
  "Entity",
  "Commitment",
  "RiskFlag",
] as const;

export const GraphNodeTypeSchema = z.enum(NODE_TYPES);
export type GraphNodeType = z.infer<typeof GraphNodeTypeSchema>;

export const ENTITY_SUBTYPES = [
  "Ticket",
  "PullRequest",
  "Deployment",
  "Incident",
  "Service",
] as const;

export const EntitySubtypeSchema = z.enum(ENTITY_SUBTYPES);
export type EntitySubtype = z.infer<typeof EntitySubtypeSchema>;

export const RELATIONSHIP_TYPES = [
  "SAID_IN",
  "ASSERTS",
  "SUPPORTED_BY",
  "ASSUMES",
  "LED_TO",
  "APPROVED_BY",
  "OWNED_BY",
  "EXECUTED_AS",
  "RESULTED_IN",
  "REFERENCES",
] as const;

export const GraphRelationshipTypeSchema = z.enum(RELATIONSHIP_TYPES);
export type GraphRelationshipType = z.infer<typeof GraphRelationshipTypeSchema>;

export const DecisionStatusSchema = z.enum([
  "proposed",
  "agreed",
  "disputed",
  "approved",
  "executed",
  "reviewed",
]);
export type DecisionStatus = z.infer<typeof DecisionStatusSchema>;

const TENANT_REGEX = /^[a-z0-9_]{1,40}$/;

export function tenantNodeLabel(tenantId: string, nodeType: GraphNodeType): string {
  const norm = tenantId.trim().toLowerCase();
  if (!TENANT_REGEX.test(norm)) {
    throw new Error(`Invalid tenant_id '${tenantId}' for graph namespace`);
  }
  return `${norm}_${nodeType}`;
}

const GraphNodeBaseShape = {
  node_id: z.string().min(1),
  tenant_id: z.string().min(1),
  created_at: z.union([z.number(), z.string()]),
  source_event_ids: z.array(z.string()).default([]),
  restricted: z.boolean().default(false),
  visible_roles: z.array(z.string()).default([]),
};

export const StatementNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Statement"),
    meeting_id: z.string().min(1),
    utterance_id: z.string().min(1),
    speaker_id: z.string().min(1),
    text: z.string().min(1),
    start_ms: z.number().int(),
    end_ms: z.number().int(),
    asr_confidence: z.number(),
  })
  .strict();
export type StatementNode = z.infer<typeof StatementNodeSchema>;

export const ClaimNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Claim"),
    claim_id: z.string().min(1),
    card_id: z.string().nullable().optional(),
    text: z.string().min(1),
    claim_type: ClaimTypeSchema,
    value: z.union([z.string(), z.number()]).nullable().optional(),
    unit: z.string().nullable().optional(),
    status: z.enum(["verified", "inferred"]),
    confidence_band: ConfidenceBandSchema.nullable().optional(),
  })
  .strict();
export type ClaimNode = z.infer<typeof ClaimNodeSchema>;

export const EvidenceNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Evidence"),
    evidence_id: z.string().min(1),
    source_type: SourceTypeSchema,
    source_id: z.string().min(1),
    uri: z.string().nullable().optional(),
    span: z.string().min(1),
    retrieved_at: z.union([z.number(), z.string()]),
    as_of: z.union([z.number(), z.string()]).nullable().optional(),
  })
  .strict();
export type EvidenceNode = z.infer<typeof EvidenceNodeSchema>;

export const AssumptionNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Assumption"),
    assumption_id: z.string().min(1),
    text: z.string().min(1),
    supported: z.boolean().default(false),
    author_id: z.string().nullable().optional(),
  })
  .strict();
export type AssumptionNode = z.infer<typeof AssumptionNodeSchema>;

export const ExpectedOutcomeSchema = z
  .object({
    metric: z.string().min(1),
    target: z.string().min(1),
    review_date: z.string().min(1),
    owner: z.string().min(1),
  })
  .strict();
export type ExpectedOutcome = z.infer<typeof ExpectedOutcomeSchema>;

export const DecisionNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Decision"),
    decision_id: z.string().min(1),
    summary: z.string().min(1),
    status: DecisionStatusSchema,
    expected_outcome: ExpectedOutcomeSchema,
    required_approvers: z.array(z.string()).default([]),
    proposer_id: z.string().nullable().optional(),
  })
  .strict();
export type DecisionNode = z.infer<typeof DecisionNodeSchema>;

export const ApprovalNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Approval"),
    approval_id: z.string().min(1),
    approver_id: z.string().min(1),
    action_class: z.enum(["read_only", "manager_approval", "ops_approval"]),
    decision: z.enum(["approved", "rejected", "pending"]),
    decided_at: z.union([z.number(), z.string()]),
    audit_event_id: z.string().min(1),
  })
  .strict();
export type ApprovalNode = z.infer<typeof ApprovalNodeSchema>;

export const ActionNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Action"),
    action_id: z.string().min(1),
    action_type: z.string().min(1),
    target_system: z.enum(["github", "jira", "linear", "slack", "internal"]),
    external_ref: z.string().min(1),
    status: z.enum(["planned", "pending_approval", "executed", "failed", "cancelled"]),
    owner_id: z.string().nullable().optional(),
    due_date: z.string().nullable().optional(),
  })
  .strict();
export type ActionNode = z.infer<typeof ActionNodeSchema>;

export const OutcomeNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Outcome"),
    outcome_id: z.string().min(1),
    metric: z.string().min(1),
    observed_value: z.union([z.string(), z.number()]),
    target_value: z.union([z.string(), z.number()]),
    met: z.boolean(),
    measured_at: z.union([z.number(), z.string()]),
    evidence_uri: z.string().nullable().optional(),
  })
  .strict();
export type OutcomeNode = z.infer<typeof OutcomeNodeSchema>;

export const MeetingNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Meeting"),
    meeting_id: z.string().min(1),
    title: z.string().min(1),
    started_at: z.union([z.number(), z.string()]),
    ended_at: z.union([z.number(), z.string()]).nullable().optional(),
    domain_pack: z.string().default("engineering"),
  })
  .strict();
export type MeetingNode = z.infer<typeof MeetingNodeSchema>;

export const ParticipantNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Participant"),
    participant_id: z.string().min(1),
    display_name: z.string().min(1),
    role: z.string().min(1),
    speaker_id: z.string().nullable().optional(),
  })
  .strict();
export type ParticipantNode = z.infer<typeof ParticipantNodeSchema>;

export const EntityNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Entity"),
    entity_id: z.string().min(1),
    subtype: EntitySubtypeSchema,
    canonical_key: z.string().min(1),
    title: z.string().min(1),
    status: z.string().nullable().optional(),
    uri: z.string().nullable().optional(),
  })
  .strict();
export type EntityNode = z.infer<typeof EntityNodeSchema>;

export const CommitmentNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("Commitment"),
    commitment_id: z.string().min(1),
    description: z.string().min(1),
    owner_id: z.string().nullable().optional(),
    due_date: z.string().nullable().optional(),
    status: z.enum(["open", "completed", "blocked", "cancelled"]).default("open"),
  })
  .strict();
export type CommitmentNode = z.infer<typeof CommitmentNodeSchema>;

export const RiskFlagNodeSchema = z
  .object({
    ...GraphNodeBaseShape,
    node_type: z.literal("RiskFlag"),
    risk_id: z.string().min(1),
    category: z.string().min(1),
    severity: z.enum(["low", "medium", "high", "critical"]),
    summary: z.string().min(1),
    resolved: z.boolean().default(false),
  })
  .strict();
export type RiskFlagNode = z.infer<typeof RiskFlagNodeSchema>;

export const GraphNodeSchema = z.discriminatedUnion("node_type", [
  StatementNodeSchema,
  ClaimNodeSchema,
  EvidenceNodeSchema,
  AssumptionNodeSchema,
  DecisionNodeSchema,
  ApprovalNodeSchema,
  ActionNodeSchema,
  OutcomeNodeSchema,
  MeetingNodeSchema,
  ParticipantNodeSchema,
  EntityNodeSchema,
  CommitmentNodeSchema,
  RiskFlagNodeSchema,
]);
export type GraphNode = z.infer<typeof GraphNodeSchema>;

export const GraphEdgeSchema = z
  .object({
    edge_id: z.string().min(1),
    relationship_type: GraphRelationshipTypeSchema,
    source_node_id: z.string().min(1),
    target_node_id: z.string().min(1),
    tenant_id: z.string().min(1),
    created_at: z.union([z.number(), z.string()]),
    source_event_ids: z.array(z.string()).default([]),
    restricted: z.boolean().default(false),
    properties: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).default({}),
  })
  .strict();
export type GraphEdge = z.infer<typeof GraphEdgeSchema>;

export const DecisionLifecycleGraphSchema = z
  .object({
    tenant_id: z.string().min(1),
    meeting_id: z.string().min(1),
    nodes: z.array(GraphNodeSchema).default([]),
    edges: z.array(GraphEdgeSchema).default([]),
  })
  .strict();
export type DecisionLifecycleGraph = z.infer<typeof DecisionLifecycleGraphSchema>;

export function validateGraphLifecycle(graph: DecisionLifecycleGraph): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];
  const nodeIds = new Set<string>();

  for (const node of graph.nodes) {
    if (nodeIds.has(node.node_id)) {
      errors.push(`Duplicate node_id '${node.node_id}'`);
    }
    nodeIds.add(node.node_id);

    if (node.tenant_id !== graph.tenant_id) {
      errors.push(
        `Tenant mismatch on node '${node.node_id}': '${node.tenant_id}' != '${graph.tenant_id}'`,
      );
    }
    if (node.restricted && (!node.visible_roles || node.visible_roles.length === 0)) {
      errors.push(`Restricted node '${node.node_id}' must declare non-empty visible_roles`);
    }
    if (node.node_type === "Claim" && node.node_id !== node.claim_id) {
      errors.push(`ClaimNode '${node.node_id}' must reuse claim_id '${node.claim_id}'`);
    }
    if (node.node_type === "Evidence" && node.node_id !== node.evidence_id) {
      errors.push(`EvidenceNode '${node.node_id}' must reuse evidence_id '${node.evidence_id}'`);
    }
  }

  const edgeIds = new Set<string>();
  for (const edge of graph.edges) {
    if (edgeIds.has(edge.edge_id)) {
      errors.push(`Duplicate edge_id '${edge.edge_id}'`);
    }
    edgeIds.add(edge.edge_id);

    if (edge.tenant_id !== graph.tenant_id) {
      errors.push(
        `Tenant mismatch on edge '${edge.edge_id}': '${edge.tenant_id}' != '${graph.tenant_id}'`,
      );
    }
    if (!nodeIds.has(edge.source_node_id)) {
      errors.push(
        `Edge '${edge.edge_id}' references missing source_node_id '${edge.source_node_id}'`,
      );
    }
    if (!nodeIds.has(edge.target_node_id)) {
      errors.push(
        `Edge '${edge.edge_id}' references missing target_node_id '${edge.target_node_id}'`,
      );
    }
  }

  return { valid: errors.length === 0, errors };
}
