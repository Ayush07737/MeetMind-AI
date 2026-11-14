"""MeetMind AI — Decision Genome Graph Schemas (Q8).

Pure data contracts for Decision Genome nodes, engineering Entity subtypes,
edges, tenant label prefixing helpers, and end-to-end lifecycle validation.

Contains ZERO Neo4j driver imports or Cypher query strings (schemas only).
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .cards import ClaimType, ConfidenceBand, SourceType

NODE_TYPES: tuple[str, ...] = (
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
)

GraphNodeType = Literal[
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
]

ENTITY_SUBTYPES: tuple[str, ...] = (
    "Ticket",
    "PullRequest",
    "Deployment",
    "Incident",
    "Service",
)

EntitySubtype = Literal[
    "Ticket",
    "PullRequest",
    "Deployment",
    "Incident",
    "Service",
]

RELATIONSHIP_TYPES: tuple[str, ...] = (
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
)

GraphRelationshipType = Literal[
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
]

DecisionStatus = Literal[
    "proposed",
    "agreed",
    "disputed",
    "approved",
    "executed",
    "reviewed",
]

_TENANT_REGEX = re.compile(r"^[a-z0-9_]{1,40}$")


def tenant_node_label(tenant_id: str, node_type: GraphNodeType) -> str:
    """Compute namespace-prefixed Neo4j label per TenantRouter isolation rules."""
    norm = tenant_id.strip().lower()
    if not _TENANT_REGEX.match(norm):
        raise ValueError(f"Invalid tenant_id '{tenant_id}' for graph namespace")
    if node_type not in NODE_TYPES:
        raise ValueError(f"Unknown graph node_type '{node_type}'")
    return f"{norm}_{node_type}"


class GraphNodeBase(BaseModel):
    """Common envelope fields present on every Decision Genome node."""

    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    created_at: int | str
    source_event_ids: list[str] = Field(default_factory=list)
    restricted: bool = False
    visible_roles: list[str] = Field(default_factory=list)


class StatementNode(GraphNodeBase):
    """Spoken utterance segment captured in a meeting."""

    node_type: Literal["Statement"] = "Statement"
    meeting_id: str = Field(min_length=1)
    utterance_id: str = Field(min_length=1)
    speaker_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    start_ms: int
    end_ms: int
    asr_confidence: float


class ClaimNode(GraphNodeBase):
    """Factual or inferred claim reusing InsightCard claim_id as node_id."""

    node_type: Literal["Claim"] = "Claim"
    claim_id: str = Field(min_length=1)
    card_id: str | None = None
    text: str = Field(min_length=1)
    claim_type: ClaimType
    value: str | float | None = None
    unit: str | None = None
    status: Literal["verified", "inferred"]
    confidence_band: ConfidenceBand | None = None


class EvidenceNode(GraphNodeBase):
    """External or internal evidence snapshot reusing evidence_id/source_id."""

    node_type: Literal["Evidence"] = "Evidence"
    evidence_id: str = Field(min_length=1)
    source_type: SourceType
    source_id: str = Field(min_length=1)
    uri: str | None = None
    span: str = Field(min_length=1)
    retrieved_at: int | str
    as_of: int | str | None = None


class AssumptionNode(GraphNodeBase):
    """Explicit or latent assumption underlying a decision."""

    node_type: Literal["Assumption"] = "Assumption"
    assumption_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    supported: bool = False
    author_id: str | None = None


class ExpectedOutcome(BaseModel):
    """Measurable outcome contract attached to every Decision node (Q8 / §22)."""

    model_config = ConfigDict(extra="forbid")

    metric: str = Field(min_length=1)
    target: str = Field(min_length=1)
    review_date: str = Field(min_length=1)
    owner: str = Field(min_length=1)


class DecisionNode(GraphNodeBase):
    """Engineering decision node with accountability and outcome target."""

    node_type: Literal["Decision"] = "Decision"
    decision_id: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    status: DecisionStatus
    expected_outcome: ExpectedOutcome
    required_approvers: list[str] = Field(default_factory=list)
    proposer_id: str | None = None


class ApprovalNode(GraphNodeBase):
    """Human approval recorded by the Trust Gate audit trail."""

    node_type: Literal["Approval"] = "Approval"
    approval_id: str = Field(min_length=1)
    approver_id: str = Field(min_length=1)
    action_class: Literal["read_only", "manager_approval", "ops_approval"]
    decision: Literal["approved", "rejected", "pending"]
    decided_at: int | str
    audit_event_id: str = Field(min_length=1)


class ActionNode(GraphNodeBase):
    """Execution artifact tracked across engineering systems."""

    node_type: Literal["Action"] = "Action"
    action_id: str = Field(min_length=1)
    action_type: str = Field(min_length=1)
    target_system: Literal["github", "jira", "linear", "slack", "internal"]
    external_ref: str = Field(min_length=1)
    status: Literal["planned", "pending_approval", "executed", "failed", "cancelled"]
    owner_id: str | None = None
    due_date: str | None = None


class OutcomeNode(GraphNodeBase):
    """Observed post-deployment metric outcome linked back to a Decision."""

    node_type: Literal["Outcome"] = "Outcome"
    outcome_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    observed_value: str | float
    target_value: str | float
    met: bool
    measured_at: int | str
    evidence_uri: str | None = None


class MeetingNode(GraphNodeBase):
    """Meeting session metadata node."""

    node_type: Literal["Meeting"] = "Meeting"
    meeting_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    started_at: int | str
    ended_at: int | str | None = None
    domain_pack: str = "engineering"


class ParticipantNode(GraphNodeBase):
    """Meeting participant node."""

    node_type: Literal["Participant"] = "Participant"
    participant_id: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    role: str = Field(min_length=1)
    speaker_id: str | None = None


class EntityNode(GraphNodeBase):
    """Engineering artifact entity node (strictly metadata, never source code)."""

    node_type: Literal["Entity"] = "Entity"
    entity_id: str = Field(min_length=1)
    subtype: EntitySubtype
    canonical_key: str = Field(min_length=1)
    title: str = Field(min_length=1)
    status: str | None = None
    uri: str | None = None


class CommitmentNode(GraphNodeBase):
    """Explicit engineering commitment assigned during discussion."""

    node_type: Literal["Commitment"] = "Commitment"
    commitment_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    owner_id: str | None = None
    due_date: str | None = None
    status: Literal["open", "completed", "blocked", "cancelled"] = "open"


class RiskFlagNode(GraphNodeBase):
    """Detected risk or unresolved disagreement signal."""

    node_type: Literal["RiskFlag"] = "RiskFlag"
    risk_id: str = Field(min_length=1)
    category: str = Field(min_length=1)
    severity: Literal["low", "medium", "high", "critical"]
    summary: str = Field(min_length=1)
    resolved: bool = False


GraphNode = Annotated[
    StatementNode
    | ClaimNode
    | EvidenceNode
    | AssumptionNode
    | DecisionNode
    | ApprovalNode
    | ActionNode
    | OutcomeNode
    | MeetingNode
    | ParticipantNode
    | EntityNode
    | CommitmentNode
    | RiskFlagNode,
    Field(discriminator="node_type"),
]


class GraphEdge(BaseModel):
    """Directed relationship connecting two Decision Genome nodes."""

    model_config = ConfigDict(extra="forbid")

    edge_id: str = Field(min_length=1)
    relationship_type: GraphRelationshipType
    source_node_id: str = Field(min_length=1)
    target_node_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    created_at: int | str
    source_event_ids: list[str] = Field(default_factory=list)
    restricted: bool = False
    properties: dict[str, str | int | float | bool] = Field(default_factory=dict)


class DecisionLifecycleGraph(BaseModel):
    """Self-contained graph bundle representing a verified engineering lifecycle."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1)
    meeting_id: str = Field(min_length=1)
    nodes: list[GraphNode] = Field(default_factory=list)
    edges: list[GraphEdge] = Field(default_factory=list)


def validate_graph_lifecycle(graph: DecisionLifecycleGraph) -> tuple[bool, list[str]]:
    """Validate structural, tenant-isolation, and ID-reuse invariants on a graph bundle."""
    errors: list[str] = []
    node_ids: set[str] = set()

    for node in graph.nodes:
        if node.node_id in node_ids:
            errors.append(f"Duplicate node_id '{node.node_id}'")
        node_ids.add(node.node_id)

        if node.tenant_id != graph.tenant_id:
            errors.append(
                f"Tenant mismatch on node '{node.node_id}': "
                f"{node.tenant_id!r} != {graph.tenant_id!r}"
            )
        if node.restricted and not node.visible_roles:
            errors.append(f"Restricted node '{node.node_id}' must declare non-empty visible_roles")
        if isinstance(node, ClaimNode) and node.node_id != node.claim_id:
            errors.append(f"ClaimNode '{node.node_id}' must reuse claim_id '{node.claim_id}'")
        if isinstance(node, EvidenceNode) and node.node_id != node.evidence_id:
            errors.append(
                f"EvidenceNode '{node.node_id}' must reuse evidence_id '{node.evidence_id}'"
            )

    edge_ids: set[str] = set()
    for edge in graph.edges:
        if edge.edge_id in edge_ids:
            errors.append(f"Duplicate edge_id '{edge.edge_id}'")
        edge_ids.add(edge.edge_id)

        if edge.tenant_id != graph.tenant_id:
            errors.append(
                f"Tenant mismatch on edge '{edge.edge_id}': "
                f"{edge.tenant_id!r} != {graph.tenant_id!r}"
            )
        if edge.source_node_id not in node_ids:
            errors.append(
                f"Edge '{edge.edge_id}' references missing source_node_id '{edge.source_node_id}'"
            )
        if edge.target_node_id not in node_ids:
            errors.append(
                f"Edge '{edge.edge_id}' references missing target_node_id '{edge.target_node_id}'"
            )

    return len(errors) == 0, errors
