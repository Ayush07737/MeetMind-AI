"""Unit tests for Decision Genome Graph Schemas (Q8)."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from meetmind_schemas.graph import (
    ENTITY_SUBTYPES,
    NODE_TYPES,
    RELATIONSHIP_TYPES,
    DecisionLifecycleGraph,
    tenant_node_label,
    validate_graph_lifecycle,
)

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "graph"
SCHEMA_PY_PATH = Path(__file__).resolve().parents[1] / "meetmind_schemas" / "graph.py"


def test_all_node_entity_and_relationship_types_covered():
    assert len(NODE_TYPES) == 13
    assert len(ENTITY_SUBTYPES) == 5
    assert len(RELATIONSHIP_TYPES) == 10

    raw = json.loads((FIXTURES_DIR / "all_nodes_and_edges.json").read_text(encoding="utf-8"))
    graph = DecisionLifecycleGraph.model_validate(raw)
    ok, errors = validate_graph_lifecycle(graph)
    assert ok is True, f"Unexpected errors: {errors}"

    present_node_types = {n.node_type for n in graph.nodes}
    assert present_node_types == set(NODE_TYPES)

    present_entity_subtypes = {n.subtype for n in graph.nodes if n.node_type == "Entity"}
    assert present_entity_subtypes == set(ENTITY_SUBTYPES)

    present_rel_types = {e.relationship_type for e in graph.edges}
    assert present_rel_types == set(RELATIONSHIP_TYPES)


def test_engineering_decision_lifecycle_end_to_end():
    raw = json.loads(
        (FIXTURES_DIR / "engineering_decision_lifecycle.json").read_text(encoding="utf-8")
    )
    graph = DecisionLifecycleGraph.model_validate(raw)
    ok, errors = validate_graph_lifecycle(graph)
    assert ok is True, f"Lifecycle validation errors: {errors}"

    # Verify JSON round-trip preserves exact validated model
    reparsed = DecisionLifecycleGraph.model_validate_json(graph.model_dump_json())
    assert reparsed == graph


def test_tenant_node_label_and_cross_tenant_rejection():
    assert tenant_node_label("FinOps_Eng", "Decision") == "finops_eng_Decision"
    with pytest.raises(ValueError):
        tenant_node_label("bad-tenant!", "Decision")

    raw = json.loads(
        (FIXTURES_DIR / "engineering_decision_lifecycle.json").read_text(encoding="utf-8")
    )
    raw["nodes"][0]["tenant_id"] = "other_tenant"
    graph = DecisionLifecycleGraph.model_validate(raw)
    ok, errors = validate_graph_lifecycle(graph)
    assert ok is False
    assert any("tenant mismatch" in e.lower() for e in errors)


def test_claim_and_evidence_id_reuse_and_missing_edge_rejection():
    raw = json.loads(
        (FIXTURES_DIR / "engineering_decision_lifecycle.json").read_text(encoding="utf-8")
    )
    # Break Claim ID reuse
    for node in raw["nodes"]:
        if node["node_type"] == "Claim":
            node["node_id"] = "different_node_id"
    # Break edge reference
    raw["edges"].append(
        {
            "edge_id": "e_broken",
            "relationship_type": "REFERENCES",
            "source_node_id": "stmt_life_01",
            "target_node_id": "non_existent_node",
            "tenant_id": "finops_eng",
            "created_at": 1793610120000,
            "source_event_ids": ["evt_x"],
            "restricted": False,
            "properties": {},
        }
    )
    graph = DecisionLifecycleGraph.model_validate(raw)
    ok, errors = validate_graph_lifecycle(graph)
    assert ok is False
    assert any("reuse claim_id" in e.lower() for e in errors)
    assert any("missing target_node_id" in e.lower() for e in errors)


def test_no_neo4j_driver_or_cypher_imports():
    source = SCHEMA_PY_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "neo4j" not in alias.name.lower()
        elif isinstance(node, ast.ImportFrom) and node.module:
            assert "neo4j" not in node.module.lower()
    lowered = source.lower()
    for cypher_kw in ("create (", "merge (", "match (", "detach delete"):
        assert cypher_kw not in lowered
