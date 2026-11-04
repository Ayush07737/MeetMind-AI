"""MeetMind AI — Latency Budget Model & Loader (Q2).

Loads and validates `docs/latency/budget.yaml` into strongly-typed Pydantic models.
Shared across backend services and test runners.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class ObservedMetrics(BaseModel):
    """Authoritative observed cloud measurements cited from benchmarks."""

    model_config = ConfigDict(extra="ignore")

    p50_ms: float
    p95_ms: float | None = None
    service_p95_ms: float | None = None
    date: str
    log_path: str
    note: str = ""


class StageBudget(BaseModel):
    """Budget specification for an individual pipeline stage."""

    model_config = ConfigDict(extra="ignore")

    name: str
    owner: str
    clock: str
    target_p50_ms: int | None = None
    target_p95_ms: int | None = None
    aspirational_goal_ms: int | None = None
    status: str = "Accepted"
    observed: ObservedMetrics | None = None
    description: str = ""
    technology_requirement: str = ""


class ClassificationConstraints(BaseModel):
    model_config = ConfigDict(extra="ignore")

    max_p50_ms: int = 200
    max_p95_ms: int = 300
    technology_constraint: str = ""


class MemoryConstraints(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tracemalloc_headroom_mb: float = 10.0
    private_bytes_headroom_mb: float = 40.0
    soak_max_meeting_seconds: int = 3600


class OverheadConstraints(BaseModel):
    model_config = ConfigDict(extra="ignore")

    max_own_overhead_per_stage_ms: float = 25.0


class BudgetConstraints(BaseModel):
    model_config = ConfigDict(extra="ignore")

    classification: ClassificationConstraints = Field(default_factory=ClassificationConstraints)
    memory: MemoryConstraints = Field(default_factory=MemoryConstraints)
    overhead: OverheadConstraints = Field(default_factory=OverheadConstraints)


class SimulatedDelays(BaseModel):
    """Mock delay allocations used for test harnesses and budget checks."""

    model_config = ConfigDict(extra="ignore")

    network_rtt_ms: float = 60.0
    gateway_overhead_ms: float = 5.0
    asr_provider_delay_ms: float = 200.0
    asr_pipeline_overhead_ms: float = 10.0
    classifier_delay_ms: float = 50.0
    retrieval_delay_ms: float = 150.0
    card_publisher_overhead_ms: float = 10.0


class HistoryEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    date: str
    source: str
    interim_p50_ms: float | None = None
    e2e_p50_ms: float | None = None
    service_p95_ms: float | None = None
    ws_rtt_ms: float | None = None
    note: str = ""


class LatencyBudget(BaseModel):
    """Complete MeetMind AI latency & stability budget model."""

    model_config = ConfigDict(extra="ignore")

    version: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    constraints: BudgetConstraints = Field(default_factory=BudgetConstraints)
    stages: dict[str, StageBudget] = Field(default_factory=dict)
    simulated: SimulatedDelays = Field(default_factory=SimulatedDelays)
    history: list[HistoryEntry] = Field(default_factory=list)


def find_budget_yaml_path() -> Path:
    """Resolve absolute path to docs/latency/budget.yaml."""
    env_path = os.environ.get("MEETMIND_BUDGET_PATH")
    if env_path and Path(env_path).is_file():
        return Path(env_path).resolve()

    # Search upwards from current file
    curr = Path(__file__).resolve()
    for parent in [curr] + list(curr.parents):
        cand = parent / "docs" / "latency" / "budget.yaml"
        if cand.is_file():
            return cand

    # Fallback to working directory
    cwd_cand = Path.cwd() / "docs" / "latency" / "budget.yaml"
    if cwd_cand.is_file():
        return cwd_cand

    raise FileNotFoundError("Could not find docs/latency/budget.yaml")


def load_budget(path: Path | str | None = None) -> LatencyBudget:
    """Load and validate the latency budget from YAML file."""
    budget_path = Path(path) if path else find_budget_yaml_path()
    with open(budget_path, encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)
    return LatencyBudget.model_validate(raw_data)
