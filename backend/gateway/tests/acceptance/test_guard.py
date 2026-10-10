"""Acceptance Test Suite Guard.

Ensures that NO mocks, patches, monkeypatches, or fake implementations
are ever introduced into the acceptance test directory.
Acceptance tests MUST run against real uvicorn, real Redis, and real database.
"""

from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_TOKENS = [
    "AsyncMock",
    "MagicMock",
    "Mock(",
    "patch(",
    "monkeypatch",
    "FakeRedis",
]

ACCEPTANCE_DIR = Path(__file__).resolve().parent


def test_no_mocks_in_acceptance_suite() -> None:
    """Verify that no acceptance test file imports or uses mocks."""
    violations: list[str] = []

    for file_path in ACCEPTANCE_DIR.glob("*.py"):
        if file_path.name == "test_guard.py":
            continue

        content = file_path.read_text(encoding="utf-8")
        for token in FORBIDDEN_TOKENS:
            if token in content:
                violations.append(f"{file_path.name} contains forbidden mock token: '{token}'")

        # AST analysis: ensure no unittest.mock or pytest monkeypatch imports
        tree = ast.parse(content, filename=str(file_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if "mock" in alias.name:
                        violations.append(f"{file_path.name} imports mock module: {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module and "mock" in node.module:
                violations.append(f"{file_path.name} imports from mock module: {node.module}")

    assert not violations, "Mock usage detected in acceptance test suite:\n" + "\n".join(violations)
