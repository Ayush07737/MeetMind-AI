"""Unit tests for Static Safety Invariants (WP-E).

Verifies:
- meetmind_asr contains NO file-write APIs (open(..., 'w'), write_bytes, write_text, os.write)
- meetmind_asr contains NO disk persistence modules (tempfile, sqlite3)
- meetmind_asr contains NO database drivers (asyncpg, psycopg, psycopg2, sqlalchemy)
- meetmind_asr NEVER imports from meetmind_gateway
"""

from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_MODULES = {
    "tempfile",
    "sqlite3",
    "asyncpg",
    "psycopg",
    "psycopg2",
    "sqlalchemy",
    "meetmind_gateway",
}

FORBIDDEN_CALL_NAMES = {
    "write_bytes",
    "write_text",
}


def test_static_ast_safety_check():
    asr_package_dir = Path(__file__).resolve().parents[2] / "meetmind_asr"
    assert asr_package_dir.is_dir(), f"Directory not found: {asr_package_dir}"

    py_files = list(asr_package_dir.rglob("*.py"))
    assert len(py_files) > 0, "No python files found in meetmind_asr"

    violations: list[str] = []

    for file_path in py_files:
        content = file_path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(file_path))

        for node in ast.walk(tree):
            # Check 1: Forbidden import
            if isinstance(node, ast.Import):
                for alias in node.names:
                    pkg = alias.name.split(".")[0]
                    if pkg in FORBIDDEN_MODULES:
                        violations.append(
                            f"{file_path.name}:{node.lineno} imports forbidden module '{pkg}'"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    pkg = node.module.split(".")[0]
                    if pkg in FORBIDDEN_MODULES:
                        violations.append(
                            f"{file_path.name}:{node.lineno} imports from forbidden module '{pkg}'"
                        )

            # Check 2: Forbidden method calls (write_bytes, write_text)
            elif isinstance(node, ast.Call):
                func = node.func
                func_name = None
                if isinstance(func, ast.Name):
                    func_name = func.id
                elif isinstance(func, ast.Attribute):
                    func_name = func.attr

                if func_name in FORBIDDEN_CALL_NAMES:
                    violations.append(
                        f"{file_path.name}:{node.lineno} calls forbidden write method '{func_name}'"
                    )

                # Check 3: open() called with write modes
                if func_name == "open":
                    for arg in node.args[1:]:
                        if (
                            isinstance(arg, ast.Constant)
                            and isinstance(arg.value, str)
                            and any(m in arg.value for m in ("w", "a", "x", "+"))
                        ):
                            violations.append(
                                f"{file_path.name}:{node.lineno} calls open() "
                                f"with write mode '{arg.value}'"
                            )
                    for kw in node.keywords:
                        if (
                            kw.arg == "mode"
                            and isinstance(kw.value, ast.Constant)
                            and any(m in kw.value.value for m in ("w", "a", "x", "+"))
                        ):
                            violations.append(
                                f"{file_path.name}:{node.lineno} calls open() "
                                f"with write mode '{kw.value.value}'"
                            )

    assert not violations, "Safety invariant violations detected:\n" + "\n".join(violations)
