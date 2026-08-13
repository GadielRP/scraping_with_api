"""Reject new production reads from frozen legacy odds identity/state fields.

The final schema has no compatibility allowlist: every detected read is a
violation.
"""

from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCAN_ROOTS = ("app", "infrastructure", "modules", "scripts")
LEGACY_CHOICE_FIELDS = {"initial_odds", "current_odds", "change"}
LEGACY_SNAPSHOT_IDENTITY_FIELDS = {
    "choice_id",
    "source",
    "source_market_id",
    "source_outcome_id",
    "bookmaker_outcome_id",
    "main_line",
    "exchange_side",
    "exchange_level",
}
SQL_CHOICE_PATTERN = re.compile(
    r"\b(?:mc|c|market_choices?)\s*\.\s*(initial_odds|current_odds|change)\b",
    re.IGNORECASE,
)
SQL_SNAPSHOT_PATTERN = re.compile(
    r"\b(?:mcs|market_choice_snapshots?)\s*\.\s*"
    r"(choice_id|source|source_market_id|source_outcome_id|bookmaker_outcome_id|"
    r"main_line|exchange_side|exchange_level)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class LegacyReadViolation:
    path: str
    line: int
    symbol: str
    rule: str
    expression: str


class _LegacyReadVisitor(ast.NodeVisitor):
    def __init__(self, *, relative_path: str, source: str) -> None:
        self.relative_path = relative_path
        self.source_lines = source.splitlines()
        self.symbols: list[str] = []
        self.model_aliases: dict[str, str] = {}
        self.variable_types: list[dict[str, str]] = [{}]
        self.violations: list[LegacyReadViolation] = []

    @property
    def symbol(self) -> str:
        return self.symbols[-1] if self.symbols else "<module>"

    def _expression(self, node: ast.AST) -> str:
        line = getattr(node, "lineno", 0)
        if 1 <= line <= len(self.source_lines):
            return self.source_lines[line - 1].strip()
        return ""

    def _record(self, node: ast.AST, rule: str) -> None:
        violation = LegacyReadViolation(
            path=self.relative_path,
            line=int(getattr(node, "lineno", 0)),
            symbol=self.symbol,
            rule=rule,
            expression=self._expression(node),
        )
        self.violations.append(violation)

    def _lookup_variable_type(self, name: str) -> Optional[str]:
        for scope in reversed(self.variable_types):
            if name in scope:
                return scope[name]
        return None

    def _bind_target(self, target: ast.AST, model_name: str) -> None:
        if isinstance(target, ast.Name):
            self.variable_types[-1][target.id] = model_name
        elif isinstance(target, (ast.Tuple, ast.List)):
            for item in target.elts:
                self._bind_target(item, model_name)

    @staticmethod
    def _relationship_type(iterable: ast.AST) -> Optional[str]:
        candidate = iterable
        if isinstance(candidate, ast.Call) and candidate.args:
            candidate = candidate.args[0]
        if isinstance(candidate, ast.Attribute):
            if candidate.attr in {"choices", "quotes"}:
                return "MarketChoice" if candidate.attr == "choices" else None
            if candidate.attr == "snapshots":
                return "MarketChoiceSnapshot"
        return None

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for item in node.names:
            if item.name in {"MarketChoice", "MarketChoiceSnapshot"}:
                self.model_aliases[item.asname or item.name] = item.name

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.symbols.append(node.name)
        scope: dict[str, str] = {}
        for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            annotation = argument.annotation
            if isinstance(annotation, ast.Name):
                model_name = self.model_aliases.get(annotation.id, annotation.id)
                if model_name in {"MarketChoice", "MarketChoiceSnapshot"}:
                    scope[argument.arg] = model_name
        self.variable_types.append(scope)
        for statement in node.body:
            self.visit(statement)
        self.variable_types.pop()
        self.symbols.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.symbols.append(node.name)
        self.variable_types.append({})
        for statement in node.body:
            self.visit(statement)
        self.variable_types.pop()
        self.symbols.pop()

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        self.variable_types.append({})
        model_name = self._relationship_type(node.iter)
        if model_name:
            self._bind_target(node.target, model_name)
        for statement in node.body:
            self.visit(statement)
        for statement in node.orelse:
            self.visit(statement)
        self.variable_types.pop()

    visit_AsyncFor = visit_For

    def visit_Attribute(self, node: ast.Attribute) -> None:
        model_name = None
        if isinstance(node.value, ast.Name):
            model_name = self.model_aliases.get(node.value.id)
            model_name = model_name or self._lookup_variable_type(node.value.id)
        if model_name == "MarketChoice" and node.attr in LEGACY_CHOICE_FIELDS:
            self._record(node, "legacy_orm_choice_state")
        elif (
            model_name == "MarketChoiceSnapshot"
            and node.attr in LEGACY_SNAPSHOT_IDENTITY_FIELDS
        ):
            self._record(node, "legacy_orm_snapshot_identity")
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if not isinstance(node.value, str):
            return
        if SQL_CHOICE_PATTERN.search(node.value):
            self._record(node, "legacy_sql_choice_state")
        if SQL_SNAPSHOT_PATTERN.search(node.value):
            self._record(node, "legacy_sql_snapshot_identity")


def _iter_python_files(paths: Iterable[Path]) -> Iterable[Path]:
    seen = set()
    for path in paths:
        if path.is_file() and path.suffix == ".py":
            candidates = (path,)
        elif path.is_dir():
            candidates = path.rglob("*.py")
        else:
            candidates = ()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen or "__pycache__" in resolved.parts:
                continue
            seen.add(resolved)
            yield resolved


def scan_legacy_odds_reads(paths: Iterable[Path]) -> tuple[LegacyReadViolation, ...]:
    violations = []
    for path in _iter_python_files(paths):
        try:
            relative_path = path.relative_to(ROOT).as_posix()
        except ValueError:
            relative_path = path.as_posix()
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        visitor = _LegacyReadVisitor(relative_path=relative_path, source=source)
        visitor.visit(tree)
        violations.extend(visitor.violations)
    return tuple(sorted(violations, key=lambda item: (item.path, item.line, item.rule)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    paths = args.paths or [ROOT / item for item in DEFAULT_SCAN_ROOTS]
    try:
        violations = scan_legacy_odds_reads(paths)
    except (OSError, SyntaxError) as exc:
        print(f"legacy_odds_guard_error: {exc}")
        return 2
    if not violations:
        print("legacy_odds_guard=ok violations=0")
        return 0
    for item in violations:
        print(
            f"{item.path}:{item.line}: {item.rule} in {item.symbol}: "
            f"{item.expression}"
        )
    print(f"legacy_odds_guard=failed violations={len(violations)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
