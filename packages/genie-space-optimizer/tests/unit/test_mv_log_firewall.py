"""No tracebacks and no exception text on the GSO metric-view path (MV-D121).

The path is every ``optimization/mv_*.py`` module plus the applier's
``apply_patch_set`` and ``rollback``. A source pin parses that code and fails on
a traceback or an exception's text reaching a log line; a seeded self-test
proves the pin bites; three behaviour pins raise with a sentinel and read the
log records.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import applier, mv_advisor, mv_attach, mv_signals

_SENTINEL = "secret_literal"

_OPTIMIZATION = Path(mv_attach.__file__).resolve().parent
_MV_MODULES = sorted(_OPTIMIZATION.glob("mv_*.py"))
_APPLIER_FUNCTIONS = ("apply_patch_set", "rollback")


def find_log_firewall_violations(
    source: str,
    filename: str = "<source>",
    functions: tuple[str, ...] | None = None,
) -> list[str]:
    """Return ``file:line: reason`` for every log-firewall violation in *source*.

    Ruling 12 of MV-D121. A violation is:

    - an ``exc_info`` keyword whose value is not the constant ``False``;
    - a ``logger.exception`` call;
    - a logging call that passes, directly or inside an f-string or a
      ``%``/``.format`` argument, a name bound by an enclosing
      ``except … as name``. ``type(name)`` (and so ``type(name).__name__``) is
      allowed.

    A logging call is ``<receiver>.<method>(…)`` where the receiver is a bare
    name ``logging``, ``log``, or one ending in ``logger``. With *functions*,
    only the module-level functions of those names are checked, and a name that
    is not found is itself a violation, so a rename cannot empty the pin.

    The backend twin is ``backend/tests/test_mv_log_firewall.py``; backend tests
    cannot import GSO test modules, so it carries its own copy of this function,
    and its ``test_the_checker_matches_its_gso_twin`` fails when the bodies drift.
    """
    log_methods = {
        "debug", "info", "warning", "warn", "error", "exception", "critical", "fatal", "log",
    }
    tree = ast.parse(source, filename=filename)
    violations: list[str] = []

    if functions is None:
        roots: list[ast.AST] = [tree]
    else:
        found = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        roots = []
        for name in functions:
            if name in found:
                roots.append(found[name])
            else:
                violations.append(f"{filename}: function {name!r} not found")

    def is_logger(node: ast.AST) -> bool:
        return isinstance(node, ast.Name) and (
            node.id in ("logging", "log") or node.id.lower().endswith("logger")
        )

    def leaks(node: ast.AST, bound: frozenset[str]) -> bool:
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "type"
            and len(node.args) == 1
            and not node.keywords
            and isinstance(node.args[0], ast.Name)
        ):
            return False
        if isinstance(node, ast.Name) and node.id in bound:
            return True
        return any(leaks(child, bound) for child in ast.iter_child_nodes(node))

    def visit(node: ast.AST, bound: frozenset[str]) -> None:
        if isinstance(node, ast.ExceptHandler) and node.name:
            bound = bound | {node.name}
        if isinstance(node, ast.Call):
            where = f"{filename}:{node.lineno}"
            for kw in node.keywords:
                if kw.arg == "exc_info" and not (
                    isinstance(kw.value, ast.Constant) and kw.value.value is False
                ):
                    violations.append(f"{where}: exc_info")
            func = node.func
            if isinstance(func, ast.Attribute) and is_logger(func.value):
                if func.attr == "exception":
                    violations.append(f"{where}: logger.exception")
                if func.attr in log_methods and bound:
                    passed = list(node.args) + [
                        kw.value for kw in node.keywords if kw.arg != "exc_info"
                    ]
                    if any(leaks(arg, bound) for arg in passed):
                        violations.append(f"{where}: exception text in a log call")
        for child in ast.iter_child_nodes(node):
            visit(child, bound)

    for root in roots:
        visit(root, frozenset())
    return violations


_TARGETS = [pytest.param(path, None, id=path.name) for path in _MV_MODULES] + [
    pytest.param(_OPTIMIZATION / "applier.py", (name,), id=f"applier.py::{name}")
    for name in _APPLIER_FUNCTIONS
]


def test_the_glob_finds_the_metric_view_modules() -> None:
    names = {path.name for path in _MV_MODULES}
    assert {"mv_advisor.py", "mv_attach.py", "mv_state.py", "mv_yaml.py"} <= names


@pytest.mark.parametrize(("path", "functions"), _TARGETS)
def test_no_traceback_or_exception_text_in_mv_logs(path: Path, functions) -> None:
    source = path.read_text(encoding="utf-8")
    assert find_log_firewall_violations(source, path.name, functions) == []


_SEEDED = {
    "exc_info": (
        "try:\n    pass\nexcept Exception:\n"
        "    logger.warning('x', exc_info=True)\n"
    ),
    "logger.exception": (
        "try:\n    pass\nexcept Exception:\n"
        "    logger.exception('x')\n"
    ),
    "exception text in a log call": (
        "try:\n    pass\nexcept Exception as exc:\n"
        "    logger.warning('x: %s', exc)\n"
    ),
}

_SEEDED_TEXT_FORMS = {
    "f-string": "logger.warning(f'x: {exc}')",
    "percent": "logger.warning('x: %s' % (exc,))",
    "format": "logger.warning('x: {}'.format(exc))",
    "str": "logger.error('x: %s', str(exc))",
    "keyword": "logger.warning('x', extra={'error': exc})",
}


@pytest.mark.parametrize("reason", sorted(_SEEDED))
def test_the_checker_catches_each_pattern(reason: str) -> None:
    violations = find_log_firewall_violations(_SEEDED[reason], "seed.py")
    assert violations == [f"seed.py:4: {reason}"]


@pytest.mark.parametrize("form", sorted(_SEEDED_TEXT_FORMS))
def test_the_checker_catches_exception_text_in_any_form(form: str) -> None:
    source = (
        "try:\n    pass\nexcept Exception as exc:\n"
        f"    {_SEEDED_TEXT_FORMS[form]}\n"
    )
    assert find_log_firewall_violations(source, "seed.py") == [
        "seed.py:4: exception text in a log call"
    ]


def test_the_checker_passes_the_type_only_form() -> None:
    clean = (
        "try:\n    pass\nexcept Exception as exc:\n"
        "    logger.warning('x (%s)', type(exc).__name__, exc_info=False)\n"
        "    errors.append(f'failed: {exc}')\n"
    )
    assert find_log_firewall_violations(clean, "seed.py") == []


def test_the_checker_reads_only_the_named_functions_and_names_a_missing_one() -> None:
    source = (
        "def kept():\n    logger.exception('x')\n\n"
        "def other():\n    logger.exception('y')\n"
    )
    assert find_log_firewall_violations(source, "seed.py", ("kept", "gone")) == [
        "seed.py: function 'gone' not found",
        "seed.py:2: logger.exception",
    ]


def _assert_type_only(caplog) -> None:
    assert caplog.records
    assert _SENTINEL not in caplog.text
    for record in caplog.records:
        assert _SENTINEL not in record.getMessage()
        assert not record.exc_info


_VIEW = "main.sales.mv_revenue"


def _space_config() -> dict:
    return {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": "main.sales.fact_orders"}],
            "metric_views": [],
            "functions": [],
        },
        "instructions": {"text_instructions": [], "example_question_sqls": []},
    }


def test_a_failed_patch_logs_the_type_only(monkeypatch, caplog) -> None:
    def raising(*_a, **_k):
        raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(applier, "patch_space_config", raising)
    with caplog.at_level("DEBUG"):
        log = applier.apply_patch_set(
            MagicMock(), "space-1", mv_attach._attach_patches([_VIEW]), _space_config(),
            force_apply=True,
        )
    assert log["patch_deployed"] is False
    assert log["patch_error_type"] == "RuntimeError"
    _assert_type_only(caplog)


def test_a_failed_rollback_logs_the_type_only(monkeypatch, caplog) -> None:
    def raising(*_a, **_k):
        raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(applier, "fetch_space_config", raising)
    with caplog.at_level("DEBUG"):
        result = applier.rollback(
            {"pre_snapshot": {"description": "restored", "data_sources": {}}},
            MagicMock(),
            "space-1",
        )
    assert result["status"] == "error"
    _assert_type_only(caplog)


def test_a_failed_advisor_phase_records_the_type_only(monkeypatch, caplog) -> None:
    stages: list[dict] = []

    def raising(*_a, **_k):
        raise RuntimeError(_SENTINEL)

    monkeypatch.setattr(mv_advisor, "_advise", raising)
    monkeypatch.setattr(
        mv_advisor,
        "write_stage",
        lambda spark, run_id, stage, status, **kw: stages.append(
            {"run_id": run_id, "stage": stage, "status": status, **kw}
        ),
    )
    with caplog.at_level("DEBUG"):
        outcome = mv_advisor.run_mv_advisor_phase(
            object(), run_id="r1", space_id="space-1", catalog="main", schema="gso",
            enabled=True,
        )
    assert outcome.status == mv_advisor.STATUS_FAILED
    assert outcome.error == "RuntimeError"
    assert stages and stages[0]["error_message"] == "RuntimeError"
    assert _SENTINEL not in repr(stages)
    _assert_type_only(caplog)


def _raising_reader(_sql: str):
    raise RuntimeError(_SENTINEL)


_SIGNAL_READS = {
    "lineage": lambda: mv_signals.lineage_signal(
        candidate_columns=["amount"],
        source_tables=["main.sales.fact_orders"],
        space_id="space-1",
        run_query=_raising_reader,
    ),
    "demand": lambda: mv_signals.demand_signal(
        space_id="space-1",
        candidate_fingerprints=["fp-1"],
        run_query=_raising_reader,
    ),
}


@pytest.mark.parametrize("read", sorted(_SIGNAL_READS))
def test_a_failed_signal_read_logs_the_reason_code_and_type_only(read: str, caplog) -> None:
    with caplog.at_level("DEBUG"):
        result = _SIGNAL_READS[read]()
    assert result.status == mv_signals.MV_SIGNAL_UNAVAILABLE
    assert result.reason.startswith(f"{mv_signals.REASON_READ_FAILED}: ")
    assert _SENTINEL in result.reason
    _assert_type_only(caplog)
    assert [r.getMessage() for r in caplog.records] == [
        f"mv_signals: {read} read unavailable ({mv_signals.REASON_READ_FAILED}, RuntimeError)"
    ]
