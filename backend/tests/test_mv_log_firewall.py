"""No tracebacks and no exception text in the backend metric-view code (MV-D121).

The scope is every ``backend/services/mv_*.py`` module and, in
``backend/routers/auto_optimize.py``, the metric-view routes, the helpers they
call and the semantic graph's metric-view reads (Ruling 11). A source pin parses
that code and fails on a traceback or an exception's text reaching a log line;
a seeded self-test proves the pin bites. The behaviour pins that raise with a
sentinel and read the log records live beside the code they exercise
(``test_mv_create.py``, ``test_mv_entitlement.py``, ``test_mv_register.py``).
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

import pytest

from backend.routers import auto_optimize
from backend.services import mv_create

_SERVICES = Path(mv_create.__file__).resolve().parent
_MV_MODULES = sorted(_SERVICES.glob("mv_*.py"))
_ROUTER = Path(auto_optimize.__file__).resolve()

_MV_ROUTE_FUNCTIONS = (
    "_load_candidate_ddl_artifact",
    "_load_candidate_ddl_fallback",
    "_space_audience_grantees",
    "_gso_sp_application_id",
    "_mv_optimizer_grant_sql",
    "probe_mv_entitlement",
    "_mv_fetch_space_config",
    "list_mv_proposals",
    "list_space_mv_proposals",
    "suggest_space_mv",
    "stream_space_mv_suggest",
    "register_space_mv",
    "create_space_mv_at_approval",
    "get_space_semantic_graph",
    "_read_metric_view_yamls",
    "_governed_measures_from_yamls",
    "get_mv_ddl",
    "decide_mv_proposal",
    "drop_mv_created",
    "_mv_lift_from_row",
    "list_mv_created",
    "_live_proposal_rows",
    "_drop_older_undecided_sibling",
)


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

    The GSO twin is
    ``packages/genie-space-optimizer/tests/unit/test_mv_log_firewall.py``;
    backend tests cannot import GSO test modules, so this file carries its own
    copy of this function. ``test_the_checker_matches_its_gso_twin`` fails when
    the two bodies drift.
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


def test_the_glob_finds_the_metric_view_services() -> None:
    names = {path.name for path in _MV_MODULES}
    assert {"mv_create.py", "mv_entitlement.py", "mv_suggest.py"} <= names


@pytest.mark.parametrize("path", _MV_MODULES, ids=lambda path: path.name)
def test_no_traceback_or_exception_text_in_mv_service_logs(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    assert find_log_firewall_violations(source, path.name) == []


def test_the_mv_routes_log_no_traceback_or_exception_text() -> None:
    missing = [
        name for name in _MV_ROUTE_FUNCTIONS
        if not callable(getattr(auto_optimize, name, None))
    ]
    assert missing == []
    source = _ROUTER.read_text(encoding="utf-8")
    assert find_log_firewall_violations(source, _ROUTER.name, _MV_ROUTE_FUNCTIONS) == []


def _is_mv_route(path: str) -> bool:
    return "/mv" in path or "mv-" in path or "semantic-graph" in path


def test_every_metric_view_route_is_in_the_pin() -> None:
    endpoints = {
        route.path: route.endpoint.__name__
        for route in auto_optimize.router.routes
        if _is_mv_route(getattr(route, "path", ""))
    }
    assert "/api/auto-optimize/spaces/{space_id}/mv/create" in endpoints
    assert {
        path: name for path, name in endpoints.items() if name not in _MV_ROUTE_FUNCTIONS
    } == {}


_GSO_TWIN = (
    Path(__file__).resolve().parents[2]
    / "packages/genie-space-optimizer/tests/unit/test_mv_log_firewall.py"
)


def _checker_dump(source: str) -> str:
    tree = ast.parse(source)
    (node,) = [
        n for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "find_log_firewall_violations"
    ]
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        node.body = body[1:]
    return ast.dump(node)


def test_the_checker_matches_its_gso_twin() -> None:
    here = Path(__file__).read_text(encoding="utf-8")
    twin = _GSO_TWIN.read_text(encoding="utf-8")
    assert _checker_dump(here) == _checker_dump(twin)


def test_an_uncoercible_lift_report_logs_no_row_value(caplog) -> None:
    row = {"regressed_question_ids": 5, "note": "zq_secret"}
    with caplog.at_level(logging.DEBUG, logger=auto_optimize.logger.name):
        assert auto_optimize._mv_lift_from_row(row) is None
    assert [r.getMessage() for r in caplog.records] == [
        "Could not coerce lift_report row (dict, TypeError)"
    ]
    assert all(not r.exc_info for r in caplog.records)


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
