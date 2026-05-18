"""AST enumerator for ``_run_lever_loop`` branch points.

Walks the ``harness.py`` source (or any source string), locates a named
``FunctionDef``, and returns a structured record per branch point. Pure
function — no I/O beyond the optional file read in the CLI wrapper.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass


# Calls inside ``_run_lever_loop`` that are useful checkpoints for the
# control-flow map. The audit's job is to find where each of these
# fires under replay, not to be exhaustive about *every* function
# called.
_CHECKPOINT_FUNCTIONS = frozenset({
    "collect_blocked_clusters",
    "_generate_lever6_proposal",
    "forced_synthesis_dispatch",
    "_emit_no_structural_candidate_record",
    "_emit_run_aborted_record",
    "_emit_slate_authoritative_skip_record",
    "_emit_rca_card_self_check_failed_record",
    "build_rca_card",
})


@dataclass(frozen=True)
class BranchPoint:
    """A single branch / decision / checkpoint in a function body.

    Fields:
      * ``lineno``: 1-based line where the statement starts.
      * ``end_lineno``: 1-based line where the statement ends.
      * ``statement_type``: one of ``continue``, ``return``, ``break``,
        ``raise``, ``if``, ``for``, ``while``, ``try``, ``except_handler``,
        ``finally``, ``with``, ``checkpoint_call``.
      * ``detail``: extra context (e.g., function name for
        ``checkpoint_call``); None when not applicable.
      * ``depth``: lexical nesting depth inside the target function
        (the function body itself is depth 0).
      * ``parent_construct``: ``statement_type`` of the immediate
        enclosing block (``for`` / ``if`` / ``try`` / ``module`` / …).
      * ``snippet``: a short single-line snippet of the source at
        ``lineno`` (whitespace stripped, capped at 120 chars).
    """

    lineno: int
    end_lineno: int
    statement_type: str
    detail: str | None
    depth: int
    parent_construct: str
    snippet: str


def enumerate_branch_points(
    source: str, *, function_name: str
) -> list[BranchPoint]:
    """Parse ``source`` and return the list of branch points inside the
    function called ``function_name``.

    Raises ``LookupError`` if no function with that name is found.
    """
    tree = ast.parse(source)
    target: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        ):
            target = node
            break
    if target is None:
        raise LookupError(f"function {function_name!r} not found in source")

    source_lines = source.splitlines()
    out: list[BranchPoint] = []
    _walk(
        target.body,
        depth=0,
        parent="module",
        out=out,
        source_lines=source_lines,
    )
    out.sort(key=lambda p: (p.lineno, p.depth))
    return out


def _snippet(source_lines: list[str], lineno: int) -> str:
    if 1 <= lineno <= len(source_lines):
        return source_lines[lineno - 1].strip()[:120]
    return ""


def _walk(
    body: list[ast.stmt],
    *,
    depth: int,
    parent: str,
    out: list[BranchPoint],
    source_lines: list[str],
) -> None:
    for stmt in body:
        yield_branch_points(
            stmt, depth=depth, parent=parent, out=out, source_lines=source_lines,
        )


def yield_branch_points(
    stmt: ast.stmt,
    *,
    depth: int,
    parent: str,
    out: list[BranchPoint],
    source_lines: list[str],
) -> None:
    if isinstance(stmt, ast.Continue):
        out.append(_mk(stmt, "continue", None, depth, parent, source_lines))
    elif isinstance(stmt, ast.Return):
        out.append(_mk(stmt, "return", None, depth, parent, source_lines))
    elif isinstance(stmt, ast.Break):
        out.append(_mk(stmt, "break", None, depth, parent, source_lines))
    elif isinstance(stmt, ast.Raise):
        out.append(_mk(stmt, "raise", None, depth, parent, source_lines))
    elif isinstance(stmt, ast.If):
        out.append(_mk(stmt, "if", None, depth, parent, source_lines))
        _walk(stmt.body, depth=depth + 1, parent="if", out=out, source_lines=source_lines)
        _walk(stmt.orelse, depth=depth + 1, parent="if", out=out, source_lines=source_lines)
    elif isinstance(stmt, ast.For):
        out.append(_mk(stmt, "for", None, depth, parent, source_lines))
        _walk(stmt.body, depth=depth + 1, parent="for", out=out, source_lines=source_lines)
        _walk(stmt.orelse, depth=depth + 1, parent="for", out=out, source_lines=source_lines)
    elif isinstance(stmt, ast.While):
        out.append(_mk(stmt, "while", None, depth, parent, source_lines))
        _walk(stmt.body, depth=depth + 1, parent="while", out=out, source_lines=source_lines)
        _walk(stmt.orelse, depth=depth + 1, parent="while", out=out, source_lines=source_lines)
    elif isinstance(stmt, ast.Try):
        out.append(_mk(stmt, "try", None, depth, parent, source_lines))
        _walk(stmt.body, depth=depth + 1, parent="try", out=out, source_lines=source_lines)
        for handler in stmt.handlers:
            out.append(_mk(handler, "except_handler", None, depth, parent, source_lines))
            _walk(
                handler.body,
                depth=depth + 1,
                parent="except_handler",
                out=out,
                source_lines=source_lines,
            )
        _walk(stmt.orelse, depth=depth + 1, parent="try_else", out=out, source_lines=source_lines)
        if stmt.finalbody:
            line = stmt.finalbody[0].lineno
            out.append(BranchPoint(
                lineno=line,
                end_lineno=line,
                statement_type="finally",
                detail=None,
                depth=depth,
                parent_construct=parent,
                snippet=_snippet(source_lines, line),
            ))
            _walk(
                stmt.finalbody,
                depth=depth + 1,
                parent="finally",
                out=out,
                source_lines=source_lines,
            )
    elif isinstance(stmt, ast.With):
        out.append(_mk(stmt, "with", None, depth, parent, source_lines))
        _walk(stmt.body, depth=depth + 1, parent="with", out=out, source_lines=source_lines)
    elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        # Nested function definitions are not iter-body branch points;
        # do not descend (caller-of-interest only).
        return
    else:
        # Look inside non-control statements for checkpoint Calls.
        for inner in ast.walk(stmt):
            if isinstance(inner, ast.Call):
                name = _call_name(inner)
                if name in _CHECKPOINT_FUNCTIONS:
                    out.append(
                        BranchPoint(
                            lineno=inner.lineno,
                            end_lineno=(
                                getattr(inner, "end_lineno", inner.lineno)
                                or inner.lineno
                            ),
                            statement_type="checkpoint_call",
                            detail=name,
                            depth=depth,
                            parent_construct=parent,
                            snippet=_snippet(source_lines, inner.lineno),
                        )
                    )


def _mk(
    stmt: ast.AST,
    statement_type: str,
    detail: str | None,
    depth: int,
    parent: str,
    source_lines: list[str],
) -> BranchPoint:
    return BranchPoint(
        lineno=stmt.lineno,
        end_lineno=getattr(stmt, "end_lineno", stmt.lineno) or stmt.lineno,
        statement_type=statement_type,
        detail=detail,
        depth=depth,
        parent_construct=parent,
        snippet=_snippet(source_lines, stmt.lineno),
    )


def _call_name(call: ast.Call) -> str | None:
    fn = call.func
    if isinstance(fn, ast.Name):
        return fn.id
    if isinstance(fn, ast.Attribute):
        return fn.attr
    return None
