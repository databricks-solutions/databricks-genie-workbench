"""Abstracted Failure Signature (AFS) — typed, leak-free cluster views.

Clusters carry raw ``question`` / ``expected_sql`` / ``generated_sql`` /
result samples. Feeding any of that into an LLM prompt whose output is
persisted to the space re-opens Bug #4. ``format_afs`` projects a cluster
onto a closed schema whose fields are derived entirely from judge
metadata (ASI) + structural signals (P2.5 AST differ) — never raw
benchmark text.

The schema is closed: any field not in ``AFS_ALLOWED_FIELDS`` is rejected
by ``_strip_unknown_fields``. Runtime leak assertion
(``validate_afs``) compares every string field against the benchmark
corpus and raises ``AFSLeakError`` on accidental pass-through, catching
regressions where a future contributor adds a free-form field that
echoes raw benchmark text.
"""

from __future__ import annotations

import copy
import logging
import os
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ── Debug escape hatch (P2.6) ─────────────────────────────────────────

def debug_raw_sql_enabled() -> bool:
    """Read ``GSO_DEBUG_RAW_SQL`` at call time (not import time) so tests
    can flip the flag via monkeypatch. When True, callers MAY emit raw
    ``expected_sql`` / ``generated_sql`` to MLflow tags and stdout for
    human diagnosis — but NEVER into any prompt. Prompt construction
    paths must never read this flag.
    """
    return os.environ.get("GSO_DEBUG_RAW_SQL", "0").lower() in {"1", "true", "yes", "on"}


def log_raw_sql_for_cluster(cluster: dict) -> None:
    """Debug helper: emit raw SQL pairs from ``cluster.sql_contexts`` to
    stdout + MLflow tags when ``GSO_DEBUG_RAW_SQL=1``. NO-OP otherwise.

    Strict contract: this function writes only to stdout + MLflow tags.
    It MUST NOT be called from inside a prompt-construction path; call
    sites are limited to diagnostic / post-run logging.
    """
    if not debug_raw_sql_enabled():
        return
    sql_contexts = cluster.get("sql_contexts") or []
    if not sql_contexts:
        return
    cid = cluster.get("cluster_id", "?")
    try:
        import mlflow
    except ImportError:
        mlflow = None
    for idx, ctx in enumerate(sql_contexts[:5]):
        exp_sql = str(ctx.get("expected_sql") or "")
        gen_sql = str(ctx.get("generated_sql") or "")
        print(
            f"[GSO_DEBUG_RAW_SQL] cluster={cid} idx={idx}\n"
            f"  expected_sql: {exp_sql[:500]}\n"
            f"  generated_sql: {gen_sql[:500]}\n"
        )
        # Only emit MLflow tags when a run is ALREADY active; never
        # auto-start one. Auto-start would leak run state into unrelated
        # test fixtures (and into concurrent eval flows in production).
        if mlflow is not None:
            try:
                if mlflow.active_run() is not None:
                    mlflow.set_tag(
                        f"bug4.raw_sql.{cid}.{idx}.expected", exp_sql[:500],
                    )
                    mlflow.set_tag(
                        f"bug4.raw_sql.{cid}.{idx}.generated", gen_sql[:500],
                    )
            except Exception:
                pass


# ── Schema ────────────────────────────────────────────────────────────

AFS_ALLOWED_FIELDS: frozenset[str] = frozenset({
    "cluster_id",
    "failure_type",
    "blame_set",
    "counterfactual_fixes",
    "judge_verdict_pattern",
    "structural_diff",
    "failure_features",
    "question_count",
    "affected_judge",
    "suggested_fix_summary",
})

# Subset of AFS_ALLOWED_FIELDS whose string values are guaranteed free of
# raw benchmark text. Only these are checked in validate_afs — non-string
# fields (counts, dicts, lists of classifications) are not.
AFS_STRING_FIELDS_TO_SCAN: frozenset[str] = frozenset({
    "suggested_fix_summary",
    "failure_type",
    "affected_judge",
    "cluster_id",
    "judge_verdict_pattern",
})

# Max similarity allowed between any AFS string field and any benchmark
# question OR expected_sql. Tighter than the firewall threshold because
# AFS content should be derivative, not reproductive.
AFS_NGRAM_MAX_SIMILARITY = 0.25


class AFSLeakError(ValueError):
    """Raised when an AFS dict contains benchmark text or SQL.

    Means an LLM prompt would have received raw benchmark content. This
    is an invariant violation — fix the upstream code that populated the
    offending field, do not downgrade this error.
    """


# ── Core helpers ──────────────────────────────────────────────────────


def _normalize_blame(blame_raw: Any) -> list[str]:
    """Normalize the heterogeneous ``asi_blame_set`` shape into a flat
    list of fully-qualified identifiers (or short names). Duplicated
    (not imported) from optimizer.py to keep this module free of cyclic
    imports; the upstream shape is stable."""
    if not blame_raw:
        return []
    if isinstance(blame_raw, (list, tuple, set)):
        out: list[str] = []
        for item in blame_raw:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for key in ("fqn", "name", "identifier", "table", "column"):
                    v = item.get(key)
                    if v:
                        out.append(str(v))
                        break
        # de-duplicate, preserve order
        seen: set[str] = set()
        result: list[str] = []
        for b in out:
            if b and b not in seen:
                seen.add(b)
                result.append(b)
        return result
    if isinstance(blame_raw, str):
        return [blame_raw]
    return []


def _failed_judges(cluster: dict) -> list[str]:
    """Project a cluster into the set of judges that emitted FAIL."""
    judges: list[str] = []
    primary = cluster.get("affected_judge")
    if primary:
        judges.append(str(primary))
    for qt in cluster.get("question_traces", []) or []:
        if not isinstance(qt, dict):
            continue
        for fj in qt.get("failed_judges", []) or []:
            if isinstance(fj, dict):
                name = fj.get("judge")
                if name and name not in judges:
                    judges.append(str(name))
    return judges


def _counterfactual_fixes(cluster: dict) -> list[str]:
    """Pull judge-supplied counterfactual fixes. These are short human-readable
    guidance strings (``"use sum() not count()"``, ``"partition by quarter"``)
    and are the synthesis LLM's primary signal after AFS removes raw SQL."""
    raw = cluster.get("asi_counterfactual_fixes") or cluster.get("counterfactual_fixes")
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(f)[:200] for f in raw[:10]]
    if isinstance(raw, str):
        return [raw[:200]]
    return []


def _structural_diff(cluster: dict) -> dict:
    """Typed classification of *what* is structurally wrong — WITHOUT
    quoting any SQL text.

    Combines the judge-supplied ``asi_wrong_clause`` / ``asi_join_assessment``
    metadata with a conservative summary of ``join_assessments``. P2.5's
    sqlglot AST differ is merged in via ``compute_ast_diff`` when the
    caller passes SQL pairs in ``cluster["_sql_pairs_for_ast_diff"]`` —
    this is an opt-in contract so the common case (no AST diff wanted)
    stays allocation-free.
    """
    diff: dict[str, Any] = {}

    wrong_clause = cluster.get("asi_wrong_clause")
    if isinstance(wrong_clause, str) and wrong_clause.strip():
        # Keep it; the classification is a short token like "WHERE" /
        # "GROUP_BY" / "aggregation_function". If future judges emit raw
        # SQL here validate_afs will catch it.
        diff["wrong_clause"] = wrong_clause.strip()[:120]

    ja_list = cluster.get("join_assessments") or []
    if isinstance(ja_list, list) and ja_list:
        # Summarize join assessments as typed counts — do NOT carry the
        # raw assessment text which sometimes echoes join SQL.
        ok = sum(1 for ja in ja_list if isinstance(ja, dict) and ja.get("join_shape_ok"))
        total = sum(1 for ja in ja_list if isinstance(ja, dict))
        diff["join_shape_summary"] = {
            "ok_count": ok,
            "total_count": total,
            "failing_count": max(0, total - ok),
        }

    ast_pairs = cluster.get("_sql_pairs_for_ast_diff")
    if ast_pairs:
        from genie_space_optimizer.optimization.afs import compute_ast_diff
        ast_diff = compute_ast_diff(
            [p.get("expected_sql", "") for p in ast_pairs],
            [p.get("generated_sql", "") for p in ast_pairs],
            schema_allowlist=cluster.get("_schema_allowlist") or set(),
        )
        if ast_diff:
            diff["ast_diff"] = ast_diff

    return diff


def _suggested_fix_summary(cluster: dict) -> str:
    """One-line, generic summary of what the patch should achieve. Derived
    from root_cause + blame_set — not from any benchmark text."""
    rc = str(cluster.get("root_cause") or "unknown")
    blame = _normalize_blame(cluster.get("asi_blame_set"))
    parts = [f"Root cause: {rc}"]
    if blame:
        parts.append(f"Blamed: {', '.join(blame[:3])}")
    qc = len(cluster.get("question_ids", []) or [])
    if qc:
        parts.append(f"{qc} question(s) affected")
    return "; ".join(parts)[:200]


def _judge_verdict_pattern(cluster: dict) -> str:
    """Short token describing the majority judge verdict pattern in this
    cluster (e.g. ``"schema_accuracy=FAIL,arbiter=FAIL"``). Used as a
    coarse clustering fingerprint for strategist prompts."""
    verdicts: dict[str, int] = {}
    for qt in cluster.get("question_traces", []) or []:
        if not isinstance(qt, dict):
            continue
        for fj in qt.get("failed_judges", []) or []:
            if isinstance(fj, dict):
                name = str(fj.get("judge") or "unknown")
                verdicts[name] = verdicts.get(name, 0) + 1
    if not verdicts:
        return ""
    top = sorted(verdicts.items(), key=lambda p: -p[1])[:3]
    return ",".join(f"{k}=FAIL({v})" for k, v in top)


# ── Public API ────────────────────────────────────────────────────────


def format_afs(cluster: dict) -> dict:
    """Return a closed-schema AFS view of ``cluster``.

    Input: a cluster dict as produced by ``cluster_failures`` (possibly
    augmented by upstream enrichment).

    Output: a new dict whose keys are a subset of ``AFS_ALLOWED_FIELDS``
    and whose string values contain NO raw benchmark text. Safe to embed
    in any LLM prompt that produces persisted content.
    """
    afs: dict[str, Any] = {
        "cluster_id": str(cluster.get("cluster_id") or "?"),
        "failure_type": str(cluster.get("root_cause") or cluster.get("asi_failure_type") or "unknown"),
        "affected_judge": str(cluster.get("affected_judge") or "unknown"),
        "question_count": int(len(cluster.get("question_ids", []) or [])),
        "blame_set": _normalize_blame(cluster.get("asi_blame_set"))[:10],
        "counterfactual_fixes": _counterfactual_fixes(cluster),
        "structural_diff": _structural_diff(cluster),
        "failure_features": cluster.get("failure_features") or {},
        "judge_verdict_pattern": _judge_verdict_pattern(cluster),
        "suggested_fix_summary": _suggested_fix_summary(cluster),
    }
    return _strip_unknown_fields(afs)


def format_afs_batch(clusters: Iterable[dict]) -> list[dict]:
    return [format_afs(c) for c in clusters]


def _strip_unknown_fields(afs: dict) -> dict:
    """Enforce the closed schema; drop any field not in AFS_ALLOWED_FIELDS.

    Belt-and-braces against a future contributor adding a new field on
    the way to a prompt that would evade the static leak tests.
    """
    return {k: v for k, v in afs.items() if k in AFS_ALLOWED_FIELDS}


def _walk_strings(obj: Any) -> Iterable[str]:
    """Yield every string leaf inside ``obj`` (dicts/lists recursed)."""
    if isinstance(obj, str):
        if obj:
            yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_strings(v)


def validate_afs(afs: dict, benchmark_corpus: Any) -> bool:
    """Assert ``afs`` contains no benchmark-derived text.

    ``benchmark_corpus`` is an ``optimization.leakage.BenchmarkCorpus`` (or
    any object exposing ``.questions`` + ``.expected_sqls`` lists). The
    check runs n-gram Jaccard against every benchmark question and
    expected SQL; any string in ``afs`` whose similarity exceeds
    ``AFS_NGRAM_MAX_SIMILARITY`` raises ``AFSLeakError``.

    Returns ``True`` on pass (never silently False — raise or pass).
    """
    if benchmark_corpus is None:
        return True
    # Late import so the optimization package wiring stays one-way:
    # leakage.py depends on afs? No — afs depends on leakage.
    from genie_space_optimizer.optimization.leakage import _jaccard, _tokenize

    questions = getattr(benchmark_corpus, "questions", None) or []
    if not questions:
        return True
    q_shingles = [_tokenize(q) for q in questions]

    for field_name in sorted(AFS_STRING_FIELDS_TO_SCAN):
        value = afs.get(field_name)
        for s in _walk_strings(value):
            t = _tokenize(s)
            if not t:
                continue
            for idx, shingles in enumerate(q_shingles):
                if _jaccard(t, shingles) >= AFS_NGRAM_MAX_SIMILARITY:
                    raise AFSLeakError(
                        f"AFS field {field_name} contains text too similar to benchmark question {idx}: {s[:120]!r}",
                    )
    return True


# ── SQL-AST differ (P2.5) ─────────────────────────────────────────────


def compute_ast_diff(
    expected_sqls: list[str],
    generated_sqls: list[str],
    *,
    schema_allowlist: set[str] | frozenset[str] | None = None,
) -> dict:
    """Typed, sqlglot-based structural diff between expected and generated SQL.

    Emits classifications, never raw tokens from the user SQL. When
    ``schema_allowlist`` is provided, identifiers in the output are filtered
    down to that allowlist — anything else is reported as ``<redacted>``.

    Gracefully degrades when ``sqlglot`` is unavailable (returns empty dict
    with a logged warning). Empty / mismatched inputs return ``{}``.
    """
    if not expected_sqls or not generated_sqls:
        return {}
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        logger.warning("sqlglot not available; AST diff skipped")
        return {}

    allowlist_lower = set()
    if schema_allowlist:
        allowlist_lower = {s.lower() for s in schema_allowlist if isinstance(s, str)}

    def _safe_ident(raw: str | None) -> str:
        if not raw:
            return ""
        lower = raw.lower()
        if allowlist_lower and lower not in allowlist_lower:
            return "<redacted>"
        return raw

    def _parse(sql: str):
        try:
            return sqlglot.parse_one(sql, read="databricks")
        except Exception:
            try:
                return sqlglot.parse_one(sql)
            except Exception:
                return None

    def _constructs(tree) -> set[str]:
        if tree is None:
            return set()
        found: set[str] = set()
        node_types = [
            ("SELECT", exp.Select),
            ("WHERE", exp.Where),
            ("GROUP_BY", exp.Group),
            ("HAVING", exp.Having),
            ("ORDER_BY", exp.Order),
            ("LIMIT", exp.Limit),
            ("JOIN", exp.Join),
            ("SUBQUERY", exp.Subquery),
            ("UNION", exp.Union),
            ("CASE", exp.Case),
            ("WINDOW", exp.Window),
        ]
        for label, cls in node_types:
            try:
                if tree.find(cls) is not None:
                    found.add(label)
            except Exception:
                continue
        return found

    def _functions(tree) -> set[str]:
        if tree is None:
            return set()
        out: set[str] = set()
        try:
            for node in tree.find_all(exp.Func):
                name = getattr(node, "sql_name", None)
                if callable(name):
                    name = name()
                if name and isinstance(name, str):
                    out.add(name.upper())
        except Exception:
            pass
        return out

    def _tables(tree) -> set[str]:
        if tree is None:
            return set()
        out: set[str] = set()
        try:
            for node in tree.find_all(exp.Table):
                tname = node.name if hasattr(node, "name") else None
                if tname:
                    out.add(_safe_ident(str(tname)))
        except Exception:
            pass
        out.discard("")
        return out

    def _columns(tree) -> set[str]:
        if tree is None:
            return set()
        out: set[str] = set()
        try:
            for node in tree.find_all(exp.Column):
                cname = node.name if hasattr(node, "name") else None
                if cname:
                    out.add(_safe_ident(str(cname)))
        except Exception:
            pass
        out.discard("")
        return out

    def _count_joins(tree) -> int:
        if tree is None:
            return 0
        try:
            return sum(1 for _ in tree.find_all(exp.Join))
        except Exception:
            return 0

    def _count_group_cols(tree) -> int:
        if tree is None:
            return 0
        try:
            g = tree.find(exp.Group)
            if g is None:
                return 0
            return len(g.expressions or [])
        except Exception:
            return 0

    missing_constructs: set[str] = set()
    extra_constructs: set[str] = set()
    wrong_functions: list[dict] = []
    wrong_tables: list[dict] = []
    wrong_columns: list[dict] = []
    join_diffs: list[dict] = []
    agg_diffs: list[dict] = []

    for exp_sql, gen_sql in zip(expected_sqls, generated_sqls):
        e_tree = _parse(exp_sql)
        g_tree = _parse(gen_sql)
        if e_tree is None or g_tree is None:
            continue

        e_con = _constructs(e_tree)
        g_con = _constructs(g_tree)
        missing_constructs |= (e_con - g_con)
        extra_constructs |= (g_con - e_con)

        e_fn = _functions(e_tree)
        g_fn = _functions(g_tree)
        for fn in sorted(e_fn - g_fn):
            # Only include the function name — it's an aggregate name
            # like "SUM"/"COUNT", no raw SQL.
            wrong_functions.append({"expected": fn, "got": "<missing>"})
        for fn in sorted(g_fn - e_fn):
            wrong_functions.append({"expected": "<missing>", "got": fn})

        e_t = _tables(e_tree)
        g_t = _tables(g_tree)
        for t in sorted(e_t - g_t):
            wrong_tables.append({"expected": t, "got": "<missing>"})
        for t in sorted(g_t - e_t):
            wrong_tables.append({"expected": "<missing>", "got": t})

        e_c = _columns(e_tree)
        g_c = _columns(g_tree)
        for c in sorted(e_c - g_c):
            wrong_columns.append({"expected": c, "got": "<missing>"})
        for c in sorted(g_c - e_c):
            wrong_columns.append({"expected": "<missing>", "got": c})

        ej = _count_joins(e_tree)
        gj = _count_joins(g_tree)
        if ej != gj:
            join_diffs.append({"expected_joins": ej, "got_joins": gj})

        eg = _count_group_cols(e_tree)
        gg = _count_group_cols(g_tree)
        if eg != gg:
            agg_diffs.append({"expected_group_by_cols": eg, "got_group_by_cols": gg})

    out: dict[str, Any] = {}
    if missing_constructs:
        out["missing_constructs"] = sorted(missing_constructs)
    if extra_constructs:
        out["extra_constructs"] = sorted(extra_constructs)
    if wrong_functions:
        out["wrong_functions"] = wrong_functions[:20]
    if wrong_tables:
        out["wrong_tables"] = wrong_tables[:20]
    if wrong_columns:
        out["wrong_columns"] = wrong_columns[:20]
    if join_diffs:
        out["join_shape_diff"] = join_diffs[:10]
    if agg_diffs:
        out["aggregation_shape_diff"] = agg_diffs[:10]
    return out
