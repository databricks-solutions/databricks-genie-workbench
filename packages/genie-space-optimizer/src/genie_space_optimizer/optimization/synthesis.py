"""Structural synthesis of example_sqls (Bug #4, Phase 3).

Replaces the verbatim-mining path closed in Phase 1. Given:

* an AFS projection of a failure cluster (from ``afs.format_afs``) — no
  raw benchmark text,
* an archetype (from ``archetypes.pick_archetype``) that matches the
  failure mode,
* a metadata snapshot describing the schema the LLM may reference,

``synthesize_example_sqls`` produces an ORIGINAL example_sql proposal
via the LLM. ``validate_synthesis_proposal`` runs a 5-gate pipeline
(parse, execute, structural, arbiter, firewall) — proposals that fail
any gate are rejected. Caps limit per-cluster / per-archetype / headroom
growth; when synthesis fails repeatedly ``instruction_only_fallback``
returns a deterministic text-instruction proposal instead.

No benchmark text enters any prompt constructed here — the only
cluster-derived context is the AFS projection, which is leak-tested by
``afs.validate_afs`` before use.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ── Caps (P3.4) ───────────────────────────────────────────────────────

MAX_SYNTHESIZED_PER_CLUSTER = int(os.environ.get("GSO_SYNTHESIS_MAX_PER_CLUSTER", "2") or "2")
MAX_SYNTHESIZED_PER_ARCHETYPE = int(os.environ.get("GSO_SYNTHESIS_MAX_PER_ARCHETYPE", "3") or "3")
MAX_EXAMPLE_SQLS_HEADROOM = int(os.environ.get("GSO_SYNTHESIS_HEADROOM", "80") or "80")
SYNTHESIS_CONSECUTIVE_FAILURE_FALLBACK = int(
    os.environ.get("GSO_SYNTHESIS_FAIL_FALLBACK", "3") or "3"
)


@dataclass
class SynthesisBudget:
    """Per-run budget + counters for synthesis caps."""
    per_cluster: dict[str, int]
    per_archetype: dict[str, int]
    total: int = 0
    consecutive_failures: int = 0
    instruction_fallbacks: int = 0

    @classmethod
    def new(cls) -> "SynthesisBudget":
        return cls(per_cluster={}, per_archetype={})

    def may_synthesize(
        self, cluster_id: str, archetype_name: str, headroom_used: int,
    ) -> tuple[bool, str]:
        if self.per_cluster.get(cluster_id, 0) >= MAX_SYNTHESIZED_PER_CLUSTER:
            return False, "cluster_cap"
        if self.per_archetype.get(archetype_name, 0) >= MAX_SYNTHESIZED_PER_ARCHETYPE:
            return False, "archetype_cap"
        if headroom_used >= MAX_EXAMPLE_SQLS_HEADROOM:
            return False, "headroom_cap"
        return True, ""

    def record_success(self, cluster_id: str, archetype_name: str) -> None:
        self.per_cluster[cluster_id] = self.per_cluster.get(cluster_id, 0) + 1
        self.per_archetype[archetype_name] = (
            self.per_archetype.get(archetype_name, 0) + 1
        )
        self.total += 1
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1

    def should_fallback(self) -> bool:
        return self.consecutive_failures >= SYNTHESIS_CONSECUTIVE_FAILURE_FALLBACK


# ── Synthesis prompt (MLflow-registered) ───────────────────────────────

_SYNTHESIS_PROMPT_NAME = "gso_example_sql_synthesis"

_SYNTHESIS_PROMPT_TEMPLATE = """\
You are synthesizing a single NEW example SQL to help a data assistant
handle a specific class of failures. You will produce an ORIGINAL
question/SQL pair that matches a structural archetype. You MUST NOT
reproduce any benchmark question or SQL; you have access only to an
abstracted failure signature (AFS).

# Failure Signature (AFS)
Cluster ID: {{ cluster_id }}
Failure Type: {{ failure_type }}
Affected Judge: {{ affected_judge }}
Affected Questions: {{ question_count }}
Blamed Objects: {{ blame_set }}
Counterfactual Fixes (from judges):
{{ counterfactual_fixes }}
Structural Diff Classification:
{{ structural_diff }}
Judge Verdict Pattern: {{ judge_verdict_pattern }}
Summary: {{ suggested_fix_summary }}

# Archetype
Name: {{ archetype_name }}
Shape Contract: {{ archetype_output_shape }}
Guidance:
{{ archetype_prompt_template }}

# Schema
You may ONLY reference identifiers from this allowlist. Any identifier
outside the allowlist is a hallucination and will cause your proposal to
be rejected.
{{ identifier_allowlist }}

# Constraints
- Produce exactly ONE example_sql proposal.
- The ``example_question`` must be a clean, customer-style business
  question (not a benchmark quote).
- The ``example_sql`` must match the archetype's shape contract.
- Use only schema-allowlisted identifiers.
- Your proposal MUST be ORIGINAL — do not echo any field from the AFS.

# Output format (strict JSON)
{
  "example_question": "...",
  "example_sql": "...",
  "usage_guidance": "one-sentence explanation of when this example applies",
  "rationale": "one-sentence reference to the failure mode you are fixing"
}
"""


def register_synthesis_prompt(w: Any = None) -> None:
    """Register the synthesis prompt with MLflow if ``mlflow.genai`` is
    available. No-op in test environments without MLflow."""
    try:
        import mlflow
        from mlflow.genai import register_prompt
    except Exception:
        return
    try:
        register_prompt(
            name=_SYNTHESIS_PROMPT_NAME,
            template=_SYNTHESIS_PROMPT_TEMPLATE,
            commit_message="Bug #4 example_sql synthesis prompt",
        )
    except Exception:
        logger.debug("MLflow prompt registration skipped", exc_info=True)


def render_synthesis_prompt(afs: dict, archetype: Any, identifier_allowlist: str) -> str:
    """Render the prompt from AFS + archetype + schema allowlist.

    Never includes raw benchmark text — AFS is already scrubbed and
    ``identifier_allowlist`` is schema-derived.
    """
    from genie_space_optimizer.common.config import format_mlflow_template

    return format_mlflow_template(
        _SYNTHESIS_PROMPT_TEMPLATE,
        cluster_id=afs.get("cluster_id", "?"),
        failure_type=afs.get("failure_type", "unknown"),
        affected_judge=afs.get("affected_judge", "unknown"),
        question_count=afs.get("question_count", 0),
        blame_set=", ".join(afs.get("blame_set") or []) or "(none)",
        counterfactual_fixes="\n".join(
            f"  - {f}" for f in (afs.get("counterfactual_fixes") or [])
        ) or "  (none provided by judges)",
        structural_diff=json.dumps(afs.get("structural_diff") or {}, indent=2),
        judge_verdict_pattern=afs.get("judge_verdict_pattern", ""),
        suggested_fix_summary=afs.get("suggested_fix_summary", ""),
        archetype_name=archetype.name,
        archetype_output_shape=json.dumps(archetype.output_shape),
        archetype_prompt_template=archetype.prompt_template,
        identifier_allowlist=identifier_allowlist,
    )


# ── 5-gate validation (P3.3) ──────────────────────────────────────────


@dataclass
class GateResult:
    passed: bool
    gate: str
    reason: str = ""


def _extract_json_proposal(raw: str) -> dict | None:
    """Pull a JSON object out of the LLM's response. Handles fenced code
    blocks and plain inline JSON."""
    if not isinstance(raw, str):
        return None
    m = re.search(r"```json\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    m = re.search(r"(\{.*\})", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def _gate_parse(proposal: dict) -> GateResult:
    """Gate 1 — structural field presence + SQL parses via sqlglot.

    Tier 3.8: pre-check for unbalanced backticks (odd count) before
    sqlglot. sqlglot's Databricks dialect is lenient with backtick
    imbalance in some edge cases; the odd-count check catches the
    Q10-style "dropped closing backtick in GROUP BY" class deterministically.

    Tier 3.14: enforce strict ``sqlglot.parse`` — no substring fallback,
    fail closed on any exception so malformed SQL never leaks through
    to the execute gate.
    """
    eq = str(proposal.get("example_question") or "").strip()
    es = str(proposal.get("example_sql") or "").strip()
    if not eq or not es:
        return GateResult(False, "parse", "empty example_question or example_sql")

    # Tier 3.8: deterministic backtick-balance precheck.
    if es.count("`") % 2 == 1:
        return GateResult(
            False, "parse",
            "unbalanced_identifier_quoting: odd number of backticks in example_sql",
        )

    try:
        import sqlglot
    except ImportError:
        # sqlglot is a core dep; if missing we fail CLOSED so malformed
        # SQL can't propagate through the synthesis pipeline without the
        # parse gate's coverage. Previously this failed open.
        return GateResult(False, "parse", "sqlglot unavailable; failing closed (Tier 3.14)")
    try:
        # Tier 3.14: use parse_one with the databricks dialect. Any
        # exception fails the gate — no substring fallback.
        _parsed = sqlglot.parse_one(es, read="databricks")
        if _parsed is None:
            return GateResult(False, "parse", "sqlglot returned None AST")
    except Exception as exc:
        return GateResult(False, "parse", f"sqlglot parse failure: {exc}")
    return GateResult(True, "parse")


_SQL_STRING_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_SQL_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_WHERE_TOKEN_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_SQL_JOIN_TOKEN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)

# ── Phase 2.R5: identifier-qualification gate ────────────────────────
#
# Captures the token immediately after FROM / JOIN / CROSS JOIN (unless
# followed by a SELECT which would be a subquery and a LATERAL / UNNEST
# which are table-valued expressions). Accepts backticked or bare
# identifiers with dot-segments. We do NOT try to be a full parser —
# the goal is to cheaply reject the "SELECT ... FROM dim_date" style
# hallucination before EXPLAIN burns a warehouse call on it.
_SQL_FROM_JOIN_TARGET_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+"
    r"(?!\(|SELECT\b|LATERAL\b|UNNEST\b|VALUES\b)"
    r"((?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*))*)",
    re.IGNORECASE,
)


def _strip_sql_noise(sql: str) -> str:
    """Remove string literals and comments so token regexes operate on
    structural SQL only. Deliberately minimal — misses edge cases like
    dollar-quoted strings which Databricks SQL doesn't use."""
    if not sql:
        return ""
    no_block = _SQL_BLOCK_COMMENT_RE.sub(" ", sql)
    no_line = _SQL_LINE_COMMENT_RE.sub(" ", no_block)
    no_strings = _SQL_STRING_LITERAL_RE.sub(" ", no_line)
    return no_strings


def _extract_from_join_targets(sql: str) -> list[str]:
    """Return the canonical-cased table references that appear after
    ``FROM`` / ``JOIN`` in ``sql``. Backticks are stripped; case is
    lowercased so comparison against the allowlist is symmetric.
    Subqueries (``FROM (SELECT ...)``) and table-valued expressions
    (``LATERAL``, ``UNNEST``, ``VALUES``) are skipped — they have no
    identifier to qualify.
    """
    cleaned = _strip_sql_noise(sql)
    targets: list[str] = []
    for m in _SQL_FROM_JOIN_TARGET_RE.finditer(cleaned):
        raw = m.group(1)
        # Normalise `a` . `b` . `c` or A . B . C → "a.b.c".
        parts = [
            p.strip().strip("`").lower()
            for p in raw.split(".")
        ]
        targets.append(".".join(p for p in parts if p))
    return targets


def _gate_identifier_qualification(
    proposal: dict,
    allowlist: set[str] | None,
) -> GateResult:
    """Gate 1.5 — reject SQL whose FROM/JOIN targets aren't in the
    slice's identifier allowlist. Runs after parse (so ``sql`` is
    syntactically valid) and before execute (so we don't pay for an
    EXPLAIN on a SQL we can already tell is unqualified).

    ``allowlist`` is a set of lowercased fully-qualified identifiers
    (``cat.sch.tbl``). ``None`` / empty allowlist ⇒ skip (e.g.
    cluster-driven path without a slice context); no regression vs.
    today.

    Parse failures on malformed SQL are handled by the real ``_gate_parse``
    — here we soft-accept when the regex extracts nothing from the
    cleaned SQL (structural anomaly; downstream gates catch it).
    """
    if not allowlist:
        return GateResult(True, "identifier_qualification", "skipped_no_allowlist")
    sql = str(proposal.get("example_sql") or "")
    if not sql.strip():
        return GateResult(True, "identifier_qualification", "skipped_empty_sql")
    try:
        targets = _extract_from_join_targets(sql)
    except Exception as exc:
        return GateResult(
            True, "identifier_qualification",
            f"skipped_parse_failed: {exc}",
        )
    if not targets:
        return GateResult(True, "identifier_qualification", "skipped_no_targets")
    allowlist_lower = {a.strip().lower() for a in allowlist}
    # PR 31 — also accept top-level CTE names declared in the SQL's
    # ``WITH`` clause. The CTE-first MV-join repair emits
    # ``WITH __mv_1 AS (...), base AS (...) SELECT ... FROM base ...``;
    # without this exception the qualification gate rejects every
    # ``FROM base`` / ``JOIN __mv_1`` reference because CTE aliases are
    # not in the slice's identifier allowlist (which only enumerates
    # real tables/MVs).
    try:
        from genie_space_optimizer.optimization.evaluation import (
            _extract_cte_names,
        )
        cte_names = _extract_cte_names(sql)
    except Exception:
        cte_names = set()
    bad = [
        t for t in targets
        if t not in allowlist_lower and t.split(".")[-1] not in cte_names
    ]
    if bad:
        preview = sorted(allowlist_lower)[:5]
        return GateResult(
            False, "identifier_qualification",
            f"UNQUALIFIED_TABLE: {bad[0]} — expected one of {preview}…",
        )
    return GateResult(True, "identifier_qualification")


def _sql_has_where_or_join(sql: str) -> bool:
    """Return True when ``sql`` contains a WHERE or JOIN token outside of
    string literals. Used by Phase 3.R5 to classify EMPTY_RESULT.

    Detection is intentionally lightweight — we strip single-quoted string
    literals (handling doubled quotes) and then regex-match the tokens.
    sqlglot would be more precise but this path must run per-candidate so
    we keep it cheap; false positives here only bias toward soft-accept,
    which the arbiter/structural gates still filter.
    """
    if not sql:
        return False
    stripped = _SQL_STRING_LITERAL_RE.sub("", sql)
    return bool(
        _SQL_WHERE_TOKEN_RE.search(stripped)
        or _SQL_JOIN_TOKEN_RE.search(stripped)
    )


def _gate_execute(
    proposal: dict,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
) -> GateResult:
    """Gate 2 — SQL executes successfully against the warehouse. Reuses
    the existing ``validate_ground_truth_sql`` contract.

    Phase 3.R5: EMPTY_RESULT is classified by WHERE/JOIN presence:

    - SQL with no WHERE/JOIN that returns 0 rows → likely empty table.
      Hard-fail, reject.
    - SQL with WHERE or JOIN that returns 0 rows → likely value mismatch
      against sample warehouse data. Soft-fail: mark the proposal with
      ``_execute_empty=True`` and return ``passed=True`` so downstream
      gates (structural, arbiter) still run. The shape may still be
      valid even when the values don't match today's data.

    Every other error (EXECUTION_ERROR, UNRESOLVED_COLUMN, cast/syntax/
    permission) stays on the hard-fail path unchanged.
    """
    if spark is None and not (w and warehouse_id):
        return GateResult(True, "execute", "skipped_no_backend")
    try:
        from genie_space_optimizer.optimization.benchmarks import (
            validate_ground_truth_sql,
        )
    except Exception:
        return GateResult(True, "execute", "skipped_no_validator")
    try:
        ok, err = validate_ground_truth_sql(
            proposal.get("example_sql", ""),
            spark,
            catalog=catalog,
            gold_schema=gold_schema,
            execute=True,
            w=w,
            warehouse_id=warehouse_id,
        )
    except Exception as exc:
        return GateResult(False, "execute", f"execution error: {exc}")
    if not ok:
        err_str = str(err or "")
        if err_str.startswith("EMPTY_RESULT"):
            sql = proposal.get("example_sql", "") or ""
            if _sql_has_where_or_join(sql):
                proposal["_execute_empty"] = True
                return GateResult(
                    True, "execute", "empty_result_soft_accept",
                )
            return GateResult(False, "execute", err_str[:200])
        return GateResult(False, "execute", err_str[:200])
    return GateResult(True, "execute")


def _gate_structural(
    proposal: dict, archetype: Any, blame_set: list[str] | None = None,
) -> GateResult:
    """Gate 3 — archetype shape contract + blame_set referenced.

    Uses sqlglot to extract constructs; if sqlglot is unavailable falls
    back to substring check (conservative — may miss some cases but does
    not reject valid proposals).
    """
    requires = archetype.output_shape.get("requires_constructs") if archetype else None
    if not requires:
        return GateResult(True, "structural")
    sql = proposal.get("example_sql", "")
    try:
        import sqlglot
        from sqlglot import exp
        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        tree = None

    missing: list[str] = []
    if tree is not None:
        construct_map: dict[str, Any] = {
            "SELECT": exp.Select,
            "WHERE": exp.Where,
            "GROUP_BY": exp.Group,
            "HAVING": exp.Having,
            "ORDER_BY": exp.Order,
            "LIMIT": exp.Limit,
            "JOIN": exp.Join,
            "WINDOW": exp.Window,
            "CASE": exp.Case,
        }
        for name in requires:
            cls = construct_map.get(name)
            if cls is None:
                continue
            try:
                if tree.find(cls) is None:
                    missing.append(name)
            except Exception:
                pass
    else:
        # Fallback: substring check.
        upper = sql.upper()
        substr_map = {
            "GROUP_BY": "GROUP BY",
            "ORDER_BY": "ORDER BY",
            "WINDOW": "OVER (",
        }
        for name in requires:
            needle = substr_map.get(name, name)
            if needle not in upper:
                missing.append(name)

    if missing:
        return GateResult(
            False, "structural",
            f"archetype requires {missing} but SQL does not contain them",
        )
    return GateResult(True, "structural")


def _gate_arbiter(
    proposal: dict,
    *,
    w: Any = None,
    metadata_snapshot: dict | None = None,
    result_rows: list[dict] | None = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
) -> GateResult:
    """Gate 4 — invoke the arbiter judge with actual result rows.

    Historical note: this gate used to call
    ``score_synthesized_example_sql`` without the required
    ``result_rows`` positional argument, which raised ``TypeError``
    inside a bare ``except Exception`` and returned
    ``passed=True, reason='arbiter error, skipped'`` — meaning every
    candidate silently passed. Bug #R3b of the unify-example-sql plan
    fixes that by:

    1. Accepting ``result_rows`` from the caller, OR capturing them
       here via :func:`evaluation._capture_result_rows` when ``spark``
       or ``w`` + ``warehouse_id`` is available.
    2. Passing ``result_rows`` to the arbiter so it can actually judge.
    3. Failing CLOSED on every infrastructure error (import failure,
       missing result rows, arbiter exception) with a distinct reason
       code. Operators see exactly why the gate rejected rather than
       a silent pass-through.

    Contract: ``passed=True`` is returned ONLY when the arbiter
    verdict is affirmative. Every other outcome fails with an explicit
    ``arbiter_*`` reason.
    """
    # Graceful-skip case: no backend configured AND caller didn't
    # supply rows AND the arbiter module can't be imported in this
    # environment. Judging-by-vibes is not a firewall, so we skip
    # rather than rubber-stamp or hard-reject. This matches the
    # behavior tests and the cluster-driven legacy path rely on.
    has_backend = (spark is not None) or (w is not None and warehouse_id)
    if result_rows is None and not has_backend:
        return GateResult(True, "arbiter", "skipped_no_backend")

    try:
        from genie_space_optimizer.optimization.scorers.arbiter import (
            score_example_sql_correctness,
        )
    except Exception:
        return GateResult(False, "arbiter", "arbiter_module_unavailable")

    sql = str(proposal.get("example_sql") or "")
    if not sql.strip():
        return GateResult(False, "arbiter", "arbiter_empty_sql")

    rows: list[dict] | None = result_rows
    capture_error_class: str | None = None
    capture_error_message: str | None = None
    if rows is None and has_backend:
        try:
            from genie_space_optimizer.optimization.evaluation import (
                ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED,
                _capture_result_rows,
            )
            rows, capture_error_class, capture_error_message = (
                _capture_result_rows(
                    sql, spark, catalog, gold_schema,
                    w=w, warehouse_id=warehouse_id,
                )
            )
        except Exception as exc:
            return GateResult(
                False, "arbiter",
                f"arbiter_row_capture_failed: {str(exc)[:100]}",
            )

    if rows is None:
        # F12 — row capture returned a diagnostic tuple. The previous
        # ``arbiter_no_result_rows`` reason code conflated two distinct
        # failure modes; emit a differentiated reason so the rejection
        # banner reflects the actual cause. Subquery-wrap incompat with
        # metric-view SQL (the dominant production case) gets its own
        # bucket; everything else falls through to the generic exec
        # failure code.
        if capture_error_class == ROW_CAPTURE_ERR_SUBQUERY_UNSUPPORTED:
            return GateResult(
                False, "arbiter",
                "arbiter_row_capture_subquery_unsupported",
            )
        if capture_error_class is not None:
            return GateResult(
                False, "arbiter",
                "arbiter_row_capture_exec_failed",
            )
        # ``capture_error_class is None`` reaches here only if the
        # caller passed ``result_rows=None`` AND the backend was
        # unavailable AND the early skip-no-backend branch above did
        # not trigger — i.e. an inconsistent state. Preserve the legacy
        # opaque reason code in that defensive corner so behavior on
        # paths the tests already cover doesn't shift.
        return GateResult(False, "arbiter", "arbiter_no_result_rows")

    try:
        verdict = score_example_sql_correctness(
            question=proposal.get("example_question", ""),
            sql=sql,
            result_rows=rows,
            w=w,
            metadata_snapshot=metadata_snapshot or {},
        )
    except Exception as exc:
        return GateResult(
            False, "arbiter",
            f"arbiter_call_failed: {str(exc)[:100]}",
        )

    if isinstance(verdict, dict):
        verdict_value = str(verdict.get("value") or "").lower()
        if verdict_value in ("yes", "pass", "correct"):
            proposal["_arbiter_verdict"] = verdict
            return GateResult(True, "arbiter")
        return GateResult(
            False, "arbiter", f"verdict={verdict_value}",
        )
    return GateResult(False, "arbiter", "arbiter_verdict_malformed")


def _gate_firewall(
    proposal: dict, benchmark_corpus: Any, *, w: Any = None,
) -> GateResult:
    """Gate 5 — reuse the shared benchmark-leakage firewall."""
    from genie_space_optimizer.optimization.leakage import is_benchmark_leak

    is_leak, reason = is_benchmark_leak(
        proposal, "add_example_sql", benchmark_corpus, w=w,
    )
    if is_leak:
        return GateResult(False, "firewall", reason)
    return GateResult(True, "firewall")


def validate_synthesis_proposal(
    proposal: dict,
    *,
    archetype: Any,
    benchmark_corpus: Any,
    metadata_snapshot: dict | None = None,
    blame_set: list[str] | None = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    w: Any = None,
    warehouse_id: str = "",
    identifier_allowlist: set[str] | None = None,
) -> tuple[bool, list[GateResult]]:
    """Run gates cheap-to-expensive. Returns ``(all_passed, gate_results)``.

    Gate order is load-bearing:
    1. Parse (instant) — catches malformed LLM output.
    2. Identifier qualification (instant) — rejects SQL whose FROM/JOIN
       targets are unqualified or not in the slice's allowlist. Added
       in Phase 2.R5; cheap to run so it short-circuits the expensive
       execute gate. No-op when ``identifier_allowlist`` is ``None``.
    3. Execute (ms-to-seconds) — catches remaining hallucinated SQL.
    4. Structural (instant) — enforces archetype contract.
    5. Arbiter (seconds; LLM) — final quality judge.
    6. Firewall (ms) — last-mile leak check.

    Any gate failure short-circuits the rest.
    """
    # PR 32 — Categorical-cast guardrail. Runs after parse and before
    # execute, and rejects ``CAST(col AS BIGINT)`` when the column's
    # sampled distinct values are non-numeric (typical Y/N flag
    # column). The deterministic repair has no clean way to flip
    # cast types without changing semantics, so we hard-fail here
    # and let the LLM correction loop pick a non-cast comparison
    # shape (string equality, CASE expression).
    def _gate_categorical_cast() -> GateResult:
        try:
            from genie_space_optimizer.optimization.evaluation import (
                check_categorical_type_coercion_violations,
            )
        except Exception:
            return GateResult(True, "categorical_cast", "skipped_no_helper")
        sql = str(proposal.get("example_sql") or "")
        if not sql.strip():
            return GateResult(True, "categorical_cast", "skipped_empty_sql")
        data_profile = (
            (metadata_snapshot or {}).get("_data_profile")
            if isinstance(metadata_snapshot, dict)
            else None
        )
        if not isinstance(data_profile, dict) or not data_profile:
            return GateResult(True, "categorical_cast", "skipped_no_profile")
        try:
            violations = check_categorical_type_coercion_violations(sql, data_profile)
        except Exception:
            return GateResult(True, "categorical_cast", "skipped_check_error")
        if not violations:
            return GateResult(True, "categorical_cast")
        col, violation_kind, samples = violations[0]
        sample_repr = ", ".join(repr(s) for s in samples)
        return GateResult(
            False,
            "categorical_cast",
            f"cast_invalid_input: {col} used in {violation_kind} "
            f"would fail or trigger implicit numeric coercion — column "
            f"values are categorical strings ({sample_repr}). Use string "
            f"equality or CASE WHEN.",
        )

    # PR 31 — Apply the shared pre-execute repair pipeline once the
    # SQL has parsed and qualified. Same deterministic repairs the
    # unified correction pipeline runs (MEASURE wrap, alias
    # collisions, ORDER BY MEASURE-of-alias strip, measure-in-WHERE
    # CTE lift, MV direct-join CTE lift, stem qualification) so the
    # cluster-synthesis path doesn't burn an EXPLAIN on a SQL the
    # repair pipeline would have fixed deterministically.
    def _apply_pre_execute_repairs() -> GateResult:
        try:
            from genie_space_optimizer.optimization.evaluation import (
                apply_pre_execute_repairs,
                build_metric_view_measures,
                effective_metric_view_identifiers_with_catalog,
            )
        except Exception:
            return GateResult(True, "pre_execute_repairs", "skipped_no_helpers")
        sql = str(proposal.get("example_sql") or "")
        if not sql.strip():
            return GateResult(True, "pre_execute_repairs", "skipped_empty_sql")
        cfg = {"_parsed_space": metadata_snapshot} if metadata_snapshot else {}
        try:
            mv_measures = build_metric_view_measures(cfg) if cfg else {}
        except Exception:
            mv_measures = {}
        try:
            mv_ids = (
                effective_metric_view_identifiers_with_catalog(cfg) if cfg else set()
            )
        except Exception:
            mv_ids = set()
        mv_short_set = {
            str(n).split(".")[-1].lower() for n in (mv_ids or set()) if n
        }
        mv_short_set.update(k.lower() for k in (mv_measures or {}).keys() if k)
        canonical_assets = (
            sorted(identifier_allowlist) if identifier_allowlist else []
        )
        counters: dict[str, int] = {}
        try:
            new_sql = apply_pre_execute_repairs(
                sql,
                mv_measures=mv_measures,
                mv_short_set=mv_short_set,
                canonical_assets=canonical_assets or None,
                counters=counters,
            )
        except Exception:
            return GateResult(True, "pre_execute_repairs", "skipped_repair_error")
        if new_sql != sql:
            proposal["example_sql"] = new_sql
            applied = ",".join(
                f"{k}={v}" for k, v in sorted(counters.items()) if v
            )
            return GateResult(
                True, "pre_execute_repairs", f"applied:{applied}" if applied else "applied",
            )
        return GateResult(True, "pre_execute_repairs", "no_repair_needed")

    results: list[GateResult] = []
    for gate_fn in (
        lambda: _gate_parse(proposal),
        # PR 31 — repairs run BEFORE qualification so deterministic
        # stem promotion can fix ``FROM dim_date`` →
        # ``FROM cat.sch.mv_<domain>_dim_date`` before the
        # qualification gate rejects the bare stem (the unified
        # correction pipeline has the same ordering — see
        # ``evaluation.py`` ~line 8410).
        _apply_pre_execute_repairs,
        lambda: _gate_identifier_qualification(proposal, identifier_allowlist),
        # PR 32 — categorical-cast guardrail runs AFTER qualification
        # (so we have a parsed, qualified SQL) but BEFORE execute (so
        # we don't pay for a warehouse round-trip on a SQL we know
        # will fail with ``CAST_INVALID_INPUT``).
        _gate_categorical_cast,
        lambda: _gate_execute(
            proposal,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        ),
        lambda: _gate_structural(proposal, archetype, blame_set),
        # Phase R3b: arbiter now receives execute-side context so it can
        # capture result rows and judge correctness; silent pass-through
        # on errors is gone — every failure mode has a distinct reason
        # code (``arbiter_*``).
        lambda: _gate_arbiter(
            proposal, w=w, metadata_snapshot=metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            warehouse_id=warehouse_id,
        ),
        lambda: _gate_firewall(proposal, benchmark_corpus, w=w),
    ):
        result = gate_fn()
        results.append(result)
        if not result.passed:
            return False, results
    return True, results


# ── Synthesis entry point (P3.2 + P3.4) ───────────────────────────────


def instruction_only_fallback(afs: dict) -> dict | None:
    """Deterministic fallback proposal when synthesis fails for a cluster.

    Contract v1: fallback text may only be published when the AFS carries an
    explicit ``publishable_instruction_candidates`` entry that is already
    written as Genie-facing guidance. ``counterfactual_fixes`` remain
    optimizer-facing diagnostics and are never directly published.
    """
    from genie_space_optimizer.optimization.instruction_publishability import (
        compile_publishable_fallback,
        is_sql_shape_failure,
    )

    failure_type = str(afs.get("failure_type") or "").strip()
    if is_sql_shape_failure(failure_type):
        logger.info(
            "instruction_only_fallback: declining SQL-shape failure for "
            "cluster %s (failure_type=%s); use example_sql/sql_snippet or retry",
            afs.get("cluster_id", "?"),
            failure_type,
        )
        return None

    proposal = compile_publishable_fallback(afs)
    if proposal is None:
        logger.info(
            "instruction_only_fallback: no publishable instruction candidate "
            "for cluster %s (failure_type=%s); skipping text fallback",
            afs.get("cluster_id", "?"),
            failure_type,
        )
        return None

    return proposal


def synthesize_example_sqls(
    cluster: dict,
    metadata_snapshot: dict,
    benchmark_corpus: Any,
    *,
    archetype: Any | None = None,
    budget: SynthesisBudget | None = None,
    existing_example_sql_count: int = 0,
    w: Any = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    llm_caller: Callable[[str], str] | None = None,
) -> dict | None:
    """Produce a single synthesized example_sql proposal for ``cluster``.

    Returns None when:
    * no archetype matches,
    * caps exhausted,
    * all synthesis + 5-gate attempts (1 initial + 1 retry) fail.

    Callers handle the ``None`` case by applying ``instruction_only_fallback``
    when the consecutive-failure threshold is reached.

    ``llm_caller`` is an injection seam for unit tests — real code passes
    nothing and the function imports ``_traced_llm_call`` lazily.
    """
    from genie_space_optimizer.optimization.afs import format_afs, validate_afs
    from genie_space_optimizer.optimization.archetypes import (
        pick_archetype,
    )

    afs = format_afs(cluster)
    try:
        validate_afs(afs, benchmark_corpus)
    except Exception:
        logger.warning("AFS leak detected for cluster %s; aborting synthesis",
                       afs.get("cluster_id", "?"), exc_info=True)
        return None

    if archetype is None:
        archetype = pick_archetype(afs, metadata_snapshot)
    if archetype is None:
        logger.info("No archetype matched for cluster %s; skipping synthesis",
                    afs.get("cluster_id", "?"))
        return None

    if budget is not None:
        ok, why = budget.may_synthesize(
            afs.get("cluster_id", "?"), archetype.name, existing_example_sql_count,
        )
        if not ok:
            logger.info(
                "Synthesis cap hit (%s) for cluster %s; skipping",
                why, afs.get("cluster_id", "?"),
            )
            return None

    try:
        from genie_space_optimizer.optimization.optimizer import (
            _build_identifier_allowlist, _format_identifier_allowlist,
        )
        allowlist = _format_identifier_allowlist(
            _build_identifier_allowlist(metadata_snapshot),
        )
    except Exception:
        allowlist = "(identifier allowlist unavailable)"

    prompt = render_synthesis_prompt(afs, archetype, allowlist)

    def _call_llm(p: str) -> str:
        if llm_caller is not None:
            return llm_caller(p)
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call
        try:
            raw, _ = _traced_llm_call(
                w, "You are a SQL example author.", p,
                span_name="synthesize_example_sql",
            )
            return raw
        except Exception:
            logger.warning("synthesize_example_sqls LLM call failed", exc_info=True)
            return ""

    # First attempt.
    raw = _call_llm(prompt)
    proposal = _extract_json_proposal(raw) or {}
    proposal.setdefault("patch_type", archetype.patch_type)

    passed, gate_results = validate_synthesis_proposal(
        proposal,
        archetype=archetype,
        benchmark_corpus=benchmark_corpus,
        metadata_snapshot=metadata_snapshot,
        blame_set=afs.get("blame_set"),
        spark=spark, catalog=catalog, gold_schema=gold_schema,
        w=w, warehouse_id=warehouse_id,
    )

    if not passed:
        # Single retry with rejection reason fed back as additional constraint.
        fail = next((g for g in gate_results if not g.passed), None)
        reason = fail.reason if fail else "unknown"
        retry_prompt = (
            prompt
            + f"\n\n# Previous attempt was rejected by the {fail.gate if fail else '?'} "
            f"gate: {reason}. Generate a DIFFERENT original proposal that "
            "addresses this rejection. Do NOT echo any rejected tokens."
        )
        raw = _call_llm(retry_prompt)
        proposal = _extract_json_proposal(raw) or {}
        proposal.setdefault("patch_type", archetype.patch_type)
        passed, gate_results = validate_synthesis_proposal(
            proposal,
            archetype=archetype,
            benchmark_corpus=benchmark_corpus,
            metadata_snapshot=metadata_snapshot,
            blame_set=afs.get("blame_set"),
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        )

    if not passed:
        if budget is not None:
            budget.record_failure()
        return None

    proposal["provenance"] = {
        "source": "structural_synthesis",
        "archetype": archetype.name,
        "cluster_id": afs.get("cluster_id", "?"),
        "failure_type": afs.get("failure_type", ""),
        "gate_results": [g.__dict__ for g in gate_results],
    }
    if budget is not None:
        budget.record_success(afs.get("cluster_id", "?"), archetype.name)
    return proposal


def _rca_theme_field(theme: Any, key: str, default: Any = "") -> Any:
    if isinstance(theme, dict):
        return theme.get(key, default)
    return getattr(theme, key, default)


def afs_from_rca_theme(theme: Any) -> dict:
    """Build an AFS-like synthesis request from an RCA theme.

    This intentionally excludes benchmark question IDs, expected SQL,
    generated SQL, and raw evidence text. The synthesis prompt may use
    RCA shape and blamed objects, but never benchmark answers.
    """
    patches = list(_rca_theme_field(theme, "patches", ()) or ())
    synth_patch = next(
        (
            p for p in patches
            if isinstance(p, dict) and p.get("type") == "request_example_sql_synthesis"
        ),
        {},
    )
    blame = (
        synth_patch.get("blame_set")
        or list(_rca_theme_field(theme, "touched_objects", ()) or ())
    )
    return {
        "cluster_id": str(_rca_theme_field(theme, "rca_id", "rca_theme")),
        "failure_type": str(
            synth_patch.get("root_cause") or "example_sql_shape_needed"
        ),
        "blame_set": [str(x) for x in blame if str(x).strip()][:8],
        "suggested_fix_summary": str(
            synth_patch.get("intent")
            or "Generate an original example SQL for this RCA shape."
        ),
        "source": "rca_theme",
    }


def synthesize_example_sqls_for_rca(
    theme: Any,
    metadata_snapshot: dict,
    benchmark_corpus: Any,
    *,
    archetype: Any | None = None,
    budget: SynthesisBudget | None = None,
    existing_example_sql_count: int = 0,
    w: Any = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    llm_caller: Callable[[str], str] | None = None,
) -> dict | None:
    """Compatibility wrapper for legacy tests and callers.

    Production RCA Example SQL synthesis is routed through
    optimizer._cluster_from_rca_example_theme plus
    cluster_driven_synthesis.run_cluster_driven_synthesis_for_single_cluster
    so RCA, strategist, cluster-driven validation, and teaching-kit support
    share one path.
    """
    afs = afs_from_rca_theme(theme)
    from genie_space_optimizer.optimization.archetypes import pick_archetype

    if archetype is None:
        archetype = pick_archetype(afs, metadata_snapshot)
    if archetype is None:
        return None

    try:
        from genie_space_optimizer.optimization.optimizer import (
            _build_identifier_allowlist,
            _format_identifier_allowlist,
        )
        allowlist = _format_identifier_allowlist(
            _build_identifier_allowlist(metadata_snapshot),
        )
    except Exception:
        allowlist = "(identifier allowlist unavailable)"

    prompt = render_synthesis_prompt(afs, archetype, allowlist)
    if llm_caller is not None:
        raw = llm_caller(prompt)
    else:
        from genie_space_optimizer.optimization.optimizer import _traced_llm_call
        try:
            raw, _ = _traced_llm_call(
                w,
                "You are a SQL example author.",
                prompt,
                span_name="synthesize_example_sql_for_rca",
            )
        except Exception:
            logger.warning("RCA example SQL synthesis LLM call failed", exc_info=True)
            return None

    proposal = _extract_json_proposal(raw) or {}
    proposal.setdefault("patch_type", archetype.patch_type)
    passed, gate_results = validate_synthesis_proposal(
        proposal,
        archetype=archetype,
        benchmark_corpus=benchmark_corpus,
        metadata_snapshot=metadata_snapshot,
        blame_set=afs.get("blame_set"),
        spark=spark,
        catalog=catalog,
        gold_schema=gold_schema,
        w=w,
        warehouse_id=warehouse_id,
    )
    if not passed:
        if budget is not None:
            budget.record_failure()
        return None
    proposal["rca_id"] = str(_rca_theme_field(theme, "rca_id", ""))
    proposal["patch_family"] = str(_rca_theme_field(theme, "patch_family", ""))
    proposal["target_qids"] = list(_rca_theme_field(theme, "target_qids", ()) or ())
    proposal["source"] = "rca_theme"
    proposal["provenance"] = {
        "source": "rca_theme_synthesis",
        "rca_id": proposal["rca_id"],
        "patch_family": proposal["patch_family"],
        "archetype": archetype.name,
    }
    return proposal
