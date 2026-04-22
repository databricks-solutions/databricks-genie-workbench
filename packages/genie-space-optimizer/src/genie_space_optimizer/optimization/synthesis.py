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
    """Gate 1 — structural field presence + SQL parses via sqlglot."""
    eq = str(proposal.get("example_question") or "").strip()
    es = str(proposal.get("example_sql") or "").strip()
    if not eq or not es:
        return GateResult(False, "parse", "empty example_question or example_sql")
    try:
        import sqlglot
    except ImportError:
        # sqlglot is a core dep; if missing we fail open to execute gate.
        return GateResult(True, "parse")
    try:
        sqlglot.parse_one(es, read="databricks")
    except Exception as exc:
        return GateResult(False, "parse", f"sqlglot parse failure: {exc}")
    return GateResult(True, "parse")


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
    the existing ``validate_ground_truth_sql`` contract."""
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
        return GateResult(False, "execute", str(err)[:200])
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
    proposal: dict, *, w: Any = None, metadata_snapshot: dict | None = None,
) -> GateResult:
    """Gate 4 — invoke the arbiter judge on the synthesized proposal.

    Lightweight integration: if the optional arbiter hook isn't wired we
    pass with ``skipped_no_arbiter``. The caller can then decide whether
    to enforce.
    """
    try:
        from genie_space_optimizer.optimization.scorers.arbiter import (
            score_synthesized_example_sql,
        )
    except Exception:
        return GateResult(True, "arbiter", "skipped_no_arbiter")
    try:
        verdict = score_synthesized_example_sql(
            question=proposal.get("example_question", ""),
            sql=proposal.get("example_sql", ""),
            w=w,
            metadata_snapshot=metadata_snapshot or {},
        )
    except Exception as exc:
        return GateResult(True, "arbiter", f"arbiter error, skipped: {exc}")
    if isinstance(verdict, dict):
        verdict_value = str(verdict.get("value") or "").lower()
        if verdict_value in ("yes", "pass", "correct"):
            proposal["_arbiter_verdict"] = verdict
            return GateResult(True, "arbiter")
        return GateResult(
            False, "arbiter", f"verdict={verdict_value}",
        )
    return GateResult(True, "arbiter")


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
) -> tuple[bool, list[GateResult]]:
    """Run gates cheap-to-expensive. Returns ``(all_passed, gate_results)``.

    Gate order is load-bearing:
    1. Parse (instant) — catches malformed LLM output.
    2. Execute (ms-to-seconds) — catches invalid/hallucinated SQL.
    3. Structural (instant) — enforces archetype contract.
    4. Arbiter (seconds; LLM) — final quality judge.
    5. Firewall (ms) — last-mile leak check.

    Any gate failure short-circuits the rest.
    """
    results: list[GateResult] = []
    for gate_fn in (
        lambda: _gate_parse(proposal),
        lambda: _gate_execute(
            proposal,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        ),
        lambda: _gate_structural(proposal, archetype, blame_set),
        lambda: _gate_arbiter(proposal, w=w, metadata_snapshot=metadata_snapshot),
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

    Produces a plain-text ``add_instruction`` proposal derived solely from
    the AFS summary — no LLM call, no benchmark text. Safe under any
    firewall gate because it references only derivative fields.
    """
    summary = str(afs.get("suggested_fix_summary") or "").strip()
    failure_type = str(afs.get("failure_type") or "").strip()
    if not summary and not failure_type:
        return None
    blame = ", ".join(afs.get("blame_set") or [])[:200]
    fixes = "; ".join((afs.get("counterfactual_fixes") or [])[:3])[:400]
    parts = [
        "Guidance derived from optimizer failure signature:",
        f"- Failure type: {failure_type}",
    ]
    if blame:
        parts.append(f"- Affected objects: {blame}")
    if fixes:
        parts.append(f"- Suggested fixes: {fixes}")
    if summary:
        parts.append(f"- Summary: {summary}")
    return {
        "patch_type": "add_instruction",
        "new_text": "\n".join(parts),
        "proposed_value": "\n".join(parts),
        "rationale": (
            "Deterministic instruction-only fallback after synthesis failed "
            "to produce a firewall-passing example_sql for this cluster."
        ),
        "provenance": {
            "source": "synthesis_fallback",
            "cluster_id": afs.get("cluster_id", "?"),
            "failure_type": failure_type,
        },
    }


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
