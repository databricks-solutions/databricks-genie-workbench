"""
All configurable constants for the Genie Space Optimizer.

Module-level constants with sensible defaults. Can be overridden via
environment variables, job parameters, or the `thresholds` argument
to `optimize_genie_space()`.
"""

from __future__ import annotations

import os
import re
from typing import Any


def format_mlflow_template(template: str, **kwargs: Any) -> str:
    """Format a template that uses MLflow's ``{ variable }`` syntax.

    Unlike Python's ``str.format()``, single braces ``{`` ``}`` are treated as
    literal characters and ``{ variable }`` is the interpolation marker.
    Missing keys are left as-is so partial formatting is safe.
    """
    def _replacer(match: re.Match) -> str:
        key = match.group(1).strip()
        if key in kwargs:
            return str(kwargs[key])
        return match.group(0)

    return re.sub(r"\{\{\s*(\w+)\s*\}\}", _replacer, template)

# ── 0. Canonical Instruction Schema (PR #178 — docs/gsl-instruction-schema.md) ──
#
# Keep in sync with docs/gsl-instruction-schema.md introduced by PR #178.
# Consolidation into a shared Python module tracked under epic #173 / issue #174;
# until that lands, this is the authoritative source for GSO. Once the shared
# module exists, delete these constants in a follow-up PR and import instead.
#
# Header rules (from the schema doc):
#   1-4: matched case-insensitively on the header line (normalized form
#        compared against these tuples).
#   5  : VERBATIM required — Databricks' blessed string for the summary-
#        rendering section. Any variant (case, wording, punctuation) is
#        rejected in strict mode.
#   All sections may be absent, never reordered.
#   Only `##` (h2) headers — `###` subheaders belong in structured targets
#   (sql_snippets, join_specs, etc.), not prose.
CANONICAL_SECTION_HEADERS: tuple[str, ...] = (
    "## PURPOSE",
    "## DISAMBIGUATION",
    "## DATA QUALITY NOTES",
    "## CONSTRAINTS",
    "## Instructions you must follow when providing summaries",  # verbatim
)
CANONICAL_SECTION_ORDER: dict[str, int] = {
    h: i for i, h in enumerate(CANONICAL_SECTION_HEADERS)
}
VERBATIM_REQUIRED_HEADERS: frozenset[str] = frozenset({
    "## Instructions you must follow when providing summaries",
})

# Scanner check #4 soft cap. Matches the threshold enforced by
# backend/services/scanner.py; prose longer than this is flagged as a finding.
MAX_TEXT_INSTRUCTIONS_CHARS = 2000

# Minimum remaining char budget below which expand-instructions skips the
# LLM call entirely. With less than this much room, the LLM can't produce
# useful content for even one section, let alone multiple. Prevents bogus
# "expand failed" log spam when the existing prose is already near the cap.
MIN_EXPAND_BUDGET = 100

# Minimum LLM-reported confidence for a prose-to-structured promotion to be
# applied. Lower-confidence candidates are dropped; they stay in prose until a
# later pass (or a human edit) raises the confidence.
PROMOTE_MIN_CONFIDENCE = 0.7

# Legacy ALL-CAPS section → promotion target, authoritative per
# docs/gsl-instruction-schema.md (the "What does NOT go in text_instructions"
# table). Used by the multi-target miner for routing hints; not a strict filter
# (the miner reads full prose and classifies every span regardless of header).
SECTION_TO_TARGET: dict[str, str] = {
    "BUSINESS DEFINITIONS": "sql_snippet",
    "AGGREGATION RULES":    "sql_snippet",
    "FUNCTION ROUTING":     "sql_snippet",
    "TEMPORAL FILTERS":     "sql_snippet",
    "JOIN GUIDANCE":        "join_spec",
    "QUERY RULES":          "example_qsql",
    "QUERY PATTERNS":       "example_qsql",
    "ASSET ROUTING":        "metadata",  # table_desc + column_synonym
}

# Single source of truth: ``genie_space_optimizer.iq_scan.scoring``. The
# legacy ``_SQL_IN_TEXT_RE`` (naïve keyword match) is re-exported as
# ``SQL_IN_TEXT_RE`` for back-compat with existing imports, but callers
# should prefer ``looks_like_sql_in_prose`` (single line) or
# ``sql_in_text_findings`` (multi-line text) which apply the scanner-v2
# structure-aware detector. Previous duplicate regex removed; consolidation
# tracked alongside the schema module in issue #174.
from genie_space_optimizer.iq_scan.scoring import (  # noqa: E402
    _SQL_IN_TEXT_RE as SQL_IN_TEXT_RE,
    looks_like_sql_in_prose,
    sql_in_text_findings,
)

# ── 1. Quality Thresholds ───────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "response_quality": 0.0,
    "result_correctness": 85.0,
    "asset_routing": 85.0,
}

INFO_ONLY_JUDGES = frozenset({
    # Tier 3.6: judges that are diagnostic-only and must NOT drive
    # clustering / soft-signal detection. Their failures are tracked
    # for observability but don't justify a lever-loop iteration.
    #
    # ``repeatability`` compares this run's SQL against prior runs —
    # failing on iteration 2 just means the SQL differs from iter 1's
    # SQL, not that something is wrong.
    # ``previous_sql`` is a sibling diagnostic that fires on every row
    # where the SQL differs from the last accepted iteration's SQL; same
    # reasoning — diagnostic, not actionable.
    "repeatability",
    "previous_sql",
})

REPEATABILITY_TARGET = 90.0

MLFLOW_THRESHOLDS = {
    "syntax_validity/mean": 0.98,
    "schema_accuracy/mean": 0.95,
    "logical_accuracy/mean": 0.90,
    "semantic_equivalence/mean": 0.90,
    "completeness/mean": 0.90,
    "response_quality/mean": 0.0,
    "result_correctness/mean": 0.85,
    "asset_routing/mean": 0.85,
}

# ── 2. Rate Limits and Timing ──────────────────────────────────────────

RATE_LIMIT_SECONDS = 12
GENIE_RATE_LIMIT_RETRIES = 3
GENIE_RATE_LIMIT_BASE_DELAY = 30
PROPAGATION_WAIT_SECONDS = int(os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT", "30"))
PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING", "90")
)
GENIE_POLL_INITIAL = 3
GENIE_POLL_MAX = 10
GENIE_MAX_WAIT = 120
JOB_POLL_INTERVAL = 30
JOB_MAX_WAIT = 3600
UI_POLL_INTERVAL = 5
SQL_STATEMENT_POLL_LIMIT = 30
SQL_STATEMENT_POLL_INTERVAL = 2
INLINE_EVAL_DELAY = 12
CONNECTION_POOL_SIZE = 20

# ── 3. Iteration and Convergence ───────────────────────────────────────

MAX_ITERATIONS = 5
MAX_ITERATIONS_PER_CLUSTER = 1
MAX_ITERATIONS_HARD_CEILING = 15
SLICE_GATE_TOLERANCE = 15.0
ENABLE_SLICE_GATE: bool = True
"""T2.15: re-enabled after iteration-1 log showed a 3-patch / 3-lever AG
applied with zero intermediate regression checks. Combined with
``SLICE_GATE_TOLERANCE_SMALL_CORPUS`` below, small-corpus noise is
absorbed without suppressing the gate."""
SLICE_GATE_MIN_REDUCTION = 0.5
REGRESSION_THRESHOLD = 5.0
MAX_NOISE_FLOOR = 5.0
SLICE_GATE_TOLERANCE_SMALL_CORPUS = max(REGRESSION_THRESHOLD * 2, MAX_NOISE_FLOOR)
"""T2.15: effective slice-gate tolerance when the full-scope corpus is
smaller than ``SLICE_GATE_SMALL_CORPUS_ROWS``. Wider than the normal
tolerance so a single-row swing doesn't spuriously fail the gate on
22-row corpora."""
SLICE_GATE_SMALL_CORPUS_ROWS = 30

ENABLE_REWRITE_SECTION_SPLIT: bool = True
"""T1.11: when True, a ``rewrite_instruction`` patch without explicit
``escalation=full_rewrite`` is parsed into its canonical section headers
(using ``INSTRUCTION_SECTION_ORDER``) and emitted as per-section
``update_instruction_section`` patches, routed to the owning lever via
``LEVER_TO_SECTIONS``. Only content with no canonical header or sections
explicitly named CONSTRAINTS in the rewrite are merged into CONSTRAINTS.
Set False to revert to the legacy ``collapse into CONSTRAINTS`` behaviour
without a code revert."""

SOFT_CLUSTER_REELEVATION_THRESHOLD = 0.6
"""T1.12: when a soft cluster's mean ``judge_failure_ratio`` (failing
non-info judges / total non-info judges) is at or above this threshold,
the 0.5 soft-dampening multiplier is bypassed during ``rank_clusters``
and the cluster is marked ``reelevated=True``. Prevents multi-judge
soft clusters (e.g. 6-of-7 judges failing with arbiter verdict
`genie_correct`) from being ranked below a small hard cluster."""

OPTIMIZATION_OBJECTIVE: str = "pre_arbiter"
"""DEPRECATED — read but no longer honoured by the gate code.

Historical: selected which accuracy metric the lever-loop gate
optimised (``pre_arbiter`` / ``post_arbiter`` / ``blended``). The
``pre_arbiter`` default is what allowed the retail run to accept AG2
with a -4.6pp post-arbiter regression: pre-arbiter improvement masked
the arbiter-adjusted loss.

Replaced by the single-criterion model in
``acceptance_policy.decide_acceptance``. Post-arbiter accuracy is now
the only signal that drives acceptance. Pre-arbiter accuracy is still
emitted as a diagnostic in the eval payload and decision-audit rows
but does not gate.

The constant is kept for one release so importers don't break. Will
be removed in a follow-up cleanup PR."""

OPTIMIZATION_OBJECTIVE_POST_ARBITER_GUARDRAIL_PP: float = 5.0
"""DEPRECATED — no longer read by the gate code.

Historical: capped how far post-arbiter could regress when
``OPTIMIZATION_OBJECTIVE='pre_arbiter'``. The 5.0pp default was looser
than typical run-to-run variance, which is how the retail AG2
acceptance slipped through.

Replaced by ``MIN_POST_ARBITER_GAIN_PP`` (the gain floor itself acts
as the guardrail — any drop or sub-threshold gain rejects). Kept for
one release for back-compat."""

# ── Task 2: strict acceptance ────────────────────────────────────────

ENABLE_LEGACY_SLICE_P0_GATES: bool = (
    os.getenv("GSO_ENABLE_LEGACY_SLICE_P0_GATES", "false").lower()
    in {"1", "true", "yes", "on"}
)
"""When True, ``harness._run_gate_checks`` runs the slice and P0
evaluation gates before the full eval. When False (the default after
Task 2 of the lever-loop improvement plan), only the single full eval
runs and acceptance is decided by
``acceptance_policy.decide_acceptance``.

The decoded retail run showed both gates passing on AG2 while the
full-eval rejection was the only honest signal; both gates also each
add a Genie round-trip per AG. Keep the flag for one release so any
operator who wants the old behaviour can opt in via
``GSO_ENABLE_LEGACY_SLICE_P0_GATES=true``."""

OPTIMIZATION_TARGET_POST_ARBITER_ACCURACY: float = float(
    os.getenv("GSO_OPTIMIZATION_TARGET_POST_ARBITER_ACCURACY", "100.0")
)
"""Target post-arbiter / arbiter-adjusted accuracy for lever-loop convergence."""

IGNORED_OPTIMIZATION_JUDGES: tuple[str, ...] = tuple(
    j.strip()
    for j in os.getenv("GSO_IGNORED_OPTIMIZATION_JUDGES", "response_quality").split(",")
    if j.strip()
)
"""Judges visible in diagnostics but excluded from optimization targeting."""

ENABLE_CONTROL_PLANE_ACCEPTANCE: bool = (
    os.getenv("GSO_ENABLE_CONTROL_PLANE_ACCEPTANCE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""Default-on kill switch for control-plane (causal) acceptance gating.

The harness always computes ``decide_control_plane_acceptance`` for
diagnostics, but only appends the rollback-driving
``control_plane_acceptance`` regression when this flag is enabled. If
the new gate over-rejects in a real workspace, set
``GSO_ENABLE_CONTROL_PLANE_ACCEPTANCE=false`` to fall back to the legacy
post-arbiter/regression-only acceptance path while keeping diagnostics
intact.
"""

MIN_POST_ARBITER_GAIN_PP: float = float(
    os.getenv("GSO_MIN_POST_ARBITER_GAIN_PP", "0.0")
)
"""Post-arbiter gain floor for accepting candidate states.

The optimizer objective is 100% arbiter-adjusted accuracy within the configured
lever-loop attempt budget. The default is 0.0 so any positive post-arbiter gain
can be accepted when target-qid and out-of-target regression checks pass.
Negative or zero deltas still reject in ``acceptance_policy.decide_acceptance``.
"""

BASELINE_DRIFT_DIAGNOSTIC_PP: float = float(
    os.getenv("GSO_BASELINE_DRIFT_DIAGNOSTIC_PP", "4.0")
)
"""Threshold for the post-hoc baseline-drift diagnostic.

At iteration N+1 entry, the harness compares the candidate's
post-arbiter accuracy against iteration N's *pre-acceptance* baseline
(the carried baseline before iter N's gate ran). If the candidate has
fallen below that snapshot by ``BASELINE_DRIFT_DIAGNOSTIC_PP`` or
more, a ``suspected_stale_baseline`` decision-audit row is written to
flag a possibly-lucky iter-N acceptance.

Diagnostic only — no auto-rollback. The acceptance gate at iter N+1
runs as usual and may reject on its own merit."""

MIN_PRIMARY_GAIN_PP: float = float(os.getenv("GSO_MIN_PRIMARY_GAIN_PP", "0.0"))
"""DEPRECATED — no longer read by the gate code.

Historical: per-confirmation-run primary-gain floor under the K-of-N
strict acceptance policy. Replaced by ``MIN_POST_ARBITER_GAIN_PP``
which applies once per iteration to a single eval. Kept for one
release for back-compat."""

MAX_POST_ARBITER_DROP_PP_SMALL_CORPUS: float = float(
    os.getenv("GSO_MAX_POST_ARBITER_DROP_PP_SMALL_CORPUS", "2.0")
)
"""DEPRECATED — no longer read by the gate code.

Historical: hard guardrail on raw post-arbiter accuracy drop, applied
per confirmation run. Replaced by ``MIN_POST_ARBITER_GAIN_PP`` (a
positive gain floor; any drop or sub-floor gain rejects). Kept for
one release for back-compat."""

SHADOW_APPLY: bool = False
"""T3.3: when True, clone the Genie space to a shadow, apply patches
there, evaluate the shadow, and promote on pass. When False (default),
patches apply in-place with rollback on regression.

Off by default because it doubles Genie API calls per iteration and
the existing rollback path is cheap. Recommended ON for high-stakes
spaces (live production) where even a brief "bad state" between apply
and rollback is unacceptable.

The promotion mechanism is not yet wired to the Genie SDK's space-clone
API. When enabled but unwired, the harness logs a warning and falls
back to in-place apply."""
PLATEAU_ITERATIONS = 2
CONSECUTIVE_ROLLBACK_LIMIT = 3
"""Stop the lever loop after this many consecutive rollbacks, indicating
the optimizer is stuck and further iterations are unlikely to help.
Root causes are only marked as tried when the limit is about to be hit,
giving the strategist a chance to retry with a different lever."""
MAX_ACTION_GROUPS_PER_STRATEGY = int(os.getenv("GSO_MAX_ACTION_GROUPS_PER_STRATEGY", "5"))
"""Maximum number of action groups the strategist may emit per iteration.

Task 15 of the lever-loop convergence plan v2 replaced the hard-coded
``action_groups[:1]`` slice (which forced the strategist to ship a
single AG per iteration) with this config knob. Multi-AG output lets a
single iteration attack independent failure clusters in parallel,
while the per-AG patch survival ledger (Task 4) and the per-question
journey ledger (Task 13) keep blast-radius accounting clean."""

MAX_AG_PATCHES = int(os.getenv("GSO_MAX_AG_PATCHES", "3"))
"""Hard cap on the number of patches applied in a single action group.

Task 5 of the lever-loop improvement plan lowered the default from 8
to 3. Rationale: AG2 in the retail run shipped 8-patch bundles whose
patches did not all target the failing questions, and a single bad
patch took down 7 others when the AG rolled back. Smaller bundles
keep rollback blast radius small and let per-question regression
attribution (Task 4) actually point at the patch responsible.

Override via ``GSO_MAX_AG_PATCHES`` for spaces that legitimately need
broader bundles (e.g. very large corpora where 3 patches cannot move
the metric). The original Tier 2.6 design — lever-boundary batch
apply with intra-AG slice gates — is no longer in the gate sequence
after Task 2 disabled slice/P0 gates by default."""

MIN_PROPOSAL_RELEVANCE = float(os.getenv("GSO_MIN_PROPOSAL_RELEVANCE", "0.1"))

ENABLE_PROACTIVE_FEATURE_MINING: bool = (
    os.getenv("GSO_ENABLE_PROACTIVE_FEATURE_MINING", "false").lower()
    in {"1", "true", "yes", "on"}
)
"""Task 9: when True, after the post-enrichment baseline eval the
harness aggregates a typed corpus profile from the passing rows and
emits enrichment patches (column descriptions, join specs, sql
snippets) gated by the same dedup contract as Task 6 reactive
mining.

Default is ``False`` because proactive mining changes pre-loop
enrichment behavior and the plan rollout (§7) ships it with its own
release flag. Set ``GSO_ENABLE_PROACTIVE_FEATURE_MINING=true`` to
opt in once Task 6 reactive mining is stable on a space."""

ENABLE_REGRESSION_MINING_STRATEGIST: bool = (
    os.getenv("GSO_ENABLE_REGRESSION_MINING_STRATEGIST", "false").lower()
    in {"1", "true", "yes", "on"}
)
"""Regression-mining lane: when True, high-confidence
``column_confusion`` insights mined from rolled-back iterations are
appended to the next strategist call as compact, non-benchmark-verbatim
hints (e.g. "Prefer contrastive metadata for is_month_to_date vs
use_mtdate_flag").

Default is ``False`` so the lane ships audit-only first. Mining itself
runs unconditionally — the flag only gates the strategist input path.
Insights below
:data:`REGRESSION_MINING_STRATEGIST_MIN_CONFIDENCE` are never fed to
the strategist regardless of this flag."""

REGRESSION_MINING_STRATEGIST_MIN_CONFIDENCE: float = float(
    os.getenv("GSO_REGRESSION_MINING_STRATEGIST_MIN_CONFIDENCE", "0.7")
)
"""Minimum confidence required for a mined insight to influence the
strategist when
:data:`ENABLE_REGRESSION_MINING_STRATEGIST` is on. Tightens the
default analyzer floor (~0.6) for the strategist input path so only
high-evidence insights leak into proposal generation; the audit lane
keeps everything for offline review."""

ENABLE_REGRESSION_MINING_RCA_LEDGER: bool = (
    os.getenv("GSO_ENABLE_REGRESSION_MINING_RCA_LEDGER", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, visible regression-mining lessons feed the audit-only RCA ledger.

This is intentionally independent of
:data:`ENABLE_REGRESSION_MINING_STRATEGIST`, which only controls prompt
exposure."""

ENABLE_RCA_LEDGER: bool = (
    os.getenv("GSO_ENABLE_RCA_LEDGER", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, build typed RCA findings from failed eval rows for audit.

Default true because audit-only ledger construction does not change
optimizer behavior."""

ENABLE_RCA_THEMES_STRATEGIST: bool = (
    os.getenv("GSO_ENABLE_RCA_THEMES_STRATEGIST", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, include selected RCA themes and conflict matrix in the
strategist prompt. Defaults true because hard-failure RCA is part of the
unified optimizer control plane, not optional diagnostics."""

ENABLE_RCA_THEME_SELECTION: bool = (
    os.getenv(
        "GSO_ENABLE_RCA_THEME_SELECTION",
        os.getenv("GSO_ENABLE_RCA_THEME_BUNDLES", "false"),
    ).lower()
    in {"1", "true", "yes", "on"}
)
"""When true, prune RCA themes to a compatible subset for strategist context.

This does not mechanically constrain proposal generation, grounding, or
apply. ``GSO_ENABLE_RCA_THEME_BUNDLES`` is accepted as a deprecated
compatibility alias."""

ENABLE_RCA_THEME_BUNDLES: bool = ENABLE_RCA_THEME_SELECTION
"""Deprecated compatibility alias for :data:`ENABLE_RCA_THEME_SELECTION`."""

RCA_MAX_THEMES_PER_ITERATION: int = int(
    os.getenv("GSO_RCA_MAX_THEMES_PER_ITERATION", "3")
)
RCA_MAX_THEME_PATCHES_PER_ITERATION: int = int(
    os.getenv("GSO_RCA_MAX_THEME_PATCHES_PER_ITERATION", "8")
)

ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS: bool = (
    os.getenv("GSO_ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, selected RCA themes may request leakage-safe example SQL synthesis.

This only creates candidate proposals through ``synthesize_example_sqls_for_rca``.
It does not bypass synthesis validation, benchmark-leakage checks, proposal
grounding, or post-iteration rollback.
"""

ENABLE_RCA_SQL_SNIPPET_BRIDGE: bool = (
    os.getenv("GSO_ENABLE_RCA_SQL_SNIPPET_BRIDGE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, RCA themes whose patches request SQL snippets
(``add_sql_snippet_measure`` / ``add_sql_snippet_filter`` /
``add_sql_snippet_expression``) deterministically trigger
``_generate_lever6_proposal`` even when the strategist did not route the
action group to Lever 6.

This does not bypass identifier validation, SQL execution checks, or
benchmark-leakage firewall in ``_generate_lever6_proposal``.
"""

ENABLE_RCA_JOIN_SPEC_BRIDGE: bool = (
    os.getenv("GSO_ENABLE_RCA_JOIN_SPEC_BRIDGE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, RCA themes whose patches request ``add_join_spec``
deterministically build a join proposal from the theme's
``expected_objects`` (qualified ``table.column`` pairs) and run it
through the existing Lever-4 validation + dedup machinery, even when
the strategist did not surface the join in its directives.

This does not bypass ``ensure_join_spec_fields`` normalization,
``validate_join_spec_types`` checks, or duplicate-pair filtering against
existing or already-proposed joins.
"""

ENABLE_RCA_LEVER1_BRIDGE: bool = (
    os.getenv("GSO_ENABLE_RCA_LEVER1_BRIDGE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When true, RCA themes whose patches request L1 metadata changes
(``update_column_description`` / ``add_column_synonym`` /
``update_description``) trigger ``_generate_lever1_rca_proposal``,
which calls the LLM to produce description text and high-quality
synonyms from the failing questions' NL phrasing + RCA evidence.

Existing column proposals from the strategist path are augmented with
RCA-derived synonyms (additive merge) rather than overwritten. The
benchmark-leakage firewall still applies via the AFS projection.
"""

ENFORCE_REFLECTION_REVALIDATION: bool = (
    os.getenv("GSO_ENFORCE_REFLECTION_REVALIDATION", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""Task 10: when True (default), the T2.2 reflection-as-validator
bypass requires a substantive ``escalation_justification`` (≥ 16
chars) to override a previously-rolled-back ``(patch_type, target)``
pair. Surviving rewrites are stamped with a fresh ``proposal_id`` +
``parent_proposal_id`` for attribution and emit a
``reflection_rewrite`` decision audit row before flowing through
grounding (Task 5), counterfactual scan, AFS / leakage firewall, and
apply.

Set ``GSO_ENFORCE_REFLECTION_REVALIDATION=false`` to fall back to the
legacy "any non-empty justification bypasses" behavior. The legacy
mode still tags rewrites and emits the audit row, but no longer
requires the justification to be non-trivial."""

"""Minimum fraction of a proposal's identifier targets that must
appear in some failing question's surface for the proposal to be kept
by ``proposal_grounding.select_patch_bundle``. Default ``0.1`` keeps
the bar low (one matching identifier among ten suffices) but still
catches the AG2 failure mode where ``zone_combination`` patches
shipped against Q011/Q009 failures that never reference the column.

Set to ``0.0`` to disable grounding entirely (legacy behavior)."""
INFRA_RETRY_BUDGET = 3
"""Stop the lever loop after this many consecutive INFRA_FAILURE
rollbacks. Infra rollbacks do not count toward
``CONSECUTIVE_ROLLBACK_LIMIT`` or ``_diminishing_returns`` because
they carry no content signal, but an unbounded infra-fail loop should
still terminate with a clear ``LEVER_LOOP_INFRA_EXHAUSTED`` reason
rather than spinning until the job timeout kills the run. See
:mod:`genie_space_optimizer.optimization.rollback_class` and
``classify_rollback_reason`` for which producer prefixes map to
``INFRA_FAILURE``."""
CONSECUTIVE_ESCALATION_LIMIT = 2
"""Stop the lever loop after this many consecutive iterations where the
strategist escalated (gt_repair, flag_for_review) instead of producing
actionable patches.  Repeated identical escalations indicate a systemic
issue (e.g. bad ground-truth SQL) that the optimizer cannot resolve."""
ARBITER_CORRECTION_TRIGGER = 3  # deprecated — use per-question thresholds below
GENIE_CORRECT_CONFIRMATION_THRESHOLD = 2
"""Minimum independent evaluations where a question must receive ``genie_correct``
before the benchmark's expected SQL is auto-corrected with Genie's SQL."""
NEITHER_CORRECT_REPAIR_THRESHOLD = 2
"""After this many ``neither_correct`` verdicts across iterations for a single
question, attempt LLM-assisted ground-truth repair."""
NEITHER_CORRECT_QUARANTINE_THRESHOLD = 3
"""Quarantine a question (exclude from accuracy denominator) after this many
consecutive ``neither_correct`` verdicts AND at least one failed GT repair."""
REPEATABILITY_EXTRA_QUERIES = 2
DIMINISHING_RETURNS_EPSILON = 2.0
"""Stop the lever loop when the last DIMINISHING_RETURNS_LOOKBACK accepted
iterations each improved mean accuracy by less than this percentage."""
DIMINISHING_RETURNS_LOOKBACK = 2
REFLECTION_WINDOW_FULL = 3
"""Number of most-recent reflection entries shown in full detail inside the
adaptive strategist prompt.  Older entries are compressed to one line."""

PERSISTENCE_MIN_FAILURES = 2
"""Minimum non-passing evaluations across iterations before a question
appears in the per-question persistence summary shown to the strategist."""

TVF_REMOVAL_MIN_ITERATIONS = 2
"""Minimum consecutive failing iterations (each with 2 evals) before TVF
removal is considered.  Effectively requires >= 4 consecutive eval failures."""

TVF_REMOVAL_BLAME_THRESHOLD = 2
"""Minimum distinct iterations where the TVF was blamed in ASI provenance
for high-confidence auto-removal."""

# ── 3a. Scoring-V2 feature flags ────────────────────────────────────────
#
# ``GSO_SCORING_V2`` gates every Group-B scoring-policy change from the
# ``baseline-eval-fix`` plan. Accepted values (case-insensitive):
#
#   ``on``      — new corrected scoring (default).
#   ``shadow``  — run both old and new paths; headline is the new value
#                 but the legacy value is logged as ``shadow.<judge>.<metric>``
#                 for side-by-side comparison in MLflow.
#   ``off``     — legacy kill-switch. Byte-identical to pre-PR behavior.
#
# ``GSO_APPLY_QUALITY_INSTRUCTIONS`` gates the Group-D applier changes
# (MV-preference, column-ordering, calendar-grounding instruction
# bullets). Each policy is rendered as a plain bullet under its target
# canonical ``##`` section — no markers or wrappers are written into
# customer-visible prose. Accepted values: ``on`` (default, inserts the
# current policy bullets), ``off`` (skips insertion). In either mode,
# bullets whose text exactly matches a known policy body (current or
# deprecated, tracked in ``applier._GSO_QUALITY_V1_POLICIES`` and
# ``_GSO_QUALITY_V1_DEPRECATED_BULLETS``) are stripped so a flip to
# ``off`` fully reverts our content. Customer-authored bullets with any
# different wording are preserved verbatim. Pre-Option-C sentinel blocks
# (``-- BEGIN/END GSO_QUALITY_V1:<key>``) are swept out on any apply.
#
# ``GSO_ASSERT_ROW_CANONICAL`` is a dev-only assertion; defaults to off.

_SCORING_V2_ALLOWED = ("on", "shadow", "off")


def _normalize_scoring_v2(raw: str | None) -> str:
    value = (raw or "on").strip().lower()
    if value in _SCORING_V2_ALLOWED:
        return value
    # Back-compat: accept the common booleans people wire into env files.
    if value in ("1", "true", "yes"):
        return "on"
    if value in ("0", "false", "no"):
        return "off"
    return "on"


def get_scoring_v2_mode() -> str:
    """Return the active scoring-v2 mode (``on``/``shadow``/``off``).

    Evaluated on every call so tests can ``monkeypatch.setenv`` without
    reloading the module.
    """
    return _normalize_scoring_v2(os.environ.get("GSO_SCORING_V2"))


def scoring_v2_is_legacy() -> bool:
    """True when ``GSO_SCORING_V2=off`` — restores legacy scoring exactly."""
    return get_scoring_v2_mode() == "off"


def scoring_v2_is_shadow() -> bool:
    """True when ``GSO_SCORING_V2=shadow`` — new headline + legacy shadow metrics."""
    return get_scoring_v2_mode() == "shadow"


def scoring_v2_is_on() -> bool:
    """True when the new scoring path is the active headline (default)."""
    return get_scoring_v2_mode() in ("on", "shadow")


_APPLY_QUALITY_INSTRUCTIONS_ALLOWED = ("on", "off")


def _normalize_quality_instructions(raw: str | None) -> str:
    value = (raw or "on").strip().lower()
    if value in _APPLY_QUALITY_INSTRUCTIONS_ALLOWED:
        return value
    if value in ("1", "true", "yes"):
        return "on"
    if value in ("0", "false", "no"):
        return "off"
    return "on"


def get_apply_quality_instructions_mode() -> str:
    """Return the active applier-quality mode (``on`` or ``off``)."""
    return _normalize_quality_instructions(
        os.environ.get("GSO_APPLY_QUALITY_INSTRUCTIONS")
    )


def apply_quality_instructions_is_on() -> bool:
    return get_apply_quality_instructions_mode() == "on"


# ── 4. LLM Configuration ──────────────────────────────────────────────

LLM_ENDPOINT = "databricks-claude-opus-4-6"
LLM_TEMPERATURE = 0
LLM_MAX_RETRIES = 3

# ── 5. Benchmark Generation ────────────────────────────────────────────

REQUIRE_GROUND_TRUTH_SQL: bool = True
"""When True, benchmarks without expected_sql are rejected at every gate:
generation (curated question-only rows become LLM generation seeds instead),
preflight validation (re-validate after top-up), and eval pre-check
(quarantine instead of silently accepting).  Set to False to restore
legacy behaviour where question-only benchmarks pass through evaluation."""

CURATED_SQL_GENERATION_MAX_RETRIES = 2
"""Maximum LLM correction attempts when generating SQL for a curated
question that originally lacked expected_sql."""

HELD_OUT_RATIO = 0.15
"""Fraction of non-curated benchmarks reserved for held-out generalization
check in Finalize.  The optimizer never sees these during the lever loop."""

PUBLISH_BENCHMARKS_TO_SPACE: bool = (
    os.environ.get("GSO_PUBLISH_BENCHMARKS_TO_SPACE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When True (default), benchmark questions used by the optimizer are
published to the Genie Space's native ``benchmarks.questions`` at finalize
via ``publish_benchmarks_to_genie_space``. Writes are merged (not replacing)
with any user-authored benchmarks and tagged with a ``[auto-optimize]``
prefix + structured source metadata so end users can distinguish them from
their own curated benchmarks. Set GSO_PUBLISH_BENCHMARKS_TO_SPACE=0 to opt
out and keep the space's benchmark section untouched."""

# Phase 4 (Bug #4) — corpus sizing for same-corpus before/after evaluation.
# All Bug-#4-era changes to these values are hidden behind GSO_NEW_SIZING so
# rollback to previous behaviour is a one-env-var flip. The legacy values
# were TARGET=24, MAX=29, HELD_OUT=0.15 (~20 train + ~4 held-out); the Phase
# 4 plan specifies 30 total (~25 train + ~5 held-out), MAX=35 cap.
_GSO_NEW_SIZING = os.environ.get("GSO_NEW_SIZING", "true").lower() in {
    "1", "true", "yes", "on",
}

if _GSO_NEW_SIZING:
    TARGET_BENCHMARK_COUNT = 30
    MAX_BENCHMARK_COUNT = 30
else:
    TARGET_BENCHMARK_COUNT = 24
    MAX_BENCHMARK_COUNT = 29
"""Hard ceiling on benchmark count. No evaluation should ever run on more
than this many questions, regardless of how many are generated or loaded.
With the Phase 4 default the corpus is exactly 30 questions (~25 train + ~5
held out via HELD_OUT_RATIO=0.15). Flip GSO_NEW_SIZING=0 to restore the
legacy 24/29 values."""

MIN_TRAIN_BENCHMARK_COUNT = 20
"""Minimum desired train benchmark count after split assignment."""

MIN_HELD_OUT_BENCHMARK_COUNT = 5
"""Minimum desired held-out benchmark count when the corpus has enough rows."""

# Phase 4 (Bug #4) — per-iteration / acceptance-gate defaults.
# The optimizer re-evaluates the full training corpus each iteration and
# derives cluster attestation as a slice of that eval (see harness.py and
# AGENTS.md Bug #4 section).
MIN_NET_DELTA = int(os.environ.get("GSO_MIN_NET_DELTA", "1") or "1")
"""Minimum net_delta within the targeted cluster for an iteration to be
accepted. net_delta = newly_passing_within_cluster - newly_failing_within_cluster.
A value of 1 (default) matches "at least one more question passes, within
the cluster". Lower to 0 to allow zero-improvement iterations (not
recommended — admits pure-noise iterations)."""

OUT_OF_CLUSTER_REGRESSION_TOLERANCE = int(
    os.environ.get("GSO_OOC_REGRESSION_TOLERANCE", "0") or "0"
)
"""Maximum number of questions outside the targeted cluster that may go
from passing to failing in a single iteration before the iteration is
rolled back. Default 0 — any out-of-cluster regression triggers rollback."""

ITERATION_ACCEPTANCE_ENABLED: bool = (
    os.environ.get("GSO_ITERATION_ACCEPTANCE", "true").lower()
    in {"1", "true", "yes", "on"}
)
"""When True, ``apply_iteration_acceptance`` in ``harness.py`` enforces the
cluster-net-delta + out-of-cluster regression check and rolls back
iterations that fail. Set to False during debugging to disable rollback
while keeping the metrics visible on the iteration row."""

FINALIZE_REPEATABILITY_PASSES = 1
"""Number of repeatability passes in Finalize.  Reduced from 2 to make room
for a held-out generalization eval without increasing total Genie API calls."""

COVERAGE_GAP_SOFT_CAP_FACTOR = 1.5

BENCHMARK_CATEGORIES = [
    "aggregation",
    "ranking",
    "time-series",
    "comparison",
    "detail",
    "list",
    "threshold",
    "multi-table",
]

TEMPLATE_VARIABLES = {
    "${catalog}": "catalog",
    "${gold_schema}": "gold_schema",
}

# ── 5b. Data Profiling ────────────────────────────────────────────────

MAX_PROFILE_TABLES = 20
"""Maximum number of tables to profile during preflight."""

PROFILE_SAMPLE_SIZE = 100
"""Number of rows sampled per table via TABLESAMPLE."""

LOW_CARDINALITY_THRESHOLD = 20
"""Columns with fewer distinct values than this threshold get their actual
distinct values collected (useful for generating realistic filter values)."""

BENCHMARK_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space evaluation expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Domain: {{ domain }}\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Genie Space Instructions\n'
    '{{ instructions_context }}\n'
    '\n'
    '## Sample Questions (from Genie Space config)\n'
    '{{ sample_questions_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate exactly {{ target_count }} diverse benchmark questions that a business user would ask.\n'
    '\n'
    '## Data-Grounded Values\n'
    'Use the Data Profile to generate realistic filter values — reference actual '
    'column values (e.g. WHERE status = \'active\') rather than inventing values. '
    'For numeric columns, use values within the profiled min/max range.\n'
    '\n'
    '## Asset Constraint (Extract-Over-Generate)\n'
    'expected_sql MUST ONLY reference tables, metric views, and functions from VALID Data Assets. '
    'Do NOT invent or hallucinate table/view names. Every FROM, JOIN, and function call must '
    'reference a real asset.\n'
    'required_columns and every column in expected_sql MUST come from the Column Allowlist. '
    'Do NOT invent column names. Before writing SQL, verify every column reference appears in the allowlist.\n'
    '\n'
    '## Metric View Query Rules\n'
    'When writing SQL for metric views:\n'
    '- NEVER use SELECT * — metric views require explicit column references.\n'
    '- ALL measure columns MUST be wrapped in MEASURE() in both SELECT and ORDER BY.\n'
    '  Example: SELECT region, MEASURE(total_revenue) FROM mv_sales GROUP BY region\n'
    '- NEVER use MEASURE() in WHERE, HAVING, ON, or CASE WHEN clauses — MEASURE() is '
    'only valid in SELECT and ORDER BY. To filter on a measure, materialize it in a '
    'CTE first, then filter on the alias.\n'
    '- NEVER use direct JOINs on metric views — they cause METRIC_VIEW_JOIN_NOT_SUPPORTED errors.\n'
    '- If a question requires metric view data PLUS dimension columns from another table, '
    'use the CTE-first pattern: materialize the metric view query in a WITH clause, then JOIN '
    'the CTE result to the dimension table.\n'
    '- Dimensions (non-measure columns) are used for GROUP BY and filtering only.\n'
    '- The Metric Views section above lists which columns are measures vs dimensions.\n'
    '\n'
    '## Common Metric View SQL Mistakes (AVOID THESE)\n'
    'BAD:  SELECT zone, MEASURE(sales) FROM mv WHERE MEASURE(pct_chg) < -2\n'
    'GOOD: WITH t AS (SELECT zone, MEASURE(sales) AS s, MEASURE(pct_chg) AS p '
    'FROM mv GROUP BY zone) SELECT * FROM t WHERE p < -2\n'
    '\n'
    'BAD:  SELECT * FROM mv_store_sales\n'
    'GOOD: SELECT zone, MEASURE(total_sales) FROM mv_store_sales GROUP BY zone\n'
    '\n'
    'BAD (METRIC_VIEW_JOIN_NOT_SUPPORTED):\n'
    '  SELECT s.location_number, l.zone_name, MEASURE(s.cy_sales) '
    'FROM mv_sales s JOIN dim_location l ON s.location_number = l.location_number GROUP BY ALL\n'
    'GOOD (CTE-first pattern — materialize metric view, then JOIN):\n'
    '  WITH sales AS (\n'
    '    SELECT location_number, MEASURE(cy_sales) AS cy_sales_value FROM mv_sales GROUP BY ALL\n'
    '  )\n'
    '  SELECT s.location_number, l.zone_name, s.cy_sales_value '
    'FROM sales s JOIN dim_location l ON s.location_number = l.location_number\n'
    '\n'
    '## CRITICAL: MEASURE() Alias Collision Rule\n'
    '- NEVER alias MEASURE(col) back to the same column name. '
    'Spark shadows the underlying measure column with the alias and '
    'fails ORDER BY / HAVING with '
    'MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.\n'
    'BAD:  SELECT zone, MEASURE(cy_sales) AS cy_sales FROM mv GROUP BY zone ORDER BY MEASURE(cy_sales) DESC\n'
    'GOOD: SELECT zone, MEASURE(cy_sales) AS cy_sales_value FROM mv GROUP BY zone ORDER BY cy_sales_value DESC\n'
    '\n'
    '## Question-SQL Alignment\n'
    '- expected_sql MUST answer EXACTLY what the question asks — no more, no less.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '- If the question is ambiguous about a filter, do NOT assume one UNLESS the Genie '
    'Space Instructions mandate it as a default.\n'
    '\n'
    '## CRITICAL: Instruction-Mandated Default Filters\n'
    'The Genie Space Instructions section above may define default filters (e.g. '
    '"Default filter: <flag_column> = <value> for all <metric>-related queries", '
    'such as a default region filter, a default active-only filter, or a default '
    'time-window filter). These are MANDATORY:\n'
    '- EVERY benchmark SQL that falls under the scope of a default filter MUST include '
    'that filter in its WHERE clause. Omitting it produces incorrect ground truth.\n'
    '- The question text MUST reflect the default filter so question and SQL stay aligned. '
    'Example: instead of "What are the metric KPIs by region?" with '
    'WHERE <flag_column> = \'<value>\', '
    'write "What are the <flag-qualified> metric KPIs by region?" so the question and SQL agree.\n'
    '- Do NOT add filters that are neither mentioned in the question NOR mandated by instructions.\n'
    '\n'
    '## Minimal SQL Principle\n'
    'Write the simplest correct SQL. Prefer fewer columns and filters. '
    'For "multi-table" category questions, JOINs are expected and encouraged.\n'
    '\n'
    '## Asset Coverage (MANDATORY)\n'
    'Every table, metric view, and function listed in VALID Data Assets MUST appear '
    'in at least one benchmark\'s expected_sql (in FROM, JOIN, or function call). '
    'A single question that JOINs multiple tables counts as coverage for all tables '
    'in that JOIN. Distribute questions across all assets first, then add variety.\n'
    '\n'
    '## Diversity\n'
    'At least 2 questions per category. Include edge cases '
    '(filters, temporal ranges, NULL handling).\n'
    '\n'
    '## Multi-Table Join Coverage\n'
    'At least 3 questions MUST use JOINs across 2+ tables (category: "multi-table").\n'
    'Use the Join Specifications above to determine valid join paths.\n'
    'These questions test whether Genie correctly understands the semantic model relationships.\n'
    'Note: JOINs are for TABLE queries only — metric views MUST NOT use JOINs.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of question objects. No markdown, just JSON.\n'
    '\n'
    'Each object:\n'
    '- "question": natural language question\n'
    '- "expected_sql": correct SQL using fully-qualified names from VALID Data Assets '
    '(metric views: MEASURE() syntax; TVFs: function call; tables: standard SQL)\n'
    '- "expected_asset": "MV" | "TVF" | "TABLE"\n'
    '- "category": one of {{ categories }}\n'
    '- "required_tables": list of table names\n'
    '- "required_columns": list of column names\n'
    '- "expected_facts": 1-2 facts the answer should contain\n'
    '</output_schema>'
)

BENCHMARK_CORRECTION_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL expert fixing invalid benchmark questions.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## VALID Data Assets (ONLY these exist)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '## Benchmarks to Fix\n'
    'Each entry below has these keys:\n'
    '- ``question`` / ``original_expected_sql`` — the input.\n'
    '- ``error`` — the raw Spark error string.\n'
    '- ``validation_reason_code`` — a stable taxonomy code (e.g. '
    '``mv_alias_collision``, ``mv_missing_measure_function``, '
    '``unknown_column``).\n'
    '- ``repair_hint`` — a class-specific instruction describing the '
    'minimal change. **Apply the repair_hint before any other rewrite.**\n'
    '{{ benchmarks_to_fix }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Fix each benchmark so expected_sql is valid using ONLY the assets and columns above. '
    'When ``repair_hint`` is present, follow it exactly — it is the deterministic fix for '
    'the error class.\n'
    '\n'
    '- Wrong table/view name: find closest matching valid asset, rewrite SQL.\n'
    '- Field drift (e.g., property_name vs property): map to closest valid column.\n'
    '- Metric views: use MEASURE() syntax for aggregates in SELECT/ORDER BY.\n'
    '- Metric view alias collision: NEVER use ORDER BY alias when alias == source column\n'
    '  for MEASURE() expressions. Use ORDER BY MEASURE(column) directly.\n'
    '- Metric views: NEVER use SELECT * or direct JOINs on metric views. '
    'All measures MUST use MEASURE().\n'
    '- Metric views: NEVER use MEASURE() in WHERE, HAVING, ON, or CASE WHEN clauses — '
    'MEASURE() is only valid in SELECT and ORDER BY. To filter on a measure, materialize '
    'it in a CTE first, then filter on the alias.\n'
    '- Metric view + JOIN: If the error is METRIC_VIEW_JOIN_NOT_SUPPORTED, rewrite using '
    'the CTE-first pattern — materialize the metric view in a WITH clause, then JOIN the '
    'CTE to the dimension table:\n'
    '  BAD:  SELECT s.id, l.name, MEASURE(s.sales) FROM mv_sales s JOIN dim l ON s.id = l.id\n'
    '  GOOD: WITH sales AS (SELECT id, MEASURE(sales) AS sales_value FROM mv_sales GROUP BY ALL) '
    'SELECT s.id, l.name, s.sales_value FROM sales s JOIN dim l ON s.id = l.id\n'
    '- TVFs: use correct function call signature.\n'
    '- Multi-table JOINs: use Join Specifications above for valid join paths.\n'
    '- If error says "Query returns 0 rows", the SQL is syntactically valid but\n'
    '  references impossible filter values. Use the Data Profile to pick realistic values.\n'
    '- If no valid asset can answer the question, set expected_sql to null with unfixable_reason.\n'
    '- Preserve original question text.\n'
    '- Apply MINIMAL SQL PRINCIPLE: corrected SQL answers exactly what the question asks.\n'
    '- If the SQL includes a domain-default filter (e.g., same-store, active status) that is '
    'not mentioned in the question, either remove the filter or update the question text to '
    'mention it so question and SQL stay aligned.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of objects. No markdown, just JSON.\n'
    '\n'
    'Each object: "question", "expected_sql" (corrected or null), "expected_asset", '
    '"category", "required_tables", "required_columns", "expected_facts", '
    '"unfixable_reason" (null if fixed).\n'
    '</output_schema>'
)


# ── 6b. Example-SQL prompts (Phase 2.R2 of unify-example-sql plan) ──────
#
# Copy-and-diverge from BENCHMARK_GENERATION_PROMPT / BENCHMARK_CORRECTION_PROMPT
# with three specific changes:
#   - <role> reframed from "evaluation expert" to "example author"
#     ("TEACH Genie", not "TEST Genie")
#   - Instructions de-emphasize evaluation-style edge cases and asset
#     coverage; emphasize common, naturally-phrased business questions
#   - Output schema drops category / expected_facts / required_tables /
#     required_columns; example_question_sqls just need question + SQL
#     (+ optional usage_guidance that helps Genie use the example)
#
# Every Metric-View rule, Column Allowlist rule, Data Profile grounding
# block, and Instruction-Mandated Default Filter block is kept verbatim —
# those are the features we adopted the benchmark engine for.
#
# Isolation: these prompts must NOT reference any benchmark-derived
# template variable. See the module-load-time assertion at the bottom of
# this file + docs/example-sql-isolation.md.

EXAMPLE_SQL_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space example-SQL author. Your output '
    'will be stored in instructions.example_question_sqls as reference '
    'material that TEACHES Genie the shape of common questions on this '
    'space — it is NOT used to evaluate Genie. Write examples a real '
    'business user would naturally ask; clarity beats cleverness.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Domain: {{ domain }}\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Generation Profile\n'
    'Profile name: {{ generation_profile_name }}\n'
    '{{ generation_profile_focus }}\n'
    '\n'
    '## Asset Coverage Guidance\n'
    '{{ asset_coverage_guidance }}\n'
    '\n'
    '## Genie Space Instructions\n'
    '{{ instructions_context }}\n'
    '\n'
    '## Sample Questions (from Genie Space config)\n'
    '{{ sample_questions_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate exactly {{ target_count }} example question + SQL pairs that '
    'a typical business user would ask on this space. Prioritize common, '
    'directly-useful business questions over edge cases.\n'
    '\n'
    '## Data-Grounded Values\n'
    'Use the Data Profile to pick realistic filter values — reference '
    'actual column values rather than inventing them. For numeric columns, '
    'use values within the profiled min/max range. If a filter cannot be '
    'grounded, omit it rather than guess.\n'
    '\n'
    '## Asset Constraint (Extract-Over-Generate)\n'
    'expected_sql MUST ONLY reference tables, metric views, and functions '
    'from VALID Data Assets. Every column MUST come from the Column '
    'Allowlist. Invented table, view, or column names are a hallucination '
    'and will be rejected.\n'
    '\n'
    '## Metric View Query Rules\n'
    'When writing SQL for metric views:\n'
    '- NEVER use SELECT * — metric views require explicit column references.\n'
    '- ALL measure columns MUST be wrapped in MEASURE() in both SELECT and ORDER BY.\n'
    '  Example: SELECT region, MEASURE(total_revenue) FROM mv_sales GROUP BY region\n'
    '- NEVER use MEASURE() in WHERE, HAVING, ON, or CASE WHEN clauses — MEASURE() is '
    'only valid in SELECT and ORDER BY. To filter on a measure, materialize it in a '
    'CTE first, then filter on the alias.\n'
    '- NEVER use direct JOINs on metric views — they cause METRIC_VIEW_JOIN_NOT_SUPPORTED errors.\n'
    '- If an example requires metric view data PLUS dimension columns from another table, '
    'use the CTE-first pattern: materialize the metric view query in a WITH clause, then JOIN '
    'the CTE result to the dimension table.\n'
    '- Dimensions (non-measure columns) are used for GROUP BY and filtering only.\n'
    '- The Metric Views section above lists which columns are measures vs dimensions.\n'
    '\n'
    '## Common Metric View SQL Mistakes (AVOID THESE)\n'
    'BAD:  SELECT zone, MEASURE(sales) FROM mv WHERE MEASURE(pct_chg) < -2\n'
    'GOOD: WITH t AS (SELECT zone, MEASURE(sales) AS s, MEASURE(pct_chg) AS p '
    'FROM mv GROUP BY zone) SELECT * FROM t WHERE p < -2\n'
    '\n'
    'BAD:  SELECT * FROM mv_store_sales\n'
    'GOOD: SELECT zone, MEASURE(total_sales) FROM mv_store_sales GROUP BY zone\n'
    '\n'
    'BAD (METRIC_VIEW_JOIN_NOT_SUPPORTED):\n'
    '  SELECT s.location_number, l.zone_name, MEASURE(s.cy_sales) '
    'FROM mv_sales s JOIN dim_location l ON s.location_number = l.location_number GROUP BY ALL\n'
    'GOOD (CTE-first pattern — materialize metric view, then JOIN):\n'
    '  WITH sales AS (\n'
    '    SELECT location_number, MEASURE(cy_sales) AS cy_sales_value FROM mv_sales GROUP BY ALL\n'
    '  )\n'
    '  SELECT s.location_number, l.zone_name, s.cy_sales_value '
    'FROM sales s JOIN dim_location l ON s.location_number = l.location_number\n'
    '\n'
    '## CRITICAL: MEASURE() Alias Collision Rule\n'
    '- NEVER alias MEASURE(col) back to the same column name. '
    'Spark shadows the underlying measure column with the alias and '
    'fails ORDER BY / HAVING with '
    'MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.\n'
    'BAD:  SELECT zone, MEASURE(cy_sales) AS cy_sales FROM mv GROUP BY zone ORDER BY MEASURE(cy_sales) DESC\n'
    'GOOD: SELECT zone, MEASURE(cy_sales) AS cy_sales_value FROM mv GROUP BY zone ORDER BY cy_sales_value DESC\n'
    '\n'
    '## Question-SQL Alignment\n'
    '- expected_sql MUST answer EXACTLY what the question asks — no more, no less.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- If the question is ambiguous about a filter, do NOT assume one UNLESS the Genie '
    'Space Instructions mandate it as a default.\n'
    '\n'
    '## CRITICAL: Instruction-Mandated Default Filters\n'
    'The Genie Space Instructions section above may define default filters. These are MANDATORY:\n'
    '- EVERY example SQL in the scope of a default filter MUST include that filter. '
    'Omitting it teaches Genie the wrong query shape.\n'
    '- The question text MUST reflect the default filter so question and SQL stay aligned.\n'
    '\n'
    '## Minimal SQL Principle\n'
    'Write the simplest correct SQL. Prefer fewer columns and filters. '
    'For multi-table questions JOINs are expected and encouraged, but only when '
    'the question really spans two assets.\n'
    '\n'
    '## Diversity Quotas For This Call\n'
    '{{ generation_profile_quotas }}\n'
    '\n'
    'Avoid repeating the same question wording, exact intent, WHERE filters, '
    'ORDER BY shape, or selected dimensions across examples in this call. '
    'Do NOT copy benchmark-style wording verbatim. The examples should teach '
    'useful business query patterns without echoing evaluation rows.\n'
    '\n'
    '## Diversity\n'
    'Cover different query shapes (aggregations, filters, temporal comparisons, '
    'ranking). Do NOT duplicate the intent of any "Already Covered Questions" '
    'block appended below this prompt.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of example objects. No markdown, just JSON.\n'
    '\n'
    'Each object:\n'
    '- "question": clean business question, customer-style phrasing\n'
    '- "expected_sql": valid Databricks SQL using fully-qualified names from VALID Data Assets '
    '(metric views: MEASURE() syntax; TVFs: function call; tables: standard SQL)\n'
    '- "usage_guidance": one short sentence telling Genie when to surface this example '
    '(e.g. "Use when the user asks about monthly revenue by region.")\n'
    '</output_schema>'
)

EXAMPLE_SQL_CORRECTION_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL expert fixing invalid Genie Space example '
    'SQLs. These examples TEACH Genie common query shapes, so the '
    'corrected SQL must be realistic and clear for a business user.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## VALID Data Assets (ONLY these exist)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '## Examples to Fix\n'
    'Each entry below has these keys:\n'
    '- ``question`` / ``original_expected_sql`` — the input.\n'
    '- ``error`` — the raw Spark error string.\n'
    '- ``validation_reason_code`` — a stable taxonomy code (e.g. '
    '``mv_alias_collision``, ``mv_missing_measure_function``, '
    '``unknown_column``).\n'
    '- ``repair_hint`` — a class-specific instruction describing the '
    'minimal change. **Apply the repair_hint before any other rewrite.**\n'
    '{{ benchmarks_to_fix }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Fix each example so expected_sql is valid using ONLY the assets and columns above. '
    'When ``repair_hint`` is present, follow it exactly — it is the deterministic fix for '
    'the error class.\n'
    '\n'
    '- Wrong table/view name: find closest matching valid asset, rewrite SQL.\n'
    '- Field drift (e.g., property_name vs property): map to closest valid column.\n'
    '- Metric views: use MEASURE() syntax for aggregates in SELECT/ORDER BY.\n'
    '- Metric view alias collision: NEVER use ORDER BY alias when alias == source column\n'
    '  for MEASURE() expressions. Use ORDER BY MEASURE(column) directly.\n'
    '- Metric views: NEVER use SELECT * or direct JOINs on metric views. '
    'All measures MUST use MEASURE().\n'
    '- Metric views: NEVER use MEASURE() in WHERE, HAVING, ON, or CASE WHEN clauses — '
    'MEASURE() is only valid in SELECT and ORDER BY. To filter on a measure, materialize '
    'it in a CTE first, then filter on the alias.\n'
    '- Metric view + JOIN: If the error is METRIC_VIEW_JOIN_NOT_SUPPORTED, rewrite using '
    'the CTE-first pattern — materialize the metric view in a WITH clause, then JOIN the '
    'CTE to the dimension table:\n'
    '  BAD:  SELECT s.id, l.name, MEASURE(s.sales) FROM mv_sales s JOIN dim l ON s.id = l.id\n'
    '  GOOD: WITH sales AS (SELECT id, MEASURE(sales) AS sales_value FROM mv_sales GROUP BY ALL) '
    'SELECT s.id, l.name, s.sales_value FROM sales s JOIN dim l ON s.id = l.id\n'
    '- TVFs: use correct function call signature.\n'
    '- If error says "Query returns 0 rows", the SQL is syntactically valid but\n'
    '  references impossible filter values. Use the Data Profile to pick realistic values.\n'
    '- If no valid asset can answer the question, set expected_sql to null with unfixable_reason.\n'
    '- Preserve original question text when possible.\n'
    '- Apply MINIMAL SQL PRINCIPLE: corrected SQL answers exactly what the question asks.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of objects. No markdown, just JSON.\n'
    '\n'
    'Each object: "question", "expected_sql" (corrected or null), '
    '"usage_guidance", "unfixable_reason" (null if fixed).\n'
    '</output_schema>'
)


CURATED_SQL_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL expert generating ground-truth SQL for benchmark questions.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## VALID Data Assets (ONLY these exist)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Genie Space Instructions (business rules — follow these)\n'
    '{{ instructions_context }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '## Curated Questions (generate SQL for each)\n'
    '{{ questions_json }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate valid Databricks SQL for each curated question using ONLY the assets and '
    'columns listed above.\n'
    '\n'
    '- The SQL must answer EXACTLY what the question asks — no more, no less.\n'
    '- Use only columns from the Column Allowlist.\n'
    '- Metric views: use MEASURE() syntax for aggregates in SELECT/ORDER BY.\n'
    '- Metric views: NEVER use direct JOINs on metric views (causes METRIC_VIEW_JOIN_NOT_SUPPORTED). '
    'If you need dimension columns from another table, use the CTE-first pattern: materialize the '
    'metric view in a WITH clause, then JOIN the CTE to the dimension table.\n'
    '- Multi-table queries: use Join Specifications for valid join paths.\n'
    '- Data Profile: use realistic filter values from the profile.\n'
    '- If a question truly cannot be answered with the available assets, set expected_sql '
    'to null with unfixable_reason explaining why.\n'
    '\n'
    '## CRITICAL: Instruction-Mandated Default Filters\n'
    'The Genie Space Instructions above define the business rules for this space, including '
    'default filters. These instructions are the SOURCE OF TRUTH.\n'
    '- If instructions say "Default filter: X = Y for all Z queries", EVERY SQL for Z-type '
    'questions MUST include WHERE X = Y. Omitting an instruction-mandated default filter '
    'produces incorrect ground truth that will penalize Genie for correct behavior.\n'
    '- Only omit a default filter if the question EXPLICITLY asks to exclude it '
    '(e.g. "including non-same-store locations").\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of objects. No markdown, just JSON.\n'
    '\n'
    'Each object: {{"question": "...", "expected_sql": "..." or null, '
    '"expected_asset": "TABLE"|"MV"|"TVF", '
    '"category": "aggregation"|"ranking"|"time-series"|"comparison"|"detail"|"list"|"threshold"|"multi-table", '
    '"required_tables": [...], "required_columns": [...], "expected_facts": [], '
    '"unfixable_reason": null or "..."}}\n'
    '</output_schema>'
)

BENCHMARK_ALIGNMENT_CHECK_PROMPT = (
    '<role>\n'
    'You are a Databricks SQL quality reviewer.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Benchmarks to Review\n'
    '{{ benchmarks_json }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Determine whether each benchmark SQL answers EXACTLY what the question asks.\n'
    '\n'
    '## Issue Types\n'
    '- EXTRA_FILTER: SQL adds WHERE conditions not mentioned in the question.\n'
    '- EXTRA_COLUMNS: SQL returns columns the question did not ask for.\n'
    '- MISSING_AGGREGATION: Question implies aggregation but SQL returns unaggregated rows.\n'
    '- WRONG_INTERPRETATION: SQL answers a materially different question.\n'
    '\n'
    '## Strictness\n'
    '- EXTRA_FILTER: Be strict. If question says "revenue by destination" without '
    'mentioning a status, booking_status filters are EXTRA.\n'
    '- EXTRA_COLUMNS: Be lenient. 1-2 contextual columns (e.g., name alongside ID) are OK.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array (one object per benchmark). No markdown, just JSON.\n'
    '\n'
    '{"question": "...", "aligned": true/false, '
    '"issues": ["ISSUE_TYPE: description", ...]}\n'
    '</output_schema>'
)

BENCHMARK_COVERAGE_GAP_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space evaluation expert.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Domain: {{ domain }}\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{{ valid_assets_context }}\n'
    '\n'
    '## Tables and Columns\n'
    '{{ tables_context }}\n'
    '\n'
    '## Column Allowlist (Extract-Over-Generate — use ONLY these column names)\n'
    '{{ column_allowlist }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{{ tvfs_context }}\n'
    '\n'
    '## Join Specifications (how tables relate)\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Uncovered Assets (MUST be targeted)\n'
    '{{ uncovered_assets }}\n'
    '\n'
    '## Already Covered Questions (do NOT duplicate these)\n'
    '{{ existing_questions }}\n'
    '\n'
    '## Data Profile (actual values from database)\n'
    '{{ data_profile_context }}\n'
    '\n'
    '{{ weak_categories_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'The uncovered assets above have ZERO benchmark questions. Generate 1-2 questions '
    'PER uncovered asset. Each question MUST reference the asset in its FROM/JOIN/function call.\n'
    '\n'
    '## Data-Grounded Values\n'
    'Use the Data Profile to generate realistic filter values — reference actual '
    'column values rather than inventing values.\n'
    '\n'
    '## Asset Constraint (Extract-Over-Generate)\n'
    'expected_sql MUST ONLY reference tables, metric views, and functions from VALID Data Assets. '
    'Do NOT invent or hallucinate names.\n'
    'required_columns and every column in expected_sql MUST come from the Column Allowlist. '
    'Do NOT invent column names. Before writing SQL, verify every column reference appears in the allowlist.\n'
    '\n'
    '## Metric View Query Rules\n'
    'When writing SQL for metric views:\n'
    '- NEVER use SELECT * — metric views require explicit column references.\n'
    '- ALL measure columns MUST be wrapped in MEASURE() in both SELECT and ORDER BY.\n'
    '- NEVER use MEASURE() in WHERE, HAVING, ON, or CASE WHEN clauses — MEASURE() is '
    'only valid in SELECT and ORDER BY. To filter on a measure, materialize it in a '
    'CTE first, then filter on the alias.\n'
    '- NEVER use JOINs at query time on metric views.\n'
    '- Dimensions (non-measure columns) are used for GROUP BY and filtering only.\n'
    '\n'
    '## Common Metric View SQL Mistakes (AVOID THESE)\n'
    'BAD:  SELECT zone, MEASURE(sales) FROM mv WHERE MEASURE(pct_chg) < -2\n'
    'GOOD: WITH t AS (SELECT zone, MEASURE(sales) AS s, MEASURE(pct_chg) AS p '
    'FROM mv GROUP BY zone) SELECT * FROM t WHERE p < -2\n'
    '\n'
    'BAD:  SELECT * FROM mv_store_sales\n'
    'GOOD: SELECT zone, MEASURE(total_sales) FROM mv_store_sales GROUP BY zone\n'
    '\n'
    '## CRITICAL: MEASURE() Alias Collision Rule\n'
    '- NEVER alias MEASURE(col) back to the same column name. '
    'Spark shadows the underlying measure column with the alias and '
    'fails ORDER BY / HAVING with '
    'MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION.\n'
    'BAD:  SELECT zone, MEASURE(cy_sales) AS cy_sales FROM mv GROUP BY zone ORDER BY MEASURE(cy_sales) DESC\n'
    'GOOD: SELECT zone, MEASURE(cy_sales) AS cy_sales_value FROM mv GROUP BY zone ORDER BY cy_sales_value DESC\n'
    '\n'
    '## Question-SQL Alignment\n'
    '- expected_sql MUST answer EXACTLY what the question asks — no more, no less.\n'
    '- Do NOT add WHERE filters the question does not mention.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '- If the Genie Space Instructions specify a default filter (e.g., same-store only, '
    'active status), and you include that filter in the SQL, you MUST mention it in the '
    'question text so the question and SQL stay aligned.\n'
    '\n'
    '## Minimal SQL Principle\n'
    'Write the simplest correct SQL. Prefer fewer columns and filters. '
    'For JOIN PATH items, JOINs are expected.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a JSON array of question objects. No markdown, just JSON.\n'
    '\n'
    'Each object:\n'
    '- "question": natural language question\n'
    '- "expected_sql": correct SQL using fully-qualified names from VALID Data Assets '
    '(metric views: MEASURE() syntax; TVFs: function call; tables: standard SQL)\n'
    '- "expected_asset": "MV" | "TVF" | "TABLE"\n'
    '- "category": one of {{ categories }}\n'
    '- "required_tables": list of table names\n'
    '- "required_columns": list of column names\n'
    '- "expected_facts": 1-2 facts the answer should contain\n'
    '</output_schema>'
)

# ── 5a. Unified RCA Engine Contract (defined before any consumer prompt) ─
#
# Shared header injected into every RCA-driven optimizer prompt so the
# strategist, lever, proposal, mining, and reactive paths all reason about
# the same control-plane invariants. Preflight example synthesis uses a
# narrower contract; see ``LEAK_SAFE_EXAMPLE_SYNTHESIS_CONTRACT_PROMPT``.

UNIFIED_RCA_ENGINE_CONTRACT_PROMPT = """\
<unified_rca_engine_contract>
## Unified RCA engine contract

The optimizer is a closed-loop control system. Every proposed action must
preserve this chain:

judge feedback -> RCA -> lever -> patch -> gateable outcome

Primary objective:
- Reach 100% post-arbiter accuracy, or exhaust the configured lever-loop budget.
- Hard failures are the first priority. Hard failures include arbiter verdicts
  `ground_truth_correct` and `neither_correct`.
- Soft signals may guide preventive improvements only when hard failures and
  mandatory regression debt are not being starved.

Mandatory causal fields:
- Every action group must declare `primary_cluster_id`, `source_cluster_ids`,
  and `affected_questions` using those exact JSON field names.
- Every proposal must be explainable as: this judge signal produced this RCA,
  this RCA maps to this lever, and this patch is expected to fix these target
  questions.
- If `regression_debt_qids` are present in context, they are mandatory priority
  and must be targeted before optional soft improvements.

Patch safety rules:
- A patch type must match RCA defect. A filter defect needs a filter patch,
  scoped instruction, or example SQL. Do not substitute a measure patch for a
  missing or wrong filter.
- A broad global instruction change is unsafe unless it is scoped to target
  questions or backed by explicit counterfactual dependents.
- Prefer narrow structured metadata, SQL expressions, join specs, or example SQL
  over broad prose when the root cause is structural SQL behavior.
- Preserve at least one causal patch per target question when proposing a bundle.

Regression policy awareness:
- Net post-arbiter gains can be accepted with bounded regression debt.
- Do not hide or ignore newly regressed hard questions; surface them as
  `regression_debt_qids`.
- Protected or required benchmark regressions must be treated as unbounded
  collateral risk.

Leakage boundary:
- Do not copy held-out benchmark expected SQL into Genie-visible examples.
- Use failure evidence and generated SQL to understand behavior, but output
  reusable guidance, scoped metadata, SQL expressions, or safe example patterns.

Precedence:
- If a downstream prompt provides a more specific lever map (for example a
  strategist `## Contract: All Instruments of Power` section), that map is
  authoritative for lever routing. This contract specifies the global control
  invariants only.
</unified_rca_engine_contract>
"""


LEAK_SAFE_EXAMPLE_SYNTHESIS_CONTRACT_PROMPT = """\
<leak_safe_example_synthesis_contract>
## Leak-safe example synthesis contract

This prompt creates one reusable Genie-visible example question/SQL pair.

Rules:
- Return exactly one single JSON object matching the requested schema.
- Do not copy held-out benchmark expected SQL into Genie-visible examples.
- Use schema metadata and the supplied failure pattern to create a reusable
  example; remove benchmark-specific literals, aliases, row limits, and wording.
- Keep the generated SQL narrow to the requested tables, joins, filters, and
  measures. Do not introduce broad global behavior.
- Prefer one precise example over a generalized rule.
</leak_safe_example_synthesis_contract>
"""

# ── Feature flag: allow operators to disable contract injection in
# emergencies without a code change. Default is ON.
_INCLUDE_UNIFIED_RCA_CONTRACT = (
    os.getenv("GSO_INCLUDE_UNIFIED_RCA_CONTRACT", "true").strip().lower()
    not in ("false", "0", "no", "off")
)

_RCA_CONTRACT_HEADER: str = (
    UNIFIED_RCA_ENGINE_CONTRACT_PROMPT + "\n\n"
    if _INCLUDE_UNIFIED_RCA_CONTRACT
    else ""
)

_EXAMPLE_SYNTHESIS_CONTRACT_HEADER: str = (
    LEAK_SAFE_EXAMPLE_SYNTHESIS_CONTRACT_PROMPT + "\n\n"
    if _INCLUDE_UNIFIED_RCA_CONTRACT
    else ""
)


# ── 5b. Proposal Generation Prompts ───────────────────────────────────

PROPOSAL_GENERATION_PROMPT = (
    '<role>\n'
    'You are a Databricks metadata optimization expert. Your job is to fix a Genie Space '
    'so that it generates correct SQL for user questions.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## Failure Analysis\n'
    '- Root cause: {{ failure_type }}\n'
    '- Blamed objects: {{ blame_set }}\n'
    '- Affected questions ({{ severity }}): {{ affected_questions }}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Metadata for Blamed Objects\n'
    '{{ current_metadata }}\n'
    '\n'
    '## Target Change Type\n'
    '{{ patch_type_description }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs. Identify EXACTLY what metadata change (column description, '
    'table description, or instruction) would guide Genie to produce the expected SQL.\n'
    '\n'
    '- Be specific — reference actual table/column names from the SQL.\n'
    '- Do NOT generate generic instructions. Generate a targeted metadata fix.\n'
    '- Instruction budget remaining: {{ instruction_char_budget }} chars. Keep additions under 500 chars.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON: {"proposed_value": "...", "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_1_2_COLUMN_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to fix table '
    'and column descriptions and synonyms so that Genie generates correct SQL.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## Failure Analysis\n'
    '- Root cause: {{ failure_type }}\n'
    '- Blamed objects: {{ blame_set }}\n'
    '- Affected questions: {{ affected_questions }}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Full Genie Space Schema\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Structured Table Metadata\n'
    'Tables relevant to the failure. [EDITABLE] sections may be updated; '
    '[LOCKED] sections are owned by another lever — do NOT modify.\n'
    '{{ structured_table_context }}\n'
    '\n'
    '## Structured Column Metadata\n'
    'Columns relevant to the failure. [EDITABLE] may be updated; [LOCKED] must not.\n'
    '{{ structured_column_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: wrong_column failure — Genie selects "store_id" instead of "location_id"\n'
    'Blamed: catalog.schema.dim_store.location_id\n'
    'SQL diff: Expected "WHERE ds.location_id = 42" vs Generated "WHERE ds.store_id = 42"\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.dim_store", "column": "location_id",\n'
    '    "entity_type": "column_key",\n'
    '    "sections": {"synonyms": "store id, store number, store identifier",\n'
    '                  "definition": "Unique numeric identifier for a store location"}}\n'
    '],\n'
    '"table_changes": [],\n'
    '"rationale": "Genie confused store_id (which does not exist) with location_id. '
    'Adding store id as a synonym will resolve the ambiguity."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Propose changes at TWO levels:\n'
    '\n'
    '## Column-level changes\n'
    'For each column that needs fixing, provide ONLY the sections you want to change.\n'
    'Valid section keys: definition, values, synonyms, aggregation, grain_note, '
    'purpose, best_for, grain, scd, join, important_filters.\n'
    '\n'
    '- **synonyms**: comma-separated alternative names. '
    'Existing synonyms are auto-preserved; provide only NEW terms.\n'
    '- **definition**: concise business description of the column.\n'
    '\n'
    '## Table-level changes\n'
    'Provide sections from: purpose, best_for, grain, scd, relationships.\n'
    '\n'
    '## Rules\n'
    '- Only include sections you want to CHANGE. Omit correct sections.\n'
    '- Only update [EDITABLE] sections. Never touch [LOCKED] sections.\n'
    '- AUGMENT existing content — incorporate existing info and add new details. '
    'Only rewrite from scratch if current value is empty or misleading.\n'
    '- If a column description is correct, prefer adding synonyms.\n'
    '- Do NOT repeat synonyms already in the metadata.\n'
    '- Be specific — reference actual table/column names from the SQL diffs.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object. No analysis or commentary — put reasoning in "rationale".\n'
    '\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>", "column": "<column_name>",\n'
    '    "entity_type": "<column_dim|column_measure|column_key>",\n'
    '    "sections": {"definition": "new value", "synonyms": "term1, term2"}}\n'
    '],\n'
    '"table_changes": [\n'
    '  {"table": "<fully_qualified_table>",\n'
    '    "sections": {"purpose": "...", "best_for": "...", "grain": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

DESCRIPTION_ENRICHMENT_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'concise, accurate column descriptions for columns that currently have NO '
    'description at all — neither in Unity Catalog nor in the Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables and Columns Needing Descriptions\n'
    '{{ columns_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Data Profile (actual values from sampled data)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input:\n'
    'Table: catalog.schema.fact_orders (Purpose: Transactional order data)\n'
    '  Columns needing descriptions:\n'
    '    - order_amount (DOUBLE) [column_measure] — cardinality=4500, range=[0.01, 99999.99]\n'
    '    - customer_id (BIGINT) [column_key] — cardinality=12000\n'
    '    - fulfillment_status (STRING) [column_dim] — cardinality=5, '
    "values=['pending', 'shipped', 'delivered', 'cancelled', 'returned']\n"
    '  Sibling columns (for context): order_id, order_date, ship_date, region\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.fact_orders", "column": "order_amount",\n'
    '    "entity_type": "column_measure",\n'
    '    "sections": {"definition": "Total monetary value of the order in USD",\n'
    '                  "aggregation": "SUM for revenue totals, AVG for average order value"}},\n'
    '  {"table": "catalog.schema.fact_orders", "column": "customer_id",\n'
    '    "entity_type": "column_key",\n'
    '    "sections": {"definition": "Foreign key referencing the customer dimension",\n'
    '                  "join": "Joins to dim_customer.customer_id"}},\n'
    '  {"table": "catalog.schema.fact_orders", "column": "fulfillment_status",\n'
    '    "entity_type": "column_dim",\n'
    '    "sections": {"definition": "Current fulfillment state of the order",\n'
    '                  "values": "pending, shipped, delivered, cancelled, returned"}}\n'
    '],\n'
    '"rationale": "Used data profile values for fulfillment_status and range for order_amount."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'For each column listed, generate structured description sections appropriate '
    'to its entity type:\n'
    '- column_dim: definition, values, synonyms\n'
    '- column_measure: definition, aggregation, grain_note, synonyms\n'
    '- column_key: definition, join, synonyms\n'
    '\n'
    'Rules:\n'
    '- Infer meaning from: column name, data type, table purpose, sibling columns, '
    'and data profile (cardinality, sample values, ranges).\n'
    '- Be CONCISE — one sentence per section. Do NOT repeat information across sections.\n'
    '- For "values": use actual distinct values from the data profile when available; '
    'otherwise only list if confidently inferrable from the column name '
    '(e.g. status, type, category columns). OMIT the values section when uncertain.\n'
    '- For "synonyms": provide alternative names a user might type when querying.\n'
    '- For "join": specify which table.column this key likely joins to, based on '
    'naming conventions and sibling context.\n'
    '- If you cannot confidently infer a meaningful description, write '
    '"General-purpose [data_type] column" for the definition and OMIT other sections.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Do NOT invent table or column names.\n'
    '- Do NOT include sections you are uncertain about — omit them entirely.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>", "column": "<column_name>",\n'
    '    "entity_type": "<column_dim|column_measure|column_key>",\n'
    '    "sections": {"definition": "...", "values": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

TABLE_DESCRIPTION_ENRICHMENT_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'concise, structured table descriptions for tables that currently have NO '
    'useful description — neither in Unity Catalog nor in the Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables Needing Descriptions\n'
    '{{ tables_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Data Profile (actual values from sampled data)\n'
    '{{ data_profile_context }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input:\n'
    'Table: catalog.schema.fact_orders\n'
    '  Current description: (none)\n'
    '  Row count: ~250000\n'
    '  Columns: order_id (BIGINT), customer_id (BIGINT), order_date (DATE), '
    'amount (DOUBLE), status (STRING), region (STRING)\n'
    '  Data profile: status: values=[\'pending\', \'shipped\', \'delivered\']; '
    'region: values=[\'US\', \'EU\', \'APAC\']\n'
    '\n'
    'Output:\n'
    '{"changes": [\n'
    '  {"table": "catalog.schema.fact_orders",\n'
    '    "sections": {\n'
    '      "purpose": "Transactional order data with one row per order",\n'
    '      "best_for": "Revenue analysis, order volume trends, fulfillment tracking",\n'
    '      "grain": "One row per order (order_id)",\n'
    '      "scd": "Append-only fact table; status column reflects latest state"}}\n'
    '],\n'
    '"rationale": "Inferred grain from ~250K rows and order_id key; used profile for context."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'For each table listed, generate structured description sections:\n'
    '- purpose: One sentence describing what data the table holds\n'
    '- best_for: Comma-separated list of analytics this table supports\n'
    '- grain: What one row represents (include the key column)\n'
    '- scd: Slowly changing dimension type or update pattern (append-only, '
    'Type 2, snapshot, etc). OMIT if not inferrable.\n'
    '\n'
    'Rules:\n'
    '- Infer meaning from: table name, column names and types, row counts, '
    'column value distributions from the data profile, and metric view context.\n'
    '- Be CONCISE — one sentence per section. Do NOT repeat information across sections.\n'
    '- If a table already has a short description, incorporate it but expand.\n'
    '- You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Do NOT invent table or column names.\n'
    '- If you cannot confidently infer the table purpose, write '
    '"General-purpose data table" for purpose and OMIT other sections.\n'
    '- Do NOT include sections you are uncertain about — omit them entirely.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"changes": [\n'
    '  {"table": "<fully_qualified_table>",\n'
    '    "sections": {"purpose": "...", "best_for": "...", "grain": "..."}}\n'
    '],\n'
    '"rationale": "..."}\n'
    '</output_schema>'
)

# ── Proactive Space Metadata Prompts ──────────────────────────────────

SPACE_DESCRIPTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to write '
    'a concise, structured description for a Genie Space that has NO '
    'description yet. The description helps users understand what data is '
    'available and what questions they can ask.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Existing Instructions\n'
    '{{ instructions_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Write a plain-text Genie Space description (150-300 words) with these '
    'ALL-CAPS sections:\n'
    '\n'
    'DATA COVERAGE:\n'
    '- Bullet points summarising the tables and domains covered\n'
    '- Include approximate entity counts if inferrable from table names\n'
    '\n'
    'AVAILABLE ANALYTICS:\n'
    '1. Numbered categories of analyses the data supports\n'
    '\n'
    'USE CASES:\n'
    '- Role-based use case bullets (e.g. "Sales managers (regional tracking)")\n'
    '\n'
    'TIME PERIODS:\n'
    '- Temporal coverage and supported granularities\n'
    '\n'
    'Rules:\n'
    '- Infer the domain from table names, column names, and metric views.\n'
    '- Do NOT invent data that is not represented in the schema.\n'
    '- Do NOT use Markdown (no #, **, ```, etc.).\n'
    '- Use plain bullet points (- or numbered lists) only.\n'
    '- Keep it factual and concise.\n'
    '- Start with a single sentence summarising the space before the sections.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY the description text — no JSON wrapper, no code fences.\n'
    '</output_schema>'
)

PROACTIVE_INSTRUCTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space configuration expert. Your job is to '
    'write the initial routing instructions for a Genie Space that has NO '
    'instructions yet (or only empty / whitespace prose). These instructions '
    'follow the canonical 5-section schema — they help Genie disambiguate '
    'the user, understand data-quality caveats, respect hard constraints, '
    'and render result summaries consistently.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Join Specifications\n'
    '{{ join_specs_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Write Markdown prose using exactly the five canonical `##` headers '
    'below, in this order, omitting any section with nothing to say. '
    'Keep total length under 2000 characters.\n'
    '\n'
    'Exact headers (case-insensitive for #1-#4; header #5 is VERBATIM, '
    'including capitalization and wording):\n'
    '\n'
    '## PURPOSE\n'
    '- 1-2 bullets stating the space scope and audience.\n'
    '- Infer from table / metric view descriptions and join specs.\n'
    '\n'
    '## DISAMBIGUATION\n'
    '- Clarification-question triggers: "When the user asks about X '
    'without specifying Y, ask them to clarify Y."\n'
    '- Term-resolution rules: "\'Q1\' means calendar Q1 unless the user '
    'says \'fiscal Q1\'."\n'
    '- Source: columns with overlapping synonyms across tables; temporal '
    'columns admitting multiple calendars.\n'
    '\n'
    '## DATA QUALITY NOTES\n'
    '- NULL handling, known bad rows, column semantics that are NOT in '
    'the column description.\n'
    '- Source: column descriptions mentioning NULL / unknown / effective '
    '/ is_current; fields flagged as low-completeness.\n'
    '\n'
    '## CONSTRAINTS\n'
    '- Hard guardrails: what NEVER to show (PII columns, secrets) and '
    'what NOT to do (cross-join, ignore a required filter).\n'
    '- Behavioural rules only — SQL-expressible filters MUST be stored '
    'as sql_snippets, NOT in this section.\n'
    '\n'
    '## Instructions you must follow when providing summaries\n'
    '- Summary customisation: rounding rules, mandatory caveats, date-'
    'range statements.\n'
    '- This header is Databricks\'s verbatim blessed string — do NOT '
    'paraphrase, re-case, or shorten it.\n'
    '\n'
    'Non-regressive rules:\n'
    '- Be factual — infer ONLY from the schema provided.\n'
    '- Do NOT invent business rules that are not evident in column names '
    'or descriptions.\n'
    '- Do NOT emit any SQL keyword or clause anywhere in the prose; '
    'SQL belongs in sql_snippets / join_specs / example_question_sqls. '
    'When describing rules in English, use verbs like "combine", "link", '
    '"pair", "associate" instead of "join"; "given that" instead of '
    '"where"; "after" instead of "order by".\n'
    '- Do NOT emit any `##` header not in the five above.\n'
    '- Do NOT use `###` subheaders — they belong to structured targets, '
    'not prose.\n'
    '- If a section has nothing to say, OMIT it rather than emitting a '
    'placeholder.\n'
    '- Use short, imperative bullets (`- …`) under each header.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY the instruction text — no JSON wrapper, no code '
    'fences, no preamble.\n'
    '</output_schema>'
)

# Used by the two-phase proactive seeding path: when a space already has
# instructions but is missing one or more canonical sections, the expand
# prompt fills in only the gaps without touching sections that already
# exist. Output is strict JSON keyed by exact canonical headers so the
# caller can merge without a re-parse of free-form prose.
EXPAND_INSTRUCTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space configuration expert. The space '
    'already has instructions but is missing one or more of the five '
    'canonical sections. Your job is to write ONLY the missing sections '
    '— never rewrite or rephrase content that already exists.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## Existing Instructions\n'
    '{{ existing_instructions }}\n'
    '\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Join Specifications\n'
    '{{ join_specs_context }}\n'
    '\n'
    '## Missing Sections (generate each one if there is meaningful '
    'content to add — otherwise omit)\n'
    '{{ missing_sections }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate content ONLY for the headers listed in "Missing Sections". '
    'Never emit a header that is not in that list. Emit each header '
    'VERBATIM as given (case-sensitive for header #5, case-insensitive '
    'parser for the others — still, copy them as-shown).\n'
    '\n'
    'Per-section guidance:\n'
    '\n'
    '- `## PURPOSE` — 1-2 bullets on space scope and audience. If such '
    'content is evident in the schema or existing prose, capture it; '
    'otherwise OMIT.\n'
    '- `## DISAMBIGUATION` — clarification-question triggers and '
    'term-resolution rules. If such content is evident in the schema or '
    'existing prose, capture it; otherwise OMIT.\n'
    '- `## DATA QUALITY NOTES` — NULL handling, known bad rows, column '
    'semantics not captured in descriptions. If such content is evident '
    'in the schema or existing prose, capture it; otherwise OMIT.\n'
    '- `## CONSTRAINTS` — hard guardrails (PII columns, forbidden '
    'operations). If such content is evident in the schema or existing '
    'prose, capture it; otherwise OMIT.\n'
    '- `## Instructions you must follow when providing summaries` — '
    'summary-rendering rules (rounding, mandatory caveats, date-range '
    'statements). Copy the header letter-for-letter. If such content is '
    'evident in the schema or existing prose, capture it; otherwise OMIT.\n'
    '\n'
    'Non-regressive rules (parity with proactive seeding — expand must '
    'not invent what seed would have omitted):\n'
    '- Be factual — infer ONLY from the schema + existing instructions.\n'
    '- Do NOT invent business rules that are not evident in column names, '
    'descriptions, or existing prose.\n'
    '- If a requested section has nothing meaningful to add, OMIT its '
    'key from the output JSON rather than emitting a placeholder.\n'
    '\n'
    '## Budget\n'
    'The existing prose is {{ existing_length }} chars. You have '
    '{{ remaining_budget }} chars remaining in the 2000-char cap for '
    '{{ missing_count }} section(s) — aim for {{ per_section_budget }} '
    'chars each. Going over will cause your output to be discarded.\n'
    '\n'
    'Strict output rules:\n'
    '- Do NOT include any `##` header not listed in "Missing Sections".\n'
    '- Do NOT emit any SQL keyword or clause anywhere in the prose; '
    'SQL belongs in sql_snippets / join_specs / example_question_sqls. '
    'When describing rules in English, use verbs like "combine", "link", '
    '"pair", "associate" instead of "join"; "given that" instead of '
    '"where"; "after" instead of "order by".\n'
    '- Do NOT include code fences or `###` subheaders.\n'
    '- Do NOT rewrite or duplicate existing content.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return strict JSON. Example:\n'
    '{\n'
    '  "sections": {\n'
    '    "## PURPOSE": "- Analytics for the sales team covering H1 revenue.\\n",\n'
    '    "## DATA QUALITY NOTES": "- The `status` column has mixed casing; '
    'normalize before filtering.\\n"\n'
    '  }\n'
    '}\n'
    '\n'
    'Keys MUST be exact canonical headers. Values are plain text with '
    'one bullet per line (each starting with `- `).\n'
    '</output_schema>'
)

SAMPLE_QUESTIONS_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space metadata expert. Your job is to '
    'generate sample questions that users can click on to quickly explore '
    'the data available in a Genie Space.\n'
    '</role>\n'
    '\n'
    '<context>\n'
    '## Space Description\n'
    '{{ description_context }}\n'
    '\n'
    '## Tables\n'
    '{{ tables_context }}\n'
    '\n'
    '## Metric Views\n'
    '{{ metric_views_context }}\n'
    '\n'
    '## Existing Instructions\n'
    '{{ instructions_context }}\n'
    '</context>\n'
    '\n'
    '<instructions>\n'
    'Generate 5-8 sample questions that:\n'
    '- Cover different tables and metric views (spread across the schema)\n'
    '- Mix query patterns: aggregation, filtering, ranking, time-based trends\n'
    '- Are phrased in natural language (NOT SQL)\n'
    '- Reference real column names and concepts from the schema\n'
    '- Are answerable with the available data\n'
    '- Vary in complexity (some simple, some multi-dimensional)\n'
    '\n'
    'Rules:\n'
    '- Each question should be a single sentence.\n'
    '- Do NOT generate questions requiring data not in the schema.\n'
    '- Do NOT duplicate questions that ask the same thing differently.\n'
    '- Prefer questions that showcase the most useful analytics.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Respond with ONLY a JSON object:\n'
    '{"questions": ["question 1", "question 2", ...],\n'
    ' "rationale": "Brief explanation of coverage choices"}\n'
    '</output_schema>'
)

LEVER_4_JOIN_SPEC_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space join optimization expert.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## SQL Diffs showing join issues\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Join Specs\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Table Relationships\n'
    '{{ table_relationships }}\n'
    '\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Expected SQL joins fact_orders to dim_customer on customer_key, '
    'but Generated SQL has no join — just queries fact_orders alone.\n'
    '\n'
    'Output:\n'
    '{"join_spec": {\n'
    '  "left": {"identifier": "catalog.schema.fact_orders", "alias": "fact_orders"},\n'
    '  "right": {"identifier": "catalog.schema.dim_customer", "alias": "dim_customer"},\n'
    '  "sql": ["`fact_orders`.`customer_key` = `dim_customer`.`customer_key`", '
    '"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]\n'
    '}, "rationale": "fact_orders references dim_customer via customer_key (both BIGINT). '
    'The generated SQL could not resolve customer name without this join."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs to determine which tables need to be joined and how. '
    'Compare expected SQL JOIN clauses with generated SQL to identify missing or '
    'incorrect join specifications.\n'
    '\n'
    '## Data Type Rule\n'
    'Join columns MUST have compatible data types. Check column types in the schema '
    'context before proposing a join. Joining INT to STRING is invalid.\n'
    '\n'
    '## Identifier Rule\n'
    'You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Metric View Join Rule\n'
    'If EITHER side of the join is a metric view (name starts with mv_ or uses MEASURE()), '
    'the join CANNOT be used as a direct SQL JOIN — it causes METRIC_VIEW_JOIN_NOT_SUPPORTED. '
    'Instead, Genie must use the CTE-first pattern: materialize the metric view in a WITH clause, '
    'then JOIN the CTE to the other table. Set the "instruction" field to explain this.\n'
    '\n'
    '## Join Spec Format\n'
    '- alias: unqualified table name (last segment of identifier)\n'
    '- join condition: backtick-quoted aliases, e.g. '
    '"`fact_sales`.`product_key` = `dim_product`.`product_key`"\n'
    '- relationship_type: one of FROM_RELATIONSHIP_TYPE_MANY_TO_ONE, '
    'FROM_RELATIONSHIP_TYPE_ONE_TO_MANY, FROM_RELATIONSHIP_TYPE_ONE_TO_ONE\n'
    '- instruction: usage guidance for this join (REQUIRED — explain when and how to use it, '
    'especially CTE-first for metric view joins)\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON:\n'
    '{"join_spec": {\n'
    '  "left": {"identifier": "<fully_qualified_table>", "alias": "<short_name>"},\n'
    '  "right": {"identifier": "<fully_qualified_table>", "alias": "<short_name>"},\n'
    '  "sql": ["<join_condition>", "--rt=<relationship_type>--"],\n'
    '  "instruction": "<usage guidance: when to use this join, CTE-first for metric views>"\n'
    '}, "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_4_JOIN_DISCOVERY_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space join optimization expert. '
    'Your task is to identify MISSING join relationships between tables.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Currently Defined Join Specs\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Heuristic Candidate Hints\n'
    'Table pairs flagged by automated analysis as potential join candidates. '
    'These are HINTS only — validate them using the schema context.\n'
    '{{ discovery_hints }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input hint: "fact_sales and dim_region share region_id columns (both INT)"\n'
    'Current join specs: [fact_sales↔dim_product]\n'
    '\n'
    'Output:\n'
    '{"join_specs": [\n'
    '  {"left": {"identifier": "catalog.schema.fact_sales", "alias": "fact_sales"},\n'
    '    "right": {"identifier": "catalog.schema.dim_region", "alias": "dim_region"},\n'
    '    "sql": ["`fact_sales`.`region_id` = `dim_region`.`region_id`", '
    '"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]}\n'
    '], "rationale": "Both tables have region_id (INT). fact_sales has many rows per region. '
    'This join is not already defined."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Review hints alongside the full schema context. For each hint, validate:\n'
    '1. Column data types MUST be compatible (INT=INT, BIGINT=INT, STRING=STRING). '
    'Do NOT propose incompatible type joins.\n'
    '2. Column names/descriptions suggest a foreign-key relationship.\n'
    '3. The join is not already defined in current join specs.\n'
    '\n'
    'Also look for additional missing joins NOT covered by the hints.\n'
    '\n'
    '## Identifier Rule\n'
    'You MUST ONLY reference tables and columns from the Identifier Allowlist. '
    'Any table or column not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Metric View Join Rule\n'
    'If either table is a metric view (name starts with mv_ or uses MEASURE()), '
    'the join cannot be used as a direct SQL JOIN. Genie must use a CTE-first pattern. '
    'Set the "instruction" field to explain this constraint.\n'
    '\n'
    '## Join Spec Format\n'
    '- alias: unqualified table name (last segment of identifier)\n'
    '- join condition: backtick-quoted aliases\n'
    '- relationship_type: one of FROM_RELATIONSHIP_TYPE_MANY_TO_ONE, '
    'FROM_RELATIONSHIP_TYPE_ONE_TO_MANY, FROM_RELATIONSHIP_TYPE_ONE_TO_ONE\n'
    '- instruction: usage guidance (REQUIRED — explain when/how to use, '
    'CTE-first for metric view joins)\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON:\n'
    '{"join_specs": [\n'
    '  {"left": {"identifier": "<fq_table>", "alias": "<short_name>"},\n'
    '    "right": {"identifier": "<fq_table>", "alias": "<short_name>"},\n'
    '    "sql": ["<join_condition>", "--rt=<relationship_type>--"],\n'
    '    "instruction": "<usage guidance>"}\n'
    '], "rationale": "..."}\n'
    '\n'
    'If no valid joins found: {"join_specs": [], "rationale": "..."}\n'
    '</output_schema>'
)

# ── Canonical Instruction Section Vocabulary ──────────────────────────
# Sections are aligned to levers so each lever's instruction_contribution
# naturally reinforces its primary fix in the corresponding section(s).

INSTRUCTION_SECTION_ORDER: list[str] = [
    "PURPOSE",
    "ASSET ROUTING",
    "BUSINESS DEFINITIONS",
    "DISAMBIGUATION",
    "AGGREGATION RULES",
    "FUNCTION ROUTING",
    "JOIN GUIDANCE",
    "QUERY RULES",
    "QUERY PATTERNS",
    "TEMPORAL FILTERS",
    "DATA QUALITY NOTES",
    "CONSTRAINTS",
]

LEVER_TO_SECTIONS: dict[int, list[str]] = {
    1: ["BUSINESS DEFINITIONS", "DISAMBIGUATION"],
    2: ["AGGREGATION RULES", "TEMPORAL FILTERS"],
    3: ["FUNCTION ROUTING"],
    4: ["JOIN GUIDANCE", "TEMPORAL FILTERS"],
    5: ["ASSET ROUTING", "QUERY RULES", "QUERY PATTERNS", "DATA QUALITY NOTES", "CONSTRAINTS"],
    6: ["AGGREGATION RULES", "QUERY PATTERNS"],
}

INSTRUCTION_FORMAT_RULES = (
    'The output is rendered as PLAIN TEXT, not Markdown.\n'
    'Use ALL-CAPS SECTION HEADERS followed by a colon. '
    'Use - for bullet points. Use blank lines between sections.\n'
    'Do NOT use Markdown syntax (no ##, no **, no backticks, no code fences).\n'
    '\n'
    'Canonical sections (use ONLY these headers, in this order, omit empty ones):\n'
    'PURPOSE:             What this Genie Space does and who it serves (1 paragraph)\n'
    'ASSET ROUTING:       When user asks about [topic], use [table/TVF/MV] (Lever 5)\n'
    'BUSINESS DEFINITIONS: [term] = [column] from [table] (Lever 1)\n'
    'DISAMBIGUATION:      When [ambiguous scenario], prefer [approach] (Lever 1)\n'
    'AGGREGATION RULES:   How to aggregate measures, grain rules, avoid double-counting (Lever 6 primary; Lever 2 may refine MV column descriptions)\n'
    'FUNCTION ROUTING:    When to use TVFs/UDFs vs raw tables, parameter guidance (Lever 3)\n'
    'JOIN GUIDANCE:       Explicit join paths and conditions (Lever 4)\n'
    'QUERY RULES:         SQL-level rules — filters, ordering, limits\n'
    'QUERY PATTERNS:      Common multi-step query patterns with actual column names\n'
    'TEMPORAL FILTERS:    Date partitioning, SCD filters, time-range rules (Lever 6 primary; Lever 4 for join-side temporal rules)\n'
    'DATA QUALITY NOTES:  Known nulls, is_current flags, data caveats\n'
    'CONSTRAINTS:         Cross-cutting behavioral constraints, output formatting\n'
    '\n'
    'Lever-to-section alignment (target your contribution to these sections):\n'
    '  Lever 1 -> BUSINESS DEFINITIONS, DISAMBIGUATION\n'
    '  Lever 2 -> MV column descriptions and synonyms only (CANNOT add measures, filters, or change MV SQL)\n'
    '  Lever 6 -> AGGREGATION RULES, TEMPORAL FILTERS\n'
    '  Lever 3 -> FUNCTION ROUTING\n'
    '  Lever 4 -> JOIN GUIDANCE, TEMPORAL FILTERS\n'
    '  Lever 5 -> ASSET ROUTING, QUERY RULES, QUERY PATTERNS, DATA QUALITY NOTES, CONSTRAINTS\n'
    '\n'
    'Non-regressive rules:\n'
    '- INCORPORATE all existing guidance into structured sections.\n'
    '- Do NOT discard existing instructions unless factually wrong.\n'
    '- AUGMENT each section with new learnings.\n'
    '- EVERY bullet must reference a specific asset (table, column, function).\n'
    '- NEVER include generic domain guidance without referencing an actual asset.\n'
)

LEVER_5_INSTRUCTION_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space instruction expert.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## SQL Diffs showing routing/disambiguation issues\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Current Text Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Routing failure — Genie queries fact_bookings table instead of '
    'calling get_booking_summary TVF for "What is the booking summary?"\n'
    '\n'
    'Output:\n'
    '{"instruction_type": "example_sql",\n'
    '  "example_question": "Show me the booking summary",\n'
    '  "example_sql": "SELECT * FROM catalog.schema.get_booking_summary(:start_date, :end_date)",\n'
    '  "parameters": [{"name": "start_date", "type_hint": "DATE", "default_value": "2024-01-01"},\n'
    '                  {"name": "end_date", "type_hint": "DATE", "default_value": "2024-12-31"}],\n'
    '  "usage_guidance": "Use when user asks about booking summaries or booking overview",\n'
    '  "rationale": "Routing failure: Genie should call get_booking_summary TVF, not query fact_bookings. '
    'Example SQL teaches Genie the correct pattern."}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    'Analyze the SQL diffs and choose the HIGHEST-PRIORITY instruction type.\n'
    '\n'
    '## Instruction Type Priority (MUST follow this hierarchy)\n'
    '1. **SQL expressions** — For business metric/filter/dimension definitions. '
    'Choose ONLY when earlier levers missed a column-level semantic definition.\n'
    '2. **Example SQL queries** — For ambiguous, multi-part, or complex question patterns. '
    'Genie pattern-matches these to learn query patterns.\n'
    '3. **Text instructions** — LAST RESORT for clarification, formatting, cross-cutting guidance.\n'
    '\n'
    '## Routing Failures MUST Use Example SQL\n'
    'When failure involves asset routing (wrong table/TVF/MV), return instruction_type '
    '"example_sql". Example SQL is far more effective for routing — Genie matches it directly.\n'
    '\n'
    '## Rules for Text Instructions\n'
    '- Use ALL-CAPS HEADERS with colon, - bullets, short lines. No Markdown (no ##, no **, no backticks).\n'
    '- Use ONLY these section headers: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '- EVERY instruction MUST reference a specific asset from Available Assets.\n'
    '- NEVER generate generic domain guidance.\n'
    '- NEVER conflict with existing instructions.\n'
    '- Budget: {{ instruction_char_budget }} chars.\n'
    '\n'
    '## Rules for Example SQL\n'
    '- Question must be a realistic user prompt matching the failure pattern.\n'
    '- SQL must be correct and executable.\n'
    '- Every FROM table, JOIN table, column reference, and function call MUST appear in the Identifier Allowlist.\n'
    '- Do NOT duplicate existing example SQL questions.\n'
    '- Use `:param_name` markers for user-variable filters. '
    'For each parameter: name, type_hint (STRING|INTEGER|DATE|DECIMAL), default_value.\n'
    '- Include usage_guidance describing when Genie should match this query.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'You MUST ONLY use identifiers from the Identifier Allowlist. '
    'Any table, column, or function not in the allowlist is INVALID and will be rejected.\n'
    'If you cannot identify a specific asset to reference, return:\n'
    '{"instruction_type": "text_instruction", "instruction_text": "", '
    '"rationale": "No actionable fix identified"}\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return JSON with one of these formats:\n'
    '\n'
    'example_sql:\n'
    '{"instruction_type": "example_sql", "example_question": "...", "example_sql": "...", '
    '"parameters": [{"name": "...", "type_hint": "STRING|INTEGER|DATE|DECIMAL", '
    '"default_value": "..."}], '
    '"usage_guidance": "...", "rationale": "..."}\n'
    '\n'
    'text_instruction:\n'
    '{"instruction_type": "text_instruction", "instruction_text": "...", "rationale": "..."}\n'
    '\n'
    'sql_expression:\n'
    '{"instruction_type": "sql_expression", "target_table": "...", "target_column": "...", '
    '"expression": "...", "rationale": "..."}\n'
    '</output_schema>'
)

LEVER_5_HOLISTIC_PROMPT = (
    '<role>\n'
    'You are a Databricks Genie Space instruction architect. '
    'Synthesize ALL evaluation learnings into a single, coherent instruction document '
    'plus targeted example SQL queries.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<context>\n'
    '## Genie Space Purpose\n'
    '{{ space_description }}\n'
    '\n'
    '## Evaluation Summary\n'
    '{{ eval_summary }}\n'
    '\n'
    '## Failure Clusters from Evaluation\n'
    'Clusters group related failures by root cause and blamed objects. '
    '"Correct-but-Suboptimal" clusters produced correct results but used fragile approaches — '
    'use for best-practice guidance, not fixes.\n'
    '{{ cluster_briefs }}\n'
    '\n'
    '## Changes Already Applied by Earlier Levers\n'
    'Levers 1-4 applied these fixes. Your instructions should COMPLEMENT, not duplicate them.\n'
    '{{ lever_summary }}\n'
    '\n'
    '## Current Text Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: 3 failure clusters — routing errors for booking queries going to wrong tables, '
    'missing temporal filters on date-partitioned tables.\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "instruction_text": "PURPOSE:\\nThis Genie Space covers hotel booking analytics.\\n\\n'
    'ASSET ROUTING:\\n- Booking summaries: use catalog.schema.get_booking_summary TVF\\n'
    '- Detailed bookings: use catalog.schema.fact_bookings\\n\\n'
    'FUNCTION ROUTING:\\n- For booking summaries, use get_booking_summary TVF with start_date and end_date params\\n\\n'
    'TEMPORAL FILTERS:\\n- Always filter fact_bookings by booking_date for performance\\n\\n'
    'DATA QUALITY NOTES:\\n- Use is_current = true when joining dim_hotel",\n'
    '  "example_sql_proposals": [\n'
    '    {\n'
    '      "example_question": "Show me booking summary for last quarter",\n'
    '      "example_sql": "SELECT * FROM catalog.schema.get_booking_summary(:start_date, :end_date)",\n'
    '      "parameters": [{"name": "start_date", "type_hint": "DATE", "default_value": "2024-01-01"}],\n'
    '      "usage_guidance": "Use for booking summary and overview questions"\n'
    '    },\n'
    '    {\n'
    '      "example_question": "What is the average booking revenue by hotel this year?",\n'
    '      "example_sql": "SELECT dh.hotel_name, AVG(fb.revenue) AS avg_revenue '
    'FROM catalog.schema.fact_bookings fb '
    'JOIN catalog.schema.dim_hotel dh ON fb.hotel_key = dh.hotel_key '
    'WHERE dh.is_current = true AND fb.booking_date >= DATE_TRUNC(\'year\', CURRENT_DATE()) '
    'GROUP BY 1 ORDER BY 2 DESC",\n'
    '      "parameters": [],\n'
    '      "usage_guidance": "Use for hotel revenue aggregations with temporal filters"\n'
    '    },\n'
    '    {\n'
    '      "example_question": "List all bookings missing a check-in date",\n'
    '      "example_sql": "SELECT fb.booking_id, fb.guest_name, fb.booking_date '
    'FROM catalog.schema.fact_bookings fb WHERE fb.checkin_date IS NULL",\n'
    '      "parameters": [],\n'
    '      "usage_guidance": "Use for data quality queries about missing fields"\n'
    '    }\n'
    '  ],\n'
    '  "rationale": "Routing errors fixed via example SQL; temporal filter and join pattern examples added."\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<instructions>\n'
    '## 1. Instruction Document (STRUCTURED REWRITE)\n'
    '\n'
    '### Structured Format and Non-Regressive Rewrite Rules\n'
    + INSTRUCTION_FORMAT_RULES +
    '- Target 30-80 lines. Prefer bullets over paragraphs.\n'
    '- Budget: {{ instruction_char_budget }} chars MAXIMUM.\n'
    '- Omit sections with no actionable content.\n'
    '\n'
    '## 2. Example SQL Proposals\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter, disambiguation), '
    'propose example SQL. Genie pattern-matches against it directly. '
    'Aim for 3-5 proposals covering distinct failure patterns.\n'
    '\n'
    '- Question must be a realistic user prompt matching a failure pattern.\n'
    '- Every FROM table, JOIN table, column reference, and function call MUST appear in the Identifier Allowlist.\n'
    '- Do NOT duplicate existing example SQL questions.\n'
    '- Use `:param_name` markers for user-variable filters.\n'
    '- Include usage_guidance for when Genie should match this query.\n'
    '- Propose at least one example SQL per distinct failure cluster when a valid SQL pattern exists.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'You MUST ONLY use identifiers from the Identifier Allowlist. '
    'Any table, column, or function not in the allowlist is INVALID and will be rejected.\n'
    'If you cannot identify specific assets or evaluation failures to address, '
    'return empty instruction_text and no example SQL.\n'
    '</instructions>\n'
    '\n'
    '<output_schema>\n'
    'Return a single JSON object:\n'
    '{\n'
    '  "instruction_text": "PURPOSE:\\n...",\n'
    '  "example_sql_proposals": [\n'
    '    {\n'
    '      "example_question": "What is the total revenue by destination?",\n'
    '      "example_sql": "SELECT ...",\n'
    '      "parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}],\n'
    '      "usage_guidance": "Use when user asks about revenue breakdown by destination"\n'
    '    }\n'
    '  ],\n'
    '  "rationale": "Explanation of key changes made and why"\n'
    '}\n'
    '\n'
    'Always propose at least one example SQL per distinct failure cluster if a valid SQL pattern exists. '
    'Only set "example_sql_proposals" to [] when there are truly no actionable failure patterns.\n'
    'If no instruction changes needed, set "instruction_text" to "".\n'
    '</output_schema>'
)

# ── 5b. Holistic Strategist Prompt ────────────────────────────────────

STRATEGIST_PROMPT = (
    '<role>\n'
    'You are the Holistic Strategist for a Databricks Genie Space optimization framework.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<instructions>\n'
    '## Purpose\n'
    'Analyze ALL benchmark evaluation failures and produce a coordinated multi-lever '
    'action plan. Each action group addresses one root cause across multiple levers '
    'simultaneously, ensuring fixes reinforce each other rather than conflict.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation, etc.)\n'
    '- Correct-but-suboptimal soft signal suggesting preventive improvement\n'
    '\n'
    '## When NOT to create an action group\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '\n'
    '## Contract: All Instruments of Power\n'
    'For each root cause, specify EVERY lever that should act:\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5 + Lever 6\n'
    '- wrong_aggregation / wrong_measure / missing_filter: Primary Lever 6 (sql_snippet), also Lever 5 (example_sql); Lever 2 only for MV-column synonym/description refinement\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1 + Lever 6\n'
    '- missing_dimension / wrong_grouping: Primary Lever 6, also Lever 1 + Lever 5\n'
    'Lever 6 adds reusable SQL expressions (measures, filters, dimensions) to the '
    'knowledge store. Use it alongside other levers when a business concept (KPI, '
    'common condition, or derived attribute) would be better captured as a structured '
    'definition than as a column description or example SQL. SQL expressions do NOT '
    'count toward the 100-slot instruction budget.\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '- INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    '- Existing synonyms are auto-preserved; propose only NEW terms.\n'
    '- global_instruction_rewrite uses section-level upsert: only include sections you change.\n'
    '  Omitted sections are preserved automatically.\n'
    '\n'
    '## Contract: No Cross-Action-Group Conflicts\n'
    'Two AGs MUST NOT touch the same object. Merge overlapping failures into ONE AG.\n'
    '\n'
    '## Contract: Coordination Notes Are Mandatory\n'
    'Each AG must explain how changes across levers reference each other.\n'
    '\n'
    '## Contract: Example SQL for Routing/Ambiguity\n'
    'Routing failures MUST include example_sql in Lever 5.\n'
    '\n'
    '## Contract: Anti-Hallucination\n'
    'Every change must cite cluster_id(s). No invented root causes.\n'
    '\n'
    '## Contract: Instruction Format (Plain Text, Structured Sections)\n'
    + INSTRUCTION_FORMAT_RULES +
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Genie Space Schema\n'
    '{{ full_schema_context }}\n'
    '\n'
    '## Failure Clusters\n'
    '{{ cluster_briefs }}\n'
    '\n'
    '## Soft Signal Clusters\n'
    '{{ soft_signal_summary }}\n'
    '\n'
    '## Current Structured Metadata\n'
    '### Tables\n'
    '{{ structured_table_context }}\n'
    '### Columns\n'
    '{{ structured_column_context }}\n'
    '### Functions\n'
    '{{ structured_function_context }}\n'
    '\n'
    '## Current Join Specifications\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL\n'
    '{{ existing_example_sqls }}\n'
    '\n'
    '## Data Values for Blamed Columns\n'
    '{{ blamed_column_values }}\n'
    '</context>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON structure:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<N>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["H001"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "lever_directives": {\n'
    '        "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value>"}}\n'
    '              ], "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '        "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '        "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]},\n'
    '        "6": {"sql_expressions": [{"snippet_type": "measure|filter|expression", '
    '"display_name": "Human-readable name", '
    '"alias": "snake_case_id (required for measure/expression, omit for filter)", '
    '"sql": "The SQL expression (raw, no SELECT/WHERE wrapper)", '
    '"synonyms": ["synonym1", "synonym2"], '
    '"instruction": "When and how Genie should use this"}]}\n'
    '      },\n'
    '      "coordination_notes": "<how levers reference each other>"\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_rewrite": {\n'
    '    "PURPOSE": "One paragraph describing what this Genie Space does.",\n'
    '    "ASSET ROUTING": "- When user asks about [topic], use [table/TVF/MV]",\n'
    '    "TEMPORAL FILTERS": "- Use run_date >= DATE_SUB(CURRENT_DATE(), N) for last-N-day queries"\n'
    '  },\n'
    '  "rationale": "<overall reasoning>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- "lever_directives" keys "1"-"6". Only include levers with work to do.\n'
    '- "sections" keys from structured metadata schema.\n'
    '- Lever 2 uses same column format as Lever 1. Lever 3: {"functions": [...]}.\n'
    '- global_instruction_rewrite: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections you want to ADD or REPLACE. Omitted sections are PRESERVED unchanged.\n'
    '  Values are plain-text bullet lists (no Markdown). Empty string means delete the section.\n'
    '- priority 1 = most impactful. Order by affected question count.\n'
    '- Cluster IDs use H### for hard-failure clusters and S### for soft-signal clusters. '
    'Populate "source_cluster_ids" using the exact IDs from the provided cluster list. '
    'Legacy C### ids (from old runs) will also be accepted.\n'
    '- If no actionable improvements:\n'
    '  {"action_groups": [], "global_instruction_rewrite": {}, "rationale": "No actionable failures"}\n'
    '</output_schema>'
)

# ── 5c. Two-Phase Strategist Prompts ──────────────────────────────────

STRATEGIST_TRIAGE_PROMPT = (
    '<role>\n'
    'You are the Triage Strategist for a Databricks Genie Space optimization framework.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<instructions>\n'
    '## Purpose\n'
    'Analyze ALL failure clusters and produce action group SKELETONS. Each skeleton '
    'identifies one root cause, which clusters it covers, which levers should act, '
    'and which tables/columns are the focus. A separate detail phase produces the '
    'actual lever directives — your job is ONLY grouping and scoping.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation)\n'
    '- Correct-but-suboptimal soft signal\n'
    '- Merge clusters that blame the SAME objects into ONE action group\n'
    '\n'
    '## When NOT to create\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '\n'
    '## Lever Capabilities\n'
    '- Lever 1: Table/column descriptions, synonyms\n'
    '- Lever 2: Metric view column descriptions, synonyms\n'
    '  NOTE: Lever 2 CANNOT add measures, filters, or change MV SQL. It can only update '
    'table/column descriptions and synonyms on existing metric views. '
    'For missing_filter / wrong_aggregation / wrong_measure, use Lever 6 (sql_snippet) instead.\n'
    '- Lever 3: Function descriptions and parameter documentation\n'
    '- Lever 4: Join specifications between tables\n'
    '- Lever 5: Instructions + example SQL queries\n'
    '- Lever 6: SQL expressions (reusable measures, filters, dimensions)\n'
    '\n'
    '## Lever Mapping (All Instruments of Power)\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5 + Lever 6\n'
    '- wrong_aggregation / wrong_measure / missing_filter: Primary Lever 6 (sql_snippet), also Lever 5 (example_sql); Lever 2 only for MV-column synonym/description refinement\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1 + Lever 6\n'
    '- missing_dimension / wrong_grouping: Primary Lever 6, also Lever 1 + Lever 5\n'
    '\n'
    '## Contracts\n'
    '- No Cross-AG Conflicts: Two AGs MUST NOT touch same table/column. Merge overlapping.\n'
    '- Anti-Hallucination: Every AG must cite cluster_ids. No invented root causes.\n'
    '- All Instruments: For each root cause, list EVERY lever that should act.\n'
    '- MV-Preference: When a Metric View covers the same measures/dimensions as a base TABLE, '
    'NEVER add instructions directing Genie to use the TABLE directly. Prefer Metric Views for '
    'aggregation queries — they define the canonical business logic. Only route to base TABLEs '
    'for lookups, filters, or columns not exposed through a Metric View.\n'
    '- TVF-Avoidance: Do NOT add ASSET ROUTING or FUNCTION ROUTING instructions directing Genie '
    'to a Table-Valued Function (TVF) when the same query can be answered by a Metric View. '
    'TVFs with hardcoded date parameters produce fragile results. '
    'Standard trend and aggregation queries MUST use Metric Views with MEASURE() syntax.\n'
    '- Temporal-Standardization: When generating TEMPORAL FILTERS or QUERY RULES instructions, '
    'include these canonical date-filter patterns:\n'
    '  "this year" -> WHERE col >= DATE_TRUNC(\'year\', CURRENT_DATE())\n'
    '  "last quarter" -> WHERE col >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND col < DATE_TRUNC(\'quarter\', CURRENT_DATE())\n'
    '  "last N months" -> WHERE col >= ADD_MONTHS(CURRENT_DATE(), -N)\n'
    '  Include these in the TEMPORAL FILTERS section of the instruction.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Schema Index\n'
    '{{ schema_index }}\n'
    '\n'
    '## Failure Clusters\n'
    '{{ cluster_summaries }}\n'
    '\n'
    '## Soft Signal Clusters\n'
    '{{ soft_signal_summary }}\n'
    '\n'
    '## Current Join Specs\n'
    '{{ current_join_summary }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ instruction_summary }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: Two failure clusters:\n'
    '  H001: wrong_column on dim_product.product_name vs product_title (3 questions)\n'
    '  H002: missing_join between fact_sales and dim_product (2 questions, overlapping with H001)\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG1",\n'
    '      "root_cause_summary": "Genie selects product_title instead of product_name and '
    'misses the fact_sales→dim_product join because column synonyms and join spec are absent",\n'
    '      "source_cluster_ids": ["H001", "H002"],\n'
    '      "affected_questions": ["Q3", "Q7", "Q12"],\n'
    '      "priority": 1,\n'
    '      "levers_needed": [1, 4, 5],\n'
    '      "focus_tables": ["catalog.schema.dim_product", "catalog.schema.fact_sales"],\n'
    '      "focus_columns": ["dim_product.product_name", "fact_sales.product_key"]\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_guidance": "BUSINESS DEFINITIONS: product name disambiguation; JOIN GUIDANCE: fact_sales to dim_product join path",\n'
    '  "rationale": "H001 and H002 both blame dim_product — merged into one AG. '
    'Lever 1 fixes the synonym, Lever 4 adds the join spec, Lever 5 adds an example SQL."\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<N>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["H001", "H003"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "levers_needed": [1, 4, 5],\n'
    '      "focus_tables": ["<fully_qualified_table_name>"],\n'
    '      "focus_columns": ["<table_short_name>.<column_name>"]\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_guidance": "<themes using canonical section names: PURPOSE, ASSET ROUTING, '
    'BUSINESS DEFINITIONS, DISAMBIGUATION, AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, '
    'QUERY RULES, QUERY PATTERNS, TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS>",\n'
    '  "rationale": "<overall reasoning>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- priority 1 = most impactful. Order by affected question count.\n'
    '- focus_tables/focus_columns must reference objects from Schema Index.\n'
    '- levers_needed: list of integers 1-5.\n'
    '- If no actionable improvements:\n'
    '  {"action_groups": [], "global_instruction_guidance": "", "rationale": "No actionable failures"}\n'
    '</output_schema>'
)


STRATEGIST_DETAIL_PROMPT = (
    '<role>\n'
    'You are the Detail Planner for a Databricks Genie Space optimization action group.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<instructions>\n'
    '## Purpose\n'
    'Given an action group skeleton and detailed evidence (SQL diffs, current metadata), '
    'produce the EXACT lever directives needed to fix this root cause.\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '[EDITABLE] sections can be updated. [LOCKED] must NOT be changed.\n'
    'INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    'Existing synonyms are auto-preserved; propose only NEW terms.\n'
    'instruction_contribution uses section-level upsert: only include sections relevant to this AG.\n'
    '\n'
    '## Contract: Coordination Notes\n'
    'Explain how changes across levers reinforce each other.\n'
    '\n'
    '## Contract: Example SQL & SQL Expressions\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter), '
    'include example_sqls in lever 5. Propose multiple example SQLs covering distinct '
    'failure patterns — aim for 1 per affected question where a valid SQL sketch exists.\n'
    'When a business concept (KPI, common condition, derived attribute) is better captured '
    'as a reusable definition than an example SQL, include sql_expressions in lever 6 instead. '
    'SQL expressions do NOT count toward the instruction budget.\n'
    '\n'
    '## Contract: Identifier Allowlist\n'
    'You MUST ONLY reference tables, columns, and functions from the Identifier Allowlist. '
    'Any name not in the allowlist is INVALID and will be rejected.\n'
    '\n'
    '## Contract: Instruction Contribution Format\n'
    'instruction_contribution MUST use ALL-CAPS SECTION HEADERS with colon (e.g. QUERY RULES:). '
    'No Markdown (no ##, no **, no backticks). Plain text only.\n'
    'Target sections aligned with your active levers:\n'
    '  Lever 1 -> BUSINESS DEFINITIONS, DISAMBIGUATION\n'
    '  Lever 2 -> MV column descriptions and synonyms only (CANNOT add measures, filters, or change MV SQL)\n'
    '  Lever 6 -> AGGREGATION RULES, TEMPORAL FILTERS\n'
    '  Lever 3 -> FUNCTION ROUTING\n'
    '  Lever 4 -> JOIN GUIDANCE, TEMPORAL FILTERS\n'
    '  Lever 5 -> ASSET ROUTING, QUERY RULES, QUERY PATTERNS, DATA QUALITY NOTES, CONSTRAINTS\n'
    '  Lever 6 -> (no instruction sections — operates via sql_expressions in lever_directives)\n'
    'Every bullet must reference a specific asset. No generic guidance.\n'
    '\n'
    '## Contract: Improvement Proposals\n'
    'When you identify a pattern where multiple questions would benefit from '
    'a new Metric View or SQL Function that does NOT yet exist, include a "proposals" '
    'array in your output. Propose METRIC_VIEW when multiple questions need the same '
    'complex aggregation. Propose FUNCTION when multiple questions need the same '
    'date/category transformation. Only propose objects that are genuinely missing.\n'
    '\n'
    '## Contract: MV-Preference\n'
    'When a Metric View covers the same measures/dimensions as a base TABLE, '
    'NEVER add ASSET ROUTING instructions directing Genie to use the TABLE directly. '
    'Prefer Metric Views for aggregation queries. Only route to base TABLEs for lookups, '
    'filters, or columns not exposed through a Metric View.\n'
    '\n'
    '## Contract: TVF-Avoidance\n'
    'Do NOT add ASSET ROUTING or FUNCTION ROUTING instructions directing Genie '
    'to a Table-Valued Function (TVF) when the same query can be answered by a Metric View. '
    'TVFs with hardcoded date parameters produce fragile results. '
    'Standard trend and aggregation queries MUST use Metric Views with MEASURE() syntax.\n'
    '\n'
    '## Contract: Temporal-Standardization\n'
    'When generating TEMPORAL FILTERS or QUERY RULES instructions, '
    'include these canonical date-filter patterns:\n'
    '  "this year" -> WHERE col >= DATE_TRUNC(\'year\', CURRENT_DATE())\n'
    '  "last quarter" -> WHERE col >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND col < DATE_TRUNC(\'quarter\', CURRENT_DATE())\n'
    '  "last N months" -> WHERE col >= ADD_MONTHS(CURRENT_DATE(), -N)\n'
    'Include these in the TEMPORAL FILTERS section of the instruction.\n'
    '\n'
    '## Contract: Section Ownership\n'
    'When proposing table/column description updates, only target sections the lever can modify:\n'
    '  Lever 1: purpose, best_for, grain, scd, definition, values, synonyms\n'
    '  Lever 2: definition, values, aggregation, grain_note, important_filters, synonyms '
    '(NOT purpose/best_for/grain/scd)\n'
    '  Lever 3: purpose, best_for, use_instead_of, parameters, example\n'
    '  Lever 4: relationships, join\n'
    '  Lever 6: (no description sections — operates via sql_expressions in lever_directives)\n'
    'Proposing sections outside the lever ownership will be rejected.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '## Action Group to Detail\n'
    '{{ action_group_skeleton }}\n'
    '\n'
    '## SQL Evidence\n'
    '{{ sql_diffs }}\n'
    '\n'
    '## Identifier Allowlist (Extract-Over-Generate)\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Current Structured Metadata for Focus Objects\n'
    '### Tables\n'
    '{{ structured_table_context }}\n'
    '### Columns\n'
    '{{ structured_column_context }}\n'
    '### Functions\n'
    '{{ structured_function_context }}\n'
    '\n'
    '## Current Join Specifications\n'
    '{{ current_join_specs }}\n'
    '\n'
    '## Current Instructions\n'
    '{{ current_instructions }}\n'
    '\n'
    '## Existing Example SQL\n'
    '{{ existing_example_sqls }}\n'
    '</context>\n'
    '\n'
    '<examples>\n'
    '<example>\n'
    'Input: AG1 skeleton — root cause: "Genie uses product_title instead of product_name '
    'and misses the fact_sales→dim_product join"\n'
    'Focus: dim_product.product_name, fact_sales.product_key\n'
    'Levers needed: [1, 4, 5]\n'
    '\n'
    'Output:\n'
    '{\n'
    '  "lever_directives": {\n'
    '    "1": {\n'
    '      "tables": [],\n'
    '      "columns": [{\n'
    '        "table": "catalog.schema.dim_product",\n'
    '        "column": "product_name",\n'
    '        "entity_type": "column_dim",\n'
    '        "sections": {"synonyms": "product title, item name", '
    '"definition": "The display name of the product as shown to customers"}\n'
    '      }]\n'
    '    },\n'
    '    "4": {\n'
    '      "join_specs": [{\n'
    '        "left_table": "catalog.schema.fact_sales",\n'
    '        "right_table": "catalog.schema.dim_product",\n'
    '        "join_guidance": "fact_sales.product_key = dim_product.product_key, MANY_TO_ONE"\n'
    '      }]\n'
    '    },\n'
    '    "5": {\n'
    '      "instruction_guidance": "Add routing rule for product name lookups",\n'
    '      "example_sqls": [\n'
    '        {\n'
    '          "question": "What are the top products by revenue?",\n'
    '          "sql_sketch": "SELECT dp.product_name, SUM(fs.revenue) FROM fact_sales fs '
    'JOIN dim_product dp ON fs.product_key = dp.product_key GROUP BY 1 ORDER BY 2 DESC",\n'
    '          "parameters": [],\n'
    '          "usage_guidance": "Use when user asks about product performance or revenue by product"\n'
    '        },\n'
    '        {\n'
    '          "question": "Show me revenue by product for the last quarter",\n'
    '          "sql_sketch": "SELECT dp.product_name, SUM(fs.revenue) FROM fact_sales fs '
    'JOIN dim_product dp ON fs.product_key = dp.product_key '
    'WHERE fs.sale_date >= ADD_MONTHS(DATE_TRUNC(\'quarter\', CURRENT_DATE()), -3) '
    'AND fs.sale_date < DATE_TRUNC(\'quarter\', CURRENT_DATE()) GROUP BY 1 ORDER BY 2 DESC",\n'
    '          "parameters": [],\n'
    '          "usage_guidance": "Use for temporal product revenue queries with date filters"\n'
    '        }\n'
    '      ]\n'
    '    }\n'
    '  },\n'
    '  "coordination_notes": "Lever 1 adds synonym so Genie resolves product_title→product_name. '
    'Lever 4 defines the join so Genie can reach dim_product from fact_sales. '
    'Lever 5 example SQL demonstrates the correct join pattern.",\n'
    '  "instruction_contribution": {\n'
    '    "BUSINESS DEFINITIONS": "- product name = product_name from catalog.schema.dim_product '
    '(synonyms: product title, item name)",\n'
    '    "JOIN GUIDANCE": "- For product name lookups, JOIN fact_sales to dim_product on product_key"\n'
    '  },\n'
    '  "proposals": []\n'
    '}\n'
    '</example>\n'
    '</examples>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON:\n'
    '{\n'
    '  "lever_directives": {\n'
    '    "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value — AUGMENT existing>"}}],\n'
    '          "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '    "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '    "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]},\n'
    '    "6": {"sql_expressions": [{"snippet_type": "measure|filter|expression", '
    '"display_name": "Human-readable name", '
    '"alias": "snake_case_id (required for measure/expression, omit for filter)", '
    '"sql": "The SQL expression (raw, no SELECT/WHERE wrapper)", '
    '"synonyms": ["synonym1", "synonym2"], '
    '"instruction": "When and how Genie should use this"}]}\n'
    '  },\n'
    '  "coordination_notes": "<how levers reference each other>",\n'
    '  "instruction_contribution": {\n'
    '    "<SECTION_HEADER>": "<plain-text content for this section>"\n'
    '  },\n'
    '  "proposals": [\n'
    '    {\n'
    '      "type": "METRIC_VIEW | FUNCTION",\n'
    '      "title": "<short name for the proposed object>",\n'
    '      "rationale": "<why this is needed — what failure pattern it fixes>",\n'
    '      "definition": "<SQL CREATE or pseudocode definition>",\n'
    '      "affected_questions": ["<question_id>", ...],\n'
    '      "estimated_impact": "<brief estimate of accuracy improvement>"\n'
    '    }\n'
    '  ]\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- Only include lever keys with work to do (from skeleton levers_needed).\n'
    '- "sections" keys from structured metadata schema.\n'
    '- instruction_contribution: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections relevant to THIS action group. Values are plain text.\n'
    '- Lever 2: same column format as Lever 1.\n'
    '- Lever 3: {"functions": [{"function": "...", "sections": {...}}]}\n'
    '- Lever 5 example_sqls: propose 1 per distinct failure pattern. Always include at least one.\n'
    '- Lever 6 sql_expressions: propose reusable measures/filters/dimensions. '
    'Prefer over example SQL when the concept applies across multiple question patterns.\n'
    '- proposals: OPTIONAL. Only include if you identify a genuinely missing Metric View or Function.\n'
    '</output_schema>'
)

# ── 5d. Adaptive Strategist Prompt (single-call, one AG) ──────────────

ADAPTIVE_STRATEGIST_PROMPT = (
    '<role>\n'
    'You are the Adaptive Strategist for a Databricks Genie Space optimization '
    'framework.  You operate in an iterative loop: after each action you receive '
    'fresh evaluation results and must decide the SINGLE best next action.\n'
    '</role>\n'
    '\n'
    + _RCA_CONTRACT_HEADER +
    '<instructions>\n'
    '## Purpose\n'
    'Analyze the CURRENT failure clusters (from the most recent evaluation) and '
    'produce exactly ONE action group — the single highest-impact fix for the '
    'remaining failures.  Prior iterations and their outcomes are provided in '
    'the reflection history so you can build on successes and avoid repeating '
    'failed approaches.\n'
    '\n'
    '## When to create an action group\n'
    '- Systematic failure pattern (wrong column, missing join, wrong aggregation, etc.)\n'
    '- Correct-but-suboptimal soft signal suggesting preventive improvement\n'
    '\n'
    '## When NOT to create an action group\n'
    '- Format-only differences (extra_columns_only, select_star, format_difference)\n'
    '- Cannot identify a specific table, column, join, or instruction to change\n'
    '- The approach was already tried and failed (see DO NOT RETRY list)\n'
    '\n'
    '## Contract: Join Assessment Evidence\n'
    'When failure clusters include a "join_assessments" array, these are structured, '
    'judge-verified join recommendations. Each entry contains:\n'
    '- issue: missing_join | wrong_condition | wrong_direction\n'
    '- left_table, right_table: fully-qualified table names\n'
    '- suggested_condition: the join ON clause\n'
    '- relationship: many_to_one | one_to_many | one_to_one\n'
    '- evidence: explanation from the judge\n'
    'If join_assessments are present and the issue is missing_join_spec or wrong_join_spec, '
    'you SHOULD include Lever 4 in your action group with join_specs derived from these '
    'assessments. This is high-confidence evidence from the evaluation judges.\n'
    '\n'
    '## Contract: All Instruments of Power\n'
    'For the root cause you target, specify EVERY lever that should act:\n'
    '- wrong_column / wrong_table / missing_synonym: Primary Lever 1, also Lever 5 + Lever 6\n'
    '- wrong_aggregation / wrong_measure / missing_filter: Primary Lever 6 (sql_snippet), also Lever 5 (example_sql); Lever 2 only for MV-column synonym/description refinement\n'
    '- tvf_parameter_error: Primary Lever 3, also Lever 5\n'
    '- wrong_join / missing_join_spec / wrong_join_spec: Primary Lever 4, also Lever 1 + 5\n'
    '- asset_routing_error / ambiguous_question: Primary Lever 5, also Lever 1 + Lever 6\n'
    '- missing_dimension / wrong_grouping: Primary Lever 6, also Lever 1 + Lever 5\n'
    'Lever 6 adds reusable SQL expressions (measures, filters, dimensions) to the '
    'knowledge store. Use it alongside other levers when a business concept (KPI, '
    'common condition, or derived attribute) would be better captured as a structured '
    'definition than as a column description or example SQL. SQL expressions do NOT '
    'count toward the 100-slot instruction budget.\n'
    '\n'
    '## Contract: Compound-Concept Queries\n'
    'When a question requires resolving MULTIPLE business concepts simultaneously '
    '(for example: "North-America wholesale revenue by region" = country filter + '
    'channel filter + metric + grouping dimension; OR: "EMEA premium-tier claim '
    'count by line-of-business" = region filter + tier filter + metric + grouping '
    'dimension), apply a multi-lever approach:\n'
    '1. Lever 6: Add SQL expressions for each atomic concept — a filter for the '
    'country/region, a filter for the channel/tier, a measure for the metric.\n'
    '2. Lever 2: Ensure column descriptions include concept-to-column mappings '
    '(e.g. "North America = region_code=NA", "wholesale = channel column = WH").\n'
    '3. Lever 5: Add an example SQL that demonstrates the FULL filter chain for '
    'this type of compound query.\n'
    'NEVER leave a compound-concept failure with just an instruction rewrite — '
    'Genie needs structured metadata (Lever 6 + Lever 2) to reliably decompose '
    'natural language into multi-filter SQL.\n'
    '\n'
    '## Contract: Instruction-Defined Default Filters\n'
    'If the Genie Space instructions define a default filter (e.g. "always filter by '
    '<flag_column> = <value> unless explicitly requested otherwise"), that filter is '
    'CORRECT BEHAVIOR. Do NOT blame it as "over-filtering" in root cause analysis. '
    'Only flag the filter as a problem if the user explicitly asked to exclude it.\n'
    '\n'
    '## Contract: Structured Metadata Format\n'
    'ALL metadata changes MUST use structured sections.\n'
    'Tables: purpose, best_for, grain, scd, relationships.\n'
    'Columns by type: column_dim (definition, values, synonyms), '
    'column_measure (definition, aggregation, grain_note, synonyms), '
    'column_key (definition, join, synonyms).\n'
    'Functions: purpose, best_for, use_instead_of, parameters, example.\n'
    'Use section KEYS, not labels.\n'
    'Each section must be a SEPARATE key. Do NOT embed section headers inside another '
    "section's value — updates with embedded headers will be REJECTED.\n"
    'WRONG (rejected): {"purpose": "Fact table. BEST FOR: Duration analysis. GRAIN: One row per run"}\n'
    'CORRECT: {"purpose": "Fact table.", "best_for": "Duration analysis.", "grain": "One row per run"}\n'
    '\n'
    '## Contract: Section Ownership\n'
    'When proposing table/column description updates, only target sections the lever can modify:\n'
    '  Lever 1: purpose, best_for, grain, scd, definition, values, synonyms\n'
    '  Lever 2: definition, values, aggregation, grain_note, important_filters, synonyms '
    '(NOT purpose/best_for/grain/scd)\n'
    '  Lever 3: purpose, best_for, use_instead_of, parameters, example\n'
    '  Lever 4: relationships, join\n'
    '  Lever 6: (no description sections — operates via sql_expressions in lever_directives)\n'
    'Proposing sections outside the lever ownership will be rejected.\n'
    '\n'
    '## Contract: Non-Regressive / Augment-Not-Overwrite\n'
    '[EDITABLE] sections can be updated. [LOCKED] must NOT be changed.\n'
    'INCORPORATE existing content and ADD new details. Only replace if empty or wrong.\n'
    'Existing synonyms are auto-preserved; propose only NEW terms.\n'
    'global_instruction_rewrite uses section-level upsert: only include sections you change.\n'
    'Omitted sections are preserved automatically.\n'
    '\n'
    '## Contract: Example SQL\n'
    'For any recurring failure pattern (routing, aggregation, temporal, join, filter), '
    'include example_sqls in lever 5. Propose multiple example SQLs covering distinct '
    'failure patterns — aim for 1 per affected question where a valid SQL sketch exists.\n'
    '\n'
    '## Identifier Allowlist\n'
    'ONLY reference identifiers from this allowlist:\n'
    '{{ identifier_allowlist }}\n'
    '\n'
    '## Refinement Mode Guidance\n'
    'When the Reflection History shows a ROLLED_BACK entry:\n'
    '- If "in_plan": The lever direction was correct but caused regressions. '
    'Refine the SAME lever with narrower scope or more specific targeting. '
    'Do NOT switch to a different root cause.\n'
    '- If "out_of_plan": The approach fundamentally did not work. Switch to a '
    'different lever class or escalate. Do NOT retry the same lever type on '
    'the same target.\n'
    '\n'
    '## Escalation for Persistent Failures\n'
    'Check the Persistent Question Failures section.  If a question is marked '
    'ADDITIVE_LEVERS_EXHAUSTED, do NOT propose more add_instruction or add_example_sql '
    'patches for it — those have already been tried multiple times without effect.\n'
    'Instead, set the optional "escalation" field in your output:\n'
    '- "remove_tvf": The root cause is a misleading TVF that overrides routing.  '
    'Only TVFs may be removed — NEVER tables or metric views.  Include the TVF identifier '
    'in lever 3.  The system will assess removal confidence and either auto-apply, '
    'flag for review, or escalate to human.\n'
    '- "gt_repair": The ground-truth SQL appears incorrect (neither_correct pattern).  '
    'The system will attempt LLM-assisted GT correction.\n'
    '- "flag_for_review": No automated fix is viable.  The question will be flagged '
    'for human review in the labeling session.\n'
    'If INTERMITTENT, the question may be non-deterministic — monitor but do not '
    'escalate unless it becomes PERSISTENT.\n'
    '\n'
    '## Contract: Improvement Proposals\n'
    'When lever fixes alone cannot resolve a pattern, propose a new object via "proposals". '
    'Propose METRIC_VIEW when 3+ questions need the same aggregation across varying dimensions, '
    'or when ratios/distinct-counts cannot be safely re-aggregated from a flat table. '
    'Propose FUNCTION when 2+ clusters need the same date/category transformation. '
    'Only propose objects genuinely missing from the Identifier Allowlist.\n'
    '</instructions>\n'
    '\n'
    '<context>\n'
    '{{ context_json }}\n'
    '</context>\n'
    '\n'
    '<output_schema>\n'
    'Return ONLY this JSON structure with EXACTLY ONE action group:\n'
    '{\n'
    '  "action_groups": [\n'
    '    {\n'
    '      "id": "AG<iteration_number>",\n'
    '      "root_cause_summary": "<one sentence>",\n'
    '      "source_cluster_ids": ["H001"],\n'
    '      "affected_questions": ["<question_id>"],\n'
    '      "priority": 1,\n'
    '      "lever_directives": {\n'
    '        "1": {"tables": [{"table": "<fq_name>", "entity_type": "table", '
    '"sections": {"<key>": "<value>"}}\n'
    '              ], "columns": [{"table": "<fq_name>", "column": "<col>", '
    '"entity_type": "<column_dim|column_measure|column_key>", '
    '"sections": {"<key>": "<value>"}}]},\n'
    '        "4": {"join_specs": [{"left_table": "<fq>", "right_table": "<fq>", '
    '"join_guidance": "<condition + type>"}]},\n'
    '        "5": {"instruction_guidance": "<text>", "example_sqls": ['
    '{"question": "<prompt>", "sql_sketch": "<SQL>", '
    '"parameters": [{"name": "...", "type_hint": "STRING", "default_value": "..."}], '
    '"usage_guidance": "<when to match>"}]},\n'
    '        "6": {"sql_expressions": [{"snippet_type": "measure|filter|expression", '
    '"display_name": "Human-readable name", '
    '"alias": "snake_case_id (required for measure/expression, omit for filter)", '
    '"sql": "The SQL expression (raw, no SELECT/WHERE wrapper)", '
    '"synonyms": ["synonym1", "synonym2"], '
    '"instruction": "When and how Genie should use this"}]}\n'
    '      },\n'
    '      "coordination_notes": "<how levers reference each other>",\n'
    '      "escalation": "<optional: remove_tvf | gt_repair | flag_for_review>",\n'
    '      "proposals": [\n'
    '        {"type": "METRIC_VIEW | FUNCTION", "title": "<short name>", '
    '"rationale": "<failure pattern it fixes>", "definition": "<SQL CREATE or YAML>", '
    '"affected_questions": ["<qid>"], "estimated_impact": "<accuracy improvement>"}\n'
    '      ]\n'
    '    }\n'
    '  ],\n'
    '  "global_instruction_rewrite": {\n'
    '    "PURPOSE": "One paragraph describing what this Genie Space does.",\n'
    '    "ASSET ROUTING": "- When user asks about [topic], use [table/TVF/MV]",\n'
    '    "TEMPORAL FILTERS": "- Use run_date >= DATE_SUB(CURRENT_DATE(), N) for last-N-day queries"\n'
    '  },\n'
    '  "rationale": "<why this action group is the highest-impact next step>"\n'
    '}\n'
    '\n'
    'Rules:\n'
    '- EXACTLY one action group. Pick the single highest-impact fix.\n'
    '- Cluster IDs use H### for hard-failure clusters and S### for soft-signal clusters. '
    'Populate "source_cluster_ids" using the exact IDs from the provided cluster list. '
    'Legacy C### ids are accepted during replay of old iterations.\n'
    '- "lever_directives" keys "1"-"6". Only include levers with work to do.\n'
    '- "sections" keys from structured metadata schema.\n'
    '- Lever 2 uses same column format as Lever 1. Lever 3: {"functions": [...]}.\n'
    '- global_instruction_rewrite: a JSON OBJECT mapping section headers to content.\n'
    '  Keys MUST be from: PURPOSE, ASSET ROUTING, BUSINESS DEFINITIONS, DISAMBIGUATION, '
    'AGGREGATION RULES, FUNCTION ROUTING, JOIN GUIDANCE, QUERY RULES, QUERY PATTERNS, '
    'TEMPORAL FILTERS, DATA QUALITY NOTES, CONSTRAINTS.\n'
    '  Only include sections you want to ADD or REPLACE. Omitted sections are PRESERVED unchanged.\n'
    '  Values are plain-text bullet lists (no Markdown). Empty string means delete the section.\n'
    '- proposals: OPTIONAL. Only include when lever fixes are insufficient and a missing MV or Function would resolve the pattern.\n'
    '- Do NOT repeat any approach listed in the DO NOT RETRY section.\n'
    '- If no actionable improvements remain:\n'
    '  {"action_groups": [], "global_instruction_rewrite": {}, '
    '"rationale": "No actionable failures"}\n'
    '</output_schema>'
)

# ── 5e. GT Repair Prompt ───────────────────────────────────────────────

GT_REPAIR_PROMPT = (
    'You are a SQL expert reviewing a benchmark question where BOTH the ground-truth SQL '
    'and Genie\'s generated SQL were judged incorrect by the arbiter.\n'
    '\n'
    'QUESTION: {{ question }}\n'
    '\n'
    'GROUND TRUTH SQL (judged incorrect):\n'
    '{{ expected_sql }}\n'
    '\n'
    'GENIE SQL (also judged incorrect):\n'
    '{{ genie_sql }}\n'
    '\n'
    'ARBITER RATIONALE(S):\n'
    '{{ rationale }}\n'
    '\n'
    'Your task: produce a CORRECTED ground-truth SQL that correctly answers the question.\n'
    '- Use proper Databricks SQL syntax\n'
    '- Respect temporal semantics (e.g. "this year" = DATE_TRUNC(\'year\', CURRENT_DATE()), '
    '"last 12 months" = ADD_MONTHS(CURRENT_DATE(), -12))\n'
    '- Use MEASURE() for metric view columns where appropriate\n'
    '- Return ONLY the corrected SQL, no explanation'
)


# ── 6. Non-Exportable Genie Config Fields ──────────────────────────────

NON_EXPORTABLE_FIELDS = {
    "id",
    "title",
    "description",
    "creator",
    "creator_id",
    "updated_by",
    "updated_at",
    "created_at",
    "warehouse_id",
    "execute_as_user_id",
    "space_status",
}

# ── 6a. Internal runtime annotations on the config/metadata_snapshot dict ──
#
# The pipeline stores runtime-only state (data profiles, failure clusters,
# cluster synthesis budgets, RLS audit, etc.) on the config dict with a
# leading underscore. These must be stripped before PATCH and must NOT be
# rejected by strict validation — they never leave the process.
#
# Known annotation keys are documented here for discoverability; the
# underscore-prefix convention is the hard contract and `is_runtime_key`
# is the single authority both the validator (`genie_schema.py`) and the
# stripper (`genie_client.py`) must defer to.

INTERNAL_RUNTIME_KEYS_PREFIX = "_"

KNOWN_INTERNAL_RUNTIME_KEYS = frozenset({
    "_data_profile",
    "_failure_clusters",
    "_cluster_synthesis_count",
    "_rls_audit",
    "_space_id",
    "_join_overlaps",
    "_join_attempts",
    "_original_instruction_sections",
})


def is_runtime_key(k: object) -> bool:
    """Return True when ``k`` is a runtime-only top-level annotation.

    Runtime-only keys live on the in-memory config/metadata_snapshot but
    must never reach the Genie API. The single contract: a leading
    ``INTERNAL_RUNTIME_KEYS_PREFIX``. Used by both
    ``common.genie_client.strip_non_exportable_fields`` and
    ``common.genie_schema._strict_validate`` so the two paths cannot
    drift (which is exactly what caused ``_data_profile`` to land
    correctly through the stripper but error out at the validator).
    """
    return isinstance(k, str) and k.startswith(INTERNAL_RUNTIME_KEYS_PREFIX)

# ── 7. Feature Flags ──────────────────────────────────────────────────

USE_PATCH_DSL = True
USE_JOB_MODE = True
USE_LEVER_AWARE = True
ENABLE_CONTINUOUS_MONITORING = False

APPLY_MODE = "genie_config"
"""Where patches are applied. One of:
  - "genie_config": All changes go to Genie Space config overlays.
  - "uc_artifact": Column descriptions go to UC via ALTER TABLE, etc.
  - "both": Apply to both targets for maximum coverage.
Levers 4-6 are always genie_config regardless of this setting."""

# ── 8. Risk Classification Sets ────────────────────────────────────────

LOW_RISK_PATCHES = {
    "add_description",
    "update_description",
    "add_column_description",
    "update_column_description",
    "hide_column",
    "unhide_column",
    "add_instruction",
    # v2 Task 12: conditional disambiguation rule renders as an
    # add_instruction-style append; classify with the same risk level.
    "add_conditional_disambiguation_instruction",
    "enable_example_values",
    "disable_example_values",
    "enable_value_dictionary",
    "disable_value_dictionary",
    "add_column_synonym",
    "remove_column_synonym",
}

MEDIUM_RISK_PATCHES = {
    "update_instruction",
    "update_instruction_section",
    "rewrite_instruction",
    "remove_instruction",
    "rename_column_alias",
    "add_default_filter",
    "remove_default_filter",
    "update_filter_condition",
    "add_tvf_parameter",
    "remove_tvf_parameter",
    "add_mv_measure",
    "update_mv_measure",
    "remove_mv_measure",
    "add_mv_dimension",
    "remove_mv_dimension",
    "add_join_spec",
    "update_join_spec",
    "remove_join_spec",
}

HIGH_RISK_PATCHES = {
    "add_table",
    "remove_table",
    "update_tvf_sql",
    "add_tvf",
    "remove_tvf",
    "update_mv_yaml",
}

# ── 9. Repeatability Classification ────────────────────────────────────

REPEATABILITY_CLASSIFICATIONS = {
    95: "IDENTICAL",
    80: "MINOR_VARIANCE",
    60: "SIGNIFICANT_VARIANCE",
    0: "CRITICAL_VARIANCE",
}

# ── 10. Repeatability Fix Routing by Asset Type ───────────────────────

REPEATABILITY_FIX_BY_ASSET = {
    "TABLE": (
        "Add structured metadata (business_definition, synonyms[], grain, join_keys[]) "
        "to column descriptions. Add UC tags: preferred_for_genie=true, domain=<value>."
    ),
    "MV": (
        "Add structured column metadata to metric view columns. "
        "Use synonyms[] and preferred_questions[] to constrain dimension selection."
    ),
    "TVF": (
        "Add instruction clarifying deterministic parameter selection. "
        "TVF signature already constrains output; focus on parameter disambiguation."
    ),
    "NONE": "Add routing instruction to direct questions to the appropriate asset type.",
}

# ── 11. Lever Descriptions ─────────────────────────────────────────────

LEVER_NAMES = {
    0: "Proactive Enrichment",   # Always runs; NOT user-selectable
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Genie Space Instructions",
    6: "SQL Expressions",
}
"""Lever ID -> display name mapping.

Lever 0 is a preparatory stage that always runs before the adaptive lever
loop. It is not included in :data:`DEFAULT_LEVER_ORDER` and should not be
shown in the UI as a toggleable option.
"""

DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5, 6]
"""Default set of user-selectable levers, in execution order."""


SCAN_CHECK_TO_LEVERS: dict[int, list[int]] = {
    # Check 2 (Table descriptions) / Check 3 (Column descriptions) → lever 1
    #   (Tables & Columns) — adds/fills descriptions and synonyms.
    2: [1],
    3: [1],
    # Check 4 (Text instructions) → lever 5 (Genie Space Instructions).
    4: [5],
    # Check 5 (Join specifications) → lever 4 (Join Specifications).
    5: [4],
    # Check 7 (8+ example SQLs) / Check 8 (SQL snippets) → lever 6 (SQL Expressions)
    #   which covers example SQLs, filters, measures, and expressions.
    7: [6],
    8: [6],
    # Check 9 (Entity / format matching) → lever 1 (Tables & Columns) which
    #   owns column_configs including enable_entity_matching / format_assistance.
    9: [1],
}
"""IQ Scan check ID → recommended optimizer levers.

1-indexed check IDs match the 12-check order in
:func:`genie_space_optimizer.iq_scan.scoring.calculate_score`. Checks that
can't be fixed by the optimizer (1 - data sources exist; 6 - data source
count; 10 - benchmarks; 11 / 12 - optimization outcomes) are intentionally
absent.

Consumed by :func:`preflight_run_iq_scan` to translate failing checks into a
``recommended_levers`` hint for the strategist and cluster-ranking tiebreaker.
"""

MAX_VALUE_DICTIONARY_COLUMNS = 120
"""Maximum number of string columns per Genie Space that can have
enable_entity_matching=true. Enforced by auto_apply_prompt_matching()."""

ENABLE_PROMPT_MATCHING_AUTO_APPLY = True
"""When True, format assistance and entity matching are applied as a
best-practice hygiene step between baseline evaluation and the lever loop."""

CATEGORICAL_COLUMN_PATTERNS = [
    "industry", "type", "status", "state", "country", "region",
    "department", "category", "segment", "code", "tier", "level",
    "stage", "phase", "class", "group", "channel", "source", "priority",
    "currency", "unit", "role", "gender", "brand", "vendor", "supplier",
]

FREE_TEXT_COLUMN_PATTERNS = [
    "description", "comment", "notes", "address", "email", "url",
    "path", "body", "message", "content", "text", "summary", "detail",
    "narrative", "reason", "explanation",
]

# ── 11b. Entity-matching slot allocation (intelligent scoring) ──────────
# Consumed by ``_entity_matching_score`` + ``auto_apply_prompt_matching``
# in ``optimization/applier.py``. The scorer returns 0 for any hard
# disqualifier below; the caller FILTERS score<=0 candidates out of the
# selection pool rather than sorting-and-taking-top-N. This prevents the
# silent PII leak that happens today on spaces with <120 STRING columns
# where every STRING column gets auto-enabled regardless of fit.

MAX_ENTITY_MATCHING_CARDINALITY = 1024
"""Genie silently drops value dictionaries for columns whose distinct value
count exceeds this threshold (see docs.databricks.com knowledge-store
docs). Slot activation on such columns is a no-op that wastes one of the
120 slots."""

MIN_ENTITY_MATCHING_CARDINALITY = 2
"""Reject constant columns (cardinality <= 1). Zero benefit from entity
matching on a column whose only value is 'ACTIVE' or NULL."""

FREE_TEXT_DISTINCT_RATIO = 0.8
"""Reject columns whose distinct_count / row_count exceeds this threshold —
near-unique-per-row columns are IDs or free-form text, neither of which
benefits from value dictionary lookup."""

PII_COLUMN_PATTERNS = [
    "email", "ssn", "social_security", "phone", "address_line",
    "dob", "date_of_birth", "tax_id", "credit_card", "passport",
    "driver_license", "account_number", "bank_account",
]
"""Column name substrings that indicate PII. Hard-rejected from entity
matching because the value dictionary is stored in the workspace storage
bucket and would leak sensitive values to the space's shared context."""

BOOLEAN_FLAG_PATTERNS = [
    "_flag", "_yn", "_bool", "is_", "has_", "can_", "should_",
]
"""Column name substrings that indicate boolean / 2-value flags. Zero
benefit from entity matching."""

DESCRIPTION_HINTS_POSITIVE = frozenset({
    "enum", "category", "lookup", "one of", "valid values",
})
"""Description keywords that boost the entity-matching score — explicit
markers of bounded-value columns."""

DESCRIPTION_HINTS_NEGATIVE = frozenset({
    "internal", "etl", "audit", "deprecated", "do not use",
})
"""Description keywords that penalize the entity-matching score — low
user-intent signal."""

DYNAMIC_VIEW_FN_RE = re.compile(
    r"\b(current_user|session_user|is_account_group_member|is_member)\s*\(",
    re.IGNORECASE,
)
"""Identity functions used by dynamic views. Per Databricks docs,
entity matching on dynamic views is silently no-op'd — treat any view
whose DDL matches this regex as RLS-tainted."""

ENABLE_SMARTER_SCORING = (
    os.getenv("GSO_SMARTER_SCORING", "true").lower() in ("1", "true", "yes")
)
"""Gate for the intelligent scorer + idempotent diff allocator. When
False, falls back to the legacy 0/1/2 scorer + enable-only sort-and-take
shim (today's pre-idempotent behaviour, including the silent PII leak on
spaces with <120 STRING columns). Default: True. Override via env var
``GSO_SMARTER_SCORING=false``. The legacy shim will be deleted in a
follow-up release; use the flag to pin today's behaviour if the new
allocator surfaces any regressions on your corpus."""

DRY_RUN_ENTITY_MATCHING = (
    os.getenv("GSO_DRY_RUN_ENTITY_MATCHING", "false").lower() in ("1", "true", "yes")
)

# S8 — Vacuous-filter rejection in ``validate_sql_snippet``.
# Lever 6 occasionally proposes filter snippets whose semantics are
# ``1 = 1`` / ``TRUE`` / ``col = col`` (tautological — select all rows).
# The validator used to accept them (EXPLAIN passes, LIMIT 1 returns a
# row), so they deployed and silently did nothing, wasting a lever
# iteration. The gate runs a cheap syntactic pre-check plus a
# selectivity post-check (``COUNT(*) total`` vs
# ``COUNT(*) WHERE <filter>``). Default: on. Flip the env var off if a
# true-positive filter is ever miscategorised.
REJECT_VACUOUS_FILTERS = (
    os.getenv("GSO_REJECT_VACUOUS_FILTERS", "true").lower() in ("1", "true", "yes", "on")
)
"""When True, log the proposed enable/disable diff without PATCHing the
space. Used for initial rollout / audit. Covers the full enable+disable
diff produced by the idempotent allocator (not just reclaim).
Override via env var ``GSO_DRY_RUN_ENTITY_MATCHING=true``."""

STRICT_RLS_MODE = (
    os.getenv("GSO_STRICT_RLS", "false").lower() in ("1", "true", "yes")
)
"""When True, RLS verdict 'unknown' is treated as 'tainted' (refuse to
enable entity matching). Default: False — unknown verdicts are treated
as clean + warned, aligning with preflight's warn-and-proceed philosophy
since ``information_schema.row_filters`` availability is inconsistent
across DBR versions and workspace configurations."""

NUMERIC_DATA_TYPES = {
    "DOUBLE", "FLOAT", "DECIMAL", "INT", "INTEGER", "BIGINT",
    "SMALLINT", "TINYINT", "LONG", "SHORT", "BYTE", "NUMBER",
}

MEASURE_NAME_PREFIXES = [
    "avg_", "sum_", "count_", "total_", "pct_", "ratio_",
    "min_", "max_", "num_", "mean_", "median_", "stddev_",
]

# ── 12. Delta Table Names ─────────────────────────────────────────────

TABLE_RUNS = "genie_opt_runs"
TABLE_STAGES = "genie_opt_stages"
TABLE_ITERATIONS = "genie_opt_iterations"
TABLE_PATCHES = "genie_opt_patches"
TABLE_ASI = "genie_eval_asi_results"
TABLE_PROVENANCE = "genie_opt_provenance"
TABLE_SUGGESTIONS = "genie_opt_suggestions"
TABLE_FINALIZE_ATTESTATION = "genie_opt_finalize_attestation_matrix"
"""Bug #4 Phase 4 — per-qid pass/fail matrix for baseline and finalize
sweeps. One row per (run_id, qid, iteration_idx)."""
TABLE_SCAN_SNAPSHOTS = "genie_opt_scan_snapshots"
"""IQ Scan snapshots captured at preflight and postflight phases of an
optimization run. One row per (run_id, phase). See
``genie_space_optimizer.optimization.scan_snapshots``."""

# ── 13. MLflow Conventions ─────────────────────────────────────────────

EXPERIMENT_PATH_TEMPLATE = "/Shared/genie-space-optimizer/{{ space_id }}/{{ domain }}"
RUN_NAME_TEMPLATE = "iter_{{ iteration }}_eval_{{ timestamp }}"
BASELINE_RUN_NAME_TEMPLATE = "baseline_eval_{{ timestamp }}"
MODEL_NAME_TEMPLATE = "genie-space-{{ space_id }}"

UC_REGISTERED_MODEL_TEMPLATE = "{{ catalog }}.{{ schema }}.genie_space_{{ space_id }}"
"""Three-level UC registered model name. Interpolated at runtime."""

ENABLE_UC_MODEL_REGISTRATION: bool = True
"""When True, finalize registers the champion as a UC Registered Model."""

DEPLOYMENT_JOB_NAME_TEMPLATE = "genie-optimizer-deploy-{{ space_id }}"
"""Name pattern for the per-space deployment job."""

PROMPT_NAME_TEMPLATE = "{{ uc_schema }}.genie_opt_{{ judge_name }}"
PROMPT_ALIAS = "production"

INSTRUCTION_PROMPT_NAME_TEMPLATE = "{{ uc_schema }}.genie_instructions_{{ space_id }}"
INSTRUCTION_PROMPT_ALIAS = "latest"

# ── 14. Patch DSL Constants ────────────────────────────────────────────

MAX_PATCH_OBJECTS = 5
MAX_INSTRUCTION_TEXT_CHARS = 2000
MAX_HOLISTIC_INSTRUCTION_CHARS = 8000

PROMPT_TOKEN_BUDGET = 70_000
"""Token budget for LLM prompts.  Claude Opus 4.6 supports 200k tokens;
we target ~70k to stay in the quality sweet-spot while leaving headroom
for the response."""

RISK_LEVEL_SCORE = {
    "low": 1,
    "medium": 2,
    "high": 3,
}

GENERIC_FIX_PREFIXES = (
    "review ",
    "check ",
    "verify ",
    "ensure ",
    "investigate ",
)

# ── 15. Assessment Sources ─────────────────────────────────────────────

CODE_SOURCE_ID = "genie-optimizer-v2"
LLM_SOURCE_ID_TEMPLATE = "databricks:/{{ endpoint }}"

# ── 16. Temporal Validation Patterns ───────────────────────────────────

TEMPORAL_PHRASES = (
    r"\b(this year|last year|last quarter|this quarter|last \d+ months?"
    r"|last \d+ days?|this month|last month|year to date|ytd)\b"
)
HARDCODED_DATE = r"'\d{4}-\d{2}-\d{2}'"

# ── 17. Patch Types (35 entries) ───────────────────────────────────────

PATCH_TYPES = {
    # Lever 1: Tables & Columns — descriptions, visibility, aliases
    "add_description": {
        "type": "add_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "update_description": {
        "type": "update_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "add_column_description": {
        "type": "add_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "update_column_description": {
        "type": "update_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "hide_column": {
        "type": "hide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "unhide_column": {
        "type": "unhide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "rename_column_alias": {
        "type": "rename_column_alias",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["column_metadata", "aliases"],
    },
    "add_table": {
        "type": "add_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    "remove_table": {
        "type": "remove_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    # Lever 2: Metric Views
    "add_mv_measure": {
        "type": "add_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "update_mv_measure": {
        "type": "update_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "remove_mv_measure": {
        "type": "remove_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "add_mv_dimension": {
        "type": "add_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "remove_mv_dimension": {
        "type": "remove_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "update_mv_yaml": {
        "type": "update_mv_yaml",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["metric_view", "mv_yaml"],
    },
    # Lever 3: Table-Valued Functions
    "add_tvf_parameter": {
        "type": "add_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "remove_tvf_parameter": {
        "type": "remove_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "update_tvf_sql": {
        "type": "update_tvf_sql",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvf_definition", "tvf_sql"],
    },
    "add_tvf": {
        "type": "add_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    "remove_tvf": {
        "type": "remove_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    # Lever 4: Join Specifications
    "add_join_spec": {
        "type": "add_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "update_join_spec": {
        "type": "update_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "remove_join_spec": {
        "type": "remove_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    # Lever 5: Column Discovery Settings
    "enable_example_values": {
        "type": "enable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_example_values": {
        "type": "disable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "enable_value_dictionary": {
        "type": "enable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_value_dictionary": {
        "type": "disable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "add_column_synonym": {
        "type": "add_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    "remove_column_synonym": {
        "type": "remove_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    # Lever 6: Genie Space Instructions (text)
    "add_instruction": {
        "type": "add_instruction",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions"],
    },
    "update_instruction": {
        "type": "update_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "remove_instruction": {
        "type": "remove_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "rewrite_instruction": {
        "type": "rewrite_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "update_instruction_section": {
        "type": "update_instruction_section",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    # Example SQL patches (used by levers 3 and 5; preferred over text instructions)
    "add_example_sql": {
        "type": "add_example_sql",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions", "example_question_sqls"],
    },
    "update_example_sql": {
        "type": "update_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
    },
    "remove_example_sql": {
        "type": "remove_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
    },
    # Shared: Filters
    "add_default_filter": {
        "type": "add_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "remove_default_filter": {
        "type": "remove_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "update_filter_condition": {
        "type": "update_filter_condition",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    # Lever 6: SQL Expressions (measures, filters, dimensions)
    "add_sql_snippet_measure": {
        "type": "add_sql_snippet_measure",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "measures"],
    },
    "update_sql_snippet_measure": {
        "type": "update_sql_snippet_measure",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "measures"],
    },
    "remove_sql_snippet_measure": {
        "type": "remove_sql_snippet_measure",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "measures"],
    },
    "add_sql_snippet_filter": {
        "type": "add_sql_snippet_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "filters"],
    },
    "update_sql_snippet_filter": {
        "type": "update_sql_snippet_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "filters"],
    },
    "remove_sql_snippet_filter": {
        "type": "remove_sql_snippet_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "filters"],
    },
    "add_sql_snippet_expression": {
        "type": "add_sql_snippet_expression",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "expressions"],
    },
    "update_sql_snippet_expression": {
        "type": "update_sql_snippet_expression",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "expressions"],
    },
    "remove_sql_snippet_expression": {
        "type": "remove_sql_snippet_expression",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["sql_snippets", "expressions"],
    },
}

# ── 18. Conflict Rules (23 pairs) ─────────────────────────────────────

CONFLICT_RULES = [
    ("add_table", "remove_table"),
    ("add_column_synonym", "remove_column_synonym"),
    ("add_instruction", "remove_instruction"),
    ("add_instruction", "update_instruction"),
    ("update_instruction", "remove_instruction"),
    ("add_join_spec", "remove_join_spec"),
    ("add_default_filter", "remove_default_filter"),
    ("add_tvf_parameter", "remove_tvf_parameter"),
    ("add_tvf", "remove_tvf"),
    ("add_mv_measure", "remove_mv_measure"),
    ("add_mv_dimension", "remove_mv_dimension"),
    ("hide_column", "unhide_column"),
    ("add_column_description", "update_column_description"),
    ("add_description", "update_description"),
    ("update_mv_measure", "remove_mv_measure"),
    ("enable_example_values", "disable_example_values"),
    ("enable_value_dictionary", "disable_value_dictionary"),
    ("update_join_spec", "remove_join_spec"),
    # Example SQL conflict pairs
    ("add_example_sql", "remove_example_sql"),
    ("add_example_sql", "update_example_sql"),
    ("update_example_sql", "remove_example_sql"),
    # Cross-type conflicts: example SQL vs text instructions on same routing
    ("add_example_sql", "add_instruction"),
    ("add_example_sql", "update_instruction"),
    # Holistic rewrite conflicts with any other instruction mutation
    ("rewrite_instruction", "add_instruction"),
    ("rewrite_instruction", "update_instruction"),
    ("rewrite_instruction", "remove_instruction"),
]

# ── 19. Failure Taxonomy (24 types) ───────────────────────────────────

FAILURE_TAXONOMY = {
    "wrong_table",
    "wrong_column",
    "wrong_join",
    "missing_filter",
    "missing_temporal_filter",
    "wrong_aggregation",
    "wrong_measure",
    "missing_instruction",
    "ambiguous_question",
    "asset_routing_error",
    "tvf_parameter_error",
    "compliance_violation",
    "performance_issue",
    "repeatability_issue",
    "missing_synonym",
    "description_mismatch",
    "stale_data",
    "data_freshness",
    "missing_join_spec",
    "wrong_join_spec",
    "missing_format_assistance",
    "missing_entity_matching",
    "missing_dimension",
    "wrong_grouping",
}

# ── 19b. Cluster Priority Weights (adaptive lever loop) ───────────────

CAUSAL_WEIGHT: dict[str, float] = {
    "syntax_validity": 5.0,
    "schema_accuracy": 4.0,
    "asset_routing": 3.5,
    "logical_accuracy": 3.0,
    "semantic_equivalence": 2.0,
    "completeness": 1.5,
    "result_correctness": 1.0,
    "response_quality": 0.5,
}
"""Weight reflecting a judge's position in the causal chain.
Upstream judges (syntax, schema) cascade failures downstream, so
fixing them has higher leverage."""

SEVERITY_WEIGHT: dict[str, float] = {
    "wrong_table": 1.0,
    "wrong_column": 0.9,
    "wrong_join": 0.9,
    "missing_join_spec": 0.85,
    "wrong_join_spec": 0.85,
    "wrong_aggregation": 0.8,
    "wrong_measure": 0.8,
    "asset_routing_error": 0.9,
    "missing_filter": 0.7,
    "missing_temporal_filter": 0.7,
    "tvf_parameter_error": 0.7,
    "missing_instruction": 0.6,
    "description_mismatch": 0.4,
    "compliance_violation": 0.5,
    "performance_issue": 0.3,
    "repeatability_issue": 0.4,
    "missing_synonym": 0.3,
    "ambiguous_question": 0.3,
    "stale_data": 0.3,
    "data_freshness": 0.3,
    "missing_format_assistance": 0.3,
    "missing_entity_matching": 0.3,
}
"""Severity weight per failure type.  Higher values mean the failure
type is more impactful and should be prioritized."""

FIXABILITY_WITH_COUNTERFACTUAL = 1.0
FIXABILITY_WITHOUT_COUNTERFACTUAL = 0.4

# ── 20. Judge Prompts (5 templates) ───────────────────────────────────

JUDGE_PROMPTS = {
    "schema_accuracy": (
        "<role>\n"
        "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: What is total revenue by region?\n"
        "Expected: SELECT region, SUM(revenue) FROM sales GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) FROM sales GROUP BY ALL\n"
        'Result: {"correct": true, "failure_type": "", "wrong_clause": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Same tables and columns; GROUP BY ALL is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Show revenue by product\n"
        "Expected: SELECT p.name, SUM(s.revenue) FROM sales s JOIN products p ON s.product_id = p.id GROUP BY 1\n"
        "Generated: SELECT product_name, SUM(revenue) FROM orders GROUP BY 1\n"
        'Result: {"correct": false, "failure_type": "wrong_table", '
        '"wrong_clause": "FROM orders", '
        '"blame_set": ["orders"], '
        '"counterfactual_fix": "Add synonym product_name to products.name; '
        'clarify sales is the revenue table", '
        '"rationale": "Generated SQL queries orders instead of sales table."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"correct": true/false, '
        '"failure_type": "<wrong_table|wrong_column|wrong_join|missing_column>", '
        '"wrong_clause": "<problematic SQL clause>", '
        '"blame_set": ["<table_or_column>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If correct, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "logical_accuracy": (
        "<role>\n"
        "You are a SQL logic expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL applies correct aggregations, filters, "
        "GROUP BY, ORDER BY, and WHERE clauses for the business question.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Top 5 customers by spend\n"
        "Expected: SELECT customer_id, SUM(amount) as total FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 5\n"
        "Generated: SELECT customer_id, SUM(amount) FROM orders GROUP BY ALL ORDER BY SUM(amount) DESC LIMIT 5\n"
        'Result: {"correct": true, "failure_type": "", "wrong_clause": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Same aggregation and ordering; GROUP BY ALL is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Total revenue by country\n"
        "Expected: SELECT country, SUM(revenue) FROM sales GROUP BY country\n"
        "Generated: SELECT country, AVG(revenue) FROM sales GROUP BY country\n"
        'Result: {"correct": false, "failure_type": "wrong_aggregation", '
        '"wrong_clause": "AVG(revenue)", '
        '"blame_set": ["revenue"], '
        '"counterfactual_fix": "Update revenue column description to specify SUM aggregation", '
        '"rationale": "Question asks for total (SUM) but generated SQL uses AVG."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"correct": true/false, '
        '"failure_type": "<wrong_aggregation|wrong_filter|wrong_groupby|wrong_orderby>", '
        '"wrong_clause": "<problematic SQL clause>", '
        '"blame_set": ["<column_or_function>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If correct, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "semantic_equivalence": (
        "<role>\n"
        "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the two SQL queries measure the SAME business metric and would "
        "answer the same question, even if written differently.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Monthly revenue\n"
        "Expected: SELECT month, SUM(amount) FROM sales GROUP BY month\n"
        "Generated: SELECT DATE_TRUNC('month', sale_date) AS month, SUM(amount) FROM sales GROUP BY 1\n"
        'Result: {"equivalent": true, "failure_type": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Both measure total revenue per month; just different date extraction."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Revenue by product\n"
        "Expected: SELECT product, SUM(revenue) FROM sales GROUP BY product\n"
        "Generated: SELECT product, COUNT(*) FROM sales GROUP BY product\n"
        'Result: {"equivalent": false, "failure_type": "different_metric", '
        '"blame_set": ["revenue"], '
        '"counterfactual_fix": "Clarify revenue column aggregation as SUM in description", '
        '"rationale": "Expected measures SUM(revenue) but generated counts rows."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"equivalent": true/false, '
        '"failure_type": "<different_metric|different_grain|different_scope>", '
        '"blame_set": ["<metric_or_dimension>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If equivalent, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "completeness": (
        "<role>\n"
        "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Generated SQL: { outputs }\n"
        "Expected SQL: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Determine if the GENERATED SQL fully answers the user's question without "
        "missing dimensions, measures, or filters.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: Revenue and order count by region\n"
        "Expected: SELECT region, SUM(revenue), COUNT(order_id) FROM orders GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) AS rev, COUNT(*) AS orders FROM orders GROUP BY 1\n"
        'Result: {"complete": true, "failure_type": "", '
        '"blame_set": [], "counterfactual_fix": "", '
        '"rationale": "Both revenue and count are present; COUNT(*) vs COUNT(order_id) is equivalent."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Revenue and order count by region\n"
        "Expected: SELECT region, SUM(revenue), COUNT(order_id) FROM orders GROUP BY region\n"
        "Generated: SELECT region, SUM(revenue) FROM orders GROUP BY region\n"
        'Result: {"complete": false, "failure_type": "missing_column", '
        '"blame_set": ["order_id"], '
        '"counterfactual_fix": "Add synonym order count to order_id column description", '
        '"rationale": "User asked for order count but generated SQL omits COUNT(order_id)."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"complete": true/false, '
        '"failure_type": "<missing_column|missing_filter|missing_temporal_filter|missing_aggregation|partial_answer>", '
        '"blame_set": ["<missing_element>"], '
        '"counterfactual_fix": "<specific metadata change referencing exact table/column names>", '
        '"rationale": "<brief explanation>"}\n'
        'If complete, set failure_type to "", blame_set to [], counterfactual_fix to "".\n'
        "</output_schema>"
    ),
    "arbiter": (
        "<role>\n"
        "You are a senior SQL arbiter for a Databricks Genie Space evaluation.\n"
        "</role>\n\n"
        "<context>\n"
        "User question and expected SQL: { inputs }\n"
        "Genie response and comparison: { outputs }\n"
        "Expected result: { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Two SQL queries attempted to answer the same business question but produced "
        "different results. Analyze both and determine which is correct.\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"genie_correct\">\n"
        "Question: Total active users\n"
        "Expected SQL: SELECT COUNT(*) FROM users (no filter)\n"
        "Genie SQL: SELECT COUNT(*) FROM users WHERE status = 'active'\n"
        "Genie returned 150 rows, Expected returned 300 rows.\n"
        'Result: {"verdict": "genie_correct", "failure_type": "wrong_filter", '
        '"blame_set": ["users.status"], '
        '"rationale": "User asked for active users; Genie correctly filters on status. '
        'GT SQL is missing the active filter."}\n'
        "</example>\n"
        "<example name=\"ground_truth_correct\">\n"
        "Question: Revenue by quarter\n"
        "Expected SQL: SELECT quarter, SUM(revenue) FROM sales GROUP BY quarter\n"
        "Genie SQL: SELECT quarter, AVG(revenue) FROM sales GROUP BY quarter\n"
        'Result: {"verdict": "ground_truth_correct", "failure_type": "wrong_aggregation", '
        '"blame_set": ["revenue"], '
        '"rationale": "Revenue should be summed, not averaged."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"verdict": "<genie_correct|ground_truth_correct|both_correct|neither_correct>", '
        '"failure_type": "<wrong_aggregation|wrong_filter|wrong_table|other>", '
        '"blame_set": ["<blamed_object>"], '
        '"rationale": "<brief explanation>"}\n'
        "</output_schema>"
    ),
    "response_quality": (
        "<role>\n"
        "You are evaluating the quality of a natural language response "
        "from a Databricks Genie Space AI assistant.\n"
        "</role>\n\n"
        "<context>\n"
        "User question: { inputs }\n"
        "Genie's natural language response:\n  { outputs }\n"
        "Genie's SQL query:\n  { sql }\n"
        "Expected SQL:\n  { expectations }\n"
        "</context>\n\n"
        "<instructions>\n"
        "Evaluate whether the natural language response:\n"
        "1. Accurately describes what the SQL query does\n"
        "2. Correctly answers the user's question\n"
        "3. Does not make claims unsupported by the query/data\n"
        "</instructions>\n\n"
        "<examples>\n"
        "<example name=\"PASS\">\n"
        "Question: How many orders last month?\n"
        "Genie response: There were 1,234 orders placed last month.\n"
        "SQL: SELECT COUNT(*) FROM orders WHERE order_date >= '2024-11-01'\n"
        'Result: {"accurate": true, "failure_type": "", "counterfactual_fix": "", '
        '"rationale": "Response accurately describes the count query result."}\n'
        "</example>\n"
        "<example name=\"FAIL\">\n"
        "Question: Total revenue this quarter\n"
        "Genie response: The average revenue this quarter is $50,000.\n"
        "SQL: SELECT SUM(revenue) FROM sales WHERE quarter = 'Q4'\n"
        'Result: {"accurate": false, "failure_type": "inaccurate_description", '
        '"counterfactual_fix": "Add instruction clarifying revenue is a SUM metric, not AVG", '
        '"rationale": "SQL computes SUM but response says average."}\n'
        "</example>\n"
        "</examples>\n\n"
        "<output_schema>\n"
        'Respond with JSON only: {"accurate": true/false, '
        '"failure_type": "<inaccurate_description|unsupported_claim|misleading_summary>", '
        '"counterfactual_fix": "<specific change to metadata/instructions that would fix this>", '
        '"rationale": "<brief explanation>"}\n'
        'If accurate, set failure_type to "" and counterfactual_fix to "".\n'
        "</output_schema>"
    ),
}

# ── 20b. Lever Prompts (registered in MLflow for traceability) ─────────

LEVER_PROMPTS: dict[str, str] = {
    "strategist": STRATEGIST_PROMPT,
    "strategist_triage": STRATEGIST_TRIAGE_PROMPT,
    "strategist_detail": STRATEGIST_DETAIL_PROMPT,
    "adaptive_strategist": ADAPTIVE_STRATEGIST_PROMPT,
    "lever_1_2_column": LEVER_1_2_COLUMN_PROMPT,
    "lever_4_join_spec": LEVER_4_JOIN_SPEC_PROMPT,
    "lever_4_join_discovery": LEVER_4_JOIN_DISCOVERY_PROMPT,
    "lever_5_instruction": LEVER_5_INSTRUCTION_PROMPT,
    "lever_5_holistic": LEVER_5_HOLISTIC_PROMPT,
    "proposal_generation": PROPOSAL_GENERATION_PROMPT,
    "description_enrichment": DESCRIPTION_ENRICHMENT_PROMPT,
    "table_description_enrichment": TABLE_DESCRIPTION_ENRICHMENT_PROMPT,
    "proactive_instruction": PROACTIVE_INSTRUCTION_PROMPT,
    "expand_instruction": EXPAND_INSTRUCTION_PROMPT,
    # "instruction_restructure" was removed in the 5-section schema
    # migration (see common/config.py CANONICAL_SECTION_HEADERS and the
    # prose rule miner). Its reorganize-without-promote semantics are
    # subsumed by the miner's canonical-grouping rewrite step.
    # ``prose_rule_mining`` (and its deprecated alias
    # ``instruction_to_sql_expression``) are registered below, after
    # ``PROSE_RULE_MINING_PROMPT`` is defined — keep the ordering.
    "space_description": SPACE_DESCRIPTION_PROMPT,
    "sample_questions": SAMPLE_QUESTIONS_PROMPT,
    "gt_repair": GT_REPAIR_PROMPT,
}

# ── 20c. Benchmark Prompts (registered in MLflow for traceability) ─────

BENCHMARK_PROMPTS: dict[str, str] = {
    "benchmark_generation": BENCHMARK_GENERATION_PROMPT,
    "benchmark_correction": BENCHMARK_CORRECTION_PROMPT,
    "benchmark_alignment_check": BENCHMARK_ALIGNMENT_CHECK_PROMPT,
    "benchmark_coverage_gap": BENCHMARK_COVERAGE_GAP_PROMPT,
    "curated_sql_generation": CURATED_SQL_GENERATION_PROMPT,
    # Phase 4.R4b — example-SQL variants. Registered alongside
    # benchmark prompts so MLflow tracing + the registry-key lookup in
    # ``get_registered_prompt_name`` find them by the same pathway.
    "example_sql_generation": EXAMPLE_SQL_GENERATION_PROMPT,
    "example_sql_correction": EXAMPLE_SQL_CORRECTION_PROMPT,
}


# ── 20d. Phase 2.R2b — Prompt isolation assertion ──────────────────────
#
# Isolation invariant #2 of the unified example-SQL generator: the
# example prompts must NOT reference any benchmark-derived template
# variable. A mis-edit to either template that accidentally pipes
# benchmark text into the generator's prompt is caught at import time
# rather than at runtime. See docs/example-sql-isolation.md.

_BENCHMARK_DERIVED_VARS: frozenset[str] = frozenset({
    "benchmarks",
    "benchmark_list",
    "existing_benchmarks",
    "benchmark_questions",
    "benchmark_sqls",
    "expected_sqls",
    "eval_questions",
    "benchmark_corpus",
})

for _fwd_var in _BENCHMARK_DERIVED_VARS:
    _forbidden_token = "{{ " + _fwd_var + " }}"
    assert _forbidden_token not in EXAMPLE_SQL_GENERATION_PROMPT, (
        f"Isolation invariant violated: EXAMPLE_SQL_GENERATION_PROMPT "
        f"references benchmark-derived template variable '{_fwd_var}'. "
        "See docs/example-sql-isolation.md."
    )
    assert _forbidden_token not in EXAMPLE_SQL_CORRECTION_PROMPT, (
        f"Isolation invariant violated: EXAMPLE_SQL_CORRECTION_PROMPT "
        f"references benchmark-derived template variable '{_fwd_var}'."
    )

del _fwd_var, _forbidden_token

# ── 21. ASI Schema (12 fields) ─────────────────────────────────────────

ASI_SCHEMA = {
    "failure_type": "str (from FAILURE_TAXONOMY)",
    "severity": "str (critical|major|minor)",
    "confidence": "float (0.0-1.0)",
    "wrong_clause": "str|None (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, MEASURE)",
    "blame_set": "list[str] (metadata fields blamed: table names, column names, instructions)",
    "quoted_metadata_text": "str|None (exact text from Genie config that caused the issue)",
    "missing_metadata": "str|None (what should exist but doesn't)",
    "ambiguity_detected": "bool",
    "expected_value": "str|None",
    "actual_value": "str|None",
    "counterfactual_fix": "str|None (suggested metadata change to fix)",
    "affected_question_pattern": "str|None (regex or description of affected questions)",
}

# ── 22. Lever-to-Patch-Type Mapping ────────────────────────────────────

_LEVER_TO_PATCH_TYPE: dict[tuple[str, int], str] = {
    # Lever 1: Tables & Columns
    ("wrong_column", 1): "update_column_description",
    ("wrong_table", 1): "update_description",
    ("description_mismatch", 1): "update_column_description",
    ("missing_synonym", 1): "add_column_synonym",
    ("select_star", 1): "update_column_description",
    ("missing_scd_filter", 1): "update_column_description",
    # Lever 2: Metric Views — route aggregation/measure issues to column descriptions
    ("wrong_aggregation", 2): "update_column_description",
    ("wrong_measure", 2): "update_column_description",
    ("missing_filter", 2): "update_mv_yaml",
    ("missing_temporal_filter", 2): "update_mv_yaml",
    ("wrong_filter_condition", 2): "update_column_description",
    # Lever 3: Table-Valued Functions (including routing example SQLs)
    ("tvf_parameter_error", 3): "add_tvf_parameter",
    ("repeatability_issue", 3): "add_tvf_parameter",
    ("asset_routing_error", 3): "add_example_sql",
    # S3 hardening: ASI blame-set rescue surfaces a missing asset (table,
    # MV, or TVF). Lever 3 owns routing / example SQL so the patch is an
    # ``add_example_sql`` that demonstrates the missing asset. Level 1 can
    # also refresh descriptions if the asset does exist but is undersold.
    ("missing_data_asset", 3): "add_example_sql",
    ("missing_data_asset", 1): "update_description",
    # S3 hardening: empty generated SQL is most plausibly a prompt /
    # instruction gap (the model refused to emit any SQL). Route the
    # default patch type to Lever 5 (instructions / example SQLs).
    ("missing_sql_generation", 5): "add_example_sql",
    ("missing_sql_generation", 1): "update_description",
    # Lever 4: Join Specifications
    ("wrong_join", 4): "update_join_spec",
    ("missing_join_spec", 4): "add_join_spec",
    ("wrong_join_spec", 4): "update_join_spec",
    ("wrong_join_type", 4): "update_join_spec",
    # Lever 5: Genie Space Instructions (example SQL preferred over text)
    ("asset_routing_error", 5): "add_example_sql",
    ("missing_instruction", 5): "add_example_sql",
    ("ambiguous_question", 5): "add_example_sql",
    ("missing_filter", 5): "add_example_sql",
    # Lever 6: SQL Expressions — Measures (aggregation / KPI failures)
    ("wrong_aggregation", 6): "add_sql_snippet_measure",
    ("wrong_measure", 6): "add_sql_snippet_measure",
    # Lever 6: SQL Expressions — Filters (condition / WHERE clause failures)
    ("missing_filter", 6): "add_sql_snippet_filter",
    ("wrong_filter_condition", 6): "add_sql_snippet_filter",
    ("missing_temporal_filter", 6): "add_sql_snippet_filter",
    # Lever 6: SQL Expressions — Dimensions (grouping / derived column failures)
    ("wrong_column", 6): "add_sql_snippet_expression",
    ("description_mismatch", 6): "add_sql_snippet_expression",
    ("ambiguous_question", 6): "add_sql_snippet_expression",
    ("missing_dimension", 6): "add_sql_snippet_expression",
    ("wrong_grouping", 6): "add_sql_snippet_expression",
    # Fallback for "other" failure types — avoids falling through to add_instruction
    ("other", 1): "update_column_description",
    ("other", 2): "update_column_description",
    ("other", 3): "update_description",
    ("other", 4): "add_join_spec",
    ("other", 5): "add_example_sql",
    ("other", 6): "add_sql_snippet_measure",
}

# ── 23. Lever 6 SQL Expression Prompt ──────────────────────────────────

_LEVER_6_SQL_EXPRESSION_BODY = """You are an expert at defining SQL Expressions for Databricks Genie Spaces.

## Context

A Genie Space is answering user questions incorrectly. Analysis of the failures
shows the root cause is: **{{ root_cause }}**

### Failed questions and SQL diffs
{{ cluster_context }}

### Current schema
{{ schema_context }}

### Existing SQL Expressions (do NOT duplicate these)
{{ existing_sql_snippets }}

### Strategist hints (optional — adopt, modify, or override as needed)
{{ strategist_hints }}

## Task

Based on the failure analysis, define ONE SQL Expression that would fix or
improve the identified questions.  Choose the most appropriate type:

- **measure**: A KPI or aggregation (e.g. `SUM(revenue) - SUM(cost)`).
  Use when the failure involves wrong aggregation, missing metric, or
  incorrect calculation.
- **filter**: A boolean condition (e.g. `order_total > 1000`).
  Use when the failure involves missing filters, wrong filter conditions,
  or common WHERE patterns that recur across questions.
- **expression** (dimension): A per-row derived value (e.g. `MONTH(date_col)`).
  Use when the failure involves missing grouping attributes, derived columns,
  or computed dimensions.

## Output format (strict JSON)

```json
{{
  "snippet_type": "measure" | "filter" | "expression",
  "display_name": "Human-readable name for the concept",
  "alias": "snake_case_identifier (required for measure/expression, omit for filter)",
  "sql": "The SQL expression (single string, no trailing semicolon)",
  "synonyms": ["synonym1", "synonym2"],
  "instruction": "When and how Genie should use this expression",
  "rationale": "Why this expression fixes the identified failures",
  "target_table": "primary table this expression references",
  "affected_questions": ["q1", "q2"]
}}
```

Rules:
- ALL column references MUST use table_name.column_name syntax (e.g. `mv_sales.revenue`, \
NOT bare `revenue`). The Genie API rejects bare column names.
- The SQL MUST reference only tables and columns that exist in the schema.
- For measures: SQL must be a valid aggregation expression (SUM, COUNT, AVG, etc.).
- For filters: SQL must evaluate to a boolean.
- For expressions: SQL must produce a scalar value per row.
- Do NOT wrap in SELECT or WHERE — provide the raw expression only.
- Do NOT duplicate an existing SQL Expression.
- Prefer concise, reusable definitions over question-specific hacks.

Naming policy (REQUIRED — be specific, never generic):
- ``display_name`` MUST be specific enough to disambiguate the table or \
business domain inside this space. If two fact tables or metric views \
in the schema could plausibly share a concept, the name MUST encode \
which one it applies to.
- When the SQL references a domain-specific table such as \
``mv_<domain>_fact_<entity>`` or ``mv_<domain>_dim_<entity>`` (for \
example ``mv_orders_fact_lines`` or ``mv_claims_dim_date``), include \
the compact qualifier (``ORDERS``, ``CLAIMS``, …) at the start of \
``display_name`` — e.g. ``ORDERS Month-to-Date Filter``, NOT \
``Month-to-Date Filter``.
- ``instruction`` MUST state when to use the expression and which \
table or domain it applies to.
- Avoid generic names like ``Month-to-Date Filter``, ``Total Revenue``, \
or ``Active Filter`` when more than one fact table or metric view in \
the schema could host that concept.
"""

LEVER_6_SQL_EXPRESSION_PROMPT = _RCA_CONTRACT_HEADER + _LEVER_6_SQL_EXPRESSION_BODY

# ── 24. Prose Rule Mining (multi-target; prose → structured) ──────────
#
# Replaces the earlier INSTRUCTION_TO_SQL_EXPRESSION_PROMPT. A single LLM
# pass classifies each rule in the space's prose as one of six targets;
# each target has its own validator and applier downstream. The prompt
# intentionally reads any vocabulary (legacy ALL-CAPS or new canonical
# ``##`` headers) so it can migrate pre-PR-#178 spaces on first run.
#
# The ``source_span`` field is REQUIRED — the rewrite step uses exact
# substring removal to strip promoted content from prose, so the LLM
# must return spans that match the input byte-for-byte.

_PROSE_RULE_MINING_BODY = """\
<role>
You are a Databricks Genie Space configuration expert. You are given the \
full text_instructions of a Genie Space and the space's schema. Your job \
is to promote every rule that belongs in structured config out of the \
prose, and to keep only the rules that truly belong in text_instructions.
</role>

<context>
## Genie Space Instructions (verbatim)
{{ instructions_text }}

## Table Schema
{{ schema_context }}

## Existing Structured Config
### Existing SQL Expressions (do NOT duplicate)
{{ existing_expressions }}

### Existing Join Specs (do NOT duplicate)
{{ existing_join_specs }}

### Existing Example Question SQLs (do NOT duplicate)
{{ existing_example_sqls }}
</context>

<instructions>
Read the instructions top-to-bottom. Split compound bullets (e.g. "join \
X to Y on Z and filter by status = 'active'") into one candidate per \
rule. For each candidate, choose exactly one ``target`` from the list \
below, produce a ``source_span`` that is an EXACT substring of the input \
(byte-for-byte; the rewrite step removes this substring from prose), \
and a ``confidence`` in [0.0, 1.0].

## Target routing

Use this table (mirrors docs/gsl-instruction-schema.md — "What does NOT \
go in text_instructions"):

| Content shape                                            | target             |
|----------------------------------------------------------|--------------------|
| Aggregation formula (SUM, AVG, ...)                      | sql_snippet (measure)    |
| Reusable WHERE / filter clause                           | sql_snippet (filter)     |
| Computed / derived column                                | sql_snippet (expression) |
| Table relationship / join path                           | join_spec          |
| Full question → SQL pattern                              | example_qsql       |
| "For X questions use table T" / asset routing            | table_desc         |
| "Term 'revenue' means column net_rev_amt"                | column_synonym     |
| Disambiguation / data-quality / PII / summary rendering  | keep_in_prose      |
| "Do not / Never join X to Y" — negative join constraint  | keep_in_prose (## CONSTRAINTS) |

## Per-target rules

### sql_snippet
- ALL column references MUST use ``catalog.schema.table.column`` syntax. \
Bare columns are rejected by the Genie serving path.
- Do NOT wrap in SELECT or WHERE — raw expression only.
- ``is_default=true`` means the rule says "apply by default"; \
``omit_when`` describes when the filter should NOT be applied.
- Drop anything the Existing SQL Expressions list already covers.
- ``display_name`` MUST be specific enough to disambiguate the source \
table or business domain inside this space. When the SQL references a \
domain-specific table such as ``mv_<domain>_fact_<entity>`` or \
``mv_<domain>_dim_<entity>`` (for example ``mv_orders_fact_lines`` or \
``mv_claims_dim_date``), prefix ``display_name`` with the compact \
qualifier (e.g. ``ORDERS Month-to-Date Filter``, \
``CLAIMS Total Premium Amount``). Avoid generic names like \
``Month-to-Date Filter`` when multiple fact tables or metric views in \
this space could plausibly share that concept.
- ``description`` MUST mention when to use the snippet AND which \
table or domain it applies to.

### join_spec
- Both ``left`` and ``right`` carry a fully-qualified ``identifier`` and \
an ``alias`` (the unqualified table name). \
- ``sql`` is a two-element array: ``[join_condition, relationship_tag]`` \
e.g. ``["`fact_sales`.`region_id` = `dim_region`.`region_id`", \
"--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]``.
- Use backtick-quoted aliases in the join condition.

### example_qsql
- Concrete ``question`` (not a placeholder) paired with a validated SQL \
statement that uses fully-qualified table names.
- Add ``usage_guidance`` telling Genie when this pattern applies.

### table_desc
- Use when a rule pins a topic or question kind to a specific table, e.g. \
"For sessions, use fact_sessions, not raw_events".
- ``description_append`` contains the sentence to append to the table's \
existing description.

### column_synonym
- Use when a rule pins a business term to a specific column, e.g. \
"revenue / sales / net sales = net_revenue_amt".
- ``synonyms`` is the list of aliases the column answers to.

### keep_in_prose
- Use ONLY when the rule cannot be expressed as SQL or metadata: \
clarification triggers, NULL handling, PII guardrails, summary-rendering \
rules.
- ``section`` MUST be one of the five canonical headers: \
``## PURPOSE``, ``## DISAMBIGUATION``, ``## DATA QUALITY NOTES``, \
``## CONSTRAINTS``, ``## Instructions you must follow when providing summaries``.
- CONSTRAINTS that are SQL-expressible filters MUST be returned as \
``sql_snippet`` (with ``is_default=true`` and ``snippet_type=filter``), \
NOT as ``keep_in_prose``.
- ``source_span`` that contains SQL keywords (SELECT, WHERE, JOIN, \
GROUP BY, ORDER BY, HAVING) MUST be returned as ``sql_snippet`` or \
``example_qsql`` — the scanner rejects SQL-in-prose.
- NEGATIVE JOIN CONSTRAINTS ("Do not / Never join X to Y") are a \
cross-cutting BEHAVIOURAL rule and belong in prose, NOT in \
``join_spec`` (which represents joins the model MAY use). Return them \
as ``target="keep_in_prose"`` with ``payload.section="## CONSTRAINTS"``. \
The structure-aware scanner allows English imperatives like "Do not \
join" to sit in prose without triggering the SQL-in-text finding.

## Confidence

- 0.9-1.0 — the rule maps unambiguously onto the target and the payload \
is fully inferable from the prose + schema.
- 0.7-0.9 — minor inference required (e.g. picking between two plausible \
tables).
- < 0.7 — speculative; the dispatcher will drop these.

Return ``[]`` if nothing is promotable.
</instructions>

<output_schema>
Respond with a single JSON array. Each element:
{{
  "target": "sql_snippet"|"join_spec"|"example_qsql"|"table_desc"|"column_synonym"|"keep_in_prose",
  "source_span": "<EXACT substring of the input instructions>",
  "confidence": 0.0-1.0,
  "payload": {{
    // sql_snippet:
    "snippet_type": "measure"|"filter"|"expression",
    "sql": "...",
    "display_name": "...",
    "description": "One sentence on what this computes/filters",
    "synonyms": ["term1", "term2"],
    "alias": "snake_case_identifier",
    "is_default": true|false,
    "omit_when": "..." or null,
    // join_spec:
    "left":  {{"identifier": "catalog.schema.t1", "alias": "t1"}},
    "right": {{"identifier": "catalog.schema.t2", "alias": "t2"}},
    "sql":   ["`t1`.`k` = `t2`.`k`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"],
    "instruction": "when and how to use this join",
    // example_qsql:
    "question": "natural-language question",
    "sql": "SELECT ... FROM catalog.schema.t ...",
    "usage_guidance": "when this pattern applies",
    // table_desc:
    "table_identifier": "catalog.schema.t",
    "description_append": "sentence to append",
    // column_synonym:
    "table_identifier": "catalog.schema.t",
    "column_name": "net_revenue_amt",
    "synonyms": ["revenue", "sales", "net sales"],
    // keep_in_prose:
    "section": "## PURPOSE" | "## DISAMBIGUATION" | "## DATA QUALITY NOTES" | "## CONSTRAINTS" | "## Instructions you must follow when providing summaries"
  }}
}}

Respond with a single JSON array and nothing else.
</output_schema>"""

PROSE_RULE_MINING_PROMPT = _RCA_CONTRACT_HEADER + _PROSE_RULE_MINING_BODY

# Backward-compat alias for call sites that haven't migrated yet. The
# registry key also gets aliased so MLflow prompt history is preserved
# across the rename.
INSTRUCTION_TO_SQL_EXPRESSION_PROMPT = PROSE_RULE_MINING_PROMPT

# Post-definition registration of the miner prompt(s). ``LEVER_PROMPTS``
# is defined earlier (line ~3315) but ``PROSE_RULE_MINING_PROMPT`` lives
# further down; registering here keeps the single authoritative location
# for the dict while respecting definition order. The deprecated
# ``instruction_to_sql_expression`` key is retained for one release so
# MLflow prompt-registry history stays linkable.
LEVER_PROMPTS["prose_rule_mining"] = PROSE_RULE_MINING_PROMPT
LEVER_PROMPTS["instruction_to_sql_expression"] = PROSE_RULE_MINING_PROMPT

# ── 25. SQL Expression Seeding (Proactive, Lever 0) ───────────────────

SQL_EXPRESSION_MIN_FREQUENCY = 2
"""Minimum benchmark occurrences for a pattern to become a candidate."""

SQL_EXPRESSION_SEEDING_MAX_CANDIDATES = 60
"""Per-run mining budget. Caps warehouse EXPLAIN+execute cost for the
validation loop. Seeding also respects the remaining per-space SQL-snippet
headroom (``MAX_SQL_SNIPPETS`` minus existing count minus
``SQL_EXPRESSION_SEEDING_LEVER_RESERVE``), whichever is smaller.
"""

SQL_EXPRESSION_SEEDING_LEVER_RESERVE = 50
"""Slots in the per-space ``MAX_SQL_SNIPPETS`` (200) budget reserved for
the lever loop's iterative additions. Proactive seeding stops contributing
once ``existing_sql_snippets + LEVER_RESERVE >= MAX_SQL_SNIPPETS``, leaving
room for lever-loop proposals later in the optimisation run.

Tune from observed production snippet-count growth distributions. A value
of 50 reserves 25% of the 200-snippet budget for the lever loop, which is
consistent with today's typical lever-loop snippet growth rate.

NOTE (budget-model mismatch, deferred):
    The Databricks knowledge-store docs
    (https://docs.databricks.com/aws/en/genie/knowledge-store) state the
    200-snippet limit combines table descriptions + join specs + SQL
    expressions. Our internal code counts only SQL-snippet buckets. Joins
    and table descriptions are effectively unbudgeted today. A separate
    follow-up issue should reconcile ``_strict_validate``,
    ``count_instruction_slots``, ``count_sql_snippets``, and any seeding
    gates against the docs' combined budget.
"""

SQL_EXPRESSION_SEEDING_THRESHOLD = 5
"""DEPRECATED: retained only so pre-existing callers and historical Delta
rows keep deserialising. The seeding gate is now headroom-based; see
``SQL_EXPRESSION_SEEDING_LEVER_RESERVE``.
"""

_SQL_EXPRESSION_SEEDING_BODY = """Given these SQL patterns extracted from \
proven benchmark queries for a Genie Space, generate business-friendly \
metadata for each:

{{ candidates }}

Schema context:
{{ schema }}

For each candidate, provide:
- display_name: A concise business-friendly name. MUST be specific \
enough to disambiguate the source table or business domain inside \
this space. When the SQL references a domain-specific table such as \
``mv_<domain>_fact_<entity>`` or ``mv_<domain>_dim_<entity>`` (for \
example ``mv_orders_fact_lines`` or ``mv_claims_dim_date``), prefix \
the name with the compact qualifier (e.g. \
``ORDERS Month-to-Date Filter``, ``CLAIMS Total Premium Amount``). \
Avoid generic names like ``Month-to-Date Filter`` or \
``Total Revenue`` when multiple fact tables or metric views in this \
space could plausibly share that concept.
- synonyms: 2-3 alternative terms users might use.
- instruction: One sentence on when Genie should use this expression. \
MUST mention the source table or business domain so Genie can pick \
the correct snippet when several tables expose similar concepts.
- alias: A snake_case identifier (for measures and expressions only).

Output strict JSON array matching the input order. Each element:
{{"display_name": "...", "synonyms": [...], "instruction": "...", "alias": "..."}}
"""

SQL_EXPRESSION_SEEDING_PROMPT = _RCA_CONTRACT_HEADER + _SQL_EXPRESSION_SEEDING_BODY

# ── 26. Pre-flight example_sql synthesis (Bug #4 follow-up; schema-driven) ──
#
# Proactive, leak-free "knowledge booster" that generates validated
# question→SQL pairs and applies them as ``instructions.example_question_sqls``
# until the target threshold is reached. Distinct from the reactive
# AFS-driven path in ``optimization/synthesis.py`` — this fires in
# pre-flight from *schema alone* with no failure cluster.
#
# Firewall invariants enforced by code:
#   1. The generator prompt has no benchmark variables (structural).
#   2. The orchestrator's context builder takes no ``benchmarks`` parameter
#      (code review + unit test).
#   3. Every candidate is fingerprint-checked against ``BenchmarkCorpus``
#      before persist (runtime).
#
# Example_question_sqls do NOT count toward the Genie 200-snippet limit
# (per docs/gsl-instruction-schema.md + public Databricks docs).

ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS = os.getenv(
    "GENIE_SPACE_OPTIMIZER_ENABLE_PREFLIGHT_EXAMPLE_SQL", "true",
).lower() in ("true", "1", "yes")
"""Feature kill switch. Default ON for all spaces; set to ``false`` to skip."""

PREFLIGHT_EXAMPLE_SQL_TARGET = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_EXAMPLE_SQL_TARGET", "20") or "20"
)
"""Target count of ``example_question_sqls`` after pre-flight synthesis.

Stage gates on ``need = max(0, TARGET - existing)`` — never produces more
than required, idempotent across re-runs. 20 is a conservative upper
cap; does not affect Genie's 200-snippet limit (example SQLs don't count).
"""

EXAMPLE_SQL_INITIAL_OVERDRAW = float(
    os.getenv("GSO_EXAMPLE_SQL_INITIAL_OVERDRAW", "3.0") or "3.0"
)
"""Multiplier over ``PREFLIGHT_EXAMPLE_SQL_TARGET`` for the unified
example-SQL generator's upfront candidate reservoir.

This is intentionally separate from the final installed target. With the
default target of 20 and overdraw of 3.0, the generator asks for about 60
raw candidates across diversified LLM calls, validates them once, then
selects at most 20 for persistence.
"""

EXAMPLE_SQL_GENERATION_CALLS = int(
    os.getenv("GSO_EXAMPLE_SQL_GENERATION_CALLS", "3") or "3"
)
"""Number of independent diversified LLM calls made by unified example-SQL
generation before final validation/selection.

Clamped by the generator to the supported profile range. Defaults to 3 so
the target=20, overdraw=3.0 path requests roughly 20 candidates per call.
"""

EXAMPLE_SQL_FIREWALL_STRICT = os.environ.get(
    "GSO_EXAMPLE_SQL_FIREWALL_STRICT", "true",
).lower() in {"1", "true", "yes", "on"}
"""When True (default), the example-SQL leakage firewall blocks any
candidate whose SQL fingerprint or n-gram overlap matches a benchmark.
Set GSO_EXAMPLE_SQL_FIREWALL_STRICT=false to fall back to the
warn-only relaxed policy. Strict mode is a methodological guard
against benchmark leakage; regression prevention lives in the
pre-promotion smoke test (see GSO_EXAMPLE_SQL_SMOKE_TEST)."""

EXAMPLE_SQL_TEACHING_SAFETY_ENABLED = os.environ.get(
    "GSO_EXAMPLE_SQL_TEACHING_SAFETY", "true",
).lower() in {"1", "true", "yes", "on"}
"""When True (default), every example-SQL candidate that survived the
correctness arbiter is run through the teaching-safety judge before
the apply step. Disable only for debugging — turning it off restores
pre-tightening behaviour, which is what caused the regressions
documented in 2026-04-30-enrichment-validation-tightening-plan.md."""


EXAMPLE_SQL_SMOKE_TEST_ENABLED = os.environ.get(
    "GSO_EXAMPLE_SQL_SMOKE_TEST", "true",
).lower() in {"1", "true", "yes", "on"}
"""When True (default), every batch of accepted example SQLs goes
through a pre-promotion smoke test against baseline ``both_correct``
questions before the actual apply. The whole batch is rejected if any
baseline-correct question regresses by more than
``GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP`` percentage points.
Disable only for debugging."""


EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP = float(
    os.environ.get("GSO_EXAMPLE_SQL_SMOKE_REGRESSION_TOLERANCE_PP", "0.0")
)
"""Tolerance (in percentage points) for the smoke test. ``0.0`` means
any regression on baseline both_correct questions rejects the patch.
Raise carefully — non-zero values knowingly accept the risk that
enrichment can degrade known-good behaviour."""


EXAMPLE_SQL_SMOKE_MAX_QUESTIONS = int(
    os.environ.get("GSO_EXAMPLE_SQL_SMOKE_MAX_QUESTIONS", "20")
)
"""Cap on the number of baseline both_correct questions sent to the
smoke test, to keep wall time bounded. Sampled randomly with a fixed
seed when the baseline pool is larger than this cap."""


EXAMPLE_SQL_TEACHING_SAFETY_PROMPT = (
    "You are a senior data engineer auditing example SQL pairs that "
    "will be permanently installed as teaching context inside a "
    "Databricks Genie space. The arbiter has already verified the "
    "SQL is self-consistent (it answers its own question on its own "
    "data). Your job is different: judge whether installing this "
    "example would BIAS Genie's future answers in a HARMFUL way.\n\n"
    "REJECT (verdict ``no``) when ANY of these apply:\n"
    "- KPI over-teaching: the SQL hard-codes one metric variant when "
    "  multiple equally valid forms exist (will bias Genie toward "
    "  this one form on similar questions).\n"
    "- Grain mismatch: question grain (monthly/daily/yearly) does "
    "  not match SQL grain.\n"
    "- Routing bias: the SQL uses an asset that is plausible but "
    "  NOT the most canonical for the question (e.g. counting via a "
    "  metric view when a dim table is the canonical source).\n"
    "- Surprising defaults: ORDER BY/LIMIT/HAVING/CASE that the "
    "  question did not ask for and that future variants will not "
    "  want.\n"
    "- Over-specification: extra columns or filters not asked for.\n"
    "- Wrong column choice: uses an obscure or DQ-suffix column "
    "  (``*_combination``, ``*_v2``, ``*_legacy``) when a plain "
    "  column exists.\n\n"
    "ACCEPT (verdict ``yes``) only when the example is canonical, "
    "minimal, schema-safe, and grain-correct.\n\n"
    "Use ``uncertain`` when the schema context is too thin to judge.\n\n"
    "OUTPUT FORMAT (strict JSON, no prose):\n"
    '{"value": "yes" | "no" | "uncertain", '
    '"rationale": "<one sentence>"}'
)

PREFLIGHT_EXAMPLE_SQL_PER_ARCHETYPE = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_PER_ARCHETYPE", "2") or "2"
)
"""Upper bound on candidates generated per archetype per run — prevents
any one query shape from dominating the applied pool."""

PREFLIGHT_EXAMPLE_SQL_OVERDRAW = float(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_OVERDRAW", "1.5") or "1.5"
)
"""Multiplier over ``need`` to absorb gate rejections. With default 1.5 and
target 20 on an empty space, the planner emits 30 candidate plans and we
apply the first 20 that pass every gate."""

PREFLIGHT_COLUMN_COVERAGE_K = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_COLUMN_K", "5") or "5"
)
"""Top-K columns per asset included in the narrowed identifier allowlist
for synthesis. Small K keeps the LLM focused and raises EXPLAIN pass rate."""

PREFLIGHT_PROFILE_VALUES_CAP = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_PROFILE_VALUES_CAP", "10") or "10"
)
"""Maximum number of distinct values rendered for any one column in the
``## Column value profile`` section of the pre-flight synthesis prompt.
Columns above this cap show ``+N more``; very high-cardinality columns
(e.g. ``user_id`` with 10k distinct values) render only the cardinality."""

PREFLIGHT_PROFILE_VALUE_LEN_CAP = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PREFLIGHT_PROFILE_VALUE_LEN_CAP", "60")
    or "60"
)
"""Maximum characters any single profile value string is rendered with.
Longer values are truncated with ``…`` so one pathological row cannot
blow up the prompt budget on its own."""

_PREFLIGHT_EXAMPLE_SYNTHESIS_BODY = """\
<role>
You are generating ONE high-quality question + SQL pair to teach a \
Databricks Genie Space. Your output will be stored as an example so \
Genie can learn the query shape for similar user questions.
</role>

<context>
## Coverage focus (this example MUST reference these assets)
Tables:
{{ slice_tables }}

Metric views:
{{ slice_metric_views }}

Join spec to exercise:
{{ slice_join_spec }}

Columns to prioritize:
{{ slice_columns }}

## Column value profile (use ONLY these values when building filters)
{{ slice_data_profile }}

## Constraint: identifier qualification (HARD)
Every table reference in FROM, JOIN, and column-qualifier position MUST \
be the EXACT identifier shown in the ``## Schema`` allowlist below. \
Never a short name, never an inferred name, never a benchmark-style \
alias you haven't declared for THIS query.

Worked example (identifier = ``{{ schema_example_identifier }}``):
- BAD   SELECT d.day_of_week FROM dim_date d
- BAD   SELECT mv_<domain>_dim_date.day_of_week FROM mv_<domain>_dim_date
- GOOD  SELECT t.day_of_week
        FROM {{ schema_example_identifier }} t

SQL aliases (``t``, ``f``, etc.) are allowed ONLY when declared in \
THIS query's FROM clause. Never carry an alias over from another \
query, a benchmark example, or an archetype snippet.

## Constraint: filter values
When writing filter predicates, quote values EXACTLY from the value \
profile above. Do not invent values. For numeric columns, use values \
within the stated range. When filter values are not in the profile \
(high-cardinality columns), omit the filter instead of guessing.

{{ metric_view_contract }}
## Archetype
Name: {{ archetype_name }}
Shape guidance: {{ archetype_prompt_template }}
Output contract: {{ archetype_output_shape }}

## Schema (identifier allowlist — ONLY these identifiers may appear)
{{ identifier_allowlist }}

## Existing questions in this space (avoid duplicating intent)
{{ existing_questions_list }}

{{ retry_feedback }}
</context>

<instructions>
Produce ONE example. Rules:

- ``example_question`` is a clean, customer-style business question.
- ``example_sql`` is a valid Databricks SQL query. Identifier \
qualification is enforced in ``## Constraint: identifier qualification`` \
above — obey that section exactly.
- Match the archetype's shape contract.
- The question MUST reference the coverage focus naturally — use the \
listed assets and columns.
- Do NOT duplicate the intent of any existing question.
- NEVER quote benchmark questions, evaluation prompts, or test queries.
- Do NOT invent columns, tables, or relationships that are not in the \
allowlist — any unknown identifier is a hallucination.
</instructions>

<output_schema>
Respond with a SINGLE JSON object, no prose, no code fences:
{{"example_question": "...", "example_sql": "...", "rationale": "..."}}
</output_schema>"""

PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT = (
    _EXAMPLE_SYNTHESIS_CONTRACT_HEADER + _PREFLIGHT_EXAMPLE_SYNTHESIS_BODY
)

LEVER_PROMPTS["preflight_example_synthesis"] = PREFLIGHT_EXAMPLE_SYNTHESIS_PROMPT


# ── 27. Cluster-driven example_sql synthesis (Bug #4 Phase 3 — reactive) ──
#
# Reactive counterpart to pre-flight. Triggered from within the lever loop
# when the strategist emits Lever 5 ``example_sqls`` for an action group.
# Replaces the historical "verbatim-from-strategist" path at
# ``optimizer.py:9597`` with the AFS-gated pre-flight synthesis engine.
#
# Three knobs:
#   - ENABLE_CLUSTER_DRIVEN_SYNTHESIS: feature flag. Default ON; setting
#     to false reverts to the legacy strategist-verbatim path (emergency
#     rollback only). Kept as a kill switch until the new path has
#     accumulated observability across multiple production runs.
#   - CLUSTER_SYNTHESIS_PER_ITERATION: hard cap on synthesis attempts
#     per lever-loop iteration. Shared counter lives in
#     ``metadata_snapshot['_cluster_synthesis_count']`` and is reset by
#     the lever loop at the top of each iteration.
#   - EXAMPLE_QUESTION_SQLS_SAFETY_CAP: ceiling on
#     ``instructions.example_question_sqls`` size. When reached, cluster-
#     driven synthesis refuses to add more; Lever 5 falls back to
#     ``instruction_only_fallback``. Pre-flight's 20-target is enforced
#     upstream independently and cannot exceed 20 by construction.

ENABLE_CLUSTER_DRIVEN_SYNTHESIS = os.getenv(
    "GENIE_SPACE_OPTIMIZER_ENABLE_CLUSTER_DRIVEN_SYNTHESIS", "true",
).lower() in ("true", "1", "yes")
"""Feature flag for the cluster-driven synthesis path. Default ON for
every space; set to ``false`` for emergency rollback to the legacy
Lever 5 free-form example_sql path."""

CLUSTER_SYNTHESIS_PER_ITERATION = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_CLUSTER_SYNTHESIS_PER_ITERATION", "3") or "3"
)
"""Upper bound on synthesis attempts per lever-loop iteration across all
action groups. Prevents runaway cost when failures are dense. 3 is the
default; tune via env var."""

EXAMPLE_QUESTION_SQLS_SAFETY_CAP = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_EXAMPLE_QUESTION_SQLS_SAFETY_CAP", "50") or "50"
)
"""Absolute cap on ``instructions.example_question_sqls`` size before
cluster-driven synthesis refuses to add more. Pre-flight's own 20-target
is orthogonal — enforced upstream by ``PREFLIGHT_EXAMPLE_SQL_TARGET`` and
cannot independently exceed 20. This cap is checked at the entry of
``run_cluster_driven_synthesis_for_single_cluster``. Env var matches
constant name for grep-ability."""


# ──────────────────────────────────────────────────────────────────────
# Optimizer Control-Plane Hardening Plan — Task 0 feature flags.
#
# Defaults flipped to ON for cycle-9 deploy: each helper returns True
# unless the env-var is explicitly set to a falsy value
# (``0``/``false``/``no``/``off``). Set ``GSO_<name>=0`` to disable.
# ──────────────────────────────────────────────────────────────────────


_TRUTHY_VALUES = {"1", "true", "yes", "on"}
_FALSY_VALUES = {"0", "false", "no", "off"}


def _flag_enabled(env_name: str) -> bool:
    raw = (os.environ.get(env_name) or "").strip().lower()
    return raw in _TRUTHY_VALUES


def _flag_default_on(env_name: str) -> bool:
    raw = (os.environ.get(env_name) or "").strip().lower()
    if raw in _FALSY_VALUES:
        return False
    return True


def target_aware_acceptance_enabled() -> bool:
    """Task A — when below thresholds, the
    ``accepted_with_attribution_drift`` branch in
    ``decide_control_plane_acceptance`` rejects instead of accepting.

    Default ON. Set ``GSO_TARGET_AWARE_ACCEPTANCE=0`` to disable.
    """
    return _flag_default_on("GSO_TARGET_AWARE_ACCEPTANCE")


def no_causal_applyable_halt_enabled() -> bool:
    """Task B — when every RCA-grounded proposal in an AG is dropped
    by upstream gates, halt the AG with reason
    ``no_causal_applyable_patch`` instead of falling back to non-causal
    proposals.

    Default ON. Set ``GSO_NO_CAUSAL_APPLYABLE_HALT=0`` to disable.
    """
    return _flag_default_on("GSO_NO_CAUSAL_APPLYABLE_HALT")


def bucket_driven_ag_selection_enabled() -> bool:
    """Task C — strategist consumes prior-iteration failure buckets:
    ``MODEL_CEILING`` qids drop from targets; clusters whose qids are
    all ``EVIDENCE_GAP`` materialize as evidence-gathering AGs.

    Default ON. Set ``GSO_BUCKET_DRIVEN_AG_SELECTION=0`` to disable.
    """
    return _flag_default_on("GSO_BUCKET_DRIVEN_AG_SELECTION")


def rca_aware_patch_cap_enabled() -> bool:
    """Task D — proposals inherit the parent AG's ``rca_id`` at the F5
    stage entry so ``select_causal_patch_cap`` can rank by
    ``causal_attribution_tier``.

    Default ON. Set ``GSO_RCA_AWARE_PATCH_CAP=0`` to disable.
    """
    return _flag_default_on("GSO_RCA_AWARE_PATCH_CAP")


def lever_aware_blast_radius_enabled() -> bool:
    """Task E — non-semantic patch types
    (``update_column_description``, ``add_column_synonym``,
    ``add_metric_view_instruction``, ``add_table_instruction``,
    ``update_table_description``) downgrade ``high_collateral_risk``
    blast-radius rejection to a warning.

    Default ON. Set ``GSO_LEVER_AWARE_BLAST_RADIUS=0`` to disable.
    """
    return _flag_default_on("GSO_LEVER_AWARE_BLAST_RADIUS")


# ──────────────────────────────────────────────────────────────────────
# Cycle 2 Optimizer Improvement Plan — proposal-survival and
# safety-gate hardening flags. All default-on; set
# ``GSO_<name>=0`` to disable.
# ──────────────────────────────────────────────────────────────────────


def intra_ag_proposal_dedup_enabled() -> bool:
    """Cycle 2 Task 1 — when on, the gates pipeline runs an intra-AG
    body-fingerprint dedup pass before blast-radius. Two proposals in
    the same AG with identical body text but different ``patch_type``
    values collapse to the first occurrence.

    Default ON. Set ``GSO_INTRA_AG_PROPOSAL_DEDUP=0`` to disable.
    """
    return _flag_default_on("GSO_INTRA_AG_PROPOSAL_DEDUP")


def shared_cause_blast_radius_enabled() -> bool:
    """Cycle 2 Task 2 — when on, ``patch_blast_radius_is_safe``
    downgrades ``high_collateral_risk_flagged`` to
    ``shared_cause_collateral_warning`` when every outside-target
    dependent is itself currently-hard. Two hard failures sharing a
    cause should not block each other's fix.

    Default ON. Set ``GSO_SHARED_CAUSE_BLAST_RADIUS=0`` to disable.
    """
    return _flag_default_on("GSO_SHARED_CAUSE_BLAST_RADIUS")


def doa_selected_proposal_signature_enabled() -> bool:
    """Cycle 2 Task 3 — when on, the DOA ledger records and dedups by
    selected-proposal-ID signatures (in addition to the applied-patch
    signatures it already records). Closes the iter-3/iter-5 same-AG
    replay loop where blast-radius drops every patch leaving an empty
    applied-patch signature.

    Default ON. Set ``GSO_DOA_SELECTED_PROPOSAL_SIGNATURE=0`` to
    disable.
    """
    return _flag_default_on("GSO_DOA_SELECTED_PROPOSAL_SIGNATURE")


def question_shape_lever_preference_enabled() -> bool:
    """Cycle 2 Task 4 — when on, single-question clusters whose
    root_cause is in the question-shape set
    (``plural_top_n_collapse``, ``count_vs_distinct``, etc.) prefer
    per-question levers (3 example_sql, 5 instructions) over the
    space-wide lever 6 (SQL expressions).

    Default ON. Set ``GSO_QUESTION_SHAPE_LEVER_PREFERENCE=0`` to
    disable.
    """
    return _flag_default_on("GSO_QUESTION_SHAPE_LEVER_PREFERENCE")
