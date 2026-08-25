from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ===== GenieIQ Models =====

class MaturityLevel(str, Enum):
    """Maturity level for a Genie Agent (3-tier)."""
    NOT_READY = "Not Ready"
    READY_TO_OPTIMIZE = "Ready to Optimize"
    TRUSTED = "Trusted"


class CheckDetail(BaseModel):
    """A single scoring check result."""
    label: str
    passed: bool
    detail: str | None = None       # Human-readable context (e.g., "3/8 tables (38%)")
    severity: Literal["pass", "warning", "fail"] | None = None


class ScanResult(BaseModel):
    """IQ scan result for a Genie Agent."""
    space_id: str
    score: int = Field(..., ge=0, le=12)
    total: int = 12
    maturity: MaturityLevel
    optimization_accuracy: float | None = None  # 0.0-1.0, None if never optimized
    checks: list[CheckDetail] = Field(default_factory=list)
    findings: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)               # Advisory findings from warning-severity checks
    warning_next_steps: list[str] = Field(default_factory=list)     # Paired with warnings
    scanned_at: str  # ISO datetime string


class SpaceListItem(BaseModel):
    """Summary item for the space list."""
    space_id: str
    display_name: str
    score: int | None = None
    maturity: str | None = None
    optimization_accuracy: float | None = None  # 0.0-1.0, None if never optimized
    is_starred: bool = False
    last_scanned: str | None = None  # ISO datetime
    space_url: str | None = None


class SpaceScanRequest(BaseModel):
    """Request to trigger an IQ scan."""
    space_id: str = Field(..., min_length=1, max_length=64)


class StarToggleRequest(BaseModel):
    """Request to toggle star on a space."""
    starred: bool


class AdminDashboardStats(BaseModel):
    """Org-wide statistics for the admin dashboard."""
    total_spaces: int
    scanned_spaces: int
    avg_score: float
    critical_count: int  # score <= 20
    maturity_distribution: dict[str, int]


class LeaderboardEntry(BaseModel):
    """Entry in the leaderboard."""
    space_id: str
    display_name: str
    score: int
    maturity: str
    last_scanned: str | None = None


class AlertItem(BaseModel):
    """Alert for a space with critical issues."""
    space_id: str
    display_name: str
    score: int
    top_finding: str | None = None


# ===== Create Wizard Models =====

class CreateSpaceRequest(BaseModel):
    """Request body for the Create Space Wizard endpoint."""
    display_name: str = Field(..., min_length=1, max_length=255)
    serialized_space: dict
    parent_path: str | None = Field(None, max_length=1000)


class CreateSpaceResponse(BaseModel):
    """Response from the Create Space Wizard endpoint."""
    space_id: str
    display_name: str
    space_url: str


class LLMModelInfo(BaseModel):
    """Selectable chat model served by Databricks Model Serving."""
    name: str
    displayName: str
    isDefault: bool = False
    optimizerPromptBudgetChars: int | None = None
    contextTier: Literal["standard", "long"] | None = None


# ── Auto-Optimize preflight permissions ──────────────────────────────────
# Mirrored on the frontend as `GSOPermissionCheck` in `frontend/src/types/index.ts`.
# Both halves must stay in sync — update together (see AGENTS.md §Models).


class SchemaAccessStatus(BaseModel):
    """Per-schema access summary returned by the Auto-Optimize preflight.

    Emitted once per UC schema the job SP would read from. ``grant_sql`` is
    populated when ``read_granted`` is ``False`` so the UI can show a
    one-click remediation hint."""

    catalog: str
    schema_name: str
    read_granted: bool
    grant_sql: str | None = None


class QueryHistoryWarehouseStatus(BaseModel):
    warehouse_id: str
    name: str
    accessible: bool = False


class QueryUsageSignal(BaseModel):
    status: Literal[
        "system_table_available",
        "warehouse_api_available",
        "partially_available",
        "unavailable",
    ] = "unavailable"
    system_table_available: bool = False
    warehouse_api_available: bool = False
    warehouses: list[QueryHistoryWarehouseStatus] = Field(default_factory=list)
    inaccessible_warehouses: list[str] = Field(default_factory=list)
    system_grant_sql: str | None = None


class PermissionCheckResponse(BaseModel):
    """Payload for ``GET /auto-optimize/permissions``.

    Shape contract for the Auto-Optimize permissions preflight. The UI's
    PermissionAlert consumes the SP and schema access fields to show
    grant-based remediation."""

    sp_display_name: str
    sp_application_id: str = ""
    sp_has_manage: bool
    schemas: list[SchemaAccessStatus]
    can_start: bool
    errors: list[str] = []
    query_usage_signal: QueryUsageSignal | None = None


# ── Metric view entitlement probe (POV §7.3.1 / MV-D8) ───────────────────
# Mirrored on the frontend as `MvProbeResult` in `frontend/src/types/index.ts`.
# Both halves must stay in sync — update together (see AGENTS.md §Models).


MvCheckStatus = Literal["GRANTED", "DENIED", "UNKNOWN"]


class MvPrivilegeRow(BaseModel):
    """One Unity Catalog privilege the signed-in user must hold to create a view.

    ``label`` is the POV §7.3.1 wording (``"CREATE TABLE on finance.sales"``) and
    is the key used in ``MvProbeResult.results``."""

    label: str
    privilege: str
    securable: str
    status: MvCheckStatus
    detail: str | None = None


class MvCapabilityRow(BaseModel):
    """One runtime capability the generated YAML depends on (MV-D8).

    ``required_dbr`` is a Databricks Runtime floor. ``UNKNOWN`` is the honest
    answer on a SQL warehouse, which reports only a DBSQL version — the
    generator then withholds the optional feature, and the create floor is left
    to fail closed at write time rather than blocking an entitled user (MV-D13).

    ``observed_warehouse_id`` records which compute the row was read on, because
    a capability belongs to the compute rather than to the user: re-verifying on
    a different warehouse invalidates the row instead of inheriting it."""

    capability: str
    label: str
    required_dbr: str
    observed_version: str | None = None
    runtime_kind: Literal["DBR", "DBSQL", "UNAVAILABLE"] = "UNAVAILABLE"
    observed_warehouse_id: str | None = None
    status: MvCheckStatus
    optional: bool = False
    detail: str | None = None


class MvProbeResult(BaseModel):
    """Payload for ``POST /auto-optimize/mv/probe``.

    Shape follows POV §7.3.1 verbatim (``results`` is the flat
    check-label-to-status map that section prints) and adds the typed
    ``privileges`` / ``capabilities`` rows the UI renders. ``capabilities`` rows
    also satisfy the ``CapabilityRow`` Protocol that the engine's
    ``mv_yaml.validate`` reads them through — structurally, since the engine does
    not import this module; keep the three fields it names. Never carries a token
    or an SP identity: every check runs under the signed-in user's OBO client."""

    probe_id: str
    checked_as: str
    auth_identity: Literal["OBO"] = "OBO"
    target: str
    checked_at: str
    results: dict[str, MvCheckStatus] = Field(default_factory=dict)
    privileges: list[MvPrivilegeRow] = Field(default_factory=list)
    capabilities: list[MvCapabilityRow] = Field(default_factory=list)
    verdict: Literal["SUFFICIENT", "INSUFFICIENT", "UNKNOWN"]
    missing: list[str] = Field(default_factory=list)
    remediation_sql: str | None = None
    fallback_mode: Literal["suggest_only"] = "suggest_only"
    materialize_consented: bool = False
    consent_recorded: bool = False
    errors: list[str] = Field(default_factory=list)


class MvConsentVerification(BaseModel):
    """Result of re-verifying a recorded consent against a fresh probe.

    Downgrades only: ``create_and_attach`` survives exactly when the fresh probe
    still says SUFFICIENT for the same identity and the same target."""

    probe_id: str
    effective_mode: Literal["create_and_attach", "suggest_only"]
    verdict: Literal["SUFFICIENT", "INSUFFICIENT", "UNKNOWN"]
    downgrade_reason: str | None = None
    fresh_probe: MvProbeResult


# ── Metric view proposals / create-and-attach (Prompt 9, MV-D1/D21/D22) ──
# TS mirror (Prompt 11): the run-config panel consumes MvConsentPayload,
# MvProposal, MvProposalsResponse, and MvSpaceProposalsResponse, all mirrored in
# `frontend/src/types/index.ts` per AGENTS.md §Models. MvDdlArtifact,
# MvProposalDecisionRequest/Response, MvDropRequest/Response, and MvCreatedObject
# still have no frontend consumer — they belong to the Prompt 13 output screen;
# mirror them there rather than adding dead TS surface before there is a caller.


class MvConsentPayload(BaseModel):
    """The scoped, recorded authorization carried on a ``create_and_attach`` run.

    ``probe_id`` keys ``genie_opt_mv_consents`` (MV-D16 — there is no
    ``consent_id`` column); ``granted_by`` / ``granted_at`` are the audit pair.
    Re-verified under OBO at trigger time before any write (MV-D1)."""

    granted_by: str
    granted_at: str
    probe_id: str


class MvProposal(BaseModel):
    """One advisor proposal (a ``genie_opt_mv_candidates`` row) as the UI reads it.

    JSON columns arrive decoded to their POV Part 4 field names. ``confidence_score``
    is 0–100; ``approved_for_rerun`` gates ``create_and_attach`` (MV-D1)."""

    suggestion_id: str
    dedup_fingerprint: str
    target_space_id: str
    run_id: str | None = None
    candidate_type: str
    confidence_score: float | None = None
    tier: str | None = None
    proposed_object: str | None = None
    score_components: dict[str, Any] | None = None
    evidence: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None
    alternatives: list[Any] | None = None
    conflicts: list[Any] | None = None
    requested_mode: str | None = None
    effective_mode: str | None = None
    decision: str | None = None
    decided_by: str | None = None
    decided_at: str | None = None
    suppressed_until: str | None = None
    approved_for_rerun: bool = False
    created_at: str | None = None
    updated_at: str | None = None


class MvProposalsResponse(BaseModel):
    """``GET /runs/{run_id}/mv-proposals`` — the run's proposals, newest first."""

    run_id: str
    proposals: list[MvProposal] = Field(default_factory=list)


class MvSpaceProposalsResponse(BaseModel):
    """``GET /spaces/{space_id}/mv-proposals`` — a space's proposals (MV-D23).

    Space-scoped twin of ``MvProposalsResponse`` for the run-config panel's
    re-run gate, which asks a space-scoped question ("what has this Agent had
    approved?") and must not borrow a prior ``run_id`` to stand in for it. The
    element type is the SAME ``MvProposal`` as the run-keyed response, so the
    proposal card renders from one shape in both the output screen (per-run) and
    this panel (per-space)."""

    space_id: str
    proposals: list[MvProposal] = Field(default_factory=list)


class MvSuggestResponse(BaseModel):
    """``POST /spaces/{space_id}/mv/suggest`` — an on-demand advice run (MV-D23).

    The IQ Scan surface asks the advisor to score a space right now, with no
    optimization run. The backend writes a born-terminal sentinel advice run,
    runs the SparkSession-free advisor over the space's curated corpus, and
    returns its outcome plus the persisted proposals.

    ``status`` is the advisor's own ``COMPLETE`` | ``SKIPPED`` | ``FAILED``;
    ``skip_reason`` distinguishes the honest empties (no parseable SQL, no
    candidates, an estate that already governs every measure) from a failure, so
    the panel can render *found* vs *EMPTY* vs *denial* without inferring intent
    from an empty list. ``proposals`` is the SAME ``MvProposal`` shape the
    space-scoped and run-keyed lists return, so ``MvSuggestOnlyPanel`` mounts
    these cards from this space-scoped source with no component change."""

    space_id: str
    run_id: str
    status: str
    skip_reason: str | None = None
    error: str | None = None
    proposals: list[MvProposal] = Field(default_factory=list)


class MvRegisterRequest(BaseModel):
    """``POST /spaces/{space_id}/mv/register`` — a bring-your-own view (MV-D24).

    ``full_name`` is the three-part UC identifier of a metric view the user
    created themselves. ``suggestion_id`` is optional: when the user claims the
    view implements a specific proposal, the backend checks the claim by
    comparing dedup fingerprints rather than trusting it."""

    full_name: str
    suggestion_id: str | None = None


class MvRegisterResponse(BaseModel):
    """Result of a bring-your-own registration (MV-D24).

    A single shape carries both the verified and refused states the panel
    renders: ``registered`` is the verdict and ``reason`` explains a refusal
    (not a metric view, not visible, failed validation, claim mismatch). On
    success ``run_id`` is the sentinel advice run hosting the ``USER_CREATED``
    ledger row and ``warnings`` are advisory lints that did not block."""

    registered: bool
    full_name: str
    provenance: str = "USER_CREATED"
    run_id: str | None = None
    suggestion_id: str | None = None
    reason: str | None = None
    warnings: list[str] = Field(default_factory=list)


class MvSemanticGraphNode(BaseModel):
    """One node in a space's semantic model graph (Prompt 12, MV-D23).

    ``kind`` is ``table`` | ``metric_view`` | ``measure``. Layout is a
    deterministic layered layout: ``col`` 0 = source/fact tables, 1 = joined
    dimension tables, 2 = metric views, 3 = measure concepts; ``row`` orders
    within a column. ``governance`` (measure concepts only) is the ladder rung —
    ``governed`` (a measure exposed by an attached metric view) / ``curated``
    (``instructions.sql_snippets.measures`` — structured name+expr, no parsing) /
    ``ungoverned`` (recurs only in proposal evidence). ``origin`` is the chip's
    non-color discriminator. ``proposed`` marks the ghosted overlay MV (the
    overlay itself is synthesized client-side; the server never emits it).

    ``coverage`` (Prompt 12b, SQL-coverage lens) is the number of curated SQL
    statements (``example_question_sqls``, plus curated ``sql_snippets.measures``)
    that touch this table or measure concept — an ADDITIVE field a Prompt 12
    client that never learned the lens simply ignores. ``0`` is a legible cold
    spot (a table no curated SQL exercises), not an error; ``None`` means the
    lens did not run for this node kind. ``benchmark_question_ids`` (evidence
    lens) carries the ``evidence.benchmark_question_ids`` of the proposal backing
    an ungoverned concept, so the question→measure evidence is a first-class,
    Prompt-14-classifiable field rather than only reachable through the card."""

    id: str
    kind: Literal["table", "metric_view", "measure"]
    label: str
    col: int
    row: int
    governance: Literal["governed", "curated", "ungoverned"] | None = None
    origin: str | None = None
    proposed: bool = False
    coverage: int | None = None
    benchmark_question_ids: list[str] | None = None


class MvSemanticGraphEdge(BaseModel):
    """One edge in the semantic model graph.

    ``join`` edges come from ``instructions.join_specs`` — ``on`` is the
    predicate (``sql[0]``), ``relationship`` is decoded from the ``--rt=…--``
    annotation (``sql[1]``), and ``scd2`` is true when the predicate carries an
    ``is_current`` guard. ``membership`` links a measure concept to its owning
    metric view. ``replaces`` is the overlay's dashed edge — client-only.

    ``weight`` (Prompt 12b, SQL-coverage lens) is the number of curated SQL
    statements that traverse a ``join`` edge (touch both endpoints) — an ADDITIVE
    field older clients ignore; ``None`` means the lens did not weight this edge
    kind."""

    from_: str = Field(..., alias="from")
    to: str
    kind: Literal["join", "membership", "replaces"]
    on: str | None = None
    relationship: str | None = None
    scd2: bool = False
    weight: int | None = None

    model_config = {"populate_by_name": True}


class MvSemanticGraph(BaseModel):
    """``GET /spaces/{space_id}/semantic-graph`` — a space's semantic model.

    Space-scoped (MV-D23: ``run_id`` presentational only). ``nodes``/``edges``
    are assembled LIVE from ``serialized_space`` (``data_sources``,
    ``instructions.join_specs``, ``instructions.sql_snippets.measures``) plus the
    Prompt 11 space-scoped proposals read — the same live, never-cached read
    ``/space/fetch`` performs, so the graph reflects what the signed-in user is
    entitled to see. ``proposals`` carries the SAME ``MvProposal`` shape the
    cards use so the client can synthesize the ghosted overlay with no new
    proposal payload.

    ``coverage_status`` / ``coverage_reason`` (Prompt 12b, SQL-coverage lens)
    report the lens outcome in the MV-D15 vocabulary — ``COMPUTED`` when curated
    SQL parsed and coverage counts are meaningful, ``EMPTY`` when the space has no
    curated SQL (the frame-7b honesty rule: show no coverage and say so), and
    ``UNAVAILABLE`` with a named ``coverage_reason`` when parsing degraded (never a
    500, never a silently-zero coverage). Both are ADDITIVE: a Prompt 12 client
    that never learned the lens ignores them and renders exactly as before."""

    space_id: str
    nodes: list[MvSemanticGraphNode] = Field(default_factory=list)
    edges: list[MvSemanticGraphEdge] = Field(default_factory=list)
    proposals: list[MvProposal] = Field(default_factory=list)
    coverage_status: str | None = None
    coverage_reason: str | None = None


class MvDdlArtifact(BaseModel):
    """``GET /runs/{run_id}/mv-ddl`` — the rendered DDL artifact plus GRANT remediation.

    ``yaml_text`` is the immutable rendered body (MV-D22); ``ddl`` is the
    render-time ``CREATE VIEW`` wrapper; ``grant_sql`` is the copy-ready
    ``GRANT SELECT`` checklist for the space's audience, never auto-applied."""

    suggestion_id: str | None = None
    dedup_fingerprint: str | None = None
    proposed_object: str | None = None
    join_strategy: str | None = None
    yaml_text: str | None = None
    ddl: str | None = None
    validation: dict[str, Any] | None = None
    grant_sql: str | None = None


class MvProposalDecisionRequest(BaseModel):
    """``POST /mv/proposals/{suggestion_id}/decision`` body.

    ``space_id`` resolves the ``(target_space_id, dedup_fingerprint)`` key the
    decision is recorded against. ``suppressed_until`` applies to a rejection."""

    space_id: str = Field(..., pattern=r"^[0-9a-zA-Z_-]{1,128}$")
    run_id: str | None = None
    decision: Literal["approved", "rejected"]
    suppressed_until: str | None = None


class MvProposalDecisionResponse(BaseModel):
    suggestion_id: str
    decision: Literal["approved", "rejected"]
    approved_for_rerun: bool


class MvDropRequest(BaseModel):
    """``POST /mv/created/{suggestion_id}/drop`` body.

    ``confirm`` must be ``true`` — the UC object may already have other consumers,
    so the drop is explicit and refuses unless ``status = DETACHED`` (MV-D6)."""

    run_id: str
    confirm: bool = False


class MvLiftReport(BaseModel):
    """The isolated-lift report persisted on ``genie_opt_mv_created_objects.lift_report_json``.

    A verbatim mirror of ``LiftReport.to_dict()`` (the engine's frozen 14-key
    contract at ``optimization/eval_runner.py`` — mirror it, never reshape it):
    accuracies and deltas are 0–1 fractions, and needs-review questions are
    counted separately, excluded from both numerator and denominator on each
    side of the comparison."""

    delta_affected: float
    delta_suite: float
    regressed_question_ids: list[str] = Field(default_factory=list)
    needs_review_count: int
    pre_eval_run_id: str
    post_eval_run_id: str
    question_subset: list[str] = Field(default_factory=list)
    pre_accuracy_affected: float
    post_accuracy_affected: float
    pre_accuracy_suite: float
    post_accuracy_suite: float
    needs_review_question_ids: list[str] = Field(default_factory=list)
    graded_affected_count: int
    graded_suite_count: int


class MvCreatedObject(BaseModel):
    """A metric view created under OBO for a run (a ``genie_opt_mv_created_objects`` row)."""

    run_id: str
    suggestion_id: str
    full_name: str
    created_by: str | None = None
    # MV-D24 create-path discriminator. NULL in the ledger reads as the legacy
    # OBO_CREATED (additive-migration convention); USER_CREATED marks a view the
    # user registered themselves, which the app never drops.
    provenance: Literal["OBO_CREATED", "USER_CREATED"] = "OBO_CREATED"
    status: Literal["CREATED", "ATTACHED", "DETACHED", "DROPPED"]
    attach_patch_id: str | None = None
    baseline_eval_run_id: str | None = None
    post_attach_eval_run_id: str | None = None
    on_regression_action: str | None = None
    created_at: str | None = None
    # Decoded from ``lift_report_json``; present once the isolated attach eval ran.
    lift_report: MvLiftReport | None = None


class MvCreatedObjectsResponse(BaseModel):
    """``GET /runs/{run_id}/mv-created`` — the run's created-object ledger.

    ``downgrade_reason`` is a run-level signal read from the consent row: why a
    ``create_and_attach`` run was downgraded to ``suggest_only`` at trigger, or
    ``None`` when it was not. It is distinct from a per-object ``DETACHED`` status,
    which is a post-attach regression revert, not a pre-write downgrade."""

    run_id: str
    created: list[MvCreatedObject] = Field(default_factory=list)
    downgrade_reason: str | None = None


class MvDropResponse(BaseModel):
    suggestion_id: str
    full_name: str
    status: Literal["CREATED", "ATTACHED", "DETACHED", "DROPPED"]
    dropped: bool


# ── Auto-Optimize current version ────────────────────────────────────────
# Mirrored on the frontend as `CurrentVersionResponse` in
# `frontend/src/types/index.ts`. Both halves must stay in sync — update
# together (see AGENTS.md §Models).


class VersionMatch(BaseModel):
    """One known optimization version matching a live-state component."""

    run_id: str
    target: Literal["baseline", "champion"]
    started_at: str | None = None
    best_accuracy: float | None = None


class CurrentVersionResponse(BaseModel):
    """Payload for ``GET /auto-optimize/spaces/{space_id}/current-version``.

    Answers "which known optimization version is the live agent on?" by
    fingerprint-matching config and benchmarks independently against every
    history-visible captured run baseline / champion:

    * ``matched`` — config and benchmarks equal the same known version;
    * ``mixed`` — both components are known, but come from different versions;
    * ``drifted`` — one or both components match no known version;
    * ``history_incomplete`` — at least one expected baseline/champion lacks
      an authoritative API-observed capture, so a non-match is inconclusive;
    * ``no_known_versions`` — no runs with captured configs (nothing to
      compare); ``unavailable`` — the check itself failed (fail-open, the UI
      renders nothing); ``optimization_in_progress`` — an active run is
      mutating the live config, so matching would be noise.
    """

    status: Literal[
        "matched",
        "mixed",
        "drifted",
        "history_incomplete",
        "no_known_versions",
        "unavailable",
        "optimization_in_progress",
    ]
    current: VersionMatch | None = None
    also_matches: list[VersionMatch] = Field(default_factory=list)
    config_match: VersionMatch | None = None
    config_also_matches: list[VersionMatch] = Field(default_factory=list)
    benchmark_match: VersionMatch | None = None
    benchmark_also_matches: list[VersionMatch] = Field(default_factory=list)
    drifted_dimensions: list[Literal["config", "benchmarks"]] = Field(
        default_factory=list
    )
    live_update_time: str | None = None
