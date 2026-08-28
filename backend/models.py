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
    # Prompt 15.8 fix #3 — the space's audience, derived from its ACL (principals
    # with CAN RUN / CAN VIEW / CAN MANAGE), so the consent modal's GRANT preview
    # names a real grantee instead of a literal ``<grantee>``. Best-effort: an
    # unreadable ACL leaves this empty and the modal falls back to the raw
    # statement. Populated in the router after the probe (the entitlement probe
    # stays audience-agnostic).
    audience_grantees: list[str] = Field(default_factory=list)


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


class MvProposalMeasure(BaseModel):
    """One member measure of a view-grained proposal bundle (MV-D30).

    A bundle governs one or more recurring measures over a shared source-table
    grain. Each member carries its own per-measure ``dedup_fingerprint`` — the
    identity suppression is enforced at — plus the recurrence evidence the card's
    justification is assembled from. A legacy single-measure proposal reads back
    as a one-element list, so a client can treat every proposal uniformly."""

    display_name: str | None = None
    expr: str | None = None
    dedup_fingerprint: str | None = None
    recurrence: int | None = None
    provenance_count: int | None = None
    benchmark_question_ids: list[str] | None = None


class MvProvenanceLabel(BaseModel):
    """A provenance id resolved to a human thing for the card (Prompt 15.9 item d).

    The advisor stamps each occurrence with a prefixed id (``sql_snippet:`` /
    ``trusted_asset:`` / ``gso_patch:``); rendering those raw at the user is the
    defect 15.9 item (d) closes. Resolved SERVE-TIME from the current space config
    so existing proposals gain labels without a re-scan: a ``sql_snippet`` id →
    the snippet's display name (``detail`` = its expression), a ``trusted_asset``
    id → its example-question text, a ``gso_patch`` id → its lever/iteration. The
    raw ``id`` rides along so the card can keep it behind a "show raw ids"
    debugging affordance. UI display only — the metadata firewall is untouched."""

    id: str
    kind: str
    label: str
    detail: str | None = None


class MvProposal(BaseModel):
    """One advisor proposal (a ``genie_opt_mv_candidates`` row) as the UI reads it.

    JSON columns arrive decoded to their POV Part 4 field names. ``confidence_score``
    is 0–100; ``approved_for_rerun`` gates ``create_and_attach`` (MV-D1).

    ``measures`` (MV-D30, Prompt 15.3) is the view-grained bundle's members,
    read from ``evidence.measures``. A pre-15.3 single-measure row synthesizes a
    one-element list from its own row, so every proposal is a bundle to the UI."""

    suggestion_id: str
    dedup_fingerprint: str
    target_space_id: str
    run_id: str | None = None
    candidate_type: str
    confidence_score: float | None = None
    tier: str | None = None
    # MV-D32 as-implemented (Prompt 15.7b): the tier the score alone earned and
    # whether MV-D15 coverage capped it. Both computed in mv_scoring, persisted
    # additively. The panel promotes a coverage-capped-strong proposal (uncapped
    # MEDIUM+ AND tier_capped_by_coverage) into the default list under a
    # "Strong (evidence-limited)" badge. None on legacy rows → tier-only split.
    uncapped_tier: str | None = None
    tier_capped_by_coverage: bool | None = None
    proposed_object: str | None = None
    measures: list[MvProposalMeasure] = []
    # MV-D35 (Prompt 15.8) — the facts row leads the card with the proven gates.
    # COMPUTED at hydration from per-row proof, never persisted: each key is
    # present ONLY when that gate provably ran for this row (a servable rendered
    # body proves validated+executable; a PROPOSE verdict with no conflicts
    # proves no-overlap). A row lacking the proof carries no key for that check,
    # so a check that did not run is never rendered — "a check that lies is
    # worse than the percent". Values are "PASS" today; the map is open so a
    # future gate outcome (e.g. a soft warning) can ride the same field.
    checks: dict[str, str] | None = None
    score_components: dict[str, Any] | None = None
    evidence: dict[str, Any] | None = None
    # Prompt 15.9 item (d): the evidence provenance ids resolved to human labels
    # (serve-time, from the current space config). None on the pure row map; the
    # proposals endpoints populate it. The card renders these and keeps the raw
    # ids behind a "show raw ids" affordance.
    provenance_labels: list[MvProvenanceLabel] | None = None
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
    # MV-D34 attach-at-approval marker: true when this proposal's ``proposed_object``
    # is already shelved on the Agent's ``data_sources.metric_views``. Computed
    # serve-time from the live config (the source of truth), NOT from a ledger
    # status, so a user detach in Genie un-marks it on the next load and a view
    # attached out-of-band is still recognized. Lets the list badge an
    # already-created proposal instead of re-offering [Create this metric view].
    attached: bool = False
    created_at: str | None = None
    updated_at: str | None = None


class MvProposalsResponse(BaseModel):
    """``GET /runs/{run_id}/mv-proposals`` — the run's proposals, newest first."""

    run_id: str
    proposals: list[MvProposal] = Field(default_factory=list)


class MvLastScan(BaseModel):
    """Summary of a space's most recent advice scan, for MV-D31 hydrate-on-mount.

    Lets the panel say "last scanned <when> — N proposals" and reproduce the last
    run's empty/skip state on mount without re-running a multi-minute scan. Every
    field is *derived* — ``scanned_at`` / ``duration_seconds`` / ``status`` come
    from the advice run's terminal ``genie_opt_stages`` row, and ``skip_reason`` /
    ``measures_found`` from that row's ``detail_json`` (``AdvisorOutcome.detail()``
    already carries both keys) — so this is a read of existing state, not a new
    ``genie_opt_runs`` column. ``None`` at the response level means the space has
    never been scanned, which the panel renders as the never-scanned state (a
    first-class "Scan" affordance) rather than as an empty result."""

    scanned_at: str | None = None
    duration_seconds: float | None = None
    status: str | None = None
    skip_reason: str | None = None
    measures_found: int | None = None
    proposal_count: int = 0


class MvSpaceProposalsResponse(BaseModel):
    """``GET /spaces/{space_id}/mv-proposals`` — a space's proposals (MV-D23).

    Space-scoped twin of ``MvProposalsResponse`` for the run-config panel's
    re-run gate, which asks a space-scoped question ("what has this Agent had
    approved?") and must not borrow a prior ``run_id`` to stand in for it. The
    element type is the SAME ``MvProposal`` as the run-keyed response, so the
    proposal card renders from one shape in both the output screen (per-run) and
    this panel (per-space).

    ``last_scan`` (MV-D31) hydrates the advice panel on mount: the timestamp,
    real duration, and empty/skip state of the space's most recent scan, so the
    surface opens showing "last scanned … — N proposals" instead of a bare
    button, and the multi-minute scan is a deliberate Re-scan, not the price of
    opening the panel. ``None`` means never scanned."""

    space_id: str
    proposals: list[MvProposal] = Field(default_factory=list)
    last_scan: MvLastScan | None = None


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
    from an empty list. ``measures_found`` disambiguates the two ``NO_CANDIDATES``
    empties the skip reason alone conflates (Prompt 15.3, the governance ladder):
    ``0`` means the scan found no recurring measure to govern, while ``> 0`` with
    ``NO_CANDIDATES`` means every recurring measure is ALREADY governed — the
    "you're in good shape" confidence empty, not a barren one. ``proposals`` is
    the SAME ``MvProposal`` shape the space-scoped and run-keyed lists return, so
    ``MvSuggestOnlyPanel`` mounts these cards from this space-scoped source with
    no component change."""

    space_id: str
    run_id: str
    status: str
    skip_reason: str | None = None
    measures_found: int | None = None
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


class MvCreateAtApprovalRequest(BaseModel):
    """``POST /spaces/{space_id}/mv/create`` — create-at-approval (MV-D34).

    The user is standing in front of the suggestion on the IQ surface, their OBO
    token is live, and a fresh probe already ran and recorded consent. This
    creates the ONE approved proposal now, under OBO, in the consented schema —
    the same rails as bring-your-own registration (advice run + ``OBO_CREATED``
    ledger), except the app issues the ``CREATE`` instead of verifying an
    existing view. ``probe_id`` keys the recorded consent to re-verify
    (downgrade-never-upgrade); attach and lift stay the next run's job."""

    suggestion_id: str = Field(..., pattern=r"^[0-9a-zA-Z_:-]{1,128}$")
    probe_id: str = Field(..., pattern=r"^[0-9a-zA-Z_:-]{1,128}$")


class MvCreateAtApprovalResponse(BaseModel):
    """Result of a create-at-approval (MV-D34).

    One shape carries the three outcomes the card renders. ``created`` true: the
    metric view exists under OBO — ``full_name`` + the sentinel advice ``run_id``
    hosting the ``OBO_CREATED`` ledger row. ``attached`` then reports whether the
    view was also shelved on the Agent config in the same call (MV-D34
    attach-at-approval): true means the semantic model and optimization already
    reflect it; ``created and not attached`` means the config PATCH failed (e.g.
    the user lacks CAN EDIT), so the card tells the user to attach it themselves.
    ``grant_sql`` is the copy-ready ``GRANT SELECT … TO <optimizer SP>`` the view
    needs so a later optimization run (a different principal) can read it — the
    one manual step left after attach. ``created`` false with ``degraded`` true:
    the fresh probe re-verified below SUFFICIENT (downgrade-never-upgrade,
    MV-D1/MV-D34), so nothing was created and the card falls back to [Approve for
    later] with ``remediation_sql`` shown copy-ready. ``created`` false with
    ``degraded`` false: a create-time failure (revalidation drop, collision)
    with ``reason``, never a silent empty."""

    created: bool
    degraded: bool = False
    attached: bool = False
    # MV-D34 idempotent re-approval: true when the view already existed (as a
    # metric view) and this call only (re)attached it — the card then says
    # "attached an existing view" instead of claiming a fresh create.
    already_existed: bool = False
    full_name: str | None = None
    run_id: str | None = None
    suggestion_id: str | None = None
    provenance: str = "OBO_CREATED"
    verdict: str | None = None
    remediation_sql: str | None = None
    grant_sql: str | None = None
    reason: str | None = None
    # Workspace URL so the created terminal can deep-link the new view in Catalog
    # Explorer (…/explore/data/<catalog>/<schema>/<view>). Resolved from the
    # caller's client config; None if unavailable (the link is then omitted).
    workspace_host: str | None = None


class MvSemanticGraphDimension(BaseModel):
    """One dimension (``fields``/``dimensions`` entry) of a metric view's YAML.

    Prompt 12f. ``binding`` is which relation the expression reads from — the
    joined table's fqn when ``expr`` is qualified by a join ALIAS declared in the
    same YAML, else the MV's own ``source``. It is resolved from the parsed
    document only: an expression whose qualifier matches no declared alias
    resolves to the source rather than guessing a table, because a binding the
    YAML does not prove is worse than no binding at all."""

    name: str
    expr: str | None = None
    binding: str | None = None


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
    # Prompt 12e / MV-D33 (metric_view nodes only). ``True`` when the MV's YAML
    # was read and parsed (a real ``source``), so its member tables and ``uses``
    # arrows are proven; ``False`` when the DESCRIBE/parse failed — the node
    # renders "definition unavailable" and draws NO arrows (unreadable is
    # unproven, MV-D33 constraint 2). ``None`` for non-MV nodes or when no read
    # was attempted (unconfigured) — additive, older clients ignore it.
    definition_available: bool | None = None
    # Prompt 12f (metric_view nodes only, and only when the YAML parsed): the
    # rest of what the definition already proved, so the curator inset can answer
    # "what IS this view" without a second read. ``mv_filter`` is the top-level
    # ``filter`` verbatim (a row filter silently applied to every query is exactly
    # the thing a curator must see); ``materialization`` is a one-line SUMMARY of
    # the ``materialization`` object (entry count + refresh cadence), never the
    # object itself, because the inset states posture, not configuration; and
    # ``dimensions`` are the declared fields with their resolved binding. All
    # three are additive and absent (``None``) for a non-MV node, an unreadable
    # YAML, or a definition that simply does not declare them.
    # The table the view reads FROM. The ``uses`` edges prove membership but not
    # which member is the root, and a root guessed from join direction is wrong
    # whenever the config declares a fact→fact join (detail rows pointing at their
    # header): the curator inset's join tree must be rooted where the YAML says.
    mv_source: str | None = None
    mv_filter: str | None = None
    materialization: str | None = None
    dimensions: list[MvSemanticGraphDimension] | None = None
    # Prompt 12f (measure concepts only): the attached metric view that ALREADY
    # exposes a measure of this name, set on an ungoverned/curated concept whose
    # name collides with a governed one. The Space-config panel raises it as an
    # overlap warning — the duplicate-definition risk the v7 contract puts on the
    # loose-measure panel. ``None`` when there is no collision.
    overlaps: str | None = None
    # Round-5 (table nodes only): the PROVEN fact/dim role, never guessed from
    # layout. ``fact`` when the table is a metric view's declared ``source``;
    # ``dim`` when it is only ever a join target (a join_specs "one" side or an
    # MV-YAML joined table). ``None`` when nothing proves it — the UI then labels
    # it a neutral "table" rather than asserting a role the data does not support
    # (the fix for facts/dims mislabelled by raw join direction).
    role: Literal["fact", "dim"] | None = None
    # Round-5 (measure concepts only): the measure's defining expression and, when
    # the source carries one, a human description — so selecting a measure can show
    # WHAT it computes in the detail panel, not just its governance. ``expr`` is the
    # canonical expression for a governed measure, the snippet SQL for a curated
    # one, and ``None`` for an ungoverned proposal (which exposes a name, not an
    # expression). ``description`` is best-effort and often ``None``.
    expr: str | None = None
    description: str | None = None
    # Phase 2 (v4 §6, table nodes only): the PARTICIPATING columns — join keys
    # parsed server-side from the join ``ON`` predicates, never the full column
    # list (which would recreate the wall-of-text the blueprint avoids). Drives
    # the Columns LOD's per-column rows and column-accurate join ports. ADDITIVE
    # and ``None`` for a non-table node or a table no parsed join key referenced,
    # so a client that predates the column model renders exactly as before.
    columns: list[str] | None = None


class MvSemanticGraphEdge(BaseModel):
    """One edge in the semantic model graph.

    ``join`` edges come from ``instructions.join_specs`` OR from a metric view's
    own YAML ``joins`` (Prompt 12e, MV-D33) — ``on`` is the predicate, decoded
    from ``sql[0]`` for config joins or the ``on``/``using`` clause for MV joins;
    ``relationship`` is decoded from the ``--rt=…--`` annotation (``sql[1]``), and
    ``scd2`` is true when the predicate carries an ``is_current`` guard.
    ``membership`` links a measure concept to its owning metric view. ``uses``
    (Prompt 12e, MV-D33) links a metric view to a table it sources — the proven
    at-rest arrow and the member set the select-time boundary wraps; emitted ONLY
    for an MV whose YAML parsed (arrows require proof). ``replaces`` is the
    overlay's dashed edge — client-only. ``derives`` (round-7) links a measure to
    a table its expression is built from — extracted from the measure's ``expr``
    (fully-qualified table references). It is the lineage a loose (Space-config)
    measure has when it belongs to no metric view: the client renders it ONLY when
    the measure is selected, so clicking a loose measure lights up its source
    tables. Emitted only where the expr proves a reference (no expr → none).

    ``weight`` (Prompt 12b, SQL-coverage lens) is the number of curated SQL
    statements that traverse a ``join`` edge (touch both endpoints) — an ADDITIVE
    field older clients ignore; ``None`` means the lens did not weight this edge
    kind."""

    from_: str = Field(..., alias="from")
    to: str
    kind: Literal["join", "membership", "replaces", "uses", "derives"]
    on: str | None = None
    relationship: str | None = None
    scd2: bool = False
    weight: int | None = None
    # Phase 2 (v4 §6, join edges only): the parsed column endpoints of the ``on``
    # predicate — ``from_column`` on the ``from`` (left) side, ``to_column`` on
    # the ``to`` (right) side — so the canvas can terminate the join line at the
    # exact column ports. ADDITIVE and ``None`` when the predicate did not parse
    # to a column = column equality (e.g. a function join); the ``on`` text is
    # retained regardless for the detail inset.
    from_column: str | None = None
    to_column: str | None = None

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


class JoinCandidate(BaseModel):
    """A data-grounded candidate join surfaced by the Semantic Blueprint's Join
    Advisor (v4 §7) — **advice to the optimizer, never a Genie Agent config edit.**

    The Workbench does not make ad-hoc edits to ``serialized_space`` (that is the
    Genie product UI's job). A candidate is grounded in (a) a declared Unity
    Catalog foreign key (``match="fk"``), (b) name+type matching of key-like
    columns (``match="name-type"``), and scored by (c) a warehouse containment
    probe: the fraction of distinct ``from.from_col`` values present in
    ``to.to_col`` in ``[0,1]`` (``probe``), or ``None`` when no warehouse could
    run it (honest-empty — never a silent 0). Field aliases mirror the frontend
    ``JoinCandidate`` (``from``/``fromCol``/``toCol``) one-to-one.
    """

    id: str
    from_: str = Field(..., alias="from")
    from_col: str = Field(..., alias="fromCol")
    to: str
    to_col: str = Field(..., alias="toCol")
    rel: Literal["N:1", "1:1"] = "N:1"
    match: Literal["fk", "name-type", "probe"] = "name-type"
    probe: float | None = None
    note: str | None = None

    model_config = {"populate_by_name": True}


class JoinCandidatesResponse(BaseModel):
    """``GET /spaces/{space_id}/join-candidates`` — Join Advisor candidate pool.

    ``status`` is the honest-empty discriminator the inset renders:
    ``ok`` (candidates found), ``fully_connected`` (no unjoined key-like column
    pairs remain), ``no_candidates`` (nothing name/FK-matched), or
    ``no_warehouse`` (candidates exist but none could be containment-probed, so
    every ``probe`` is ``None``). Candidates are never applied — they are seeded
    as advice via ``POST /join-advice``."""

    space_id: str
    status: Literal["ok", "fully_connected", "no_candidates", "no_warehouse"] = "ok"
    candidates: list[JoinCandidate] = Field(default_factory=list)


class JoinAdvicePayload(BaseModel):
    """``POST /spaces/{space_id}/join-advice`` body — the seeded candidate set.

    Persists the operator's checked candidates as **advice the next
    Auto-Optimize run validates and adds itself** (via ``add_join_spec``), never
    a declared ``join_spec`` written by the Workbench (the optimizer can add or
    update joins but cannot remove them — ``unified_loop.py:_ALLOWED_PATCH_TYPES``
    — so a locked wrong join would be a foot-gun). An empty ``seeds`` clears the
    space's pending advice."""

    seeds: list[JoinCandidate] = Field(default_factory=list, max_length=50)


class JoinAdviceResponse(BaseModel):
    """``GET``/``POST /spaces/{space_id}/join-advice`` — the persisted advice set.

    ``seeds`` is the pending advice the next run will consume; ``updated_at`` and
    ``seeded_by`` record who last seeded it. Empty ``seeds`` means no pending
    advice."""

    space_id: str
    seeds: list[JoinCandidate] = Field(default_factory=list)
    updated_at: str | None = None
    seeded_by: str | None = None


class MvDdlArtifact(BaseModel):
    """``GET /runs/{run_id}/mv-ddl`` — the rendered DDL artifact plus GRANT remediation.

    ``yaml_text`` is the immutable rendered body (MV-D22); ``ddl`` is the
    render-time ``CREATE VIEW`` wrapper; ``grant_sql`` is the copy-ready
    ``GRANT SELECT`` checklist for the space's audience, never auto-applied."""

    suggestion_id: str | None = None
    dedup_fingerprint: str | None = None
    proposed_object: str | None = None
    join_strategy: str | None = None
    # The view's source tables, parsed serve-time from ``yaml_text`` (the ``source:``
    # of the base plus each join). Lets the card show a "Source" attribute column
    # (deployed review #3) for existing candidates without a re-scan; the rendered
    # YAML is the authority, so this is right whether the row is new or legacy.
    source_tables: list[str] | None = None
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
