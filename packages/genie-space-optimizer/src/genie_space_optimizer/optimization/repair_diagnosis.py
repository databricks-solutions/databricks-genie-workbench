"""P4 C1 — Structured repair diagnosis from Stage 1.

The existing :mod:`repair_intent` module owns the synthesis-time
``RepairIntent`` carrier (repair_shape + patch_type, threaded
end-to-end through proposals and the applier). This module owns a
*separate*, upstream concept: a structured *diagnosis* produced by
Stage 1's LLM that gates whether the structural-repair lane may fire
on a given cluster.

The Stage 1 LLM continues to emit open-ended freeform RCA text. P4 C1
adds a parallel typed structure — :class:`RepairDiagnosis` — with the
five fields the plan calls out:

  * ``behavior_delta``    — one sentence describing the observed-vs-
    expected behavior gap. Free text.
  * ``sql_shape_delta``   — one sentence describing the SQL/output
    shape change needed (``None`` if Stage 1 cannot identify one).
  * ``implicated_assets`` — tuple of canonical asset references the
    diagnosis points to (``catalog.schema.table[.column]``).
  * ``evidence_citations``— tuple of evidence references (judge
    trace IDs, benchmark row IDs).
  * ``candidate_mechanisms`` — tuple of coarse mechanism names the
    LLM nominates (see :class:`PatchMechanism` in
    :mod:`patch_mechanism`).

Plus ``rca_freeform`` (the open-ended diagnosis text) for back-
compatibility with the existing freeform path.

The structural-repair gate consumes a ``RepairDiagnosis`` (when
present) via :func:`gate_repair_diagnosis_sufficient`. The predicate
is:

    repair_diagnosis is sufficient
      iff   len(implicated_assets) > 0
      and   sql_shape_delta is non-empty

When the predicate fails, the caller MUST abstain with
``AbstainReason.REPAIR_INTENT_INDETERMINATE`` instead of falling
through to a generic ``generic_judge_guidance`` shape.

The dataclass is JSON round-trippable so it can be carried on stage
I/O boundaries (Phase H bundle, MLflow span attributes, replay
fixtures).
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@dataclass(frozen=True, slots=True)
class AssetRef(JsonRoundTrip):
    """Canonical asset reference cited by the diagnosis.

    ``catalog`` / ``schema`` / ``table`` are required; ``column`` is
    optional (None for table-level references). The combination
    ``f"{catalog}.{schema}.{table}"`` (plus ``f".{column}"`` when
    present) is the canonical string form.
    """

    catalog: str
    schema: str
    table: str
    column: str | None = None

    def canonical(self) -> str:
        base = f"{self.catalog}.{self.schema}.{self.table}"
        if self.column:
            return f"{base}.{self.column}"
        return base

    def to_json(self) -> dict:  # type: ignore[override]
        payload: dict = {
            "catalog": self.catalog,
            "schema": self.schema,
            "table": self.table,
        }
        if self.column is not None:
            payload["column"] = self.column
        return payload

    @classmethod
    def from_json(cls, payload: dict) -> "AssetRef":  # type: ignore[override]
        return cls(
            catalog=str(payload["catalog"]),
            schema=str(payload["schema"]),
            table=str(payload["table"]),
            column=(
                str(payload["column"])
                if payload.get("column") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class EvidenceRef(JsonRoundTrip):
    """One evidence anchor cited by the diagnosis.

    ``source`` names the producer (``"judge_asi"``, ``"benchmark_row"``,
    ``"counterfactual_scan"`` …). ``ref_id`` is the producer-scoped
    identifier the postmortem can use to follow the citation back to
    its origin row.
    """

    source: str
    ref_id: str
    detail: str = ""

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "source": self.source,
            "ref_id": self.ref_id,
            "detail": self.detail,
        }

    @classmethod
    def from_json(cls, payload: dict) -> "EvidenceRef":  # type: ignore[override]
        return cls(
            source=str(payload["source"]),
            ref_id=str(payload["ref_id"]),
            detail=str(payload.get("detail") or ""),
        )


@dataclass(frozen=True, slots=True)
class RepairDiagnosis(JsonRoundTrip):
    """Stage 1 structured diagnosis. Gates the structural repair lane.

    The structural lane only fires when this diagnosis is *sufficient*
    (see :func:`gate_repair_diagnosis_sufficient`). When the LLM
    cannot produce sufficient evidence after one retry-with-sharpened-
    feedback, Stage 1 abstains with
    ``AbstainReason.REPAIR_INTENT_INDETERMINATE``.
    """

    cluster_id: str
    rca_freeform: str
    behavior_delta: str
    sql_shape_delta: str | None
    implicated_assets: tuple[AssetRef, ...]
    evidence_citations: tuple[EvidenceRef, ...]
    candidate_mechanisms: tuple[str, ...]

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "cluster_id": self.cluster_id,
            "rca_freeform": self.rca_freeform,
            "behavior_delta": self.behavior_delta,
            "sql_shape_delta": self.sql_shape_delta,
            "implicated_assets": [a.to_json() for a in self.implicated_assets],
            "evidence_citations": [e.to_json() for e in self.evidence_citations],
            "candidate_mechanisms": list(self.candidate_mechanisms),
        }

    @classmethod
    def from_json(cls, payload: dict) -> "RepairDiagnosis":  # type: ignore[override]
        return cls(
            cluster_id=str(payload["cluster_id"]),
            rca_freeform=str(payload.get("rca_freeform") or ""),
            behavior_delta=str(payload.get("behavior_delta") or ""),
            sql_shape_delta=(
                str(payload["sql_shape_delta"])
                if payload.get("sql_shape_delta")
                else None
            ),
            implicated_assets=tuple(
                AssetRef.from_json(a)
                for a in (payload.get("implicated_assets") or ())
            ),
            evidence_citations=tuple(
                EvidenceRef.from_json(e)
                for e in (payload.get("evidence_citations") or ())
            ),
            candidate_mechanisms=tuple(
                str(m) for m in (payload.get("candidate_mechanisms") or ())
            ),
        )


@dataclass(frozen=True, slots=True)
class RepairDiagnosisGateVerdict:
    """Outcome of :func:`gate_repair_diagnosis_sufficient`.

    ``outcome`` is one of:
      * ``"admitted"``      — diagnosis is sufficient; structural lane may fire.
      * ``"indeterminate"`` — diagnosis missing required evidence; caller
        MUST abstain with ``AbstainReason.REPAIR_INTENT_INDETERMINATE``.

    ``missing_fields`` lists the field names that failed the predicate
    (``"implicated_assets"``, ``"sql_shape_delta"``). Empty when admitted.
    ``feedback`` is a short imperative the Stage 1 retry may include in
    its sharpened prompt.
    """

    outcome: str
    missing_fields: tuple[str, ...]
    feedback: str


def gate_repair_diagnosis_sufficient(
    diagnosis: RepairDiagnosis | None,
) -> RepairDiagnosisGateVerdict:
    """P4 C1 predicate. Returns admitted / indeterminate.

    A ``RepairDiagnosis`` is sufficient iff:
      * ``len(implicated_assets) > 0``, AND
      * ``sql_shape_delta`` is non-empty (not None, not whitespace).

    When ``diagnosis is None``, the result is indeterminate with both
    fields missing — the structural lane MUST NOT fall through to a
    generic shape just because Stage 1 produced no structured diagnosis.
    """
    missing: list[str] = []
    if diagnosis is None or not diagnosis.implicated_assets:
        missing.append("implicated_assets")
    if (
        diagnosis is None
        or not diagnosis.sql_shape_delta
        or not str(diagnosis.sql_shape_delta).strip()
    ):
        missing.append("sql_shape_delta")
    if not missing:
        return RepairDiagnosisGateVerdict(
            outcome="admitted",
            missing_fields=(),
            feedback="",
        )
    feedback_parts = []
    if "implicated_assets" in missing:
        feedback_parts.append(
            "name the concrete catalog.schema.table[.column] assets the "
            "failure points to"
        )
    if "sql_shape_delta" in missing:
        feedback_parts.append(
            "describe the SQL/output shape change in one sentence"
        )
    feedback = "; ".join(feedback_parts)
    return RepairDiagnosisGateVerdict(
        outcome="indeterminate",
        missing_fields=tuple(missing),
        feedback=feedback,
    )


@dataclass(frozen=True, slots=True)
class RequiredAssetsVerdict:
    """Outcome of :func:`required_assets_for_patch_family`.

    ``outcome`` is one of ``"admitted"`` / ``"drop"``. ``drop_reason``
    is the closed-vocabulary :class:`DropReason` name (uppercase) the
    Trial 21 Evidence Actuator emits when this verdict drops the
    proposal; empty when admitted.

    ``required`` is the human-readable label for the asset shape the
    patch family expects (``"resolved_table_column"``,
    ``"justification_and_no_repeat"``, etc.). Pinned by the
    postmortem-replay fixture so Trial 21's W1 regression suite can
    audit the table without re-reading source.
    """

    outcome: str
    required: str
    drop_reason: str
    feedback: str


# Trial 21 W6+C1 — per-patch-family asset gate table. Each entry
# describes what evidence the Evidence Actuator demands from the
# proposal's accompanying ``RepairDiagnosis`` before admitting the
# patch. The vocabulary is pinned by the postmortem-replay regression
# suite (``expected_after_w6_per_family`` in the Run B fixture); any
# new patch family added must update this table in the same commit.
_REQUIRED_ASSET_TABLE: dict[str, tuple[str, str, str]] = {
    # patch_type_wire → (required_label, drop_reason, feedback)
    "add_column_description": (
        "resolved_table_column",
        "MISSING_IMPLICATED_ASSETS",
        "add_column_description requires a resolved (table, column) pair "
        "from RepairDiagnosis.implicated_assets",
    ),
    "update_column_description": (
        "resolved_table_column",
        "MISSING_IMPLICATED_ASSETS",
        "update_column_description requires a resolved (table, column) pair",
    ),
    "add_table_description": (
        "resolved_table",
        "MISSING_IMPLICATED_ASSETS",
        "add_table_description requires a resolved catalog.schema.table",
    ),
    "add_description": (
        "resolved_table_or_column",
        "MISSING_IMPLICATED_ASSETS",
        "add_description requires a resolved table or column asset",
    ),
    "update_description": (
        "resolved_table_or_column",
        "MISSING_IMPLICATED_ASSETS",
        "update_description requires a resolved table or column asset",
    ),
    "add_sql_snippet_filter": (
        "shape_plus_targets_plus_stamp",
        "MISSING_IMPLICATED_ASSETS",
        "add_sql_snippet_filter requires a non-empty implicated_assets "
        "tuple AND a non-empty sql_shape_delta",
    ),
    "add_sql_snippet_expression": (
        "shape_plus_targets_plus_stamp",
        "MISSING_IMPLICATED_ASSETS",
        "add_sql_snippet_expression requires implicated_assets + sql_shape_delta",
    ),
    "add_sql_snippet_measure": (
        "shape_plus_targets_plus_stamp",
        "MISSING_IMPLICATED_ASSETS",
        "add_sql_snippet_measure requires implicated_assets + sql_shape_delta",
    ),
    "add_sql_snippet_join": (
        "join_plus_table_assets",
        "MISSING_IMPLICATED_ASSETS",
        "add_sql_snippet_join requires both sides of the join as "
        "implicated_assets",
    ),
    "add_example_sql": (
        "expected_sql_shape",
        "MISSING_IMPLICATED_ASSETS",
        "add_example_sql requires a non-empty sql_shape_delta describing "
        "the expected SQL/output shape",
    ),
    "update_example_sql": (
        "expected_sql_shape",
        "MISSING_IMPLICATED_ASSETS",
        "update_example_sql requires a non-empty sql_shape_delta",
    ),
    "add_instruction": (
        "justification_and_no_repeat",
        "UNJUSTIFIED_SINGLE_LEVER",
        "add_instruction requires a non-empty justification AND must "
        "not repeat a prior kept_insufficient single-lever signature",
    ),
    "update_instruction": (
        "justification_and_no_repeat",
        "UNJUSTIFIED_SINGLE_LEVER",
        "update_instruction requires a non-empty justification",
    ),
}


def required_assets_for_patch_family(
    *,
    patch_type: str,
    implicated_assets: list[str] | tuple[str, ...],
    justification: str = "",
    sql_shape_delta: str = "",
    in_multi_lever_kit: bool = False,
) -> RequiredAssetsVerdict:
    """Trial 21 W6+C1 — gate one proposal on the asset shape its
    patch family demands.

    Returns an ``"admitted"`` verdict when the proposal carries the
    required evidence; otherwise ``"drop"`` with the typed
    :class:`DropReason` label the Evidence Actuator emits.

    Unrecognized ``patch_type`` values fail-open with
    ``outcome="admitted"`` — the table is the source of truth for what
    families have asset requirements and adding a new family is a
    deliberate act (new entry + new postmortem-replay assertion).

    Trial 24 W24.3 — ``in_multi_lever_kit`` waives the
    ``justification_and_no_repeat`` requirement for instruction families
    (``add_instruction`` / ``update_instruction``) when the proposal is a
    member of a >= 2-lever-family kit. The companion structural lever IS
    the justification: an instruction that ships alongside a SQL snippet
    or metadata description is grounded by construction, so the
    ``UNJUSTIFIED_SINGLE_LEVER`` drop must not fire on it. The waiver is
    scoped to the justification shape only — asset-bearing shapes
    (``expected_sql_shape``, ``*_assets``) are unaffected because a kit
    member still has to carry its own structural evidence.
    """
    wire = str(patch_type or "").lower()
    spec = _REQUIRED_ASSET_TABLE.get(wire)
    if spec is None:
        return RequiredAssetsVerdict(
            outcome="admitted",
            required="none",
            drop_reason="",
            feedback="",
        )
    required, drop_reason, feedback = spec

    assets = list(implicated_assets or [])
    shape = str(sql_shape_delta or "").strip()
    just = str(justification or "").strip()

    if required == "justification_and_no_repeat":
        if in_multi_lever_kit:
            # Kit membership IS the justification — admit even with an
            # empty justification slot. The single-lever no-repeat guard
            # cannot apply to a member of a multi-family kit.
            return RequiredAssetsVerdict(
                outcome="admitted",
                required=required,
                drop_reason="",
                feedback="",
            )
        if not just:
            return RequiredAssetsVerdict(
                outcome="drop",
                required=required,
                drop_reason=drop_reason,
                feedback=feedback,
            )
        return RequiredAssetsVerdict(
            outcome="admitted",
            required=required,
            drop_reason="",
            feedback="",
        )

    if required == "expected_sql_shape":
        if not shape:
            return RequiredAssetsVerdict(
                outcome="drop",
                required=required,
                drop_reason=drop_reason,
                feedback=feedback,
            )
        return RequiredAssetsVerdict(
            outcome="admitted",
            required=required,
            drop_reason="",
            feedback="",
        )

    # All remaining shapes require at least one implicated asset.
    if not assets:
        return RequiredAssetsVerdict(
            outcome="drop",
            required=required,
            drop_reason=drop_reason,
            feedback=feedback,
        )
    return RequiredAssetsVerdict(
        outcome="admitted",
        required=required,
        drop_reason="",
        feedback="",
    )


def repair_diagnosis_from_per_qid_diagnosis(
    *,
    cluster_id: str,
    per_qid: object,  # PerQidDiagnosis — typed via duck-type to avoid import cycle
    asset_refs: tuple[AssetRef, ...] = (),
    evidence_refs: tuple[EvidenceRef, ...] = (),
    candidate_mechanisms: tuple[str, ...] = (),
) -> RepairDiagnosis:
    """Deterministic adapter from existing ``PerQidDiagnosis`` to
    :class:`RepairDiagnosis`.

    Used during the Plan 11 transition so existing Stage 1 emissions
    can be threaded through the new gate without rewriting the prompt
    in this PR. ``asset_refs`` and ``evidence_refs`` are passed
    explicitly because the existing carrier stores asset references as
    free-text strings; the producer (Stage 1 wrapper) is responsible
    for parsing those strings into typed refs.

    The behavior_delta is derived from the existing
    ``observed_failure`` / ``expected_sql_shape`` fields:
    ``"{observed_failure}; expected {expected_sql_shape}"`` when both
    are present.
    """
    rca_freeform = str(getattr(per_qid, "rca_kind_label", "") or "")
    observed = str(getattr(per_qid, "observed_failure", "") or "").strip()
    expected = str(getattr(per_qid, "expected_sql_shape", "") or "").strip()
    if observed and expected:
        behavior_delta = f"{observed}; expected {expected}"
    else:
        behavior_delta = observed or expected
    sql_shape_delta = expected or None
    return RepairDiagnosis(
        cluster_id=cluster_id,
        rca_freeform=rca_freeform,
        behavior_delta=behavior_delta,
        sql_shape_delta=sql_shape_delta,
        implicated_assets=tuple(asset_refs),
        evidence_citations=tuple(evidence_refs),
        candidate_mechanisms=tuple(candidate_mechanisms),
    )
