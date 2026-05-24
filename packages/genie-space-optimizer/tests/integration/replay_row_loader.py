"""Production replay case loader for the GSO harness.

A ``ProductionCase`` is one sanitized snapshot of the input the canonical
Stage 1 evidence-card builder sees for a single hard QID in a single
production run. Snapshots live under
``tests/integration/fixtures/production_replay/<run_tag>__<qid>.json`` and
are extracted from the postmortem evidence directories
(``docs/runid_analysis/<run_id>/evidence/``). See ``SCHEMA.md`` in the
fixtures directory for the file shape and sanitization rules.

Why this loader exists
----------------------
The pre-existing ``production_eval_rows.json::hydration_rows`` corpus is
shape-only — every row embeds the question text in some MLflow variant.
The actual production rows for hard QIDs carry the needed fields at
production-specific paths such as ``request.question``,
``expected_response/value``, ``response.response``, per-judge rationale
keys, and flat ASI metadata keys. Tests that assert "production rows
reach DIAGNOSED" using synthetic join shortcuts give false green signal.
This loader returns the captured production-shaped row, typed RCA
evidence, and expected contract violations so a test consuming it can
prove the exact row-shape failure mode before deployment.

Public API
----------
* :data:`PRODUCTION_REPLAY_DIR` — directory holding committed cases.
* :class:`ProductionCase` — frozen dataclass, the harness-facing handle.
* :func:`load_production_case(run_tag, qid)` — load one case by tag/qid.
* :func:`list_production_cases()` — iterate the committed corpus.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType

PRODUCTION_REPLAY_DIR: Path = (
    Path(__file__).resolve().parent / "fixtures" / "production_replay"
)


@dataclass(frozen=True)
class ProductionCase:
    """One sanitized production replay case.

    ``row`` mirrors the captured eval row production ships into Stage 1.
    ``joined_row`` is retained as a compatibility alias for older tests;
    current case files should not define ``joined_row_fields`` because the
    row itself carries the production evidence paths. ``typed_evidence``
    is the reconstructed ``PerQidRcaEvidence`` carrier the builder
    receives via its ``typed_evidence=`` kwarg.

    ``expected_card_violations`` snapshots the violations the canonical
    builder produces *today* against this case — usually
    ``[]`` for the current real-row corpus. Future production drift can
    introduce non-empty snapshots, but the consuming positive contract
    should fail until the accessor boundary handles that row shape.
    """

    run_tag: str
    qid: str
    row: dict
    joined_row: dict
    typed_evidence: PerQidRcaEvidence
    expected_card_violations: tuple[str, ...]
    field_sources_snapshot: dict[str, str]
    source_run_id: str
    source_qid: str


def _case_path(run_tag: str, qid: str) -> Path:
    return PRODUCTION_REPLAY_DIR / f"{run_tag}__{qid}.json"


def list_production_cases() -> tuple[tuple[str, str], ...]:
    """Return every committed ``(run_tag, qid)`` pair, sorted.

    Used by tests that want to enumerate the whole corpus rather than
    hard-coding QIDs. The sort makes the iteration deterministic so
    failure messages name cases in a stable order.
    """
    pairs: list[tuple[str, str]] = []
    for path in sorted(PRODUCTION_REPLAY_DIR.glob("*.json")):
        stem = path.stem
        if "__" not in stem:
            continue
        run_tag, qid = stem.split("__", 1)
        pairs.append((run_tag, qid))
    return tuple(pairs)


def load_production_case(run_tag: str, qid: str) -> ProductionCase:
    """Load one ``(run_tag, qid)`` case as a typed :class:`ProductionCase`.

    Raises ``FileNotFoundError`` if the case is not committed; the error
    message names the corpus directory so the failure is actionable from
    a test trace alone.
    """
    path = _case_path(run_tag, qid)
    if not path.exists():
        raise FileNotFoundError(
            f"Production replay case {run_tag}__{qid}.json not found in "
            f"{PRODUCTION_REPLAY_DIR}. To add a new case, follow "
            f"'How to add a new case' in {PRODUCTION_REPLAY_DIR / 'SCHEMA.md'}."
        )
    payload = json.loads(path.read_text())
    schema = payload.get("_schema_version") or ""
    if schema != "production_case_v1":
        raise ValueError(
            f"Production case {path} declares _schema_version={schema!r}; "
            f"loader expects 'production_case_v1'."
        )
    row = dict(payload["row"])
    joined_row = dict(row)
    joined_row.update(payload.get("joined_row_fields") or {})

    typed_payload = dict(payload["typed_evidence"])
    repair_hint = typed_payload.pop("repair_hint_patch_type")
    typed_evidence = PerQidRcaEvidence(
        qid=str(typed_payload["qid"]),
        observed_failure=str(typed_payload.get("observed_failure") or ""),
        generated_sql_issue=str(typed_payload.get("generated_sql_issue") or ""),
        expected_sql_shape=str(typed_payload.get("expected_sql_shape") or ""),
        blame_set=tuple(str(b) for b in typed_payload.get("blame_set") or ()),
        suggested_repair_family=str(typed_payload.get("suggested_repair_family") or ""),
        repair_hint_patch_type=PatchType[str(repair_hint)],
        confidence=str(typed_payload.get("confidence") or "high"),  # type: ignore[arg-type]
        quoted_evidence=tuple(str(q) for q in typed_payload.get("quoted_evidence") or ()),
    )

    provenance = payload.get("_provenance") or {}
    return ProductionCase(
        run_tag=run_tag,
        qid=str(payload["qid"]),
        row=row,
        joined_row=joined_row,
        typed_evidence=typed_evidence,
        expected_card_violations=tuple(
            str(v) for v in payload.get("expected_card_violations") or ()
        ),
        field_sources_snapshot=dict(provenance.get("field_sources_snapshot") or {}),
        source_run_id=str(provenance.get("source_run_id") or ""),
        source_qid=str(provenance.get("source_qid") or ""),
    )


def load_all_production_cases() -> tuple[ProductionCase, ...]:
    """Iterate every committed case in a stable order."""
    return tuple(
        load_production_case(run_tag, qid)
        for run_tag, qid in list_production_cases()
    )


def expected_violations_for(cases: Iterable[ProductionCase]) -> dict[str, tuple[str, ...]]:
    """Project ``{qid: expected_violations}`` for a set of cases.

    Convenience helper for tests that assert one violation set per QID.
    """
    return {c.qid: c.expected_card_violations for c in cases}


__all__ = [
    "PRODUCTION_REPLAY_DIR",
    "ProductionCase",
    "expected_violations_for",
    "list_production_cases",
    "load_all_production_cases",
    "load_production_case",
]
