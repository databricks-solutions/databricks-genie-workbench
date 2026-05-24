"""Phase 7 — ``Plan11DiagnoseOutput`` Pydantic schema-cap audit + truncation contract.

Trial 11 (run ``98ec8950-...``) postmortem ``analysis_inputs.json``
flagged ``plan11_stage1_field_length_errors`` for at least one
``evidence_summary`` response from production. The pydantic
``ValidationError`` it raised was caught by the legacy error
classifier as a generic ``llm_error`` rather than a recoverable
shape mismatch, so the shadow Plan 11 lane silently lost diagnoses
that would otherwise have driven downstream stages.

Two independent contracts:

1. **Static schema audit** — every string field on
   ``Plan11DiagnoseOutput.DiagnosisItem`` must have a
   ``max_length`` large enough to absorb realistic LLM verbosity.
   The audit pins a floor of 200 chars for every string field and
   a floor of 1000 chars for the four narrative fields the
   plan11_diagnose prompt explicitly invites multi-sentence
   answers on. Fails today on multiple fields; the XFAIL strict
   marker keeps the suite green until the follow-up cap-bump PR.

2. **Truncation contract** — invoking
   ``Plan11DiagnoseOutput.model_validate(...)`` against an
   over-long payload must NOT raise ``ValidationError``; the
   payload should round-trip via graceful truncation. Today the
   call raises. Pinned with XFAIL strict so the follow-up
   adapter-truncation PR is forced to remove the marker when it
   lands.

Why a static audit AND a truncation test?  Either can land
independently. If the caps are raised to absorb all observed
production strings, the truncation adapter becomes redundant
(but harmless). If the truncation adapter lands without the cap
bump, validation continues to live downstream of the truncator —
the caps still matter because they describe the contract the
prompt promises the LLM.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    DiagnosisItem,
    Plan11DiagnoseOutput,
)

# Floors keyed off realistic LLM behaviour observed in Trial 11
# postmortem: production strings up to ~1.5k chars were emitted for
# narrative fields. 200 chars is the universal minimum (one to two
# sentences). 1000 chars is the floor for fields the prompt asks for
# a multi-sentence explanation on.

MIN_STRING_CAP = 200

NARRATIVE_FIELDS = frozenset({
    "observed_failure",
    "generated_sql_issue",
    "expected_sql_shape",
    "evidence_summary",
})
MIN_NARRATIVE_CAP = 1000

# Fields exempt from the audit:
# - ``qid``: machine-emitted identifier; bounded by the upstream
#   admission predicate.
# - ``rca_kind_label``: closed-vocab tag; intentionally short.
EXEMPT_FROM_CAP_AUDIT = frozenset({"qid", "rca_kind_label"})


def _max_length_of(field_name: str) -> int | None:
    """Return the declared ``max_length`` for a ``DiagnosisItem`` field
    or ``None`` if no cap is declared.

    Pydantic v2 exposes field-level metadata through
    ``model_fields[<name>].metadata`` (a list of constraint
    descriptors). We scan for the ``MaxLen`` descriptor; this is
    forward-compatible with Pydantic versions that fold ``max_length``
    into ``StringConstraints`` rather than a top-level kwarg.
    """
    info = DiagnosisItem.model_fields.get(field_name)
    if info is None:
        return None
    for entry in getattr(info, "metadata", ()) or ():
        max_length = getattr(entry, "max_length", None)
        if isinstance(max_length, int):
            return max_length
    direct = getattr(info, "max_length", None)
    return int(direct) if isinstance(direct, int) else None


# ── Static schema audit ───────────────────────────────────────────────


def test_every_diagnosis_item_string_field_is_introspectable() -> None:
    """The audit relies on Pydantic field metadata. If a future
    refactor moves the caps onto a different annotation site, the
    audit must be updated rather than silently passing because
    ``_max_length_of`` returns ``None`` for every field.

    This sentinel test fails fast in that scenario.
    """
    expected_fields = {
        "qid",
        "rca_kind_label",
        "observed_failure",
        "generated_sql_issue",
        "expected_sql_shape",
        "evidence_summary",
    }
    declared = set(DiagnosisItem.model_fields.keys())
    missing = expected_fields - declared
    assert not missing, (
        f"Plan11DiagnoseOutput.DiagnosisItem dropped fields the audit "
        f"depends on: {sorted(missing)}. Update the audit or the "
        f"schema; today's drift breaks one of the two."
    )


def test_diagnosis_item_string_caps_absorb_production_verbosity() -> None:
    """Every narrative string field must carry ``max_length >= 1000``;
    every other string field must carry ``max_length >= 200``.

    Failing today on observed_failure=200, generated_sql_issue=300,
    expected_sql_shape=300, evidence_summary=400. The XFAIL strict
    marker means the moment any one of these is raised to clear the
    floor, the suite goes red and the maintainer must address the
    rest (or update the audit list).
    """
    violations: list[str] = []
    for field_name in DiagnosisItem.model_fields:
        if field_name in EXEMPT_FROM_CAP_AUDIT:
            continue
        cap = _max_length_of(field_name)
        if cap is None:
            # No cap is acceptable — the audit only fails on tight caps.
            continue
        floor = (
            MIN_NARRATIVE_CAP if field_name in NARRATIVE_FIELDS else MIN_STRING_CAP
        )
        if cap < floor:
            violations.append(
                f"{field_name}: max_length={cap} < floor={floor}"
            )

    assert not violations, (
        "Plan11DiagnoseOutput.DiagnosisItem cap audit failed:\n  "
        + "\n  ".join(violations)
        + "\nRaise the caps or land the graceful-truncation adapter."
    )


# ── Truncation contract ───────────────────────────────────────────────


def _overlong_diagnose_payload(*, multiplier: int = 10) -> dict:
    """Build the same shape ``diagnose_overlong_response_tape`` emits.

    Used to validate the typed schema directly so the contract
    exists independently of whether the tape harness routes through
    Pydantic.
    """
    return {
        "diagnoses": [
            {
                "qid": "domain_a_gs_009",
                "rca_kind_label": "wrong_aggregation",
                "observed_failure": "x" * (MIN_NARRATIVE_CAP * multiplier),
                "generated_sql_issue": "x" * (MIN_NARRATIVE_CAP * multiplier),
                "expected_sql_shape": "x" * (MIN_NARRATIVE_CAP * multiplier),
                "blame_set": ["main.public.orders.amount"],
                "evidence_summary": "x" * (MIN_NARRATIVE_CAP * multiplier),
                "confidence": "high",
            }
        ]
    }


def test_overlong_payload_truncates_via_phase6_validator() -> None:
    """Trial 13 Phase 6 (post-fix sentinel) — overlong payloads no
    longer raise ``ValidationError``; the graceful-truncation
    ``field_validator(mode="before")`` clips each oversized narrative
    to its declared cap and the response parses cleanly.

    This test used to be the ``test_overlong_payload_raises_today``
    sentinel pinning the bug. Phase 6 of Trial 13 lands the
    truncation adapter so it now asserts the *post-fix* observable.
    """
    payload = _overlong_diagnose_payload()
    parsed = Plan11DiagnoseOutput.model_validate(payload)
    assert parsed.diagnoses, "truncation must preserve the list"


def test_overlong_payload_should_truncate_not_raise() -> None:
    """Target contract — ``Plan11DiagnoseOutput.model_validate(...)``
    on an overlong payload must succeed by truncating each
    string field to its declared cap rather than raising
    ``ValidationError``.

    The truncated values must still round-trip through
    ``model_dump`` to a non-empty payload — i.e. truncation, not
    deletion.
    """
    payload = _overlong_diagnose_payload()
    parsed = Plan11DiagnoseOutput.model_validate(payload)

    assert parsed.diagnoses, (
        "Truncation must preserve the diagnoses list; got empty."
    )
    for item in parsed.diagnoses:
        for field_name in NARRATIVE_FIELDS:
            value = getattr(item, field_name, "")
            cap = _max_length_of(field_name)
            if cap is not None:
                assert len(value) <= cap, (
                    f"{field_name}: truncated value length "
                    f"{len(value)} exceeds declared cap {cap}"
                )
            assert value, (
                f"{field_name}: truncation produced empty string "
                f"({value!r}); should preserve a non-empty prefix."
            )
