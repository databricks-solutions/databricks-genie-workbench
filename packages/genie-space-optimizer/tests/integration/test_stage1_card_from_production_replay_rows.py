"""Phase 3 — Stage 1 card-completeness from production-grounded rows.

Trial 11 (run ``98ec8950-...``) and Trial 12 (run ``dc89d1a9-...``)
both shipped a Genie deploy whose Stage 1 input card came up
``question_text_empty`` for every hard QID. The pre-existing forward
harness validated the card against a synthetic ``hydration_rows``
fixture that bakes the question into every row, so the failure was
invisible locally.

This module re-anchors the contract on real per-(run, qid) replay
snapshots committed under
``tests/integration/fixtures/production_replay/``. Each case is a
sanitized capture of exactly the row, the upstream typed RCA
evidence, and the field-source / violation summary that
``analysis_inputs_*.json::stage1_input_card_sample`` reported in
production. Two layers of assertion sit on the corpus:

* :func:`test_card_violations_today_match_postmortem_snapshot` —
  pins the **current** broken state. Passes today; turns into a
  regression alarm if the card builder accidentally heals one of the
  field sources without addressing the rest.
* :func:`test_card_violations_for_production_replay_should_be_empty` —
  pins the live positive contract. The replay rows carry the real
  ``request.question`` path directly, so the Stage 1 card must be
  violation-free for every case.

See ``packages/genie-space-optimizer/.cursor/plans/production-grounded_harness_*``
for the plan; see ``tests/integration/fixtures/production_replay/SCHEMA.md``
for the case-file format and sanitization rules.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    build_stage1_evidence_card,
)
from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
)
from tests.integration.replay_row_loader import (
    PRODUCTION_REPLAY_DIR,
    ProductionCase,
    list_production_cases,
    load_all_production_cases,
    load_production_case,
)
from tests.integration.replay_row_sanitizer import load_case_payload


# ── Parametrization helpers ───────────────────────────────────────────


def _case_id(case: ProductionCase) -> str:
    return f"{case.run_tag}__{case.qid}"


_ALL_CASES = load_all_production_cases()


@pytest.fixture(scope="module")
def production_cases() -> tuple[ProductionCase, ...]:
    """Module-scoped tuple of every committed production case.

    Tests should prefer parametrization over iterating this fixture so
    a single failing case names itself in the pytest report.
    """
    return _ALL_CASES


# ── Corpus health ─────────────────────────────────────────────────────


def test_production_replay_corpus_is_non_empty() -> None:
    """The corpus must carry at least the seven cases the plan committed.

    A bare corpus is itself a regression — Phase 3 / Phase 4 tests are
    parametrized over it, so an empty corpus turns every downstream
    test into a silent no-op.
    """
    pairs = list_production_cases()
    assert len(pairs) >= 7, (
        f"Production replay corpus has only {len(pairs)} case(s); "
        f"Phase 1 committed 7 (3 from 98ec, 4 from dc89). Inspect "
        f"tests/integration/fixtures/production_replay/."
    )


def test_every_committed_case_loads_cleanly() -> None:
    """Every ``(run_tag, qid)`` pair the loader enumerates must round-trip.

    Validates the case-file schema version and exercises the typed
    ``PerQidRcaEvidence`` reconstruction so a malformed fixture fails
    at the corpus boundary rather than deep inside a parametrized
    contract test.
    """
    for run_tag, qid in list_production_cases():
        case = load_production_case(run_tag, qid)
        assert case.qid, f"{run_tag}__{qid}.json missing qid"
        assert case.typed_evidence.qid == case.qid, (
            f"{run_tag}__{qid}.json: typed_evidence.qid="
            f"{case.typed_evidence.qid!r} disagrees with case.qid={case.qid!r}"
        )


def test_cases_use_real_captured_rows_not_join_shortcuts() -> None:
    """Replay cases must pin the real eval-row shape.

    The production failure was an eval-row accessor gap, so a fixture
    that hydrates evidence via invented ``joined_row_fields`` can go
    green while the deployed row still fails. Each case's bare ``row``
    must carry the captured ``request.question`` path directly.
    """
    for run_tag, qid in list_production_cases():
        path = PRODUCTION_REPLAY_DIR / f"{run_tag}__{qid}.json"
        payload = load_case_payload(path)
        row = payload.get("row") or {}
        request = row.get("request") if isinstance(row, dict) else None
        assert isinstance(request, dict), f"{path.name}: row.request missing"
        assert str(request.get("question") or "").strip(), (
            f"{path.name}: row.request.question missing; replay fixture "
            f"must use the captured production row, not joined_row_fields"
        )
        assert "joined_row_fields" not in payload, (
            f"{path.name}: remove joined_row_fields; the row itself must "
            f"contain the real production evidence paths"
        )


# ── Today's contract (passes today; guards against silent drift) ──────


@pytest.mark.integration
@pytest.mark.parametrize("case", _ALL_CASES, ids=_case_id)
def test_card_violations_today_match_postmortem_snapshot(
    case: ProductionCase,
) -> None:
    """Card-builder output today must equal each case's snapshot.

    Each case file records the violations the canonical builder
    produced when the postmortem was taken (``question_text_empty``
    for all seven). This test pins that broken state: if the builder
    starts producing a different violation set without an explicit
    case-file update, the change is silently re-shaping what the
    harness considers "the production failure mode" and almost
    certainly indicates an incomplete fix.

    When the follow-up multi-source ``row_question`` ladder lands,
    every case's ``expected_card_violations`` is updated to ``[]`` in
    the same diff. This test then passes vacuously and the next
    function below becomes the load-bearing positive contract.
    """
    card = build_stage1_evidence_card(
        case.qid, case.joined_row, typed_evidence=case.typed_evidence
    )
    actual = tuple(v.field for v in DEFAULT_STAGE1_CONTRACT.validate(card))
    expected = case.expected_card_violations
    assert actual == expected, (
        f"Production replay case {case.run_tag}__{case.qid}: card "
        f"violations drifted from the postmortem snapshot.\n"
        f"  expected (from case file): {list(expected)}\n"
        f"  actual   (from builder)  : {list(actual)}\n"
        f"  field_sources_today      : "
        f"{DEFAULT_STAGE1_CONTRACT.field_sources(card)}\n"
        f"If this drift is intentional (e.g. an implementation PR "
        f"fixed one of the field sources), update the case file's "
        f"expected_card_violations in the same commit."
    )


# ── Positive contract ──────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.parametrize("case", _ALL_CASES, ids=_case_id)
def test_card_violations_for_production_replay_should_be_empty(
    case: ProductionCase,
) -> None:
    """The card builder must produce a violation-free card for every
    production case.

    This is the contract that prevents a fifth row-shape surprise: the
    fixture row itself carries the production ``request.question`` path
    plus SQL, ASI metadata, and judge-rationale surfaces. Any future
    drift must fail here before deployment.
    """
    card = build_stage1_evidence_card(
        case.qid, case.joined_row, typed_evidence=case.typed_evidence
    )
    violations = DEFAULT_STAGE1_CONTRACT.validate(card)
    assert violations == [], (
        f"Production replay case {case.run_tag}__{case.qid} must "
        f"hydrate every Stage 1 contract field once the multi-source "
        f"ladder lands.\n"
        f"  violation fields : "
        f"{[v.field for v in violations]}\n"
        f"  field_sources    : "
        f"{DEFAULT_STAGE1_CONTRACT.field_sources(card)}\n"
        f"Hint: inspect the captured row path before adding another "
        f"ad hoc accessor branch."
    )


# ── Sanitization audit (catches accidental customer-literal leaks) ────


def test_production_replay_corpus_has_no_forbidden_literals() -> None:
    """No committed case file may carry raw customer-domain literals.

    Walks every case JSON and greps for the audit list defined in
    :mod:`tests.integration.replay_row_sanitizer`. ``_provenance``
    subtrees are exempt — they intentionally name the source run id /
    source QID so engineers can trace a fixture back to its postmortem.
    """
    from tests.integration.replay_row_loader import PRODUCTION_REPLAY_DIR
    from tests.integration.replay_row_sanitizer import (
        find_forbidden_literals,
        load_case_payload,
    )

    failures: list[str] = []
    for path in sorted(PRODUCTION_REPLAY_DIR.glob("*.json")):
        payload = load_case_payload(path)
        findings = find_forbidden_literals(payload)
        if findings:
            for json_path, literal in findings:
                failures.append(
                    f"{path.name}: {literal!r} at {json_path}"
                )
    assert not failures, (
        "Forbidden customer literals found in production replay corpus:\n"
        + "\n".join(f"  {f}" for f in failures)
        + "\nUpdate the substitution rules in replay_row_sanitizer.py "
        "and re-sanitize the offending case file."
    )
