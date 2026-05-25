"""Trial 16.3 — Stage 2 (`cluster_diagnoses`) must include
``forbidden_signatures`` in the LLM prompt payload.

Why this test exists:
    The producer side of the typed-feedback channel is wired correctly
    (applier_gate, evaluated_gate, acceptance_gate, synthesize_llm all
    set ``TerminalRecord.forbidden_signature`` to typed strings like
    ``add_column_description:dropped_no_op:missing_table``), but the
    consumer side is dead-ended. ``Stage2BatchInput.forbidden_signatures``
    is populated from ``ctx.forbidden_signatures`` (cluster_batch.py:264)
    but ``cluster_diagnoses`` in ``stages/cluster_plan11.py`` does not
    accept the kwarg — so the LLM never sees the prior-iteration
    forbidden signatures.

    Production postmortem 813949510175466 evidence: gs_013 emits
    ``dropped_no_op:missing_table`` for ``add_column_description`` in
    one iteration, then again for ``update_column_description`` in a
    later iteration. If the channel were end-to-end, the strategist
    would have learned that ``missing_table`` was the underlying issue
    and proposed a different lever.

The fix is narrow: thread ``forbidden_signatures`` through
``cluster_diagnoses`` → ``_build_request`` and add a top-level key to
the Stage 2 JSON user_prompt so the LLM can reason about them.
Architectural principle: LLM for reasoning, code for validation. We
plumb the strings; the model decides what to do with them.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages.cluster_plan11 import (
    _build_request,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    PerQidDiagnosis,
)


def _make_diagnosis(qid: str) -> PerQidDiagnosis:
    return PerQidDiagnosis(
        qid=qid,
        rca_kind_label="missing_metadata",
        observed_failure="",
        generated_sql_issue="",
        expected_sql_shape="",
        blame_set=(),
        evidence_summary="",
        confidence="high",
    )


def test_build_request_includes_forbidden_signatures_in_user_prompt():
    """Stage 2 user_prompt JSON must carry a ``forbidden_signatures``
    field with the prior-iteration typed-rejection strings.

    Pre-Trial-16.3 the ``_build_request`` signature did not accept the
    kwarg and the prompt JSON had only ``{iteration, namespace,
    per_qid_diagnosis, schema_columns}``. Post-Trial-16.3 the strategist
    sees the strings and can reason "the prior iteration tried
    add_column_description and applier rejected with dropped_no_op:
    missing_table, so the underlying issue is missing_table — don't
    cluster this hard QID around add_column_description again".
    """
    forbidden = (
        "add_column_description:dropped_no_op:missing_table",
        "update_column_description:dropped_no_op:missing_table",
    )
    request = _build_request(
        diagnoses=[_make_diagnosis("gs_013"), _make_diagnosis("gs_026")],
        schema_columns=["a", "b"],
        iteration=2,
        namespace="hard",
        forbidden_signatures=forbidden,
    )
    payload = json.loads(request.user_prompt)
    assert "forbidden_signatures" in payload, (
        "Stage 2 user_prompt JSON must carry a 'forbidden_signatures' "
        f"key for the LLM to reason about — got keys "
        f"{sorted(payload.keys())!r}"
    )
    assert list(payload["forbidden_signatures"]) == list(forbidden), (
        f"Stage 2 user_prompt 'forbidden_signatures' must round-trip the "
        f"prior-iteration typed-rejection strings — got "
        f"{payload['forbidden_signatures']!r}"
    )


def test_build_request_default_forbidden_signatures_is_empty_tuple():
    """When the caller does not supply ``forbidden_signatures``, the
    field still appears in the prompt (as an empty list) so the
    prompt schema is stable across iterations and the LLM sees an
    explicit "no prior forbidden signatures" signal rather than a
    missing field."""
    request = _build_request(
        diagnoses=[_make_diagnosis("gs_001")],
        schema_columns=[],
        iteration=1,
        namespace="hard",
    )
    payload = json.loads(request.user_prompt)
    assert payload.get("forbidden_signatures") == [], (
        f"Default 'forbidden_signatures' must serialize to [] for prompt "
        f"schema stability — got {payload.get('forbidden_signatures')!r}"
    )
