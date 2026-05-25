"""Trial 16.3 — Stage 3 (`synthesize`) must include
``forbidden_signatures`` in the LLM prompt payload.

Symmetric to ``test_cluster_plan11_forbidden_signatures.py`` for Stage
2. The Stage 3 prompt is the lever-selection prompt: it chooses
``patch_type`` and constructs the patch body. Without the typed
prior-iteration rejections in context, the lever LLM re-proposes the
same shapes that just got rejected. Postmortem 813949510175466
evidence: gs_013 emits ``dropped_no_op:missing_table`` for
``add_column_description``, then again later for
``update_column_description`` — different lever, same root cause.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    _build_request,
)


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="missing_metadata",
        member_qids=("gs_013",),
        unifying_evidence="",
        repair_hypothesis="",
        primary_blame_set=(),
        confidence="high",
    )


def test_build_request_includes_forbidden_signatures_in_user_prompt():
    """Stage 3 user_prompt JSON must carry the typed prior-iteration
    rejection strings so the lever LLM can reason about which
    patch_types have already been tried.

    Pre-Trial-16.3 the prompt JSON had only ``{iteration, cluster,
    member_qid_evidence, schema_slice, history}``. Post-Trial-16.3 the
    LLM also sees a ``forbidden_signatures: [...]`` field so it can
    explicitly avoid re-proposing patch_types whose typed rejection
    appears there.
    """
    forbidden = (
        "add_column_description:dropped_no_op:missing_table",
        "update_column_description:dropped_no_op:missing_table",
    )
    request = _build_request(
        cluster=_make_cluster(),
        schema_slice={},
        member_qid_evidence=[],
        history=[],
        iteration=2,
        forbidden_signatures=forbidden,
    )
    payload = json.loads(request.user_prompt)
    assert "forbidden_signatures" in payload, (
        "Stage 3 user_prompt JSON must carry a 'forbidden_signatures' "
        f"key — got keys {sorted(payload.keys())!r}"
    )
    assert list(payload["forbidden_signatures"]) == list(forbidden), (
        f"Stage 3 user_prompt 'forbidden_signatures' must round-trip "
        f"the prior-iteration typed-rejection strings — got "
        f"{payload['forbidden_signatures']!r}"
    )


def test_build_request_default_forbidden_signatures_is_empty_list():
    """Default empty case: prompt schema is stable, LLM sees [] instead
    of a missing field."""
    request = _build_request(
        cluster=_make_cluster(),
        schema_slice={},
        member_qid_evidence=[],
        history=[],
        iteration=1,
    )
    payload = json.loads(request.user_prompt)
    assert payload.get("forbidden_signatures") == [], (
        f"Default Stage 3 'forbidden_signatures' must serialize to [] — "
        f"got {payload.get('forbidden_signatures')!r}"
    )
