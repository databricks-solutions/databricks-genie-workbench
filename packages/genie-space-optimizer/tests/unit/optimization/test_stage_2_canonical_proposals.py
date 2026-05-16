"""Each Stage-2 adapter must emit proposals with canonical
``patch_type`` and ``target`` keys. Trial-5 Run B proved that adapters
that stored ``_patch_type`` (underscore-prefixed) caused 100% of
proposals to be dropped by ``narrow_replacement_diagnosis``."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.activation_bundle import (
    ActivationBundle,
)


def _make_bundle(
    skill_id: str,
    targets: tuple[str, ...] = ("catalog.schema.t1",),
) -> ActivationBundle:
    return ActivationBundle(
        skill_id=skill_id,
        ag_id="AG_PIPELINE",
        target_objects=targets,
        cluster_afs=(
            {"cluster_id": "C1", "failure_type": "wrong_column"},
        ),
        metadata_snapshot={
            "tables": [], "metric_views": [], "functions": [],
            "data_sources": {}, "instructions": {"text_instructions": []},
            "_active_ag_id": "AG_PIPELINE",
            "config": {},
        },
        identifier_allowlist="catalog.schema.t1",
        evidence_refs=("trace://q1",),
        expected_impact_qids=("Q1",),
        raw_evidence=(),
        lever_directives_legacy=None,
        discovery_rationale="r",
        priority=1,
    )


@patch(
    "genie_space_optimizer.optimization.optimizer._call_llm_for_proposal",
    return_value={"proposed_value": "v", "rationale": "r"},
)
def test_stage_2_l1_proposals_carry_canonical_patch_type(_mock_llm):
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l1,
    )
    bundle = _make_bundle("lever-1-table-column-description")
    result = _stage_2_l1(bundle, w=MagicMock())
    assert result["proposals"], "adapter must return non-empty proposals"
    for proposal in result["proposals"]:
        assert proposal.get("patch_type"), (
            f"L1 proposal missing patch_type: keys={list(proposal)}"
        )
        assert "_patch_type" not in proposal
        assert proposal.get("target") == "catalog.schema.t1"
        assert proposal["provenance"]["skill_id"] == "lever-1-table-column-description"


@patch(
    "genie_space_optimizer.optimization.optimizer._call_llm_for_proposal",
    return_value={"proposed_value": "v", "rationale": "r"},
)
def test_stage_2_l2_proposals_carry_canonical_patch_type(_mock_llm):
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l2,
    )
    bundle = _make_bundle(
        "lever-2-mv-column-refinement",
        targets=("mv.catalog.schema.fact.amount",),
    )
    result = _stage_2_l2(bundle, w=MagicMock())
    assert result["proposals"]
    for proposal in result["proposals"]:
        assert proposal.get("patch_type") == "add_column_description"
        assert "_patch_type" not in proposal
        assert proposal["target"] == "mv.catalog.schema.fact.amount"


@patch(
    "genie_space_optimizer.optimization.optimizer._call_llm_for_proposal",
    return_value={"proposed_value": "v", "rationale": "r"},
)
def test_stage_2_l3_proposals_carry_canonical_patch_type(_mock_llm):
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l3,
    )
    bundle = _make_bundle(
        "lever-3-tvf-routing",
        targets=("catalog.schema.tvf_revenue_for_year",),
    )
    result = _stage_2_l3(bundle, w=MagicMock())
    assert result["proposals"]
    for proposal in result["proposals"]:
        assert proposal.get("patch_type") == "add_tvf_description"
        assert proposal["target"] == "catalog.schema.tvf_revenue_for_year"


@patch(
    "genie_space_optimizer.optimization.optimizer._call_llm_for_join_discovery",
    return_value=[{"join_spec": {"left": "a", "right": "b"}, "rationale": "r"}],
)
def test_stage_2_l4_proposals_carry_canonical_patch_type(_mock_join):
    """L4 (join discovery) returns join-spec proposals — canonical
    patch_type is ``add_join_spec``."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l4,
    )
    bundle = _make_bundle(
        "lever-4-join-discovery",
        targets=("catalog.schema.fact_orders", "catalog.schema.dim_customer"),
    )
    result = _stage_2_l4(bundle, w=MagicMock())
    assert result["proposals"], "L4 must return proposals when the join generator does"
    for proposal in result["proposals"]:
        assert proposal.get("patch_type") == "add_join_spec"
        assert proposal["provenance"]["skill_id"] == "lever-4-join-discovery"


@patch(
    "genie_space_optimizer.optimization.optimizer._call_llm_for_lever_5a_instructions",
    return_value={"instruction_text": "Always filter by active=true", "rationale": "r"},
)
def test_stage_2_l5a_proposal_carries_canonical_patch_type(_mock):
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l5a,
    )
    bundle = _make_bundle("lever-5a-instructions", targets=())
    result = _stage_2_l5a(bundle, w=MagicMock())
    assert result["proposals"], "L5a must return a single AG-level proposal"
    p = result["proposals"][0]
    # L5a is AG-scoped — its canonical patch_type is add_instruction; target is "".
    assert p.get("patch_type") == "add_instruction"
    assert p.get("target") == ""


@patch(
    "genie_space_optimizer.optimization.optimizer._dispatch_lever_5b_for_cluster",
    return_value=[{"example_sql": "SELECT * FROM t", "rationale": "r"}],
)
def test_stage_2_l5b_proposals_carry_canonical_patch_type(_mock):
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l5b,
    )
    bundle = _make_bundle("lever-5b-example-sql", targets=("",))
    result = _stage_2_l5b(bundle, w=MagicMock())
    assert result["proposals"]
    for p in result["proposals"]:
        assert p.get("patch_type") == "add_example_sql"
        assert p["provenance"]["skill_id"] == "lever-5b-example-sql"


@patch(
    "genie_space_optimizer.optimization.optimizer._generate_lever6_proposal",
    return_value={
        "patch_type": "add_sql_snippet_measure",
        "target": "mv.fact.revenue",
        "proposed_value": "SUM(amount)",
        "provenance": {"skill_id": "lever-6-sql-expression", "lever": 6},
    },
)
def test_stage_2_l6_idempotent_on_already_canonical_proposals(_mock):
    """L6's generator already returns a canonical proposal. The
    canonicalize call must be a no-op on it (idempotent)."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_l6,
    )
    bundle = _make_bundle(
        "lever-6-sql-expression", targets=("mv.fact.revenue",),
    )
    result = _stage_2_l6(bundle, w=MagicMock())
    assert len(result["proposals"]) == 1
    p = result["proposals"][0]
    assert p["patch_type"] == "add_sql_snippet_measure"
    # provenance.lever must be preserved by the canonicalizer.
    assert p["provenance"].get("lever") == 6


@pytest.mark.parametrize("adapter_name,skill_id", [
    ("_stage_2_l1", "lever-1-table-column-description"),
    ("_stage_2_l2", "lever-2-mv-column-refinement"),
    ("_stage_2_l3", "lever-3-tvf-routing"),
    ("_stage_2_l5a", "lever-5a-instructions"),
    ("_stage_2_l5b", "lever-5b-example-sql"),
    ("_stage_2_l6", "lever-6-sql-expression"),
])
def test_no_adapter_leaks_underscore_prefixed_keys(adapter_name, skill_id):
    """For every adapter, the canonical output must NOT contain
    ``_patch_type`` or ``_target`` keys — those are exactly the keys
    that broke ``narrow_replacement_diagnosis`` in Trial-5 Run B."""
    module = importlib.import_module(
        "genie_space_optimizer.optimization.three_stage_pipeline"
    )
    adapter = getattr(module, adapter_name)

    # Each adapter has its own LLM patch target. Mock them all generously.
    with patch(
        "genie_space_optimizer.optimization.optimizer._call_llm_for_proposal",
        return_value={"proposed_value": "v", "rationale": "r"},
    ), patch(
        "genie_space_optimizer.optimization.optimizer._call_llm_for_lever_5a_instructions",
        return_value={"instruction_text": "x", "rationale": "r"},
    ), patch(
        "genie_space_optimizer.optimization.optimizer._dispatch_lever_5b_for_cluster",
        return_value=[{"example_sql": "SELECT 1", "rationale": "r"}],
    ), patch(
        "genie_space_optimizer.optimization.optimizer._generate_lever6_proposal",
        return_value={
            "patch_type": "add_sql_snippet_measure", "target": "mv.t.r",
            "proposed_value": "SUM(x)", "provenance": {"lever": 6},
        },
    ):
        bundle = _make_bundle(skill_id)
        result = adapter(bundle, w=MagicMock())

    for proposal in result.get("proposals", []):
        assert "_patch_type" not in proposal, (
            f"{adapter_name}: proposal leaked _patch_type — canonicalizer not wired"
        )
        assert "_target" not in proposal, (
            f"{adapter_name}: proposal leaked _target"
        )
        # Every canonical proposal must carry a non-empty patch_type.
        assert proposal.get("patch_type"), (
            f"{adapter_name}: proposal missing canonical patch_type"
        )
