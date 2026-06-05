"""P4 C4 unit tests — shared metadata-patch target resolver."""
from __future__ import annotations

from dataclasses import dataclass
from unittest import mock

import pytest

from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.metadata_target_resolver import (
    METADATA_PATCH_TYPES_WITH_TARGETS,
    resolve_metadata_patch_target,
    stamp_target_resolved_on_body,
    validate_and_stamp_metadata_patch_target,
)


@dataclass
class _StubDecision:
    applyable: bool
    reason: str = ""
    table: str = ""


def test_skipped_for_non_metadata_patch_type():
    verdict = resolve_metadata_patch_target(
        {"table": "main.sales.orders"},
        patch_type_wire="add_example_sql",
        metadata_snapshot={"some": "snapshot"},
    )
    assert verdict.outcome == "skipped"
    assert verdict.abstain_reason is None


def test_skipped_when_metadata_snapshot_empty():
    verdict = resolve_metadata_patch_target(
        {"table": "main.sales.orders"},
        patch_type_wire="update_column_description",
        metadata_snapshot={},
    )
    assert verdict.outcome == "skipped"


def test_skipped_when_body_uses_object_id_encoding():
    verdict = resolve_metadata_patch_target(
        {"object_id": "main.sales.orders:amount"},
        patch_type_wire="update_column_description",
        metadata_snapshot={"any": "thing"},
    )
    assert verdict.outcome == "skipped"
    assert "opaque" in verdict.error_message


def test_resolved_when_check_applyability_succeeds_and_no_stamp():
    body = {
        "table": "main.sales.orders",
        "column": "amount",
        "description": "Total amount paid by the customer.",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=True, table="main.sales.orders"
        ),
    ):
        verdict = resolve_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot={"main.sales.orders": {"columns": ["amount"]}},
        )
    assert verdict.outcome == "resolved"
    assert verdict.resolved_table == "main.sales.orders"
    assert verdict.resolved_column == "amount"
    # No stamping when stamp=False.
    assert "target_resolved" not in body


def test_unresolvable_when_table_missing():
    body = {
        "table": "main.sales.orders_typo",
        "column": "amount",
        "description": "x",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=False, reason="missing_table", table=""
        ),
    ):
        verdict = resolve_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot={"main.sales.orders": {"columns": ["amount"]}},
        )
    assert verdict.outcome == "unresolvable"
    assert verdict.abstain_reason == AbstainReason.TARGET_UNRESOLVABLE
    assert verdict.error_message == "missing_table"


def test_unresolvable_when_column_missing():
    body = {
        "table": "main.sales.orders",
        "column": "amount_typo",
        "description": "x",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=False,
            reason="invalid_column_target",
            table="main.sales.orders",
        ),
    ):
        verdict = resolve_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot={"main.sales.orders": {"columns": ["amount"]}},
        )
    assert verdict.outcome == "unresolvable"
    assert verdict.abstain_reason == AbstainReason.TARGET_UNRESOLVABLE


def test_validate_and_stamp_mutates_body_on_success():
    body = {
        "table": "main.sales.orders",
        "column": "amount",
        "description": "x",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=True, table="main.sales.orders"
        ),
    ):
        verdict = validate_and_stamp_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot={"main.sales.orders": {"columns": ["amount"]}},
        )
    assert verdict.outcome == "resolved"
    assert body["target_resolved"] is True


def test_validate_and_stamp_does_not_mutate_on_unresolvable():
    body = {
        "table": "main.sales.orders_typo",
        "column": "amount",
        "description": "x",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=False, reason="missing_table"
        ),
    ):
        verdict = validate_and_stamp_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot={"main.sales.orders": {"columns": ["amount"]}},
        )
    assert verdict.outcome == "unresolvable"
    assert "target_resolved" not in body


def test_stamp_target_resolved_helper_is_idempotent():
    body = {"table": "main.sales.orders"}
    stamp_target_resolved_on_body(
        body,
        resolved_table="main.sales.orders",
        resolved_column="amount",
    )
    assert body["target_resolved"] is True
    assert body["table"] == "main.sales.orders"
    assert body["column"] == "amount"
    # Second call must not change anything.
    body_snapshot = dict(body)
    stamp_target_resolved_on_body(
        body,
        resolved_table="main.sales.orders",
        resolved_column="amount",
    )
    assert body == body_snapshot


def test_metadata_patch_types_with_targets_covers_plan_list():
    """Plan-pinned: producer-side resolver must canonicalize all
    column-touching patch types."""
    for required in (
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "remove_column_synonym",
        "add_description",
        "update_description",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
        "add_join_spec",
        "update_join_spec",
        "remove_join_spec",
    ):
        assert required in METADATA_PATCH_TYPES_WITH_TARGETS


def test_target_unresolvable_abstain_reason_value():
    assert AbstainReason.TARGET_UNRESOLVABLE.value == "target_unresolvable"


# ---------------------------------------------------------------------
# Trial 21 W7 — bare-name canonicalization against deployed Genie config
# ---------------------------------------------------------------------


def test_w7_bare_table_name_canonicalizes_to_fqn_via_snapshot():
    """Run B postmortem (d13938e7): the LLM emitted
    ``{"table": "mv_7now_store_sales"}`` while the deployed Genie config
    stores the table as
    ``prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales``.
    Trial 21 W7: the resolver must canonicalize the bare name to the FQN
    when exactly one snapshot entry matches by trailing component, then
    proceed through applyability with the canonical name.
    """
    body = {
        "table": "mv_7now_store_sales",
        "column": "store_id",
        "description": "Store identifier.",
    }
    snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales",
                    "name": "mv_7now_store_sales",
                    "column_configs": [
                        {"column_name": "store_id"},
                    ],
                }
            ],
            "metric_views": [],
        }
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=True,
            table="prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales",
        ),
    ) as patched_check:
        verdict = validate_and_stamp_metadata_patch_target(
            body,
            patch_type_wire="add_column_description",
            metadata_snapshot=snapshot,
        )
    assert verdict.outcome == "resolved", (
        f"W7: bare-name MV must canonicalize to FQN; got {verdict}"
    )
    assert (
        verdict.resolved_table
        == "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales"
    )
    assert verdict.resolved_column == "store_id"
    assert body["target_resolved"] is True
    assert (
        body["table"]
        == "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales"
    ), "W7: body must be rewritten with the canonical FQN identifier"

    # The patched applyability check must have seen the canonical FQN
    # name, not the bare input — otherwise the canonicalization is
    # cosmetic only.
    assert patched_check.call_count == 1
    _kwargs = patched_check.call_args.kwargs or patched_check.call_args[1]
    canonical_patch = _kwargs.get("patch") or {}
    assert (
        canonical_patch.get("table")
        == "prashanth_subrahmanyam_catalog.sales_reports.mv_7now_store_sales"
    )


def test_w7_bare_name_with_metric_view_match():
    """The canonicalizer must also consider ``data_sources.metric_views``,
    not just ``data_sources.tables``."""
    body = {
        "table": "weekly_sales_metric",
        "column": "week_id",
        "description": "Week ISO id.",
    }
    snapshot = {
        "data_sources": {
            "tables": [],
            "metric_views": [
                {
                    "identifier": "main.metrics.weekly_sales_metric",
                    "name": "weekly_sales_metric",
                }
            ],
        }
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=True, table="main.metrics.weekly_sales_metric"
        ),
    ) as patched_check:
        verdict = validate_and_stamp_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot=snapshot,
        )
    assert verdict.outcome == "resolved"
    assert verdict.resolved_table == "main.metrics.weekly_sales_metric"
    _kwargs = patched_check.call_args.kwargs or patched_check.call_args[1]
    assert (
        _kwargs.get("patch", {}).get("table")
        == "main.metrics.weekly_sales_metric"
    )


def test_w7_bare_name_ambiguous_falls_through_unchanged():
    """If two snapshot entries share the same trailing component, the
    canonicalizer must NOT pick one; leave the bare name for the
    applier-side check to reject with full error context."""
    body = {
        "table": "orders",
        "column": "amount",
        "description": "x",
    }
    snapshot = {
        "data_sources": {
            "tables": [
                {"identifier": "main.sales.orders"},
                {"identifier": "main.shipping.orders"},
            ],
            "metric_views": [],
        }
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(
            applyable=False, reason="missing_table", table=""
        ),
    ) as patched_check:
        verdict = resolve_metadata_patch_target(
            body,
            patch_type_wire="add_column_description",
            metadata_snapshot=snapshot,
        )
    assert verdict.outcome == "unresolvable"
    _kwargs = patched_check.call_args.kwargs or patched_check.call_args[1]
    # Patch body must still carry the bare name (no auto-canonicalization
    # on ambiguity).
    assert _kwargs.get("patch", {}).get("table") == "orders"


def test_w7_fqn_input_unchanged_by_canonicalizer():
    """Inputs that already contain a ``.`` (presumed FQN) must pass
    through the canonicalizer unchanged."""
    body = {
        "table": "main.sales.orders",
        "column": "amount",
        "description": "x",
    }
    snapshot = {
        "data_sources": {
            "tables": [{"identifier": "main.sales.orders"}],
            "metric_views": [],
        }
    }
    with mock.patch(
        "genie_space_optimizer.optimization.patch_applyability."
        "check_patch_applyability",
        return_value=_StubDecision(applyable=True, table="main.sales.orders"),
    ) as patched_check:
        verdict = resolve_metadata_patch_target(
            body,
            patch_type_wire="update_column_description",
            metadata_snapshot=snapshot,
        )
    assert verdict.outcome == "resolved"
    _kwargs = patched_check.call_args.kwargs or patched_check.call_args[1]
    assert _kwargs.get("patch", {}).get("table") == "main.sales.orders"
