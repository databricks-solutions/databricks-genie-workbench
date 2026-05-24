"""Trial 13l — production injector unit tests.

Covers all four members of :data:`SCHEMA_COLUMNS_INJECTION_SOURCES`
plus the asymmetric write semantic (overwrite on ``genie_api``;
preserve any prior value on every other source).

Why no ``already_present`` case: the injector deliberately omits an
``already_present`` short-circuit so every iteration re-fetches and
post-Stage-3-apply schema drift is reflected before the next Stage 1.
A vocabulary-size regression test guards against that label being
re-added by accident.
"""
from __future__ import annotations

from unittest import mock

from genie_space_optimizer.optimization.schema_columns import (
    SCHEMA_COLUMNS_INJECTION_SOURCES,
    inject_schema_columns_into_metadata_snapshot,
)


def _fake_serialized_space_with_two_fqns() -> dict:
    """A ``fetch_space_config`` response yielding exactly 2 FQNs."""
    parsed = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.airlines.coupons",
                    "column_configs": [
                        {"column_name": "ORIG_AIRPORT_CD"},
                        {"column_name": "DEST_AIRPORT_CD"},
                    ],
                },
            ],
        },
    }
    return {
        "space_id": "01f143dfbeec15a3a0e87ced8662f4ed",
        "serialized_space": parsed,
        "_parsed_space": parsed,
    }


def _fake_serialized_space_with_three_fqns() -> dict:
    parsed = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.airlines.coupons",
                    "column_configs": [
                        {"column_name": "ORIG_AIRPORT_CD"},
                        {"column_name": "DEST_AIRPORT_CD"},
                        {"column_name": "ROUTE_ID"},
                    ],
                },
            ],
        },
    }
    return {
        "space_id": "01f143dfbeec15a3a0e87ced8662f4ed",
        "serialized_space": parsed,
        "_parsed_space": parsed,
    }


def _fake_serialized_space_with_zero_parseable_tables() -> dict:
    """Genie config that parses but yields zero 4-part FQNs."""
    parsed = {
        "data_sources": {
            "tables": [
                # Identifier is not 3-part → silently dropped by the extractor.
                {"identifier": "no_catalog_no_schema", "columns": [{"name": "x"}]},
            ],
        },
    }
    return {
        "space_id": "01f143dfbeec15a3a0e87ced8662f4ed",
        "serialized_space": parsed,
        "_parsed_space": parsed,
    }


def test_injects_when_metadata_snapshot_empty() -> None:
    """Happy path: empty snapshot + successful fetch → mutation + ``genie_api``."""
    snapshot: dict = {}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space_with_two_fqns(),
    ) as patched:
        injected, source, count, latency_ms = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id="01f143dfbeec15a3a0e87ced8662f4ed",
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_called_once()
    assert injected is True
    assert source == "genie_api"
    assert count == 2
    assert latency_ms >= 0
    assert snapshot["schema_columns"] == (
        "main.airlines.coupons.ORIG_AIRPORT_CD",
        "main.airlines.coupons.DEST_AIRPORT_CD",
    )


def test_overwrites_when_metadata_snapshot_already_has_schema_columns() -> None:
    """The critical iter-N+1-post-apply case: live Genie wins over stale value."""
    stale = ("main.airlines.coupons.OLD_COLUMN",)
    snapshot: dict = {"schema_columns": stale}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space_with_three_fqns(),
    ) as patched:
        injected, source, count, _ = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id="01f143dfbeec15a3a0e87ced8662f4ed",
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_called_once()
    assert injected is True
    assert source == "genie_api"
    assert count == 3
    # Overwrite, not merge: prior value is gone, new value is exactly the
    # freshly-fetched tuple.
    assert snapshot["schema_columns"] == (
        "main.airlines.coupons.ORIG_AIRPORT_CD",
        "main.airlines.coupons.DEST_AIRPORT_CD",
        "main.airlines.coupons.ROUTE_ID",
    )
    assert snapshot["schema_columns"] != stale


def test_preserves_prior_value_on_api_error() -> None:
    """API failure: prior iteration's value retained, no exception."""
    prior = ("main.airlines.coupons.PRIOR_COLUMN",)
    snapshot: dict = {"schema_columns": prior}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=RuntimeError("Genie API outage"),
    ) as patched:
        injected, source, count, _ = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id="01f143dfbeec15a3a0e87ced8662f4ed",
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_called_once()
    assert injected is False
    assert source == "api_error"
    assert count == 0
    # Preserve: graceful degradation through transient failures.
    assert snapshot["schema_columns"] == prior


def test_preserves_prior_value_on_empty_extract() -> None:
    """Parseable response with zero 4-part FQNs is a no-op for mutation."""
    prior = ("main.airlines.coupons.PRIOR_COLUMN",)
    snapshot: dict = {"schema_columns": prior}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space_with_zero_parseable_tables(),
    ) as patched:
        injected, source, count, _ = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id="01f143dfbeec15a3a0e87ced8662f4ed",
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_called_once()
    assert injected is False
    assert source == "empty_extract"
    assert count == 0
    assert snapshot["schema_columns"] == prior


def test_no_op_when_space_id_empty() -> None:
    """Caller bug guard: empty space_id short-circuits before any API call."""
    prior = ("main.airlines.coupons.PRIOR_COLUMN",)
    snapshot: dict = {"schema_columns": prior}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
    ) as patched:
        injected, source, count, _ = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id="",
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_not_called()
    assert injected is False
    assert source == "no_space_id"
    assert count == 0
    # Empty space_id is a caller bug, but we still preserve prior values
    # so a misconfigured iteration does not erase good state.
    assert snapshot["schema_columns"] == prior


def test_no_op_when_space_id_is_none() -> None:
    """``None`` space_id also short-circuits (defensive)."""
    snapshot: dict = {}
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
    ) as patched:
        injected, source, count, _ = (
            inject_schema_columns_into_metadata_snapshot(
                snapshot,
                genie_space_id=None,
                client=mock.MagicMock(name="WorkspaceClient"),
            )
        )
    patched.assert_not_called()
    assert injected is False
    assert source == "no_space_id"
    assert count == 0
    assert "schema_columns" not in snapshot


def test_source_vocabulary_closed_and_size_exactly_four() -> None:
    """Vocabulary has exactly 4 members; regression guard against ``already_present``."""
    assert SCHEMA_COLUMNS_INJECTION_SOURCES == frozenset({
        "genie_api",
        "no_space_id",
        "api_error",
        "empty_extract",
    })
    # Explicit size assertion makes accidental re-addition of
    # ``already_present`` (which would re-introduce the per-run
    # caching bug Trial 13l deliberately avoids) fail loudly.
    assert len(SCHEMA_COLUMNS_INJECTION_SOURCES) == 4
    assert "already_present" not in SCHEMA_COLUMNS_INJECTION_SOURCES


def test_every_returned_source_label_is_in_closed_vocabulary() -> None:
    """End-to-end vocabulary invariant across all reachable branches."""
    snapshot: dict = {}
    # Branch 1: no_space_id
    _, src1, _, _ = inject_schema_columns_into_metadata_snapshot(
        snapshot, genie_space_id=""
    )
    assert src1 in SCHEMA_COLUMNS_INJECTION_SOURCES

    # Branch 2: genie_api
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space_with_two_fqns(),
    ):
        _, src2, _, _ = inject_schema_columns_into_metadata_snapshot(
            snapshot, genie_space_id="abc", client=mock.MagicMock()
        )
    assert src2 in SCHEMA_COLUMNS_INJECTION_SOURCES

    # Branch 3: api_error
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=RuntimeError("boom"),
    ):
        _, src3, _, _ = inject_schema_columns_into_metadata_snapshot(
            {}, genie_space_id="abc", client=mock.MagicMock()
        )
    assert src3 in SCHEMA_COLUMNS_INJECTION_SOURCES

    # Branch 4: empty_extract
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space_with_zero_parseable_tables(),
    ):
        _, src4, _, _ = inject_schema_columns_into_metadata_snapshot(
            {}, genie_space_id="abc", client=mock.MagicMock()
        )
    assert src4 in SCHEMA_COLUMNS_INJECTION_SOURCES

    assert {src1, src2, src3, src4} == SCHEMA_COLUMNS_INJECTION_SOURCES
