"""Trial 13j — ``_fetch_schema_columns_for_space`` provenance contract.

Three branches, all required to be deterministic from unit-test
fixtures so the capture path can be exercised without a live Genie
workspace:

1. Successful fetch → ``"genie_api"`` source + 4-part column FQNs.
2. ``fetch_space_config`` raises → ``"unavailable"`` source +
   empty columns + empty serialized_space (no exception propagated).
3. Empty ``space_id`` short-circuits before calling the API at all.
"""
from __future__ import annotations

from unittest import mock

import pytest

from local_lever_workbench import mlflow_eval_capture as mec


def _fake_serialized_space() -> dict:
    """Mirror the shape :func:`fetch_space_config` returns.

    The convenience keys (``_parsed_space``, ``_tables``, ...) are
    included so the helper's ``_parsed_space`` short-circuit branch
    is exercised exactly as it would be in production.
    """
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
                {
                    "identifier": "main.airlines.itinerary",
                    "columns": [
                        {"name": "ITINERARY_ID"},
                        # Malformed entries are silently dropped.
                        {"name": ""},
                        {"description": "missing-name"},
                    ],
                },
                # Non-FQN table identifier — dropped.
                {"identifier": "no_catalog_no_schema", "columns": [{"name": "x"}]},
            ]
        }
    }
    return {
        "space_id": "01f143dfbeec15a3a0e87ced8662f4ed",
        "title": "Airlines",
        "serialized_space": parsed,
        "_parsed_space": parsed,
        "_tables": ["main.airlines.coupons", "main.airlines.itinerary"],
    }


@pytest.mark.workbench
def test_fetch_schema_columns_success_returns_4part_fqns_and_genie_api() -> None:
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        return_value=_fake_serialized_space(),
    ) as patched:
        cols, ss, source = mec._fetch_schema_columns_for_space(
            mock.MagicMock(name="WorkspaceClient"),
            space_id="01f143dfbeec15a3a0e87ced8662f4ed",
        )
    patched.assert_called_once()
    assert source == "genie_api"
    assert cols == (
        "main.airlines.coupons.ORIG_AIRPORT_CD",
        "main.airlines.coupons.DEST_AIRPORT_CD",
        "main.airlines.itinerary.ITINERARY_ID",
    )
    assert ss["space_id"] == "01f143dfbeec15a3a0e87ced8662f4ed"


@pytest.mark.workbench
def test_fetch_schema_columns_swallows_api_failure() -> None:
    """Capture must not abort when the Genie API is unreachable."""
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        side_effect=RuntimeError("permissions denied"),
    ) as patched:
        cols, ss, source = mec._fetch_schema_columns_for_space(
            mock.MagicMock(name="WorkspaceClient"),
            space_id="01f143dfbeec15a3a0e87ced8662f4ed",
        )
    patched.assert_called_once()
    assert source == "unavailable"
    assert cols == ()
    assert ss == {}


@pytest.mark.workbench
def test_fetch_schema_columns_skips_when_space_id_empty() -> None:
    """No API round-trip when the resolver could not infer a space id."""
    with mock.patch(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
    ) as patched:
        cols, ss, source = mec._fetch_schema_columns_for_space(
            mock.MagicMock(name="WorkspaceClient"), space_id=""
        )
    patched.assert_not_called()
    assert source == "unavailable"
    assert cols == ()
    assert ss == {}


@pytest.mark.workbench
def test_extract_fqn_columns_accepts_serialized_space_root() -> None:
    """The helper accepts both the API envelope and the bare parsed dict.

    Sub-component test for the loader path: the v2 loader passes the
    ``serialized_space`` dict directly (no ``_parsed_space`` wrapper)
    so the helper must walk the direct ``data_sources.tables`` path
    too.
    """
    parsed = _fake_serialized_space()["_parsed_space"]
    cols = mec._extract_fqn_columns(parsed)
    assert cols == (
        "main.airlines.coupons.ORIG_AIRPORT_CD",
        "main.airlines.coupons.DEST_AIRPORT_CD",
        "main.airlines.itinerary.ITINERARY_ID",
    )
