"""Proactive sample_questions enrichment must build a FULL, valid
``serialized_space`` from the AUTHORITATIVE re-fetched config.

Root cause (v2 E2E, notebook 03_optimize Stage 2.5 proactive
enrichment):
:func:`genie_space_optimizer.optimization.harness._run_space_metadata_enrichment`
mutated whatever the passed-in ``config['_parsed_space']`` held and
shipped THAT as the entire ``serialized_space`` via
``patch_space_config`` — it never re-fetched/merged. When the base was
empty/degenerate, ``parsed.setdefault('config', {})['sample_questions']``
fabricated a partial object ``{'config': {'sample_questions': [...]}}``
missing the mandatory top-level ``data_sources`` and ``version``. The
strict validator inside ``patch_space_config`` correctly rejected the
partial object BEFORE any HTTP call
(``serialized_space must contain 'data_sources'`` +
``version: required field is missing``). Non-fatal — the loop
continued — but it emitted a scary ERROR + traceback and silently
skipped sample-question enrichment.

A Genie ``serialized_space`` update is FULL-REPLACEMENT (see the Update
Genie Space API validation rules): to change only sample_questions the
whole object must be GET → mutate → PATCH, echoing the existing
``version`` and preserving ``data_sources``. ``fetch_space_config``
already requests ``include_serialized_space=true``, so the fix re-fetches
the authoritative config, mutates ``config.sample_questions`` on a deep
copy, and PATCHes the full object. When the base has no
``data_sources``/``version`` it logs a clean INFO skip instead of
fabricating a partial PATCH.
"""

from __future__ import annotations

import inspect
import logging
from unittest.mock import MagicMock, patch

import genie_space_optimizer.common.genie_client as _genie_client_mod
import genie_space_optimizer.optimization.optimizer as _optimizer_mod
from genie_space_optimizer.optimization import harness

# A valid 32-char lowercase hex sample-question id.
_SQ_ID = "a" * 32
_NEW_SQS = [{"id": _SQ_ID, "question": ["What is total revenue?"]}]

# The exact fabricated shape the old bug produced from a degenerate base.
_PARTIAL_FABRICATED = {"config": {"sample_questions": _NEW_SQS}}

_SPACE_ID = "01f1756be5bf1db8895263c014de28c4"


def _authoritative_full_config() -> dict:
    """Shape ``fetch_space_config`` returns for a populated space: a full
    ``serialized_space`` under ``_parsed_space`` (version + data_sources +
    config + instructions)."""
    return {
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.fact_sales"}],
                "metric_views": [],
            },
            "config": {"sample_questions": []},
            "instructions": {"text_instructions": []},
        },
    }


def _working_full_config() -> dict:
    """Shape passed into ``_run_space_metadata_enrichment`` after the
    lever-loop preparation path has a populated working snapshot."""
    return {
        "description": "x" * 20,
        "_parsed_space": {
            "version": 1,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.stale_name"}],
                "metric_views": [],
            },
            "config": {"sample_questions": []},
            "instructions": {"text_instructions": []},
        },
    }


def _run(
    config: dict,
    *,
    fetch_return: dict,
    generate_return=_NEW_SQS,
    metadata_snapshot: dict | None = None,
):
    """Invoke ``_run_space_metadata_enrichment`` with all I/O mocked out.

    Returns ``(result, patch_mock, fetch_mock, logger_mock)``.
    """
    patch_mock = MagicMock(name="patch_space_config", return_value={})
    fetch_mock = MagicMock(name="fetch_space_config", return_value=fetch_return)
    gen_mock = MagicMock(name="_generate_sample_questions", return_value=generate_return)
    logger_mock = MagicMock(name="logger")
    if metadata_snapshot is None:
        metadata_snapshot = config.get("_parsed_space", config)

    with (
        patch.object(_genie_client_mod, "patch_space_config", patch_mock),
        patch.object(_genie_client_mod, "fetch_space_config", fetch_mock),
        patch.object(_optimizer_mod, "_generate_sample_questions", gen_mock),
        patch.object(harness, "write_stage", MagicMock()),
        patch.object(harness, "write_patch", MagicMock()),
        patch.object(harness, "logger", logger_mock),
    ):
        result = harness._run_space_metadata_enrichment(
            MagicMock(name="w"),
            MagicMock(name="spark"),
            "run-1",
            _SPACE_ID,
            config,
            metadata_snapshot,
            "cat",
            "sch",
        )
    return result, patch_mock, fetch_mock, logger_mock


# ═══════════════════════════════════════════════════════════════════════
# Populated base — build a FULL serialized_space from the authoritative fetch
# ═══════════════════════════════════════════════════════════════════════


class TestPopulatedBase:
    def test_patches_full_object_from_authoritative_fetch(self):
        """When the working snapshot is populated, the PATCH is built from
        the authoritative re-fetched config and carries top-level
        ``version`` + ``data_sources`` with sample_questions updated in
        place."""
        passed_in = _working_full_config()
        fetch_return = _authoritative_full_config()

        result, patch_mock, fetch_mock, _ = _run(passed_in, fetch_return=fetch_return)

        # Re-fetched the authoritative serialized_space exactly once.
        assert fetch_mock.call_count == 1
        # PATCHed exactly once, with the FULL object.
        assert patch_mock.call_count == 1
        _w, space_id, target = patch_mock.call_args.args
        assert space_id == _SPACE_ID

        # Top-level version echoed UNCHANGED and data_sources PRESERVED.
        assert target["version"] == 2
        assert target["data_sources"] == {
            "tables": [{"identifier": "cat.sch.fact_sales"}],
            "metric_views": [],
        }
        # sample_questions updated in place on config.
        assert target["config"]["sample_questions"] == _NEW_SQS

        # Deep copy — the fetched current object was NOT mutated.
        assert fetch_return["_parsed_space"]["config"]["sample_questions"] == []

        # Result reflects a real write, not a skip.
        assert result["questions_generated"] is True
        assert result["questions_count"] == 1
        assert result["questions_skipped_empty_base"] is False

    def test_patched_object_passes_strict_validation(self):
        """The object handed to ``patch_space_config`` must itself be a
        strict-valid ``serialized_space`` (this is precisely what the old
        code failed to guarantee)."""
        from genie_space_optimizer.common.genie_schema import (
            validate_serialized_space,
        )

        passed_in = _working_full_config()
        _, patch_mock, _, _ = _run(
            passed_in, fetch_return=_authoritative_full_config()
        )
        target = patch_mock.call_args.args[2]
        ok, errors = validate_serialized_space(target, strict=True)
        assert ok, f"enrichment PATCH payload must be strict-valid; got: {errors}"


# ═══════════════════════════════════════════════════════════════════════
# Empty / degenerate base — clean skip, never fabricate a partial PATCH
# ═══════════════════════════════════════════════════════════════════════


class TestEmptyBase:
    def test_skips_patch_logs_clean_info_never_raises(self):
        passed_in = {"description": "x" * 20, "_parsed_space": {}}
        # Genuinely empty space: the API exports a serialized_space, but it has
        # no data sources to ground sample questions.
        result, patch_mock, fetch_mock, logger_mock = _run(
            passed_in,
            fetch_return={
                "_parsed_space": {
                    "version": 2,
                    "data_sources": {"tables": [], "metric_views": [], "functions": []},
                },
            },
        )

        # Re-fetched for the stale-snapshot guard and reused for the empty-base
        # questions guard, but did NOT PATCH.
        assert fetch_mock.call_count == 1
        patch_mock.assert_not_called()

        # Recorded as a clean skip, not a failure.
        assert result["questions_generated"] is False
        assert result["questions_count"] == 0
        assert result["questions_skipped_empty_base"] is True

        # Clean INFO skip — no WARNING/ERROR/traceback.
        logger_mock.warning.assert_not_called()
        logger_mock.error.assert_not_called()
        logger_mock.exception.assert_not_called()
        info_fmt = " ".join(
            str(c.args[0]) for c in logger_mock.info.call_args_list if c.args
        )
        assert "empty space" in info_fmt
        assert "skipping" in info_fmt

    def test_missing_data_sources_key_is_treated_as_empty(self):
        """A base with a ``version`` but no ``data_sources`` is still an
        empty/degenerate space and must be skipped, not PATCHed with a
        payload the API would reject."""
        passed_in = {"description": "x" * 20, "_parsed_space": {"version": 2}}
        result, patch_mock, _, _ = _run(
            passed_in, fetch_return={"_parsed_space": {"version": 2}}
        )
        patch_mock.assert_not_called()
        assert result["questions_skipped_empty_base"] is True

    def test_missing_version_is_treated_as_empty(self):
        """A base with ``data_sources`` but no ``version`` is skipped —
        PATCHing without echoing ``version`` would trip the validator."""
        passed_in = {
            "description": "x" * 20,
            "_parsed_space": {"data_sources": {"tables": []}},
        }
        result, patch_mock, _, _ = _run(
            passed_in,
            fetch_return={
                "_parsed_space": {"data_sources": {"tables": []}},
            },
        )
        patch_mock.assert_not_called()
        assert result["questions_skipped_empty_base"] is True


class TestStaleEmptyWorkingSnapshot:
    def test_populated_live_space_skips_empty_snapshot_metadata_writes(self):
        """If the working snapshot is empty but live is populated, do not
        generate from the empty context or PATCH either metadata field."""
        passed_in = {"description": "", "_parsed_space": {}}
        fetch_mock = MagicMock(
            name="fetch_space_config",
            return_value=_authoritative_full_config(),
        )
        gen_desc = MagicMock(name="_generate_space_description", return_value="empty space")
        gen_questions = MagicMock(
            name="_generate_sample_questions",
            return_value=[{"id": _SQ_ID, "question": ["What data is available?"]}],
        )
        update_desc = MagicMock(name="update_space_description")
        patch_config = MagicMock(name="patch_space_config")

        with (
            patch.object(_genie_client_mod, "fetch_space_config", fetch_mock),
            patch.object(_genie_client_mod, "update_space_description", update_desc),
            patch.object(_genie_client_mod, "patch_space_config", patch_config),
            patch.object(_optimizer_mod, "_generate_space_description", gen_desc),
            patch.object(_optimizer_mod, "_generate_sample_questions", gen_questions),
            patch.object(harness, "write_stage", MagicMock()),
            patch.object(harness, "write_patch", MagicMock()),
        ):
            result = harness._run_space_metadata_enrichment(
                MagicMock(name="w"),
                MagicMock(name="spark"),
                "run-1",
                _SPACE_ID,
                passed_in,
                {"data_sources": {"tables": []}},
                "cat",
                "sch",
            )

        assert result["metadata_skipped_stale_empty_snapshot"] is True
        assert result["description_generated"] is False
        assert result["questions_generated"] is False
        fetch_mock.assert_called_once()
        gen_desc.assert_not_called()
        gen_questions.assert_not_called()
        update_desc.assert_not_called()
        patch_config.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Source-level guard — pin the fix wiring
# ═══════════════════════════════════════════════════════════════════════


class TestSourceWiring:
    def test_builds_patch_from_authoritative_fetch_not_passed_in_parsed(self):
        src = inspect.getsource(harness._run_space_metadata_enrichment)
        # Re-fetches the authoritative serialized_space.
        assert "fetch_space_config(w, space_id)" in src
        # Deep-copies before mutating (no aliasing of the fetched object).
        assert "copy.deepcopy(current)" in src
        # Empty-base guard present on BOTH required top-level fields, and it
        # treats data_sources with no assets as empty.
        assert "space_config_has_data_sources" in src
        assert 'current.get("version") is None' in src
        assert "questions_skipped_empty_base" in src
        assert "metadata_skipped_stale_empty_snapshot" in src
        # The old antipattern — mutating and PATCHing the passed-in parsed
        # object — must be gone.
        assert "patch_space_config(w, space_id, parsed)" not in src


# ═══════════════════════════════════════════════════════════════════════
# patch_space_config full-replacement contract — partial object rejected
# ═══════════════════════════════════════════════════════════════════════


class TestPatchSpaceConfigFullReplacement:
    def test_rejects_partial_object_before_any_http(self):
        """The exact fabricated shape the old bug produced must be rejected
        by ``patch_space_config`` BEFORE any HTTP call."""
        from genie_space_optimizer.common.genie_client import patch_space_config

        w = MagicMock()
        try:
            patch_space_config(w, _SPACE_ID, dict(_PARTIAL_FABRICATED))
        except ValueError as exc:
            msg = str(exc)
            assert "data_sources" in msg
            assert "version" in msg
        else:
            raise AssertionError(
                "expected ValueError for partial serialized_space"
            )
        # Validation short-circuits before the PATCH is sent.
        w.api_client.do.assert_not_called()

    def test_validator_reproduces_exact_partial_failure(self):
        """Direct validator reproduction of the two error messages seen in
        the failing E2E run."""
        from genie_space_optimizer.common.genie_schema import (
            validate_serialized_space,
        )

        ok, errors = validate_serialized_space(
            dict(_PARTIAL_FABRICATED), strict=True
        )
        assert not ok
        joined = "\n".join(errors)
        assert "data_sources" in joined
        assert "version: required field is missing" in joined
