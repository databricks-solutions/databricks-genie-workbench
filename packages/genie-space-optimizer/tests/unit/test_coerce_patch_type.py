"""Trial 13e — :func:`coerce_patch_type` unit tests.

The synthesis LLM was emitting UPPER_SNAKE ``patch_type`` values
(e.g. ``"ADD_INSTRUCTION"``) while :class:`PatchType` (a ``StrEnum``)
only accepts the lower-case ``.value`` form. Every Stage 3 proposal
was silently dropped. The shared :func:`coerce_patch_type` helper
adds case-folding tolerance at every call site (synthesize.py,
repair_loop.py, narrow_replacement.py) while leaving the strict
``validate_patch`` dispatcher untouched as a deliberate contract gate.

These tests pin the helper's invariants so future drift is loud.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    coerce_patch_type,
)


def test_passes_through_existing_patch_type_instance() -> None:
    """A ``PatchType`` instance is returned unchanged (no re-lookup)."""
    assert coerce_patch_type(PatchType.ADD_INSTRUCTION) is PatchType.ADD_INSTRUCTION


def test_lower_case_value_round_trips() -> None:
    """The canonical lower-case ``.value`` parses to the matching member."""
    assert coerce_patch_type("add_instruction") is PatchType.ADD_INSTRUCTION
    assert coerce_patch_type("add_example_sql") is PatchType.ADD_EXAMPLE_SQL


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("ADD_INSTRUCTION", PatchType.ADD_INSTRUCTION),
        ("ADD_EXAMPLE_SQL", PatchType.ADD_EXAMPLE_SQL),
        ("ADD_COLUMN_DESCRIPTION", PatchType.ADD_COLUMN_DESCRIPTION),
        ("Add_Instruction", PatchType.ADD_INSTRUCTION),
        ("aDd_InStRuCtIoN", PatchType.ADD_INSTRUCTION),
    ],
)
def test_case_variants_fold_to_lower_case_enum(raw: str, expected: PatchType) -> None:
    """The exact UPPER set captured from the dc89/98ec workbench probe
    all coerce successfully — that is the regression contract."""
    assert coerce_patch_type(raw) is expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  add_instruction  ", PatchType.ADD_INSTRUCTION),
        ("\tADD_INSTRUCTION\n", PatchType.ADD_INSTRUCTION),
    ],
)
def test_whitespace_is_stripped(raw: str, expected: PatchType) -> None:
    assert coerce_patch_type(raw) is expected


@pytest.mark.parametrize(
    "raw",
    [
        "add_nothing",
        "do_the_thing",
        "ADD_NOTHING",
        "patch_type",  # the field name itself, not a value
    ],
)
def test_unknown_strings_return_none(raw: str) -> None:
    """Genuinely unknown vocabulary returns ``None`` so callers can
    decide whether to drop the proposal (synthesize.py) or pass through
    to the strict dispatcher (repair_loop.py / narrow_replacement.py)."""
    assert coerce_patch_type(raw) is None


@pytest.mark.parametrize("raw", [None, "", "   ", "\t\n"])
def test_empty_and_none_inputs_return_none(raw: object) -> None:
    assert coerce_patch_type(raw) is None


@pytest.mark.parametrize("raw", [0, 1, 3.14, [], {}, object()])
def test_non_string_inputs_return_none_without_raising(raw: object) -> None:
    """Defensive — the helper must never raise; all non-string inputs
    map to ``None``."""
    assert coerce_patch_type(raw) is None
