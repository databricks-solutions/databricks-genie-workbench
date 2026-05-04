"""Phase G-lite Task 7: assert F8 deletions stay deleted.

The plan assumed F8 absorbed ag_outcome.py and post_eval.py into
stages/acceptance.py and deleted the originals. In this codebase F8
inlined the helpers but DEFERRED the original-module deletion (per the
F8 commit message), so the modules still exist as importable shims.

This test is marked ``xfail(strict=True)`` so it acts as a signal flag:

* While the modules still exist, the test is xfail-expected.
* When a follow-up actually deletes them, the test will start passing
  (raising ``ModuleNotFoundError`` as the assertion expects), which
  ``strict=True`` flips to a failure — that failure is the prompt to
  remove the ``xfail`` marker and let the test serve as a real
  no-resurrection guard.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.xfail(
    strict=True,
    reason=(
        "F8 deferred deletion of ag_outcome.py / post_eval.py; the "
        "modules still exist as shims. Activate this test by removing "
        "the xfail marker once the follow-up plan deletes the modules."
    ),
)
@pytest.mark.parametrize(
    "deleted_module",
    [
        "genie_space_optimizer.optimization.ag_outcome",
        "genie_space_optimizer.optimization.post_eval",
    ],
)
def test_phase_f8_deleted_modules_stay_deleted(deleted_module: str) -> None:
    """G-lite Task 7: F8 deletions stay deleted. Their bodies live
    inside stages/acceptance.py as private helpers."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(deleted_module)
