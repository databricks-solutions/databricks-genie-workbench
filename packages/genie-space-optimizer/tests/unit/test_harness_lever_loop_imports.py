"""Pin against re-introducing the function-local import that shadows
``load_latest_state_iteration`` for the entire ``_run_lever_loop`` body.

Python scoping rule: any name bound anywhere in a function body is treated
as local for the whole body. A redundant inner ``from ... import``
statement therefore makes earlier reads of that name raise
``UnboundLocalError``, even when the same name is imported at module level.

This test reads ``_run_lever_loop``'s source via ``inspect`` and asserts
that no nested re-import of ``load_latest_state_iteration`` exists.
"""

from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness


def test_run_lever_loop_does_not_reimport_load_latest_state_iteration() -> None:
    src = inspect.getsource(harness._run_lever_loop)

    # Match either single-line or parenthesised multi-line imports.
    pattern = re.compile(
        r"^\s+from\s+genie_space_optimizer\.optimization\.state\s+import\s+\(?[^)]*"
        r"load_latest_state_iteration",
        re.MULTILINE,
    )
    matches = pattern.findall(src)

    assert not matches, (
        "Found a function-local re-import of load_latest_state_iteration "
        "inside _run_lever_loop. Python treats the name as local for the "
        "entire function body, which causes UnboundLocalError at the "
        "earlier baseline-init call site. Remove the redundant import; "
        "the module-level import in harness.py already binds the name."
    )


def test_load_latest_state_iteration_is_module_level_in_harness() -> None:
    """Sanity check: the module-level import must remain in place."""
    assert hasattr(harness, "load_latest_state_iteration"), (
        "load_latest_state_iteration must be imported at module level in "
        "harness.py so _run_lever_loop can use it without redefining the "
        "binding inside the function body."
    )
