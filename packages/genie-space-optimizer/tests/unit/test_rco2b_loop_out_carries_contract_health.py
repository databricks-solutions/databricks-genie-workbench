"""RCO-2b — loop_out carries the typed contract_health_summary so the
lever-loop notebook can ``enforce_merge_gate(loop_out)`` without
re-parsing stdout.
"""
from __future__ import annotations

import pathlib
import re


def test_harness_assigns_contract_health_summary_local() -> None:
    """Source-level guard: the harness must capture the
    ``_emit_contract_health_summary`` return value into a local named
    ``_contract_health_summary``.

    Without this, Step 3's loop_out_base entry is unreachable.
    """
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert re.search(
        r"_contract_health_summary\s*=\s*_emit_contract_health_summary\(",
        src,
    ), (
        "harness.py must capture _emit_contract_health_summary's "
        "return value into a local named _contract_health_summary"
    )


def test_loop_out_base_carries_contract_health_summary_key() -> None:
    """Source-level guard: ``_loop_out_base`` must declare the
    ``contract_health_summary`` key projected from
    ``_contract_health_summary.to_json_dict() if _contract_health_summary else None``.
    """
    src = pathlib.Path(
        "src/genie_space_optimizer/optimization/harness.py"
    ).read_text(encoding="utf-8")
    assert '"contract_health_summary"' in src, (
        "harness.py must declare a 'contract_health_summary' key on "
        "_loop_out_base"
    )
    assert (
        "_contract_health_summary.to_json_dict()"
        in src
    ), (
        "harness.py must project the typed summary through "
        "to_json_dict() so loop_out carries a plain dict (JSON-safe "
        "for the dbutils.notebook.exit task-values round trip)"
    )
