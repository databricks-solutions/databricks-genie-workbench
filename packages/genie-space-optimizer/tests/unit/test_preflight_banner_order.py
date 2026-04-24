"""F9 — PR 9: banner counter ordering must match runtime gate order.

The preflight example-SQL synthesis banner renders gate counters in a
top-down order. The order must mirror ``validate_synthesis_proposal``'s
runtime gate execution so operators reading the banner can attribute
yield-shortfall to the correct stage.

Runtime order (source: ``synthesis.validate_synthesis_proposal``):
    1. parse
    2. identifier_qualification
    3. execute
    4. structural
    5. arbiter
    6. firewall
    7. genie_agreement       (opt-in, outside validate_synthesis_proposal)
    8. dedup
    9. applied

Retry blocks nest under the gate whose failure class triggered them:
    - retries on qualification  → under identifier_qualification
    - retries on EMPTY_RESULT   → under execute
    - retries on MEASURE()      → under execute
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.preflight_synthesis import (
    _print_summary,
)


def _render(result: dict) -> list[str]:
    buf = io.StringIO()
    with redirect_stdout(buf):
        _print_summary(result)
    return buf.getvalue().splitlines()


def _find(lines: list[str], needle: str) -> int:
    for i, line in enumerate(lines):
        if needle in line:
            return i
    raise AssertionError(f"{needle!r} not found in banner:\n" + "\n".join(lines))


class TestBannerGateOrder:
    def _populated_result(self) -> dict:
        """Return a result dict with every gate + retry block populated.

        Forces every conditional line to render so we can assert on
        the full ordered sequence.
        """
        return {
            "target": 10,
            "existing": 2,
            "need": 8,
            "generated": 20,
            "passed_parse": 18,
            "passed_identifier_qualification": 15,
            "passed_execute": 12,
            "passed_structural": 10,
            "passed_arbiter": 9,
            "passed_firewall": 9,
            "passed_genie_agreement": 9,
            "dedup_rejected": 1,
            "applied": 8,
            # Retry blocks — populate every one so the conditional
            # branches all fire.
            "retries_fired": 3,
            "retries_succeeded": 2,
            "retries_still_empty": 1,
            "retries_on_qualification_fired": 2,
            "retries_on_qualification_attempts": 4,
            "retries_on_qualification_succeeded": 1,
            "repaired_stemmed_identifiers": 3,
            "retries_on_measure_fired": 1,
            "retries_on_measure_attempts": 2,
            "retries_on_measure_succeeded": 1,
            "repaired_measure_refs": 2,
            "rejected_by_gate": {},
        }

    def test_top_level_gate_order_matches_runtime(self):
        """Top-level gate counters appear in runtime order."""
        lines = _render(self._populated_result())

        idx_parse = _find(lines, "Passed parse")
        idx_qual = _find(lines, "Passed identifier_qualification")
        idx_exec = _find(lines, "Passed EXPLAIN+execute")
        idx_struct = _find(lines, "Passed structural")
        idx_arb = _find(lines, "Passed arbiter gate")
        idx_fw = _find(lines, "Passed firewall")
        idx_genie = _find(lines, "Passed genie agreement")
        idx_dedup = _find(lines, "Dedup rejected")
        idx_applied = _find(lines, "Applied")

        assert (
            idx_parse
            < idx_qual
            < idx_exec
            < idx_struct
            < idx_arb
            < idx_fw
            < idx_genie
            < idx_dedup
            < idx_applied
        ), (
            "Banner lines out of runtime order:\n"
            f"  parse={idx_parse}\n"
            f"  identifier_qualification={idx_qual}\n"
            f"  EXPLAIN+execute={idx_exec}\n"
            f"  structural={idx_struct}\n"
            f"  arbiter={idx_arb}\n"
            f"  firewall={idx_fw}\n"
            f"  genie_agreement={idx_genie}\n"
            f"  dedup={idx_dedup}\n"
            f"  applied={idx_applied}\n"
        )

    def test_qualification_retry_block_nests_under_qualification(self):
        """retries on qualification must appear between qualification and execute."""
        lines = _render(self._populated_result())

        idx_qual = _find(lines, "Passed identifier_qualification")
        idx_qual_retry = _find(lines, "retries on qualification")
        idx_exec = _find(lines, "Passed EXPLAIN+execute")

        assert idx_qual < idx_qual_retry < idx_exec, (
            f"qualification retry block misplaced: "
            f"qual={idx_qual}, retry={idx_qual_retry}, exec={idx_exec}"
        )

    def test_empty_result_and_measure_retries_nest_under_execute(self):
        """EMPTY_RESULT and MEASURE() retries belong under execute, before structural."""
        lines = _render(self._populated_result())

        idx_exec = _find(lines, "Passed EXPLAIN+execute")
        idx_empty = _find(lines, "retries on EMPTY_RESULT")
        idx_meas = _find(lines, "retries on MEASURE()")
        idx_struct = _find(lines, "Passed structural")

        assert idx_exec < idx_empty < idx_struct, (
            f"EMPTY_RESULT retry misplaced: "
            f"exec={idx_exec}, empty={idx_empty}, struct={idx_struct}"
        )
        assert idx_exec < idx_meas < idx_struct, (
            f"MEASURE retry misplaced: "
            f"exec={idx_exec}, meas={idx_meas}, struct={idx_struct}"
        )

    def test_structural_precedes_arbiter_precedes_firewall(self):
        """Post-execute gate order: structural → arbiter → firewall."""
        lines = _render(self._populated_result())

        idx_struct = _find(lines, "Passed structural")
        idx_arb = _find(lines, "Passed arbiter gate")
        idx_fw = _find(lines, "Passed firewall")

        assert idx_struct < idx_arb < idx_fw, (
            "post-execute gate order should be structural → arbiter → firewall; "
            f"got struct={idx_struct}, arb={idx_arb}, fw={idx_fw}"
        )


class TestBannerConditionalSections:
    def test_qualification_line_hidden_when_never_run(self):
        """Legacy runs without qualification data don't render the qualification line."""
        result = {
            "target": 5,
            "existing": 0,
            "need": 5,
            "generated": 10,
            "passed_parse": 10,
            "passed_identifier_qualification": 0,
            "passed_execute": 8,
            "passed_structural": 7,
            "passed_arbiter": 6,
            "passed_firewall": 6,
            "passed_genie_agreement": 6,
            "dedup_rejected": 0,
            "applied": 6,
            "rejected_by_gate": {},
        }
        lines = _render(result)
        joined = "\n".join(lines)
        assert "Passed identifier_qualification" not in joined

    def test_retry_blocks_hidden_when_counters_zero(self):
        """Retry blocks are conditional — no retries fired → no lines."""
        result = {
            "target": 5,
            "existing": 0,
            "need": 5,
            "generated": 10,
            "passed_parse": 10,
            "passed_execute": 10,
            "passed_structural": 10,
            "passed_arbiter": 10,
            "passed_firewall": 10,
            "passed_genie_agreement": 10,
            "dedup_rejected": 0,
            "applied": 5,
            "rejected_by_gate": {},
        }
        lines = _render(result)
        joined = "\n".join(lines)
        assert "retries on EMPTY_RESULT" not in joined
        assert "retries on qualification" not in joined
        assert "retries on MEASURE()" not in joined
