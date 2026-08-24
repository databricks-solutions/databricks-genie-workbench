from __future__ import annotations

from genie_space_optimizer.jobs._helpers import _diagnostic


def test_diagnostic_prints_readable_bounded_block(capsys) -> None:
    _diagnostic(
        "OPTIMIZE",
        "Attempt accepted",
        run_id="run-123",
        patch_types=["update_column", "add_instruction"],
        optional_value=None,
        multiline="first line\nsecond line",
        oversized="x" * 600,
    )

    output = capsys.readouterr().out
    assert output.startswith("\n[GSO DIAGNOSTIC] [OPTIMIZE] Attempt accepted\n")
    assert "  Run Id: run-123\n" in output
    assert '  Patch Types: ["update_column", "add_instruction"]\n' in output
    assert "  Optional Value: (none)\n" in output
    assert "  Multiline: first line\\nsecond line\n" in output

    oversized_line = next(
        line for line in output.splitlines() if line.startswith("  Oversized: ")
    )
    assert oversized_line.endswith("...")
    assert len(oversized_line.removeprefix("  Oversized: ")) == 500
