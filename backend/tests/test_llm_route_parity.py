"""Byte-identical parity pin for the two AI Gateway seam modules.

The Workbench app (backend/) and the GSO wheel
(packages/genie-space-optimizer/) do not share a Python package, so the seam
ships as two copies. This test fails the moment they drift — the same
mechanism as the rules-parity pin. Keep both files identical; never run a
formatter on one without the other.
"""

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND = _REPO_ROOT / "backend" / "services" / "llm_route.py"
_GSO = (
    _REPO_ROOT
    / "packages" / "genie-space-optimizer" / "src" / "genie_space_optimizer"
    / "optimization" / "llm_route.py"
)


def test_both_modules_exist():
    assert _BACKEND.is_file(), _BACKEND
    assert _GSO.is_file(), _GSO


def test_seam_modules_are_byte_identical():
    assert _BACKEND.read_bytes() == _GSO.read_bytes(), (
        "backend/services/llm_route.py and the GSO optimization/llm_route.py have "
        "drifted. They MUST stay byte-identical (spec Appendix A)."
    )
