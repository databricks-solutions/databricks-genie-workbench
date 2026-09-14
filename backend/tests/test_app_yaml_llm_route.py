"""Pin the AI Gateway route flag default-off in app.yaml (Phase 0).

The backend reads GENIE_LLM_ROUTE from its environment (Appendix A.1
get_llm_route). app.yaml MUST ship it as "classic" so the deployed app stays
on the classic path until the flag is deliberately flipped (spec §0, §9
Decision 4).
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_llm_route_declared_default_off():
    env = _app_env()
    assert "GENIE_LLM_ROUTE" in env, "app.yaml must declare GENIE_LLM_ROUTE"
    assert env["GENIE_LLM_ROUTE"] == "classic"
