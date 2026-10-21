"""Pin how app.yaml carries the AI Gateway route.

The backend reads GENIE_LLM_ROUTE from its environment (llm_route.get_llm_route).
app.yaml ships it as an installer placeholder, so the operator's choice
(notebook llm_route widget / deploy.sh GENIE_LLM_ROUTE) is what the deployed app
runs with; an unfilled placeholder fails the install.
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_llm_route_is_an_installer_placeholder():
    env = _app_env()
    assert env.get("GENIE_LLM_ROUTE") == "__GENIE_LLM_ROUTE__"
