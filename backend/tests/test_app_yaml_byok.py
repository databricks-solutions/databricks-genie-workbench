"""Pin the BYOK flag default-off in app.yaml (Phase 2).

validate_chat_model reads GENIE_BYOK_ENABLED; app.yaml MUST ship it as
"false" so BYOK acceptance stays off until deliberately enabled
(spec §2, §9 Decision 2).
"""

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _app_env() -> dict[str, str]:
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load((_REPO_ROOT / "app.yaml").read_text())
    return {e["name"]: e.get("value") for e in doc["env"] if "name" in e}


def test_genie_byok_enabled_declared_default_off():
    env = _app_env()
    assert "GENIE_BYOK_ENABLED" in env, "app.yaml must declare GENIE_BYOK_ENABLED"
    assert env["GENIE_BYOK_ENABLED"] == "false"
