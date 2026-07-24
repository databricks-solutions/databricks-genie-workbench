"""Architecture guard for the registry-free GSO prompt contract."""

from pathlib import Path


_PACKAGE_ROOT = Path(__file__).resolve().parents[2]
_REPO_ROOT = Path(__file__).resolve().parents[4]


def test_gso_runtime_has_no_prompt_registry_api_calls() -> None:
    source_root = _PACKAGE_ROOT / "src" / "genie_space_optimizer"
    forbidden = (
        "mlflow.genai.register_prompt",
        "mlflow.genai.load_prompt",
        "mlflow.genai.set_prompt_alias",
        "mlflow.genai.delete_prompt",
        "mlflow.genai.search_prompts",
        "common.prompt_registry",
    )

    violations = []
    for path in source_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            if marker in text:
                violations.append(f"{path.relative_to(_PACKAGE_ROOT)}: {marker}")

    assert violations == []
    assert not (source_root / "common" / "prompt_registry.py").exists()


def test_app_does_not_configure_an_mlflow_registry_uri() -> None:
    app_yaml = (_REPO_ROOT / "app.yaml").read_text(encoding="utf-8")
    assert "MLFLOW_REGISTRY_URI" not in app_yaml
