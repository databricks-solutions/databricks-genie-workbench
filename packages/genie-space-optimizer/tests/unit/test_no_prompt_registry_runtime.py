"""Architecture guard for the registry-free GSO prompt contract."""

import ast
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


def test_gso_mlflow_runtime_calls_are_tracing_only() -> None:
    """Prevent datasets, runs, models, or evaluation APIs from creeping back."""
    source_root = _PACKAGE_ROOT / "src" / "genie_space_optimizer"
    allowed = {
        "mlflow.openai.autolog",
        "mlflow.set_experiment",
        "mlflow.start_span",
        "mlflow.trace",
    }
    violations: list[str] = []

    def dotted_name(node: ast.AST) -> str:
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts))

    for path in source_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for imported in (
            node for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        ):
            module = str(imported.module or "")
            if module.startswith("mlflow") and module != "mlflow.entities":
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}:{imported.lineno}: "
                    f"from {module} import ..."
                )
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            name = dotted_name(call.func)
            if name.startswith("mlflow.") and name not in allowed:
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}:{call.lineno}: {name}"
                )

    assert violations == []
