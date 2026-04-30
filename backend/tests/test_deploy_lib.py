from pathlib import Path

import pytest

from scripts.deploy_lib.app_yaml import render_text
from scripts.deploy_lib.apps import patch_app_resources
from scripts.deploy_lib.config import InstallConfig, LakebaseInfo
from scripts.deploy_lib.gso_job import build_job_settings
from scripts.deploy_lib.lakebase import get_database_resource
from scripts.deploy_lib.uc import update_grants
from scripts.deploy_lib.workspace_source import should_copy


class FakeApiClient:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def do(self, *, method, path, body=None):
        self.calls.append((method, path, body))
        key = (method, path)
        if key in self.responses:
            return self.responses[key]
        return {}


class FakeWorkspaceClient:
    def __init__(self, responses=None):
        self.api_client = FakeApiClient(responses)


def test_render_text_replaces_placeholders_and_fails_on_unresolved():
    rendered = render_text(
        "warehouse=__WAREHOUSE_ID__ model=__LLM_MODEL__",
        {"WAREHOUSE_ID": "abc", "LLM_MODEL": "databricks-claude"},
    )
    assert rendered == "warehouse=abc model=databricks-claude"

    with pytest.raises(ValueError, match="__GSO_JOB_ID__"):
        render_text("job=__GSO_JOB_ID__", {})


def test_all_current_app_yaml_placeholders_are_covered():
    app_yaml = Path("app.yaml").read_text()
    rendered = render_text(
        app_yaml,
        {
            "WAREHOUSE_ID": "wh",
            "GSO_CATALOG": "main",
            "GSO_JOB_ID": "123",
            "LAKEBASE_INSTANCE": "genie-workbench-lakebase",
            "LLM_MODEL": "databricks-claude-sonnet-4-6",
            "MLFLOW_EXPERIMENT_ID": "",
        },
    )
    assert "__" not in rendered


def test_config_validation_normalizes_lakebase_defaults():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="abc",
        repo_root="/Workspace/Repos/me/repo",
        lakebase_mode="create",
    ).normalized()

    assert cfg.lakebase_instance == "genie-workbench-lakebase"
    cfg.validate()

    with pytest.raises(ValueError, match="app_name"):
        InstallConfig(app_name="Bad Name", catalog="main", warehouse_id="abc", repo_root="/tmp").validate()

    with pytest.raises(ValueError, match="lakebase_instance"):
        InstallConfig(
            app_name="genie-workbench",
            catalog="main",
            warehouse_id="abc",
            repo_root="/tmp",
            lakebase_mode="existing",
        ).validate()


def test_workspace_source_inclusion_rules(tmp_path):
    repo = tmp_path / "repo"
    files = [
        "backend/main.py",
        "backend/references/schema.md",
        "README.md",
        "requirements.txt",
        ".env.deploy",
        "scripts/deploy.sh",
        "notebooks/install.py",
        "frontend/package.json",
        "frontend/node_modules/pkg/index.js",
        "packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_preflight.py",
        "packages/genie-space-optimizer/tests/test_x.py",
    ]
    for rel in files:
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")

    assert should_copy(repo / "backend/main.py", repo)
    assert should_copy(repo / "backend/references/schema.md", repo)
    assert should_copy(repo / "frontend/package.json", repo)
    assert should_copy(
        repo / "packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_preflight.py",
        repo,
    )
    assert not should_copy(repo / "README.md", repo)
    assert not should_copy(repo / "requirements.txt", repo)
    assert not should_copy(repo / ".env.deploy", repo)
    assert not should_copy(repo / "scripts/deploy.sh", repo)
    assert not should_copy(repo / "notebooks/install.py", repo)
    assert not should_copy(repo / "frontend/node_modules/pkg/index.js", repo)
    assert not should_copy(repo / "packages/genie-space-optimizer/tests/test_x.py", repo)


def test_patch_app_resources_preserves_existing_and_adds_postgres():
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): {
                "resources": [
                    {"name": "keep-me", "secret": {"scope": "s", "key": "k"}},
                    {"name": "postgres"},
                ]
            }
        }
    )
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="warehouse-1",
        repo_root="/tmp",
    )
    lakebase = LakebaseInfo(
        project_name="lb",
        branch_resource="projects/lb/branches/production",
        database_resource="projects/lb/branches/production/databases/databricks_postgres",
        endpoint_resource="projects/lb/branches/production/endpoints/primary",
        grants_applied=True,
    )

    payload = patch_app_resources(w, cfg, lakebase)

    resources = {r["name"]: r for r in payload["resources"]}
    assert resources["sql-warehouse"]["sql_warehouse"]["id"] == "warehouse-1"
    assert resources["postgres"]["postgres"]["permission"] == "CAN_CONNECT_AND_CREATE"
    assert "keep-me" in resources
    assert any(call[0] == "PATCH" and call[1] == "/api/2.0/apps/genie-workbench" for call in w.api_client.calls)


def test_gso_job_settings_match_persistent_dag_shape():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
    )
    settings = build_job_settings(
        cfg,
        "/Workspace/Users/me/.genie-workbench-deploy/genie-workbench/gso/jobs",
        "/Volumes/main/genie_space_optimizer/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl",
    )

    assert settings["name"] == "gso-optimization-job"
    assert settings["queue"]["enabled"] is True
    assert settings["environments"][0]["spec"]["environment_version"] == "4"
    task_keys = [task["task_key"] for task in settings["tasks"]]
    assert task_keys == ["preflight", "baseline_eval", "enrichment", "lever_loop", "finalize", "deploy"]
    assert settings["tasks"][1]["depends_on"] == [{"task_key": "preflight"}]
    assert settings["tasks"][-1]["condition_task"]["right"] == "disabled"


def test_uc_update_grants_uses_permissions_api():
    w = FakeWorkspaceClient()
    update_grants(
        w,
        securable_type="schema",
        full_name="main.genie_space_optimizer",
        principal="sp-id",
        add=["USE_SCHEMA"],
    )
    assert w.api_client.calls == [
        (
            "PATCH",
            "/api/2.1/unity-catalog/permissions/schema/main.genie_space_optimizer",
            {"changes": [{"principal": "sp-id", "add": ["USE_SCHEMA"]}]},
        )
    ]


def test_lakebase_get_database_resource_reads_first_database_name():
    w = FakeWorkspaceClient(
        {
            (
                "GET",
                "/api/2.0/postgres/projects/lb/branches/production/databases",
            ): {
                "databases": [
                    {"name": "projects/lb/branches/production/databases/databricks_postgres"}
                ]
            }
        }
    )
    assert (
        get_database_resource(w, "lb")
        == "projects/lb/branches/production/databases/databricks_postgres"
    )

