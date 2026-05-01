from pathlib import Path

import pytest

from scripts.deploy_lib.app_yaml import render_text
from scripts.deploy_lib.apps import get_app_service_principal, patch_app_resources, require_successful_deployment
from scripts.deploy_lib.config import InstallConfig, LakebaseInfo
from scripts.deploy_lib.genie_spaces import optionally_grant_genie_spaces
from scripts.deploy_lib.gso_job import build_job_settings, find_existing_job, upsert_job
from scripts.deploy_lib.lakebase import get_database_resource
from scripts.deploy_lib.uc import update_grants
from scripts.deploy_lib.workspace_source import mkdirs, should_copy, upload_source_notebook, workspace_api_path


class FakeApiClient:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls = []

    def do(self, *, method, path, body=None):
        self.calls.append((method, path, body))
        key = (method, path)
        if key in self.responses:
            response = self.responses[key]
            if isinstance(response, list):
                if len(response) > 1:
                    response = response.pop(0)
                else:
                    response = response[0]
            if isinstance(response, Exception):
                raise response
            return response
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


def test_workspace_api_path_normalizes_workspace_prefix():
    assert workspace_api_path("/Workspace/Users/me/app") == "/Users/me/app"
    assert workspace_api_path("/Users/me/app") == "/Users/me/app"


def test_workspace_import_uses_object_path_for_workspace_prefixed_paths(tmp_path):
    src = tmp_path / "run_preflight.py"
    src.write_text("print('ok')")
    w = FakeWorkspaceClient()

    upload_source_notebook(w, src, "/Workspace/Users/me/app/gso/jobs/run_preflight")

    assert w.api_client.calls[0] == (
        "POST",
        "/api/2.0/workspace/mkdirs",
        {"path": "/Users/me/app/gso/jobs"},
    )
    assert w.api_client.calls[1][2]["path"] == "/Users/me/app/gso/jobs/run_preflight"


def test_mkdirs_uses_object_path_for_workspace_prefixed_paths():
    w = FakeWorkspaceClient()
    mkdirs(w, "/Workspace/Users/me/app")
    assert w.api_client.calls == [
        ("POST", "/api/2.0/workspace/mkdirs", {"path": "/Users/me/app"})
    ]


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


def test_get_app_service_principal_waits_for_async_app_create(monkeypatch):
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"name": "genie-workbench"},
                {"name": "genie-workbench", "service_principal_client_id": "sp-client-id"},
            ]
        }
    )
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)

    sp = get_app_service_principal(
        w,
        "genie-workbench",
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert sp["client_id"] == "sp-client-id"
    get_calls = [call for call in w.api_client.calls if call[0] == "GET"]
    assert len(get_calls) == 2


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
    assert settings["tags"]["app"] == "genie-workbench"
    assert settings["tags"]["managed-by"] == "notebook-installer"
    assert settings["environments"][0]["spec"]["environment_version"] == "4"
    task_keys = [task["task_key"] for task in settings["tasks"]]
    assert task_keys == ["preflight", "baseline_eval", "enrichment", "lever_loop", "finalize", "deploy"]
    assert settings["tasks"][1]["depends_on"] == [{"task_key": "preflight"}]
    assert settings["tasks"][-1]["condition_task"]["right"] == "disabled"


def test_gso_job_settings_tag_with_actual_app_name():
    cfg = InstallConfig(
        app_name="genie-workbench-dh2",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
    )
    settings = build_job_settings(
        cfg,
        "/Workspace/Users/me/.genie-workbench-deploy/genie-workbench-dh2/gso/jobs",
        "/Volumes/main/genie_space_optimizer/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl",
    )

    assert settings["tags"]["app"] == "genie-workbench-dh2"
    assert settings["tags"]["managed-by"] == "notebook-installer"


def test_genie_space_grant_patches_can_manage_without_replacing_acl():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        grant_genie_spaces=True,
    )
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/genie/spaces"): {
                "spaces": [{"space_id": "space-1"}]
            }
        }
    )

    assert optionally_grant_genie_spaces(w, cfg, "sp-client-id") == 1

    grant_call = w.api_client.calls[1]
    assert grant_call == (
        "PATCH",
        "/api/2.0/permissions/dashboards.genie/space-1",
        {
            "access_control_list": [
                {
                    "service_principal_name": "sp-client-id",
                    "permission_level": "CAN_MANAGE",
                }
            ]
        },
    )
    assert all(call[0] != "PUT" for call in w.api_client.calls)


def test_genie_space_grants_count_successes_and_skip_failures():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        grant_genie_spaces=True,
    )
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/genie/spaces"): {
                "spaces": [{"space_id": "space-ok"}, {"space_id": "space-fail"}]
            },
            ("PATCH", "/api/2.0/permissions/dashboards.genie/space-fail"): RuntimeError("denied"),
        }
    )

    assert optionally_grant_genie_spaces(w, cfg, "sp-client-id") == 1


def test_find_existing_job_scopes_reuse_to_current_notebook_app():
    cfg = InstallConfig(
        app_name="genie-workbench-dh2",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
    )
    settings = build_job_settings(cfg, "/Workspace/Users/me/gso/jobs", "/Volumes/main/schema/wheel.whl")
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.1/jobs/list?limit=100&expand_tasks=false"): {
                "jobs": [
                    {
                        "job_id": 100,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": {
                                "app": "genie-workbench",
                                "managed-by": "notebook-installer",
                                "pattern": "persistent-dag",
                            },
                        },
                    },
                    {
                        "job_id": 101,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": {
                                "app": "genie-workbench-dh2",
                                "managed-by": "databricks-bundle",
                                "pattern": "persistent-dag",
                            },
                        },
                    },
                    {
                        "job_id": 102,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": settings["tags"],
                        },
                    },
                ]
            }
        }
    )

    assert find_existing_job(w, settings) == 102


def test_find_existing_job_paginates_to_matching_job():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
    )
    settings = build_job_settings(cfg, "/Workspace/Users/me/gso/jobs", "/Volumes/main/schema/wheel.whl")
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.1/jobs/list?limit=100&expand_tasks=false"): {
                "jobs": [
                    {
                        "job_id": 100,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": {
                                "app": "other-app",
                                "managed-by": "notebook-installer",
                                "pattern": "persistent-dag",
                            },
                        },
                    }
                ],
                "next_page_token": "page 2",
            },
            ("GET", "/api/2.1/jobs/list?limit=100&expand_tasks=false&page_token=page%202"): {
                "jobs": [
                    {
                        "job_id": 200,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": settings["tags"],
                        },
                    }
                ]
            },
        }
    )

    assert find_existing_job(w, settings) == 200


def test_upsert_job_creates_when_same_name_job_is_not_current_app():
    cfg = InstallConfig(
        app_name="genie-workbench-dh2",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
    )
    settings = build_job_settings(cfg, "/Workspace/Users/me/gso/jobs", "/Volumes/main/schema/wheel.whl")
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.1/jobs/list?limit=100&expand_tasks=false"): {
                "jobs": [
                    {
                        "job_id": 100,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": {
                                "app": "genie-workbench",
                                "managed-by": "notebook-installer",
                                "pattern": "persistent-dag",
                            },
                        },
                    }
                ]
            },
            ("POST", "/api/2.1/jobs/create"): {"job_id": 300},
        }
    )

    assert upsert_job(w, settings) == 300
    assert not any(call[1] == "/api/2.1/jobs/reset" for call in w.api_client.calls)


def test_require_successful_deployment_raises_on_failed_state():
    app = {"pending_deployment": {"status": {"state": "FAILED"}}}

    with pytest.raises(RuntimeError, match="genie-workbench.*FAILED"):
        require_successful_deployment("genie-workbench", app)


def test_require_successful_deployment_returns_successful_deployment():
    deployment = {"status": {"state": "SUCCEEDED"}, "deployment_id": "dep-1"}
    assert require_successful_deployment(
        "genie-workbench",
        {"active_deployment": deployment},
    ) == deployment


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
