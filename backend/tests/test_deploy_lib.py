from pathlib import Path

import pytest

from scripts.deploy_lib.app_yaml import render_text
from scripts.deploy_lib.apps import (
    deploy_app_from_workspace,
    get_app_service_principal,
    patch_app_resources,
    require_successful_deployment,
    wait_for_deployment,
)
from scripts.deploy_lib.config import InstallConfig, LakebaseInfo
from scripts.deploy_lib.genie_spaces import optionally_grant_genie_spaces
from scripts.deploy_lib.gso_job import (
    TASKS,
    build_job_settings,
    find_existing_job,
    upload_job_notebooks,
    upsert_job,
)
from scripts.deploy_lib.lakebase import ensure_lakebase, get_database_resource
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


class FakeApps:
    def __init__(self):
        self.deployments = []

    def deploy_and_wait(self, app_name, app_deployment, timeout):
        self.deployments.append((app_name, app_deployment.as_dict(), timeout.total_seconds()))
        return type(
            "Deployment",
            (),
            {
                "as_dict": lambda _self: {
                    "deployment_id": "dep-1",
                    "status": {"state": "SUCCEEDED"},
                }
            },
        )()


class FakeOp:
    def wait(self):
        return None


class FakeProject:
    def __init__(self, name, *, delete_time=None):
        self.name = name
        self.delete_time = delete_time


class FakePostgres:
    def __init__(self, *, project_exists=True, soft_deleted=False, reserved_soft_deleted=None):
        self.project_exists = project_exists
        # When True, get_project returns a project whose delete_time is set —
        # Lakebase reports soft-deleted projects as if live.
        self.soft_deleted = soft_deleted
        # Project names that only surface as soft-deleted via list_projects:
        # the reservation that makes a same-name create fail after get 404s.
        self.reserved_soft_deleted = list(reserved_soft_deleted or [])
        self.created_projects = []
        self.created_roles = []
        self.purged_projects = []

    def get_project(self, *, name):
        if not self.project_exists:
            from databricks.sdk.errors import NotFound

            raise NotFound(f"{name} not found")
        return FakeProject(name, delete_time="2026-06-20T00:00:00Z" if self.soft_deleted else None)

    def list_projects(self, *, show_deleted=False):
        if not show_deleted:
            return []
        return [
            FakeProject(f"projects/{name}", delete_time="2026-06-20T00:00:00Z")
            for name in self.reserved_soft_deleted
        ]

    def delete_project(self, *, name, purge=False):
        self.purged_projects.append((name, purge))
        # Purging frees the reserved name so the subsequent create succeeds.
        self.soft_deleted = False
        self.project_exists = False
        self.reserved_soft_deleted = [n for n in self.reserved_soft_deleted if f"projects/{n}" != name]
        return FakeOp()

    def create_project(self, *, project, project_id):
        self.created_projects.append(project_id)
        self.project_exists = True
        self.soft_deleted = False
        return FakeOp()

    def create_role(self, **kwargs):
        self.created_roles.append(kwargs)
        return FakeOp()

    def get_endpoint(self, *, name):
        raise RuntimeError(f"{name} unavailable")


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
    assert cfg.gso_job_name == "genie-workbench-gso-optimization-job"
    cfg.validate()

    explicit_cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="abc",
        repo_root="/Workspace/Repos/me/repo",
        lakebase_mode="create",
        lakebase_instance="custom-lakebase",
    ).normalized()
    assert explicit_cfg.lakebase_instance == "custom-lakebase"

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


def test_notebook_installer_uses_streamlined_widgets_and_defaults():
    notebook_source = Path("notebooks/install.py").read_text()

    assert 'dbutils.widgets.text("lakebase_project_name", "")' in notebook_source
    assert 'dbutils.widgets.get("lakebase_project_name").strip()' in notebook_source
    assert 'lakebase_instance = f"{app_name}-lakebase"' in notebook_source
    assert 'dbutils.widgets.text("lakebase_instance"' not in notebook_source
    assert 'dbutils.widgets.get("lakebase_instance")' not in notebook_source

    assert 'dbutils.widgets.text("mlflow_experiment_id"' not in notebook_source
    assert 'dbutils.widgets.get("mlflow_experiment_id")' not in notebook_source
    assert "mlflow_experiment_id=None" in notebook_source

    assert 'dbutils.widgets.dropdown("grant_genie_spaces"' not in notebook_source
    assert 'dbutils.widgets.get("grant_genie_spaces")' not in notebook_source
    assert "grant_genie_spaces=True" in notebook_source


def test_workspace_source_inclusion_rules(tmp_path):
    repo = tmp_path / "repo"
    files = [
        "backend/main.py",
        "backend/references/schema.md",
        "README.md",
        "requirements.txt",
        ".env.deploy",
        ".env.local",
        ".env.production",
        "backend/.env.local",
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
    assert not should_copy(repo / ".env.local", repo)
    assert not should_copy(repo / ".env.production", repo)
    assert not should_copy(repo / "backend/.env.local", repo)
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
    assert "iam.access-control:read" not in payload["user_api_scopes"]
    assert any(call[0] == "PATCH" and call[1] == "/api/2.0/apps/genie-workbench" for call in w.api_client.calls)


def test_deploy_app_from_workspace_uses_sdk_waiter():
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): {
                "compute_status": {"state": "ACTIVE"}
            }
        }
    )
    w.apps = FakeApps()

    deployment = deploy_app_from_workspace(
        w,
        "genie-workbench",
        "/Workspace/Users/me/.genie-workbench-deploy/app",
        timeout_seconds=30,
    )

    assert deployment["deployment_id"] == "dep-1"
    assert deployment["status"]["state"] == "SUCCEEDED"
    assert w.apps.deployments == [
        (
            "genie-workbench",
            {
                "mode": "SNAPSHOT",
                "source_code_path": "/Workspace/Users/me/.genie-workbench-deploy/app",
            },
            30.0,
        )
    ]


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


# GSO v2 linear 5-task DAG (mirrors packages/genie-space-optimizer/databricks.yml,
# validated by tests/unit/test_phase7_job_dag.py).
_EXPECTED_5TASK_KEYS = [
    "00_intake_and_snapshot",
    "01_benchmark_qc_and_repair",
    "02_baseline_eval_and_triage",
    "03_optimize",
    "publish_and_audit",
]

_EXPECTED_5TASK_NOTEBOOKS = {
    "00_intake_and_snapshot": "run_00_intake_and_snapshot",
    "01_benchmark_qc_and_repair": "run_01_benchmark_qc_and_repair",
    "02_baseline_eval_and_triage": "run_02_baseline_eval_and_triage",
    "03_optimize": "run_03_optimize",
    "publish_and_audit": "run_publish_and_audit",
}


def test_gso_job_settings_match_5task_dag_shape():
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        llm_model="custom-gso-model",
        repo_root="/tmp",
    )
    settings = build_job_settings(
        cfg,
        "/Workspace/Users/me/.genie-workbench-deploy/genie-workbench/gso/jobs",
        "/Volumes/main/genie_space_optimizer/app_artifacts/genie_space_optimizer-0.0.0-py3-none-any.whl",
    )

    # Unchanged job identity: name + tags stay stable so find_existing_job can
    # re-discover and upsert prior notebook installs across the cutover.
    assert settings["name"] == "genie-workbench-gso-optimization-job"
    assert settings["queue"]["enabled"] is True
    assert settings["tags"]["app"] == "genie-workbench"
    assert settings["tags"]["managed-by"] == "notebook-installer"
    assert settings["tags"]["pattern"] == "persistent-dag"
    assert settings["environments"][0]["spec"]["environment_version"] == "4"

    # 5-task linear DAG — no condition task, no deploy task, correct entrypoints.
    tasks = settings["tasks"]
    assert [t["task_key"] for t in tasks] == _EXPECTED_5TASK_KEYS
    assert all("condition_task" not in t for t in tasks)
    for t in tasks:
        stem = t["notebook_task"]["notebook_path"].rsplit("/", 1)[-1]
        assert stem == _EXPECTED_5TASK_NOTEBOOKS[t["task_key"]]
    by_key = {t["task_key"]: t for t in tasks}
    assert "depends_on" not in by_key["00_intake_and_snapshot"]
    for prev, cur in zip(_EXPECTED_5TASK_KEYS, _EXPECTED_5TASK_KEYS[1:]):
        assert by_key[cur]["depends_on"] == [{"task_key": prev}]

    # New v2 loop params present with defaults; retired params gone.
    params = {p["name"]: p["default"] for p in settings["parameters"]}
    assert params["max_attempts"] == "3"
    assert params["target_accuracy"] == "0.90"
    assert params["benchmark_repair_max_tries"] == "3"
    assert "experiment_name" not in params
    assert "deploy_target" not in params
    # llm_model stays a Workbench-specific param defaulted to cfg.llm_model.
    assert params["llm_model"] == "custom-gso-model"

    # Every task carries run_id/catalog/schema (D9 — bootstrap from job params,
    # no task-value plumbing) and the operator llm_model.
    for t in tasks:
        bp = t["notebook_task"]["base_parameters"]
        assert bp["llm_model"] == "{{job.parameters.llm_model}}"
        for required in ("run_id", "catalog", "schema"):
            assert bp[required] == f"{{{{job.parameters.{required}}}}}"


def test_gso_job_settings_mirror_package_bundle_5task():
    """The notebook-installer job (gso_job.py) and the package bundle
    (packages/genie-space-optimizer/databricks.yml, validated by
    test_phase7_job_dag.py) must not silently drift: identical task keys/order,
    entrypoints, per-task base_parameters and declared params, EXCEPT for the
    Workbench-specific ``llm_model`` extra."""
    yaml = pytest.importorskip("yaml")
    repo_root = Path(__file__).resolve().parents[2]
    bundle_path = repo_root / "packages" / "genie-space-optimizer" / "databricks.yml"
    bundle = yaml.safe_load(bundle_path.read_text())
    (pkg_job,) = bundle["resources"]["jobs"].values()

    cfg = InstallConfig(app_name="genie-workbench", catalog="main", warehouse_id="wh", repo_root="/tmp")
    settings = build_job_settings(cfg, "/Workspace/Users/me/gso/jobs", "/Volumes/main/schema/wheel.whl")

    def _stem(path: str) -> str:
        return path.rsplit("/", 1)[-1].removesuffix(".py")

    # Same task keys + order.
    assert [t["task_key"] for t in settings["tasks"]] == [t["task_key"] for t in pkg_job["tasks"]]
    # Same entrypoint stems.
    assert [_stem(t["notebook_task"]["notebook_path"]) for t in settings["tasks"]] == [
        _stem(t["notebook_task"]["notebook_path"]) for t in pkg_job["tasks"]
    ]

    # Declared params identical except llm_model.
    gso_params = {p["name"] for p in settings["parameters"]}
    pkg_params = {p["name"] for p in pkg_job["parameters"]}
    assert gso_params - pkg_params == {"llm_model"}
    assert pkg_params - gso_params == set()

    # Per-task base_parameters identical except llm_model.
    gso_bp = {t["task_key"]: set(t["notebook_task"]["base_parameters"]) for t in settings["tasks"]}
    pkg_bp = {t["task_key"]: set(t["notebook_task"].get("base_parameters", {})) for t in pkg_job["tasks"]}
    for key in gso_bp:
        assert gso_bp[key] - pkg_bp[key] == {"llm_model"}, key
        assert pkg_bp[key] - gso_bp[key] == set(), key


def test_upload_job_notebooks_uploads_all_five_task_notebooks(tmp_path):
    """Exercise the TASKS upload loop end-to-end. Guards against the 4-tuple
    arity regression: upload_job_notebooks must iterate every TASKS entry and
    import exactly the 5 v2 entrypoints (this loop is what the install.py path
    runs before creating the job)."""
    jobs_dir = tmp_path / "packages" / "genie-space-optimizer" / "src" / "genie_space_optimizer" / "jobs"
    jobs_dir.mkdir(parents=True)
    expected_stems = [notebook_stem for _key, notebook_stem, _dep, _bp in TASKS]
    for stem in expected_stems:
        (jobs_dir / f"{stem}.py").write_text(f"# {stem}\n")

    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root=str(tmp_path),
    )
    w = FakeWorkspaceClient()

    notebooks_path = upload_job_notebooks(w, cfg, "me@example.com")

    assert notebooks_path.endswith("/genie-workbench/gso/jobs")
    import_paths = [
        body["path"]
        for method, path, body in w.api_client.calls
        if method == "POST" and path == "/api/2.0/workspace/import"
    ]
    # Every task notebook imported, in DAG order, with no arity error.
    assert [p.rsplit("/", 1)[-1] for p in import_paths] == expected_stems
    assert expected_stems == [
        "run_00_intake_and_snapshot",
        "run_01_benchmark_qc_and_repair",
        "run_02_baseline_eval_and_triage",
        "run_03_optimize",
        "run_publish_and_audit",
    ]


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
    assert settings["name"] == "genie-workbench-dh2-gso-optimization-job"


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
                            "name": settings["name"],
                            "tags": settings["tags"],
                        },
                    },
                ]
            }
        }
    )

    assert find_existing_job(w, settings) == 102


def test_find_existing_job_prefers_prefixed_name_over_legacy_same_app_job():
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
                        "job_id": 101,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": settings["tags"],
                        },
                    },
                    {
                        "job_id": 102,
                        "settings": {
                            "name": settings["name"],
                            "tags": settings["tags"],
                        },
                    },
                ]
            }
        }
    )

    assert find_existing_job(w, settings) == 102


def test_find_existing_job_reuses_legacy_same_app_job_for_rename():
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
                        "job_id": 101,
                        "settings": {
                            "name": "gso-optimization-job",
                            "tags": settings["tags"],
                        },
                    }
                ]
            }
        }
    )

    assert find_existing_job(w, settings) == 101


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
                            "name": settings["name"],
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
                            "name": settings["name"],
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


def test_wait_for_deployment_ignores_old_active_deployment(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}}},
                {
                    "pending_deployment": {"deployment_id": "new", "status": {"state": "RUNNING"}},
                    "active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}},
                },
                {"active_deployment": {"deployment_id": "new", "status": {"state": "SUCCEEDED"}}},
            ]
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={"deployment_id": "new"},
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert app["pending_deployment"]["deployment_id"] == "new"
    assert app["pending_deployment"]["status"]["state"] == "SUCCEEDED"


def test_wait_for_deployment_polls_submitted_deployment_by_id(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}}},
                {"active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}}},
            ],
            ("GET", "/api/2.0/apps/genie-workbench/deployments/new"): [
                {"deployment_id": "new", "status": {"state": "IN_PROGRESS"}},
                {"deployment_id": "new", "status": {"state": "SUCCEEDED"}},
            ],
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={"deployment_id": "new"},
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert app["pending_deployment"]["deployment_id"] == "new"
    assert app["pending_deployment"]["status"]["state"] == "SUCCEEDED"


def test_wait_for_deployment_without_token_waits_for_new_pending(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}}},
                {
                    "pending_deployment": {"deployment_id": "new", "status": {"state": "RUNNING"}},
                    "active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}},
                },
                {"active_deployment": {"deployment_id": "new", "status": {"state": "SUCCEEDED"}}},
            ]
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={},
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert app["pending_deployment"]["deployment_id"] == "new"
    assert app["pending_deployment"]["status"]["state"] == "SUCCEEDED"


def test_wait_for_deployment_accepts_changed_active_when_submitted_token_differs(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}}},
                {"active_deployment": {"deployment_id": "new", "status": {"state": "SUCCEEDED"}}},
            ]
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={"deployment_id": "post-response-token"},
        baseline_active_token="old",
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert app["pending_deployment"]["deployment_id"] == "new"
    assert app["pending_deployment"]["status"]["state"] == "SUCCEEDED"


def test_wait_for_deployment_accepts_changed_active_without_deployment_token(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): [
                {"active_deployment": {"status": {"state": "SUCCEEDED"}, "create_time": "old"}},
                {"active_deployment": {"status": {"state": "SUCCEEDED"}, "create_time": "new"}},
            ]
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={"deployment_id": "post-response-token"},
        baseline_active_fingerprint='{"create_time": "old", "status": {"state": "SUCCEEDED"}}',
        timeout_seconds=1,
        poll_seconds=0,
    )

    assert app["pending_deployment"]["create_time"] == "new"
    assert app["pending_deployment"]["status"]["state"] == "SUCCEEDED"


def test_wait_for_deployment_selects_failed_submitted_deployment(monkeypatch):
    monkeypatch.setattr("scripts.deploy_lib.apps.time.sleep", lambda _seconds: None)
    w = FakeWorkspaceClient(
        {
            ("GET", "/api/2.0/apps/genie-workbench"): {
                "pending_deployment": {"deployment_id": "new", "status": {"state": "FAILED"}},
                "active_deployment": {"deployment_id": "old", "status": {"state": "SUCCEEDED"}},
            }
        }
    )

    app = wait_for_deployment(
        w,
        "genie-workbench",
        submitted_deployment={"deployment_id": "new"},
        timeout_seconds=1,
        poll_seconds=0,
    )

    with pytest.raises(RuntimeError, match="genie-workbench.*FAILED"):
        require_successful_deployment("genie-workbench", app)


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


def test_lakebase_existing_mode_requires_existing_project():
    w = FakeWorkspaceClient()
    w.postgres = FakePostgres(project_exists=False)
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        lakebase_mode="existing",
        lakebase_instance="missing-lakebase",
    )

    with pytest.raises(RuntimeError, match="missing-lakebase.*does not exist"):
        ensure_lakebase(w, cfg, "sp-client-id")

    assert w.postgres.created_projects == []


def test_lakebase_create_mode_creates_missing_project():
    w = FakeWorkspaceClient(
        {
            (
                "GET",
                "/api/2.0/postgres/projects/new-lakebase/branches/production/databases",
            ): {
                "databases": [
                    {"name": "projects/new-lakebase/branches/production/databases/databricks_postgres"}
                ]
            }
        }
    )
    w.config = type("Config", (), {"client_id": None})()
    w.postgres = FakePostgres(project_exists=False)
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        lakebase_mode="create",
        lakebase_instance="new-lakebase",
    )

    lakebase = ensure_lakebase(w, cfg, "sp-client-id")

    assert w.postgres.created_projects == ["new-lakebase"]
    assert w.postgres.purged_projects == []
    assert lakebase.database_resource == "projects/new-lakebase/branches/production/databases/databricks_postgres"


def _databases_response():
    return {
        (
            "GET",
            "/api/2.0/postgres/projects/new-lakebase/branches/production/databases",
        ): {
            "databases": [
                {"name": "projects/new-lakebase/branches/production/databases/databricks_postgres"}
            ]
        }
    }


def test_lakebase_create_mode_purges_soft_deleted_name_reservation():
    # Reported failure: get_project 404s, but the name is still reserved by a
    # soft-deleted project that surfaces via list_projects(show_deleted=True).
    w = FakeWorkspaceClient(_databases_response())
    w.config = type("Config", (), {"client_id": None})()
    w.postgres = FakePostgres(project_exists=False, reserved_soft_deleted=["new-lakebase"])
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        lakebase_mode="create",
        lakebase_instance="new-lakebase",
    )

    lakebase = ensure_lakebase(w, cfg, "sp-client-id")

    assert w.postgres.purged_projects == [("projects/new-lakebase", True)]
    assert w.postgres.created_projects == ["new-lakebase"]
    assert lakebase.database_resource == "projects/new-lakebase/branches/production/databases/databricks_postgres"


def test_lakebase_create_mode_purges_project_reported_soft_deleted_by_get():
    # get_project returns a soft-deleted project as if live (delete_time set);
    # create mode must purge and recreate it rather than treat it as usable.
    w = FakeWorkspaceClient(_databases_response())
    w.config = type("Config", (), {"client_id": None})()
    w.postgres = FakePostgres(project_exists=True, soft_deleted=True)
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        lakebase_mode="create",
        lakebase_instance="new-lakebase",
    )

    lakebase = ensure_lakebase(w, cfg, "sp-client-id")

    assert w.postgres.purged_projects == [("projects/new-lakebase", True)]
    assert w.postgres.created_projects == ["new-lakebase"]
    assert lakebase.database_resource == "projects/new-lakebase/branches/production/databases/databricks_postgres"


def test_lakebase_existing_mode_rejects_soft_deleted_project():
    w = FakeWorkspaceClient()
    w.postgres = FakePostgres(project_exists=True, soft_deleted=True)
    cfg = InstallConfig(
        app_name="genie-workbench",
        catalog="main",
        warehouse_id="wh",
        repo_root="/tmp",
        lakebase_mode="existing",
        lakebase_instance="stale-lakebase",
    )

    with pytest.raises(RuntimeError, match="stale-lakebase.*soft-deleted"):
        ensure_lakebase(w, cfg, "sp-client-id")

    assert w.postgres.created_projects == []
    assert w.postgres.purged_projects == []
