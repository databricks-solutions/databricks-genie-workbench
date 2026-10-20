"""Configuration models for the notebook installer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Literal

from genie_space_optimizer.common.ai_gateway import DEFAULT_CHAT_MODEL, normalize_model_name


LakebaseMode = Literal["create", "existing", "skip"]

APP_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
GSO_JOB_BASENAME = "gso-optimization-job"


def default_gso_job_name(app_name: str) -> str:
    app_name = (app_name or "").strip()
    if not app_name:
        return GSO_JOB_BASENAME
    return f"{app_name}-{GSO_JOB_BASENAME}"


@dataclass(frozen=True)
class LakebaseInfo:
    project_name: str
    branch_resource: str
    database_resource: str | None
    endpoint_resource: str | None
    grants_applied: bool


@dataclass(frozen=True)
class GsoJobInfo:
    job_id: int
    job_name: str
    notebooks_path: str
    wheel_path: str


@dataclass(frozen=True)
class InstallConfig:
    app_name: str
    catalog: str
    warehouse_id: str
    llm_model: str = DEFAULT_CHAT_MODEL
    mlflow_experiment_id: str | None = None
    lakebase_mode: LakebaseMode = "create"
    lakebase_instance: str | None = None
    gso_schema: str = "genie_space_optimizer"
    repo_root: str | None = None
    deploy_workspace_path: str | None = None
    grant_genie_spaces: bool = False
    update_only: bool = False
    gso_job_name: str = ""
    gso_wheel_path: str | None = None

    def normalized(self) -> "InstallConfig":
        app_name = self.app_name.strip()
        lakebase_instance = (self.lakebase_instance or "").strip() or None
        if self.lakebase_mode == "create" and not lakebase_instance:
            lakebase_instance = f"{app_name}-lakebase"
        if self.lakebase_mode == "skip":
            lakebase_instance = None
        gso_job_name = (self.gso_job_name or "").strip() or default_gso_job_name(app_name)
        return InstallConfig(
            app_name=app_name,
            catalog=self.catalog.strip(),
            warehouse_id=self.warehouse_id.strip(),
            llm_model=normalize_model_name(self.llm_model) or DEFAULT_CHAT_MODEL,
            mlflow_experiment_id=(self.mlflow_experiment_id or "").strip() or None,
            lakebase_mode=self.lakebase_mode,
            lakebase_instance=lakebase_instance,
            gso_schema=(self.gso_schema or "genie_space_optimizer").strip(),
            repo_root=(self.repo_root or "").strip() or None,
            deploy_workspace_path=(self.deploy_workspace_path or "").strip() or None,
            grant_genie_spaces=bool(self.grant_genie_spaces),
            update_only=bool(self.update_only),
            gso_job_name=gso_job_name,
            gso_wheel_path=(self.gso_wheel_path or "").strip() or None,
        )

    def validate(self) -> None:
        cfg = self.normalized()
        errors: list[str] = []
        if not APP_NAME_RE.match(cfg.app_name):
            errors.append(
                "app_name must be lowercase letters, numbers, and hyphens, "
                "1-63 chars, and cannot start or end with a hyphen"
            )
        if not cfg.catalog:
            errors.append("catalog is required")
        if not cfg.warehouse_id:
            errors.append("warehouse_id is required")
        if cfg.lakebase_mode not in ("create", "existing", "skip"):
            errors.append("lakebase_mode must be one of: create, existing, skip")
        if cfg.lakebase_mode == "existing" and not cfg.lakebase_instance:
            errors.append("lakebase_instance is required when lakebase_mode is existing")
        if not cfg.repo_root:
            errors.append("repo_root is required")
        if errors:
            raise ValueError("; ".join(errors))


@dataclass(frozen=True)
class InstallResult:
    app_name: str
    app_url: str | None
    source_path: str
    service_principal_client_id: str
    gso_job: GsoJobInfo
    lakebase: LakebaseInfo | None
    genie_spaces_granted: int
    deployment: dict[str, Any]
    verification: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
