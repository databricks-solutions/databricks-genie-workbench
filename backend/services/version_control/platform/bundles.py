"""Read-only bundle inventory. Provisioning never owns Genie content."""

from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256

import yaml


class GovernedContentError(ValueError):
    def __init__(self, path, location):
        super().__init__(f"DAB dual authority: genie_spaces resource at {location}")
        self.path = path
        self.location = location


@dataclass(frozen=True)
class BundleDetectionEvidence:
    schema_version: str
    root: str
    status: str
    observed_at: str
    file_digests: dict[str, str]
    findings: tuple[str, ...]


def bundle_detection_evidence(path: Path) -> BundleDetectionEvidence:
    path = Path(path).resolve()
    paths = {path}
    status, findings = "clear", ()
    try:
        paths.update(guard_bundle(path))
    except GovernedContentError as error:
        paths.add(error.path)
        status, findings = "dual_authority", (error.location,)
    except (OSError, ValueError, yaml.YAMLError, RecursionError):
        status, findings = "unknown", ("Bundle inventory could not be resolved",)
    digests = {}
    for source in paths:
        try:
            digests[str(source)] = sha256(source.read_bytes()).hexdigest()
        except OSError:
            status = "unknown"
    return BundleDetectionEvidence("VC/1.0", str(path), status,
                                   datetime.now(timezone.utc).isoformat(), digests, findings)


def preflight_deployment(root: Path):
    evidence = tuple(bundle_detection_evidence(root / path) for path in
                     ("databricks.yml", "packages/genie-space-optimizer/databricks.yml"))
    if any(item.status != "clear" for item in evidence):
        raise PermissionError("VC bundle content guard blocked unknown or dual-authority deployment")
    return evidence


@dataclass(frozen=True)
class DeploymentEntry:
    path: str
    transition: str
    authoritative_optimizer: bool = False
    governed_content: bool = False


def deployment_inventory(root: Path) -> tuple[DeploymentEntry, ...]:
    entries = (
        DeploymentEntry("databricks.yml", "Retain sole Workbench optimizer Job authority", True),
        DeploymentEntry("packages/genie-space-optimizer/databricks.yml",
                        "Standalone only; never deploy alongside root into the same installation"),
        DeploymentEntry("scripts/deploy.sh", "Retain app deploy until verified DAB app import/cutover"),
        DeploymentEntry("scripts/deploy_lib/install.py", "Notebook installer; same app cutover barrier"),
        DeploymentEntry("app.yaml", "Preserve app runtime configuration; all VC writers disabled"),
    )
    for entry in entries:
        if not (root / entry.path).is_file():
            raise ValueError(f"Missing deployment path: {entry.path}")
        if entry.path.endswith("databricks.yml"):
            guard_bundle(root / entry.path)
    return entries


def guard_bundle(path: Path) -> tuple[Path, ...]:
    visited = set()
    active = set()

    def inspect(value, path, location=""):
        if isinstance(value, dict):
            if "genie_spaces" in value:
                raise GovernedContentError(path, f"{location}.genie_spaces".lstrip("."))
            for key, nested in value.items():
                inspect(nested, path, f"{location}.{key}".lstrip("."))
        elif isinstance(value, list):
            for nested in value:
                inspect(nested, path, location)

    def visit(current):
        current = current.resolve()
        if current in active:
            raise ValueError("Cyclic bundle include")
        if current in visited:
            return
        active.add(current)
        data = yaml.safe_load(current.read_text())
        if not isinstance(data, dict):
            raise ValueError("Bundle must be a mapping")
        inspect(data, current)
        includes = data.get("include", [])
        if not isinstance(includes, list):
            raise ValueError("Bundle include must be a list")
        for pattern in includes:
            if not isinstance(pattern, str) or Path(pattern).is_absolute():
                raise ValueError("Unresolved bundle include")
            matches = sorted(current.parent.glob(pattern))
            if not matches:
                raise ValueError(f"Unresolved bundle include: {pattern}")
            for included in matches:
                visit(included)
        active.remove(current)
        visited.add(current)

    visit(Path(path))
    return tuple(sorted(visited))
