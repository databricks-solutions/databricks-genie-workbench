import os
import re
import shlex
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from unittest.mock import Mock

import pytest

from backend.services.version_control import platform

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("scenario", ["clear", "dual_authority", "unknown", "missing_yaml"])
def test_bundle_guard_runs_on_destroy_and_separates_tooling_failure_from_dual_authority(tmp_path, scenario):
    bundle = tmp_path / "databricks.yml"
    contents = {
        "clear": "resources: {jobs: {}}\n",
        "dual_authority": "resources: {genie_spaces: {managed: {}}}\n",
        "unknown": "include: [missing.yml]\n",
        "missing_yaml": "resources: {jobs: {}}\n",
    }
    bundle.write_text(contents[scenario])
    environment = dict(os.environ, PYTHONPATH=str(ROOT))
    if scenario == "missing_yaml":
        (tmp_path / "yaml.py").write_text("raise ImportError('simulated missing PyYAML')\n")
        environment["PYTHONPATH"] = os.pathsep.join((str(tmp_path), str(ROOT)))
    expected = {"clear": 0, "dual_authority": 1, "unknown": 2, "missing_yaml": 2}[scenario]
    command = [sys.executable, str(ROOT / "scripts/version_control/bundle_guard.py"), str(bundle)]
    result = subprocess.run(command, env=environment, capture_output=True, text=True)
    assert result.returncode == expected, result.stdout + result.stderr
    if scenario != "missing_yaml":
        assert scenario in result.stdout
    else:
        assert "tooling" in result.stderr.lower()
        assert "dual_authority" not in result.stdout

    project = tmp_path / "project"
    scripts = project / "scripts"
    (scripts / "version_control").mkdir(parents=True)
    for relative in ("deploy.sh", "deploy-config.sh", "preflight.sh", "version_control/bundle_guard.py"):
        shutil.copyfile(ROOT / "scripts" / relative, scripts / relative)
    shutil.copyfile(bundle, project / "databricks.yml")
    optimizer = project / "packages/genie-space-optimizer"
    optimizer.mkdir(parents=True)
    (optimizer / "databricks.yml").write_text("resources: {jobs: {}}\n")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    platform_calls = tmp_path / "platform-calls"
    (binaries / "databricks").write_text(f"#!/bin/sh\necho called >> {shlex.quote(str(platform_calls))}\nexit 99\n")
    (binaries / "databricks").chmod(0o755)
    (binaries / "python3").write_text("#!/bin/sh\necho 'bare python3 invoked' >&2\nexit 99\n")
    (binaries / "python3").chmod(0o755)
    interpreter = project / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} "$@"\n')
    interpreter.chmod(0o755)
    environment.update(PATH=f"{binaries}:/usr/bin:/bin", GENIE_WAREHOUSE_ID="offline",
                       GENIE_CATALOG="offline", GENIE_DEPLOY_PROFILE="explicit-test-profile")
    shell = ["bash", "-c", 'set -euo pipefail; PROJECT_DIR="$1"; source "$1/scripts/preflight.sh"; _preflight_check_vc_bundle_content', "guard", str(project)]
    result = subprocess.run(shell, env=environment, capture_output=True, text=True)
    assert result.returncode == expected, result.stdout + result.stderr
    if expected:
        assert ("dual_authority" if expected == 1 else "tooling") in result.stderr.lower()
        for flags in ([], ["--update"], ["--destroy", "--auto-approve"]):
            result = subprocess.run(["bash", str(scripts / "deploy.sh"), *flags],
                                    env=environment, capture_output=True, text=True)
            assert result.returncode == expected, result.stdout + result.stderr
            assert not platform_calls.exists(), "Guard refusal must precede every platform call"
    deploy = (scripts / "deploy.sh").read_text()
    destroy = deploy.split('if [ "$DESTROY_MODE" = "true" ]; then', 1)[1]
    assert destroy.index("_preflight_check_vc_bundle_content") < destroy.index("databricks bundle destroy")

    interpreter.unlink()
    fallback = binaries / "uv"
    fallback.write_text(f'#!/bin/sh\n[ "$1" = run ] && [ "$2" = --frozen ] && [ "$3" = --project ] && [ "$4" = {shlex.quote(str(project))} ] && [ "$5" = python ] || exit 99\nshift 5\nexec {shlex.quote(sys.executable)} "$@"\n')
    fallback.chmod(0o755)
    result = subprocess.run(shell, env=environment, capture_output=True, text=True)
    assert result.returncode == expected, result.stdout + result.stderr
    fallback.unlink()
    result = subprocess.run(shell, env=environment, capture_output=True, text=True)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "tooling" in result.stderr.lower()


def test_platform_package_import_requires_only_declared_root_dependencies():
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    assert "pyyaml==6.0.3" in dependencies
    script = """
import importlib.metadata
import re
import sys
import tomllib
from pathlib import Path
dependencies = tomllib.loads(Path('pyproject.toml').read_text())['project']['dependencies']
declared = {re.split(r'[=<>!~\\[]', name)[0].lower().replace('_', '-') for name in dependencies}
distributions = importlib.metadata.packages_distributions()
before = set(sys.modules)
import backend.services.version_control.platform as platform
for module in set(sys.modules) - before:
    root = module.split('.')[0]
    if root == 'backend' or root in sys.stdlib_module_names:
        continue
    owners = {name.lower().replace('_', '-') for name in distributions.get(root, [])}
    assert owners & declared, (module, owners)
for name in ('bundles', 'provisioning', 'permissions', 'capabilities', 'identity', 'jobs', 'termination'):
    assert f'{platform.__name__}.{name}' not in sys.modules, name
assert 'backend.services.version_control.contracts' not in sys.modules
provider = platform.PlatformIdentityProvider
assert f'{platform.__name__}.identity' in sys.modules
assert platform.__dict__['PlatformIdentityProvider'] is provider
assert platform.PlatformIdentityProvider is provider
assert f'{platform.__name__}.jobs' not in sys.modules
try:
    platform.nonexistent_adapter
except AttributeError:
    pass
else:
    raise AssertionError('Unknown exports must raise AttributeError')
"""
    result = subprocess.run([sys.executable, "-c", script], cwd=ROOT,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    installer = (ROOT / "scripts/deploy_lib/install.py").read_text()
    assert "from backend.services.version_control.platform.bundles import preflight_deployment" in installer
    assert "from backend.services.version_control.platform import preflight_deployment" not in installer


def test_bundle_guard_precedes_every_databricks_invocation_on_every_branch():
    preflight = (ROOT / "scripts/preflight.sh").read_text()
    deploy = (ROOT / "scripts/deploy.sh").read_text()
    lines = deploy.splitlines()
    invocations = [
        number for number, line in enumerate(lines)
        if not line.lstrip().startswith(("#", "echo ", "echo -"))
        and re.search(r"(^|[^#\w])databricks ", line)
    ]
    guard = lines.index("_preflight_check_vc_bundle_content")
    violations = []
    if preflight.index("_error()") > preflight.index("_preflight_check_vc_bundle_content()"):
        violations.append("guard helper must follow status helpers")
    assert invocations, "Must enumerate every platform invocation"
    for number in invocations:
        if number <= guard:
            violations.append(f"unguarded platform invocation at line {number + 1}: {lines[number]}")
    if guard >= lines.index('if [ "$DESTROY_MODE" = "true" ]; then'):
        violations.append("guard must run unconditionally before the destroy/deploy branch")
    assert not violations, "\n".join(violations)


def test_deployment_contains_no_governed_genie_content_resource(tmp_path):
    guard = getattr(platform, "guard_bundle", None)
    assert callable(guard), "Provisioning must reject nested governed content before deploy"
    root = tmp_path / "databricks.yml"
    child = tmp_path / "child.yml"
    root.write_text("include: [child.yml]\nresources: {jobs: {}}\n")
    child.write_text("targets: {prod: {resources: {genie_spaces: {managed: {}}}}}\n")
    with pytest.raises(ValueError, match="genie_spaces"):
        guard(root)
    child.write_text("include: [missing.yml]\n")
    with pytest.raises(ValueError, match="include"):
        guard(root)
    child.write_text("resources: {jobs: {worker: {name: worker}}}\n")
    assert set(guard(root)) == {root, child}
    guard(ROOT / "databricks.yml")


def test_existing_optimizer_bundle_and_app_deploy_paths_remain_accounted_for():
    inventory = getattr(platform, "deployment_inventory", None)
    assert callable(inventory), "Existing deployments need an explicit transition inventory"
    entries = inventory(ROOT)
    assert {entry.path for entry in entries} == {
        "databricks.yml", "packages/genie-space-optimizer/databricks.yml",
        "scripts/deploy.sh", "scripts/deploy_lib/install.py", "app.yaml",
    }
    assert all((ROOT / entry.path).is_file() and entry.transition for entry in entries)
    assert sum(entry.authoritative_optimizer for entry in entries) == 1
    assert all(entry.governed_content is False for entry in entries)


def test_ddl_migrations_are_owned_ordered_idempotent_and_not_content_deploys():
    run = getattr(platform, "provision", None)
    assert callable(run), "Provisioning must validate owner manifests before executing"
    runner = Mock()
    manifests = [
        {"owner": owner, "kind": kind, "name": name, "idempotent": True, "revision": "sha256:" + "a" * 64}
        for owner, kind, name in [
            ("M02", "table", "genie_space_versions"),
            ("M02", "table", "genie_space_registry"),
            ("M03", "table", "genie_ops_coordination"),
            ("M06", "table", "genie_space_operations"),
            ("M09", "table", "genie_space_version_tags"),
            ("M02", "volume", "vc_snapshots"),
            ("M07", "volume", "vc_outbound_packages"),
            ("M06", "volume", "vc_approval_evidence"),
            ("M07", "volume", "vc_target_receipts"),
        ]
    ]
    runner.validate_owner_spec.return_value = True
    run(list(reversed(manifests)), runner)
    assert [call.args[0]["kind"] for call in runner.apply_owner_spec.call_args_list] == ["table"] * 5 + ["volume"] * 4
    runner.apply_grants.assert_called_once_with()
    runner.reset_mock()
    for invalid in [manifests[:-1], manifests + [manifests[0]],
                    [dict(manifests[0], owner="M08")] + manifests[1:],
                    [dict(manifests[0], idempotent=False)] + manifests[1:],
                    [dict(manifests[0], revision="unverified")] + manifests[1:],
                    [dict(manifests[0], kind="genie_spaces")] + manifests[1:]]:
        with pytest.raises(ValueError):
            run(invalid, runner)
    runner.apply_owner_spec.assert_not_called()
    runner.apply_grants.assert_not_called()
    runner.validate_owner_spec.return_value = False
    with pytest.raises(ValueError, match="owner"):
        run(manifests, runner)
    runner.apply_owner_spec.assert_not_called()


def test_owner_manifests_are_built_from_ddl_files_and_accepted_by_provision():
    build = getattr(platform, "build_owner_manifests", None)
    assert callable(build), "Provisioning must build owner manifests from the DDL files"
    from backend.services.version_control.platform.provisioning import OWNER_SPECS

    manifests = build()
    assert {(m["owner"], m["kind"], m["name"]) for m in manifests} == set(OWNER_SPECS)
    for manifest in manifests:
        assert manifest["idempotent"] is True
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", manifest["revision"])
        assert manifest["statements"]
        qualified = "${catalog}.${control_schema}." + manifest["name"]
        create = manifest["statements"][0]
        verb = "TABLE" if manifest["kind"] == "table" else "VOLUME"
        assert qualified in create
        assert f"CREATE {verb} IF NOT EXISTS" in create
        # Any statements after the CREATE attach CHECK constraints via ALTER
        # (Databricks CREATE TABLE supports only PK/FK inline).
        for statement in manifest["statements"][1:]:
            assert qualified in statement
            assert statement.upper().startswith("ALTER TABLE")
            assert "CONSTRAINT" in statement.upper()
    runner = Mock()
    runner.validate_owner_spec.return_value = True
    platform.provision(manifests, runner)
    assert runner.apply_owner_spec.call_count == len(OWNER_SPECS)
    runner.apply_grants.assert_called_once_with()


def test_owner_migration_runner_validates_renders_and_grants():
    runner_cls = getattr(platform, "OwnerMigrationRunner", None)
    assert callable(runner_cls), "Owner migrations execute reviewed bytes, never author DDL"
    executed = []
    grants = ("GRANT SELECT ON TABLE ${catalog}.${control_schema}.genie_space_versions TO `exec`",)
    runner = runner_cls(executed.append, "sandbox_cat", "vc_ctl", grants)
    manifests = platform.build_owner_manifests()

    assert all(runner.validate_owner_spec(manifest) is True for manifest in manifests)
    for manifest in manifests:
        runner.apply_owner_spec(manifest)
    runner.apply_grants()

    assert executed, "Owner migrations and grants must reach the SQL executor"
    assert all("${" not in statement for statement in executed), "Templates must be fully rendered"
    assert any("`sandbox_cat`.`vc_ctl`.genie_space_versions" in statement for statement in executed)
    assert executed[-1].startswith("GRANT SELECT ON TABLE `sandbox_cat`.`vc_ctl`.genie_space_versions")


def test_owner_migration_runner_rejects_tampered_specs_and_bad_identifiers():
    runner_cls = platform.OwnerMigrationRunner
    runner = runner_cls(lambda _sql: None, "cat", "ctl", ())
    good = platform.build_owner_manifests()[0]
    assert runner.validate_owner_spec(good) is True
    assert runner.validate_owner_spec(dict(good, idempotent=False)) is False
    assert runner.validate_owner_spec(dict(good, revision="sha256:" + "0" * 64)) is False
    assert runner.validate_owner_spec(dict(good, name="genie_space_registry")) is False
    injected = dict(good, statements=[good["statements"][0] + " ${evil}"])
    assert runner.validate_owner_spec(injected) is False
    for bad in ("bad-cat", "1cat", "cat;drop", "", "cat.schema"):
        with pytest.raises(ValueError):
            runner_cls(lambda _sql: None, bad, "ctl", ())
        with pytest.raises(ValueError):
            runner_cls(lambda _sql: None, "cat", bad, ())


_MATRIX_PRINCIPALS = {
    "runtime": "11111111-1111-4111-8111-111111111111",
    "executor": "22222222-2222-4222-8222-222222222222",
    "enrollment": "33333333-3333-4333-8333-333333333333",
    "observer": "44444444-4444-4444-8444-444444444444",
    "approval": "55555555-5555-4555-8555-555555555555",
    "source": "66666666-6666-4666-8666-666666666666",
}


def test_grant_matrix_enforces_least_privilege_uc_select_modify():
    build = getattr(platform, "build_grant_matrix", None)
    assert callable(build), "M08 owns the least-privilege grant matrix"
    assert set(platform.PROVISION_ROLES) == set(_MATRIX_PRINCIPALS)
    grants = build(_MATRIX_PRINCIPALS)
    joined = "\n".join(grants)
    rt = _MATRIX_PRINCIPALS["runtime"]
    ex = _MATRIX_PRINCIPALS["executor"]
    en = _MATRIX_PRINCIPALS["enrollment"]

    # Unity Catalog exposes only SELECT/MODIFY on tables — never INSERT/UPDATE
    # (invalid privileges) nor broad ALL PRIVILEGES / MANAGE / ownership.
    assert "INSERT" not in joined
    assert "UPDATE" not in joined
    assert "DELETE" not in joined
    assert "ALL PRIVILEGES" not in joined
    assert "MANAGE" not in joined
    for grant in grants:
        assert grant.startswith("GRANT ")
        assert "${catalog}" in grant
        assert " TO `" in grant

    # Fact tables: runtime SELECT+MODIFY only; delta.appendOnly + non-owner make
    # this append-only (Delta blocks UPDATE/DELETE, non-ownership blocks ALTER).
    for table in ("genie_space_versions", "genie_space_registry", "genie_space_operations"):
        assert (f"GRANT SELECT, MODIFY ON TABLE ${{catalog}}.${{control_schema}}.{table} "
                f"TO `{rt}`") in grants

    # Coordination (mutable): executor and enrollment are distinct SPs, both with
    # SELECT+MODIFY; the write separation is protocol-enforced, not grant-enforced.
    assert (f"GRANT SELECT, MODIFY ON TABLE ${{catalog}}.${{control_schema}}.genie_ops_coordination "
            f"TO `{ex}`") in grants
    assert (f"GRANT SELECT, MODIFY ON TABLE ${{catalog}}.${{control_schema}}.genie_ops_coordination "
            f"TO `{en}`") in grants
    assert ex != en

    # Volumes: exactly one writer each; designated readers are read-only.
    writer_of = {"vc_snapshots": "observer", "vc_approval_evidence": "approval",
                 "vc_outbound_packages": "source", "vc_target_receipts": "executor"}
    for volume, role in writer_of.items():
        appid = _MATRIX_PRINCIPALS[role]
        assert (f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME "
                f"${{catalog}}.${{control_schema}}.{volume} TO `{appid}`") in grants
    assert (f"GRANT READ VOLUME ON VOLUME ${{catalog}}.${{control_schema}}.vc_outbound_packages "
            f"TO `{ex}`") in grants
    assert (f"GRANT READ VOLUME ON VOLUME ${{catalog}}.${{control_schema}}.vc_target_receipts "
            f"TO `{_MATRIX_PRINCIPALS['source']}`") in grants

    # Source never touches the operations table (single grant boundary).
    assert not any("genie_space_operations" in g and f"`{_MATRIX_PRINCIPALS['source']}`" in g
                   for g in grants)

    # Authority separation: the provision job runs as the provisioner, which owns
    # only the objects it creates, so it can grant object privileges but NOT
    # namespace access. USE CATALOG / USE SCHEMA are admin/catalog-owner grants
    # issued during namespace setup, never by the provision job's matrix.
    assert "USE CATALOG" not in joined
    assert "USE SCHEMA" not in joined
    for grant in grants:
        assert " ON TABLE " in grant or " ON VOLUME " in grant

    # The runner accepts and fully renders every grant.
    executed = []
    runner = platform.OwnerMigrationRunner(executed.append, "cat", "ctl", grants)
    runner.apply_grants()
    assert len(executed) == len(grants)
    assert all("${" not in statement for statement in executed)


def test_grant_matrix_rejects_incomplete_or_unsafe_principals():
    build = platform.build_grant_matrix
    good = {role: f"{role}-appid" for role in platform.PROVISION_ROLES}
    assert build(good)
    with pytest.raises(ValueError):
        build({role: appid for role, appid in good.items() if role != "source"})
    with pytest.raises(ValueError):
        build(dict(good, observer=""))
    for bad in ("a`b", "a;b", "a b", "a`", "a)", "a'b"):
        with pytest.raises(ValueError):
            build(dict(good, runtime=bad))


def test_fact_permissions_require_append_only_nonowner_select_modify():
    verify = getattr(platform, "verify_fact_permissions", None)
    assert callable(verify), "Effective fact-table grants must fail closed"
    # UC exposes only SELECT/MODIFY; append-only is enforced by delta.appendOnly
    # plus a non-owner principal, not by a (nonexistent) INSERT privilege.
    facts = {name: {"append_only": True, "owner": "provisioner",
                    "principal": "runtime", "privileges": {"SELECT", "MODIFY"}}
             for name in ("genie_space_versions", "genie_space_registry", "genie_space_operations")}
    assert verify(facts) is True
    for field, value in (("append_only", False), ("owner", "runtime"),
                         ("privileges", {"SELECT"}),
                         ("privileges", {"SELECT", "MODIFY", "MANAGE"}),
                         ("privileges", {"ALL PRIVILEGES"})):
        bad = {name: dict(row, **{field: value}) for name, row in facts.items()}
        assert verify(bad) is False
    assert verify({}) is False


def test_coordination_grants_require_separate_serialized_enrollment():
    verify = getattr(platform, "verify_coordination_permissions", None)
    assert callable(verify), "Enrollment and executor must be distinct serialized SPs"
    # UC cannot split UPDATE vs INSERT, so both hold SELECT+MODIFY; separation is
    # via distinct SPs + single concurrent run + Serializable + CAS (protocol).
    proof = dict(owner="provisioner", executor="executor", enrollment="enrollment",
                 executor_privileges={"SELECT", "MODIFY"},
                 enrollment_privileges={"SELECT", "MODIFY"},
                 max_concurrent_runs=1, queue_enabled=True, isolation="Serializable")
    assert verify(proof) is True
    for key, value in (("executor_privileges", {"SELECT"}),
                       ("executor_privileges", {"SELECT", "MODIFY", "MANAGE"}),
                       ("enrollment_privileges", {"SELECT"}),
                       ("enrollment", "executor"), ("owner", "executor"),
                       ("max_concurrent_runs", 2), ("queue_enabled", False),
                       ("isolation", "WriteSerializable")):
        assert verify(dict(proof, **{key: value})) is False
    assert verify({}) is False


def test_missing_fine_grained_dml_or_serializable_disables_writes():
    check = getattr(platform, "storage_write_ready", None)
    assert callable(check), "Unknown or unsupported capabilities must deny every write"
    valid = dict(fine_grained_dml=True, serializable=True, nonowner_runtime=True,
                 append_only=True, enrollment_isolated=True)
    probe = Mock(return_value=valid)
    assert check(probe) is True
    for capability in valid:
        for value in (False, None, "true", 1):
            probe.return_value = dict(valid, **{capability: value})
            assert check(probe) is False
        probe.return_value = {key: value for key, value in valid.items() if key != capability}
        assert check(probe) is False
    probe.side_effect = TimeoutError("warehouse unavailable")
    assert check(probe) is False
    assert check(None) is False


def test_bundle_inventory_guard_rejects_governed_content_and_exports_detection_evidence(tmp_path):
    scan = getattr(platform, "bundle_detection_evidence", None)
    assert callable(scan), "M05 needs read-only dual-authority detection evidence"
    root = tmp_path / "databricks.yml"
    child = tmp_path / "child.yml"
    root.write_text("include: [child.yml]\n")
    child.write_text("resources: {genie_spaces: {managed: {}}}\n")
    evidence = scan(root)
    assert evidence.status == "dual_authority"
    assert evidence.schema_version == "VC/1.0"
    assert str(child) in evidence.file_digests
    assert evidence.findings == ("resources.genie_spaces",)
    assert not hasattr(evidence, "actor_id")
    original = child.read_bytes()
    import subprocess
    import sys
    command = [sys.executable, str(ROOT / "scripts/version_control/bundle_guard.py"), str(root)]
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 1
    assert "dual_authority" in result.stdout
    assert child.read_bytes() == original
    child.write_text("resources: {jobs: {worker: {name: worker}}}\n")
    assert scan(root).status == "clear"
    assert subprocess.run(command, capture_output=True).returncode == 0
    child.unlink()
    assert scan(root).status == "unknown"
    deploy = (ROOT / "scripts/deploy.sh").read_text()
    assert deploy.index("_preflight_check_vc_bundle_content") < deploy.index("DEPLOYER=$(databricks current-user")
    installer = (ROOT / "scripts/deploy_lib/install.py").read_text()
    assert installer.index("preflight_deployment(Path(cfg.repo_root") < installer.index("ensure_app(w, cfg)")


def test_artifact_grants_require_distinct_volume_securables_and_writers():
    verify = getattr(platform, "verify_artifact_permissions", None)
    assert callable(verify), "Volume prefixes are not separate grant boundaries"
    writers = dict(vc_snapshots="observer", vc_approval_evidence="approval-service",
                   vc_outbound_packages="source", vc_target_receipts="target")
    volumes = {name: {"securable": f"catalog.control.{name}", "writers": {writer},
                      "owner": "provisioner", "privileges_verified": True}
               for name, writer in writers.items()}
    assert verify(volumes, writers, {"target"}, {"source"}) is True
    for name in ("vc_approval_evidence", "vc_target_receipts"):
        bad = {key: dict(row) for key, row in volumes.items()}
        bad[name]["writers"] = {"source", writers[name]}
        assert verify(bad, writers, {"target"}, {"source"}) is False
    shared = {key: dict(row, securable="catalog.control.shared") for key, row in volumes.items()}
    assert verify(shared, writers, {"target"}, {"source"}) is False
    assert verify(volumes, writers, {"target", "source"}, {"source"}) is False
    assert verify(volumes, writers, {"target"}, {"source", "provisioner"}) is False


def test_repeatable_provision_requires_explicit_nonproduction_selection():
    run = getattr(platform, "repeatable_sandbox_provision", None)
    assert callable(run), "Real provisioning must require explicit sandbox selection"
    selection = dict(profile="sandbox-owner", target="vc-sandbox", package_target="dev",
                     disposable_nonproduction=True,
                     operation_id="00000000-0000-4000-8000-000000000016", variables={"catalog": "disposable"})
    runner = Mock()
    runner.verify_selection.return_value = True
    runner.content_fingerprint.return_value = "unchanged"
    runner.provisioning_fingerprint.return_value = "stable"
    run(ROOT, selection, runner)
    commands = [call.args[0] for call in runner.command.call_args_list]
    assert sum("validate" in command for command in commands) == 4
    assert sum("deploy" in command for command in commands) == 2
    assert sum("run" in command for command in commands) == 2
    assert all("--profile" in command and "sandbox-owner" in command for command in commands)
    assert all("--strict" in command for command in commands if "validate" in command)
    assert not any("--no-wait" in command for command in commands)
    for key, value in (("profile", ""), ("profile", "DEFAULT"), ("target", "prod"),
                       ("disposable_nonproduction", False)):
        runner.reset_mock()
        with pytest.raises(ValueError):
            run(ROOT, dict(selection, **{key: value}), runner)
        runner.command.assert_not_called()
    runner.content_fingerprint.side_effect = ["before", "changed"]
    with pytest.raises(PermissionError, match="content"):
        run(ROOT, selection, runner)


def test_unverified_cross_metastore_artifact_transport_disables_topology():
    verify = getattr(platform, "topology_read_ready", None)
    assert callable(verify), "Unsupported Delta Sharing topology must remain disabled"
    proof = dict(source_workspace="source", target_workspace="target", source_metastore="shared",
                 target_metastore="shared", packages_readable=True, receipts_readable=True,
                 source_target_write_denied=True, remote_write_credentials=False,
                 shared_uc_grants_verified=True)
    assert verify(proof) is True
    proof["target_metastore"] = "other"
    assert verify(proof) is False
    proof.update(delta_sharing_verified=True, volume_sharing_verified=True, artifact_representation="volumes")
    assert verify(proof) is True
    proof["volume_sharing_verified"] = False
    assert verify(proof) is False
    proof.update(artifact_representation="reviewed_readonly", representation_reviewed=True)
    assert verify(proof) is True
    proof["remote_write_credentials"] = True
    assert verify(proof) is False
