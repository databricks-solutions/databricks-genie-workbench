"""Execute reviewed owner migrations, never author DDL or Genie content."""

import re
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from .bundles import preflight_deployment

# Owner migration bytes live in backend/version_control_ddl/ and are authored by
# the DDL owners (M02/M03/M06/M07). M08 invokes them; it never edits them.
_DDL_ROOT = Path(__file__).resolve().parents[3] / "version_control_ddl"
_STATEMENT = re.compile(
    r"CREATE\s+(TABLE|VOLUME)\s+IF\s+NOT\s+EXISTS\s+"
    r"\$\{catalog\}\.\$\{control_schema\}\.(\w+)",
    re.IGNORECASE,
)
# CHECK constraints are attached to tables via ALTER because Databricks
# CREATE TABLE only supports PRIMARY KEY / FOREIGN KEY inline. Owners emit an
# idempotent DROP-then-ADD pair per constraint; nothing else is permitted after
# a CREATE (no ALTER SET TBLPROPERTIES, no DROP COLUMN, etc.).
_ALTER = re.compile(
    r"ALTER\s+TABLE\s+\$\{catalog\}\.\$\{control_schema\}\.(\w+)\s+"
    r"(?:ADD|DROP)\s+CONSTRAINT\b",
    re.IGNORECASE,
)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


OWNER_SPECS = (
    ("M02", "table", "genie_space_versions"),
    ("M02", "table", "genie_space_registry"),
    ("M03", "table", "genie_ops_coordination"),
    ("M06", "table", "genie_space_operations"),
    ("M09", "table", "genie_space_version_tags"),
    ("M02", "volume", "vc_snapshots"),
    ("M06", "volume", "vc_approval_evidence"),
    ("M07", "volume", "vc_outbound_packages"),
    ("M07", "volume", "vc_target_receipts"),
)


def _split_statements(text):
    # Drop full-line `--` comments so they don't bleed into the following
    # statement chunk when splitting on `;` (owners annotate the ALTER blocks).
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [part.strip() for part in body.split(";") if part.strip()]


def _revision(statements):
    return "sha256:" + sha256("\n".join(statements).encode()).hexdigest()


def _classify(statement):
    """Return ``(verb, kind, name)`` for a recognized owner statement, else None.

    ``verb`` is ``"create"`` (CREATE TABLE/VOLUME) or ``"alter"`` (ALTER TABLE
    ADD/DROP CONSTRAINT). Anything else is unrecognized.
    """

    match = _STATEMENT.match(statement)
    if match is not None:
        kind = "table" if match.group(1).upper() == "TABLE" else "volume"
        return ("create", kind, match.group(2))
    match = _ALTER.match(statement)
    if match is not None:
        return ("alter", "table", match.group(1))
    return None


def build_owner_manifests(ddl_root=None):
    """Read the reviewed owner DDL files into provisioning manifests.

    Each `OWNER_SPECS` object is matched to its `CREATE ... IF NOT EXISTS`
    statement by name; any following `ALTER TABLE ... CONSTRAINT` statements in
    the same file attach to that object (in file order). The revision is the
    SHA-256 of the exact reviewed bytes. This authors no DDL — it only
    transcribes and hashes what the owners wrote.
    """

    root = Path(ddl_root) if ddl_root is not None else _DDL_ROOT
    objects = {}
    for path in sorted(root.glob("*.sql")):
        for statement in _split_statements(path.read_text()):
            classified = _classify(statement)
            if classified is None:
                raise ValueError(f"Unrecognized owner migration statement in {path.name}")
            verb, kind, name = classified
            if verb == "create":
                if name in objects:
                    raise ValueError(f"Duplicate owner migration for {name}")
                objects[name] = {"kind": kind, "statements": [statement]}
            else:  # alter — must follow its object's CREATE
                if name not in objects:
                    raise ValueError(f"ALTER before CREATE for {name} in {path.name}")
                objects[name]["statements"].append(statement)
    manifests = []
    for owner, kind, name in OWNER_SPECS:
        if name not in objects:
            raise ValueError(f"Missing owner migration for {name}")
        found = objects[name]
        if found["kind"] != kind:
            raise ValueError(f"Owner migration kind mismatch for {name}")
        statements = list(found["statements"])
        manifests.append({"owner": owner, "kind": kind, "name": name,
                          "idempotent": True, "revision": _revision(statements),
                          "statements": statements})
    return manifests


class OwnerMigrationRunner:
    """Execute reviewed owner migration bytes and grants against warehouse SQL.

    Runs as the provisioner identity. `execute` is a blocking callable that runs
    one rendered SQL statement. `${catalog}`/`${control_schema}` are the only
    permitted templates and are replaced with validated, backtick-quoted
    identifiers — never caller input.
    """

    def __init__(self, execute, catalog, control_schema, grants):
        if not (_IDENTIFIER.fullmatch(catalog) and _IDENTIFIER.fullmatch(control_schema)):
            raise ValueError("Catalog and control schema must be simple identifiers")
        self._execute = execute
        self._catalog = catalog
        self._control_schema = control_schema
        self._grants = tuple(grants)

    def _render(self, text):
        rendered = (text.replace("${catalog}", f"`{self._catalog}`")
                        .replace("${control_schema}", f"`{self._control_schema}`"))
        if "${" in rendered:
            raise ValueError("Unresolved template placeholder in owner migration")
        return rendered

    def validate_owner_spec(self, spec):
        statements = spec.get("statements")
        if not statements or spec.get("idempotent") is not True:
            return False
        if spec.get("revision") != _revision(statements):
            return False
        for index, statement in enumerate(statements):
            residual = statement.replace("${catalog}", "").replace("${control_schema}", "")
            if "${" in residual:
                return False
            classified = _classify(statement)
            if classified is None:
                return False
            verb, kind, name = classified
            if name != spec.get("name"):
                return False
            if index == 0:
                # The object is created first; kind must match the reviewed spec.
                if verb != "create" or kind != spec.get("kind"):
                    return False
            elif verb != "alter":
                # Only ALTER TABLE ... CONSTRAINT may follow the CREATE.
                return False
        return True

    def apply_owner_spec(self, spec):
        for statement in spec["statements"]:
            self._execute(self._render(statement))

    def apply_grants(self):
        for grant in self._grants:
            self._execute(self._render(grant))


# --- Least-privilege grant matrix (M08-owned) ---------------------------------
# Grants attach to the objects the owner migrations create. Principals are
# already-resolved application ids, quoted with backticks; the only templates are
# ${catalog}/${control_schema}, rendered by OwnerMigrationRunner. Denials are by
# omission. Contract (contracts.md §"Grant boundaries and append semantics"):
#   - Unity Catalog tables expose only SELECT and MODIFY (there is no INSERT or
#     UPDATE privilege). Least privilege is therefore SELECT+MODIFY scoped to
#     exactly the writer role on exactly its object, plus a table-level
#     enforcement mechanism — never ALL PRIVILEGES / MANAGE / ownership.
#   - Fact tables carry delta.appendOnly='true', so a non-owner runtime holding
#     SELECT+MODIFY can only append: Delta rejects UPDATE/DELETE and non-ownership
#     rejects ALTER/DROP/REPLACE. That is the append-only guarantee.
#   - Coordination is mutable (Serializable). Executor and enrollment are distinct
#     SPs that both hold SELECT+MODIFY; their INSERT-only/UPDATE-only separation is
#     enforced by the coordination protocol (separate jobs, single concurrent run,
#     Serializable isolation, compare-and-swap), not by DML-verb grants which UC
#     cannot express. See contracts.md for this platform-forced deviation.
#   - Each Volume has exactly one writer; designated consumers get READ only.
PROVISION_ROLES = ("runtime", "executor", "enrollment", "observer", "approval", "source")
_FACT_TABLES = ("genie_space_versions", "genie_space_registry", "genie_space_operations")
_COORDINATION = "genie_ops_coordination"
# Mutable, non-authoritative UX metadata (version tags/comments). Written in place by the
# observe runtime's executor identity (adapters.sql("executor")); SELECT+MODIFY covers the
# MERGE upsert and DELETE. Not a fact table, so it is intentionally not appendOnly.
_VERSION_TAGS = "genie_space_version_tags"
_VOLUME_WRITER = {
    "vc_snapshots": "observer",
    "vc_approval_evidence": "approval",
    "vc_outbound_packages": "source",
    "vc_target_receipts": "executor",
}
_VOLUME_READER = {
    "vc_outbound_packages": ("executor",),
    "vc_target_receipts": ("source",),
}
_PRINCIPAL = re.compile(r"[A-Za-z0-9._@-]+")


def _quote_principal(appid):
    if not appid or not _PRINCIPAL.fullmatch(appid):
        raise ValueError(f"Unsafe or empty principal identifier: {appid!r}")
    return f"`{appid}`"


def build_grant_matrix(principals):
    """Author the reviewed least-privilege grants for `principals` (role->appid).

    Returns render-ready statements (``${catalog}``/``${control_schema}``
    templates, backtick-quoted principals) in a deterministic order. Raises if a
    role is missing or a principal is unsafe/empty.
    """

    missing = [role for role in PROVISION_ROLES if not principals.get(role)]
    if missing:
        raise ValueError(f"Grant matrix requires every role; missing: {missing}")
    quoted = {role: _quote_principal(principals[role]) for role in PROVISION_ROLES}
    table = "${catalog}.${control_schema}"
    grants = []

    # Object-level grants only. The provision job runs as the provisioner, which
    # owns the tables/Volumes it creates and can therefore grant privileges on
    # them, but it does NOT own the catalog/schema. USE CATALOG / USE SCHEMA are
    # admin/catalog-owner authority and are granted during namespace setup
    # (scripts/version_control/provision_sandbox.py), never here.

    # Fact tables: runtime appends only. UC has no INSERT privilege, so grant
    # SELECT+MODIFY; delta.appendOnly='true' + non-owner make it append-only.
    for name in _FACT_TABLES:
        grants.append(f"GRANT SELECT, MODIFY ON TABLE {table}.{name} TO {quoted['runtime']}")

    # Coordination (mutable): executor mutates rows, enrollment inserts them. UC
    # cannot split UPDATE vs INSERT, so both hold SELECT+MODIFY as distinct SPs;
    # the coordination protocol (separate jobs, single run, Serializable, CAS)
    # enforces the write separation.
    grants.append(f"GRANT SELECT, MODIFY ON TABLE {table}.{_COORDINATION} TO {quoted['executor']}")
    grants.append(f"GRANT SELECT, MODIFY ON TABLE {table}.{_COORDINATION} TO {quoted['enrollment']}")

    # Version tags (mutable UX metadata): the observe runtime upserts/deletes rows via the
    # executor identity. UC has no INSERT/UPDATE/DELETE privilege, so SELECT+MODIFY is least
    # privilege for a MERGE/DELETE on a non-appendOnly table.
    grants.append(f"GRANT SELECT, MODIFY ON TABLE {table}.{_VERSION_TAGS} TO {quoted['executor']}")

    # Volumes: one writer each (read+write), designated consumers read-only.
    for volume, role in _VOLUME_WRITER.items():
        grants.append(f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {table}.{volume} "
                      f"TO {quoted[role]}")
    for volume, readers in _VOLUME_READER.items():
        for role in readers:
            grants.append(f"GRANT READ VOLUME ON VOLUME {table}.{volume} TO {quoted[role]}")

    return tuple(grants)


def provision(manifests, runner) -> None:
    specs = {}
    for manifest in manifests:
        key = tuple(manifest.get(field) for field in ("owner", "kind", "name"))
        if (key not in OWNER_SPECS or key in specs
                or manifest.get("idempotent") is not True
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", manifest.get("revision", ""))):
            raise ValueError("Invalid, duplicate, or unreviewed owner spec")
        specs[key] = dict(manifest)
    if set(specs) != set(OWNER_SPECS):
        raise ValueError("All four owner tables and separate Volumes are required")
    for key in OWNER_SPECS:
        if runner.validate_owner_spec(specs[key]) is not True:
            raise ValueError("Unverified owner migration bytes or grants")
    for key in OWNER_SPECS:
        runner.apply_owner_spec(specs[key])
    runner.apply_grants()


def repeatable_sandbox_provision(root, selection, runner):
    if (not selection.get("profile") or selection["profile"].upper() == "DEFAULT"
            or selection.get("target") != "vc-sandbox"
            or not selection.get("package_target")
            or selection.get("disposable_nonproduction") is not True):
        raise ValueError("Explicit nonproduction profile and sandbox target required")
    operation_id = selection["operation_id"]
    if str(UUID(operation_id)) != operation_id:
        raise ValueError("Canonical provisioning operation_id required")
    root = Path(root).resolve()
    preflight_deployment(root)
    if runner.verify_selection(selection) is not True:
        raise PermissionError("Live provisioning identity/host/workspace not verified")
    # `bundle validate --strict` rejects the gitignored `.build` sync globs when
    # the wheel output is absent, so build wheels first (mirrors a real deploy
    # pipeline, which builds artifacts before validate/deploy).
    runner.build_artifacts(root)
    profile_args = ["--profile", selection["profile"]]
    variables = [f"--var={key}={value}" for key, value in sorted(selection.get("variables", {}).items())]
    root_args = ["--target", selection["target"], *profile_args, *variables]
    before = runner.content_fingerprint()
    previous = None
    for cycle in range(2):
        runner.command(["databricks", "bundle", "validate", "--strict", *root_args], cwd=root)
        runner.command(["databricks", "bundle", "validate", "--strict", "--target", selection["package_target"],
                        *profile_args, *selection.get("package_variables", [])],
                       cwd=root / "packages/genie-space-optimizer")
        runner.command(["databricks", "bundle", "deploy", *root_args], cwd=root)
        runner.command(["databricks", "bundle", "run", "vc-provision", *root_args,
                        "--params", f"operation_id={operation_id}"], cwd=root)
        if runner.content_fingerprint() != before:
            raise PermissionError("Provisioning changed governed Genie content")
        current = runner.provisioning_fingerprint()
        if cycle and current != previous:
            raise PermissionError("Provisioning was not repeatable")
        previous = current
