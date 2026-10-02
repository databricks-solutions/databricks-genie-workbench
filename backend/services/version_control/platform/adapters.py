"""Shared platform adapters for the observe-only VC runtime.

Neutral, governed-free home for the low-level infrastructure the observe runtime
is built on: the deployment clock, control-table name qualifier, executor
selection, the single-writer status reader, the content-addressed Volume file
store, and ``PlatformAdapters`` (credential-bound SQL/Volume/SDK access bound to
explicit per-role OAuth credentials).

``PlatformAdapters`` is never exercised offline; the seams that consume it are
validated against fakes, and the class is validated on the live platform. Only
the observe-surface methods live here — the promotion/gate mutation graph that
formerly shared this module has been removed with the governed control plane.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from backend.services.version_control import contracts as vc


class _Clock:
    """Deployment-owned wall clock; never derived from request evidence.

    The durable subsystem injects one clock into consumers with two contracts:
    ``CoordinationService`` calls ``clock.now()`` while the durable stores
    (``DeltaCoordinationStore``/``DeltaRegistry``) call ``clock()`` (offline they
    pass ``lambda: NOW``). This adapter satisfies both so a single instance can
    back the whole graph.
    """

    @staticmethod
    def now() -> datetime:
        return datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now()


def _qualified(config: dict, table: str) -> str:
    return f"`{config['catalog']}`.`{config['control_schema']}`.`{table}`"


def _selection(spec: dict) -> vc.ExplicitExecutorSelection:
    return vc.ExplicitExecutorSelection(
        spec["workspace_id"], spec["host"], spec["principal_id"],
        spec["execution_ref"], spec.get("profile"))


def _clean_status_reader(coordination_store, registry) -> Callable[[Any, Any], Any]:
    """Target-local single-writer status reader for the observe surface.

    Reports drift ``CLEAN`` and not-stale, deriving only quarantine / unresolved
    operation / heads from the durable coordination row (fail-closed to the row's
    own state). It never fabricates optimistic drift: the observer independently
    compares the live GET against the ledger head, so a real divergence still
    surfaces through the read-back checks.
    """

    def read(binding, _actor):
        now = datetime.now(UTC)
        try:
            row = coordination_store.read(binding.binding_id)
        except Exception:  # noqa: BLE001 - coordination outage -> fail-closed stale
            return vc.BindingStatus(
                binding_id=binding.binding_id, binding_revision=binding.binding_revision,
                heads=vc.Heads(None, None, None), drift=vc.DriftState.UNKNOWN,
                quarantined=True, unresolved_operation_id=None, observed_at=None,
                projection_as_of=now, stale=True, allowed_actions=(),
                reasons=("Coordination authority unavailable",))
        if row is None:
            return vc.BindingStatus(
                binding_id=binding.binding_id, binding_revision=binding.binding_revision,
                heads=vc.Heads(None, None, None), drift=vc.DriftState.CLEAN,
                quarantined=False, unresolved_operation_id=None, observed_at=None,
                projection_as_of=now, stale=False, allowed_actions=(), reasons=())
        quarantined = bool(getattr(row, "quarantine_reason", None))
        unresolved = getattr(row, "active_operation_id", None) if getattr(row, "unresolved", False) else None
        return vc.BindingStatus(
            binding_id=binding.binding_id, binding_revision=binding.binding_revision,
            heads=getattr(row, "heads", None) or vc.Heads(None, None, None),
            drift=vc.DriftState.CLEAN, quarantined=quarantined,
            unresolved_operation_id=unresolved, observed_at=getattr(row, "updated_at", None),
            projection_as_of=now, stale=False, allowed_actions=(), reasons=())

    return read


class FilesVolumeStore:
    def __init__(self, *, read: Callable[[str], bytes], put: Callable[[str, bytes], Any],
                 exists: Callable[[str], bool], publish: Callable[[Any], Any] | None = None):
        self._read = read
        self._put = put
        self._exists = exists
        self._publish = publish

    def read(self, path: str) -> bytes:
        return self._read(path)

    def put_if_absent(self, path: str, content: bytes) -> None:
        # Content-addressed paths are write-once. If the artifact is already
        # present we leave it untouched; the caller's read-back comparison
        # (immutable_put) still verifies the bytes match, so a divergent object
        # at the same path fails closed instead of being silently overwritten.
        if self._exists(path):
            return
        self._put(path, content)

    def publish(self, reference: Any) -> Any:
        if self._publish is not None:
            self._publish(reference)
        return reference


def files_volume_store(client, *, publish: Callable[[Any], Any] | None = None) -> FilesVolumeStore:
    """Bind a `FilesVolumeStore` to a credentialed `WorkspaceClient`.

    `read` streams the Volume file, `put` uploads with `overwrite=False` (the
    Files API rejects clobbering an existing object), and `exists` probes
    metadata, treating a not-found as absent and re-raising every other error so
    a transient outage never masquerades as "safe to overwrite".
    """

    from io import BytesIO

    from databricks.sdk.errors import NotFound

    def read(path: str) -> bytes:
        return client.files.download(path).contents.read()

    def put(path: str, content: bytes) -> None:
        client.files.upload(path, BytesIO(content), overwrite=False)

    def exists(path: str) -> bool:
        try:
            client.files.get_metadata(path)
            return True
        except NotFound:
            return False

    return FilesVolumeStore(read=read, put=put, exists=exists, publish=publish)


class PlatformAdapters:
    """Live, credential-bound low-level access for the observe runtime.

    Every method binds SQL/Volume/SDK access to one explicit role's OAuth
    credentials from `config["roles"][role]` (profile + warehouse + host). This is
    never exercised offline; the seams that consume it are validated against fakes,
    and this class is validated on the live platform.
    """

    def __init__(self, config: dict):
        self.config = config
        self._clients: dict[str, Any] = {}

    # -- SDK clients (explicit role credentials) --------------------------
    def client(self, role: str):
        if role not in self._clients:
            import os

            from databricks.sdk import WorkspaceClient

            selection = self.config["roles"][role]
            profile = selection.get("profile")
            auth = selection.get("auth")
            # `auth` (in-Job governed credential storage) is authoritative when present:
            # in-Job configs also carry `profile` purely as the identity-provider routing
            # key (`_profiles`), so it must NOT be interpreted as a local profile here.
            if auth and auth.get("mode") == "m2m":
                # OAuth M2M from Job-injected secret env vars, pinned to the role's
                # explicit host. Ambient DATABRICKS_* is isolated so the run_as identity
                # cannot conflict with these explicit credentials.
                kwargs: dict[str, Any] = {"host": auth.get("host") or selection["host"],
                                          "client_id": os.environ[auth["client_id_env"]],
                                          "client_secret": os.environ[auth["client_secret_env"]]}
                self._clients[role] = self._isolated_client(WorkspaceClient, kwargs)
            elif profile:
                # Local path (offline/operator): a named profile is authoritative, so
                # isolate ambient DATABRICKS_* during construction to avoid unified-auth
                # conflicts, then restore it.
                kwargs = {"profile": profile}
                if self.config.get("config_file"):
                    kwargs["config_file"] = self.config["config_file"]
                self._clients[role] = self._isolated_client(WorkspaceClient, kwargs)
            else:
                # Ambient run_as identity. DATABRICKS_* is exactly how the platform
                # injects that credential, so it must NOT be popped.
                self._clients[role] = WorkspaceClient()
        return self._clients[role]

    @staticmethod
    def _isolated_client(factory, kwargs):
        """Build a WorkspaceClient with explicit credentials (profile or M2M) while
        temporarily removing every DATABRICKS_* env var, so ambient env cannot shadow
        or conflict with the explicit auth. Restores the env unconditionally."""
        import os

        saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("DATABRICKS_")}
        try:
            return factory(**kwargs)
        finally:
            os.environ.update(saved)

    # -- SQL (REST Statements API; thrift is IP-ACL blocked) --------------
    def sql(self, role: str) -> Callable[..., list]:
        import time

        warehouse = self.config["roles"][role]["warehouse_id"]
        client = self.client(role)

        def execute(statement: str, parameters: Any = None) -> list:
            body: dict[str, Any] = {"warehouse_id": warehouse, "statement": statement,
                                    "wait_timeout": "30s", "on_wait_timeout": "CONTINUE"}
            rendered = _statement_parameters(parameters)
            if rendered:
                body["parameters"] = rendered
            response = client.api_client.do("POST", "/api/2.0/sql/statements", body=body)
            deadline = time.monotonic() + 180
            while response["status"]["state"] in {"PENDING", "RUNNING"}:
                if time.monotonic() >= deadline:
                    raise RuntimeError("SQL statement timed out")
                time.sleep(2)
                response = client.api_client.do(
                    "GET", f"/api/2.0/sql/statements/{response['statement_id']}")
            if response["status"]["state"] != "SUCCEEDED":
                error = response["status"].get("error", {})
                raise RuntimeError(f"{error.get('error_code', 'SQL_FAILED')}: {error.get('message', '')}")
            return _rows(response)

        return execute

    # -- Volume files (content-addressed store) ---------------------------
    def files_store(self, role: str):
        return files_volume_store(self.client(role))

    def read_evidence(self, uri: str) -> bytes:
        return self.client("approval").files.download(uri).contents.read()

    # -- explicit-executor authentication for GenieTransport --------------
    def authenticate(self, _executor) -> dict:
        return self.client("executor").config.authenticate()

    # -- identity provider (SCIM-backed, per role) ------------------------
    def identity_provider(self, role: str):
        from backend.services.version_control.platform.identity import (
            PlatformIdentityProvider,
        )

        selection = self.config["roles"][role]
        profile = selection.get("profile")
        directory = self.client(self.config.get("directory_role", "executor"))

        def resolve_groups(subject_id, _workspace_id):
            try:
                user = directory.users.get(subject_id)
                return frozenset(group.display for group in (user.groups or []))
            except Exception:  # noqa: BLE001 - SCIM user miss -> SP lookup
                for sp in directory.service_principals.list(
                        filter=f'applicationId eq "{subject_id}"'):
                    return frozenset(group.display for group in (sp.groups or []))
            raise PermissionError(f"Unknown subject for group resolution: {subject_id}")

        def resolve_actor(reference):
            try:
                directory.users.get(reference)
                kind = "human"
            except Exception:  # noqa: BLE001 - SCIM user miss -> SP
                if not list(directory.service_principals.list(
                        filter=f'applicationId eq "{reference}"')):
                    raise PermissionError(f"Unknown authenticated subject: {reference}")
                kind = "service"
            return vc.ActorContext(reference, selection["workspace_id"], kind)

        _EDIT_LEVELS = frozenset({"CAN_EDIT", "CAN_MANAGE", "IS_OWNER"})

        def resolve_edit(subject_id, binding):
            # A release requester must genuinely hold edit rights on the target
            # Genie space. Reading the object ACL requires CAN_MANAGE, which the
            # executor already holds, so this works both in the driver (admin
            # directory) and in the Job (executor directory).
            if not getattr(binding, "space_id", None):
                return False
            acl = directory.api_client.do(
                "GET", f"/api/2.0/permissions/genie/{binding.space_id}")
            for entry in acl.get("access_control_list", []) or []:
                ref = (entry.get("service_principal_name") or entry.get("user_name")
                       or entry.get("group_name"))
                if ref == subject_id and any(
                        perm.get("permission_level") in _EDIT_LEVELS
                        for perm in entry.get("all_permissions", []) or []):
                    return True
            return False

        return PlatformIdentityProvider(
            profiles={profile: (lambda role=role: self.client(role))} if profile else None,
            request_resolver=resolve_actor, group_resolver=resolve_groups,
            edit_resolver=resolve_edit, job_client=self.client(role))

    # -- capability + topology proofs -------------------------------------
    def capability_probe(self) -> Callable[[], dict]:
        proof = self.config.get("capabilities", {})
        return lambda: dict(proof)

    def topology_proof(self) -> dict:
        return dict(self.config["topology"])

    # -- termination readers (positive termination only) ------------------
    def attempt_inventory(self, execution_ref, attempt_id):
        return None

    def job_reader(self, execution_ref, attempt_id):
        return None

    def worker_supervisor_reader(self, execution_ref, attempt_id):
        return None


def _statement_parameters(parameters):
    """Render a named-parameter dict into the SQL Statements API list with
    explicit types (mirrors the integration harness renderer)."""
    rendered = []
    for name, value in (parameters or {}).items():
        if value is None:
            rendered.append({"name": name, "value": None})
        elif isinstance(value, bool):
            rendered.append({"name": name, "value": str(value).lower(), "type": "BOOLEAN"})
        elif isinstance(value, int):
            rendered.append({"name": name, "value": str(value), "type": "BIGINT"})
        elif name.endswith("_at"):
            moment = datetime.fromisoformat(str(value))
            rendered.append({"name": name, "value": moment.strftime("%Y-%m-%d %H:%M:%S.%f"),
                             "type": "TIMESTAMP"})
        else:
            rendered.append({"name": name, "value": str(value), "type": "STRING"})
    return rendered


_INTEGER_TYPES = frozenset({"BYTE", "SHORT", "INT", "INTEGER", "LONG", "BIGINT"})
_FLOAT_TYPES = frozenset({"FLOAT", "DOUBLE"})


def _coerce(type_name: str, value):
    """Coerce a SQL Statements API string cell into the store's native Python type.

    The REST Statements API returns every cell as a string (thrift is IP-ACL
    blocked), but the durable stores compare typed values — integer fences
    (`binding_revision`, `row_version`, `generation`), booleans and `datetime`
    timestamps. Coercing here from the result manifest's `type_name` makes the REST
    seam behave like the typed connector the M03 CAS gate validated against; string
    columns (e.g. SHOW GRANTS) pass through unchanged.
    """
    if value is None:
        return None
    if type_name in _INTEGER_TYPES:
        return int(value)
    if type_name == "BOOLEAN":
        return value == "true"
    if type_name in _FLOAT_TYPES:
        return float(value)
    if type_name.startswith("DECIMAL"):
        return float(value)
    if type_name.startswith("TIMESTAMP"):
        return datetime.fromisoformat(value.replace(" ", "T", 1))
    return value


def _rows(response) -> list:
    manifest = response.get("manifest", {}) or {}
    schema_columns = (manifest.get("schema", {}) or {}).get("columns", [])
    names = [c["name"] for c in schema_columns]
    types = [c.get("type_name", "STRING") for c in schema_columns]
    data = (response.get("result", {}) or {}).get("data_array", []) or []
    return [{name: _coerce(type_name, cell)
             for name, type_name, cell in zip(names, types, row, strict=False)}
            for row in data]
