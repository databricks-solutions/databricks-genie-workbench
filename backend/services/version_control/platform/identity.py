"""Server-side identity verification without authority repair or credential fallback."""

import logging
from urllib.parse import urlsplit

from ..contracts import ActorContext, ExecutorContext

logger = logging.getLogger(__name__)


def canonical_host(host: str) -> str:
    parsed = urlsplit(host)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in {"", "/"} or parsed.query or parsed.fragment or parsed.port):
        raise ValueError("Explicit HTTPS workspace host required")
    return f"https://{parsed.hostname.lower()}"


class PlatformIdentityProvider:
    def __init__(self, *, profiles=None, obo_executors=None, request_resolver=None,
                 group_resolver=None, edit_resolver=None, job_client=None):
        self._profiles = dict(profiles or {})
        self._obo_executors = dict(obo_executors or {})
        self._request_resolver = request_resolver
        self._group_resolver = group_resolver
        self._edit_resolver = edit_resolver
        self._job_client = job_client
        self._contexts = {}

    def actor(self, request):
        if self._request_resolver is None or not request.authentication_reference:
            raise PermissionError("Server-authenticated request required")
        actor = self._request_resolver(request.authentication_reference)
        if (not isinstance(actor, ActorContext) or not actor.subject_id
                or actor.workspace_id != request.workspace_id):
            raise PermissionError("Authenticated actor workspace mismatch")
        return actor

    def groups(self, subject_id, workspace_id):
        if self._group_resolver is None or not subject_id or not workspace_id:
            raise PermissionError("Verified group resolution unavailable")
        groups = self._group_resolver(subject_id, workspace_id)
        if not isinstance(groups, (set, frozenset)) or any(not isinstance(group, str) for group in groups):
            raise PermissionError("Unverified group membership")
        return frozenset(groups)

    def can_edit(self, subject_id, binding):
        if self._edit_resolver is None or not subject_id:
            return False
        try:
            return self._edit_resolver(subject_id, binding) is True
        except Exception:
            return False

    def executor(self, selection) -> ExecutorContext:
        expected_host = canonical_host(selection.host)
        if not all((selection.workspace_id, selection.principal_id, selection.execution_ref)):
            raise ValueError("Explicit workspace, principal and execution required")
        if selection.profile is None:
            factory = self._obo_executors.get(selection.execution_ref)
            allowed_auth = {"pat"}
            actor_kind = "human"
        else:
            if not selection.profile.strip() or selection.profile.upper() == "DEFAULT":
                raise PermissionError("Default profile is not an explicit VC identity")
            factory = self._profiles.get(selection.profile)
            allowed_auth = {"oauth-m2m"}
            actor_kind = "service"
        if factory is None:
            raise PermissionError("No explicitly bound executor credentials")
        client = factory()
        if client.config.auth_type not in allowed_auth:
            raise PermissionError("Executor must use explicitly bound OAuth M2M or OBO")
        self._verify_client(client, expected_host, selection.workspace_id, selection.principal_id)
        context = ExecutorContext(selection.workspace_id, expected_host, selection.principal_id,
                                  actor_kind, client, selection.execution_ref)
        self._contexts[id(context)] = context
        return context

    @staticmethod
    def _verify_client(client, host, workspace_id, principal_id):
        if canonical_host(client.config.host) != host:
            raise PermissionError("Executor host/workspace mismatch")
        identity = client.api_client.do("GET", "/api/2.0/preview/scim/v2/Me",
                                        response_headers=["X-Databricks-Org-Id"])
        if str(identity.get("X-Databricks-Org-Id", "")) != workspace_id:
            raise PermissionError("Executor host/workspace mismatch")
        if client.config.auth_type == "oauth-m2m":
            actual = identity.get("applicationId") or identity.get("userName")
        elif client.config.auth_type == "pat":
            actual = identity.get("id")
        else:
            raise PermissionError("Executor authentication type changed")
        if actual != principal_id:
            raise PermissionError("Executor principal mismatch")

    def _client(self, executor):
        if self._contexts.get(id(executor)) is not executor:
            raise PermissionError("Executor was not verified by this identity provider")
        self._verify_client(executor.credential_handle, executor.host,
                            executor.workspace_id, executor.principal_id)
        return executor.credential_handle

    def sql_client(self, executor):
        return self._client(executor).statement_execution

    def verified_client(self, executor):
        return self._client(executor)

    def volume_client(self, executor):
        return self._client(executor).files

    def verify_run_as(self, execution_ref, expected_principal_id):
        if self._job_client is None:
            raise PermissionError("No explicit target-local Job identity reader")
        verify_job_run_as(self._job_client, execution_ref, expected_principal_id)


class TrustedSnapshotReader:
    def __init__(self, identity, authorize_snapshot, readers):
        self._identity = identity
        self._authorize_snapshot = authorize_snapshot
        self._readers = dict(readers)

    def read(self, request, binding):
        actor = self._identity.actor(request)
        if actor.workspace_id != binding.workspace_id:
            raise PermissionError("Snapshot reader must be target-local")
        if self._authorize_snapshot(actor, binding) is not True:
            raise PermissionError("Requester lacks snapshot-view authorization")
        reader = self._readers.get(binding.workspace_id)
        if reader is None:
            raise PermissionError("No explicitly bound target-local full-config reader")
        return reader(binding)


def verify_job_run_as(client, execution_ref: str, expected_principal_id: str) -> None:
    # The governed execute path (promotion/restore/optimizer/gate) carries the
    # executor selection as ``job/<id>`` (validated by each `_executor`); the
    # optimizer boot check passes a bare ``GSO_JOB_ID``. Accept either form.
    job_ref = execution_ref.removeprefix("job/")
    if not job_ref.isdigit() or int(job_ref) <= 0 or not expected_principal_id:
        raise ValueError("Explicit Job and expected principal required")
    job = client.jobs.get(job_id=int(job_ref))
    settings = getattr(job, "settings", None)
    run_as = getattr(settings, "run_as", None)
    if (getattr(run_as, "service_principal_name", None) != expected_principal_id
            or getattr(run_as, "user_name", None)):
        raise PermissionError("Job run_as does not match expected service principal")


def verify_configured_job_run_as(environment, client_factory) -> None:
    execution_ref = environment.get("GSO_JOB_ID", "")
    # "Not configured" and "configured but wrong" are different states; only the
    # second is an authority violation. An empty value, the unsubstituted deploy
    # placeholder "__GSO_JOB_ID__", or any non-digit / non-positive value all mean
    # the optimizer integration is NOT configured. Log and return — never raise —
    # so a half-configured deploy degrades the optimizer instead of failing the
    # whole app boot and taking history reads down with it (FM-OUTAGE). This is not
    # startup authority repair: nothing is written; we simply do not verify what was
    # never configured. Only a CONFIGURED (all-digit, positive) Job whose run_as is
    # wrong or unreadable refuses.
    if not execution_ref or not execution_ref.isdigit() or int(execution_ref) <= 0:
        logger.warning(
            "Optimizer integration not configured (GSO_JOB_ID=%r); skipping Job "
            "run_as verification", execution_ref)
        return
    client = client_factory()
    principal = client.config.client_id or environment.get("DATABRICKS_CLIENT_ID", "")
    verify_job_run_as(client, execution_ref, principal)
