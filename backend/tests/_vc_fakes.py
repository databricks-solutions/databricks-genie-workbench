"""Shared offline fakes for the observe-only VC runtime tests.

Neutral home for the low-level ``adapters`` seam fake and the in-memory Volume
file store used to assemble ``build_observe_runtime`` offline (see
``test_vc_observe_seams`` / ``test_vc_app_observe``). Only the lowest-level
clients are faked; the real leaf constructors are exercised.
"""

from types import SimpleNamespace

from backend.services.version_control import contracts as vc

TARGET_HOST = "https://target.example.com"
SOURCE_HOST = "https://source.example.com"


class MemoryFiles:
    def __init__(self):
        self.files = {}
        self.published = []

    def read(self, path):
        return self.files[path]

    def put_if_absent(self, path, content):
        self.files.setdefault(path, content)

    def publish(self, reference):
        self.published.append(reference)


def _executor(spec):
    return vc.ExecutorContext(spec["workspace_id"], spec["host"], spec["principal_id"],
                              "service", object(), spec["execution_ref"])


class FakeAdapters:
    """Low-level seam surface used by ``build_observe_runtime``, all faked."""

    def __init__(self):
        self.stores = {}

    def sql(self, _role):
        return lambda statement, parameters=None: []

    def files_store(self, role):
        return self.stores.setdefault(role, MemoryFiles())

    def read_evidence(self, _uri):
        return b""

    def authenticate(self, _executor):
        return {"Authorization": "Bearer test"}

    def identity_provider(self, role):
        host = TARGET_HOST if role != "source" else SOURCE_HOST
        ws = "target" if role != "source" else "source"
        return SimpleNamespace(
            executor=lambda selection: _executor(
                {"workspace_id": ws, "host": host,
                 "principal_id": f"{role}-sp", "execution_ref": "job/123"}),
            actor=lambda request: vc.ActorContext(request.authentication_reference, ws, "service"))

    def capability_probe(self):
        return lambda: {"ok": True}

    def topology_proof(self):
        return {"source_workspace": "source", "target_workspace": "target",
                "source_metastore": "meta-1", "target_metastore": "meta-1",
                "remote_write_credentials": False, "packages_readable": True}

    def attempt_inventory(self, *_a):
        return None

    def job_reader(self, *_a):
        return None

    def worker_supervisor_reader(self, *_a):
        return None
