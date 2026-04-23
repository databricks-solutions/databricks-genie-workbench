from __future__ import annotations

import re
from pathlib import Path

import pytest

from scripts import setup_lakebase
from scripts import setup_workbench


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeLakebaseConn:
    def __init__(
        self,
        *,
        schema_owner: str,
        table_owners: dict[str, str],
        sequence_owners: dict[str, str] | None = None,
        can_grant_membership: bool = True,
        allow_object_grants: bool = True,
    ):
        self.schema_owner = schema_owner
        self.table_owners = dict(table_owners)
        self.sequence_owners = dict(sequence_owners or {})
        self.can_grant_membership = can_grant_membership
        self.allow_object_grants = allow_object_grants
        self.memberships: set[str] = set()
        self.statements: list[str] = []

    def execute(self, statement: str, params=None):
        sql = " ".join(statement.split())
        self.statements.append(sql)

        if "FROM pg_namespace WHERE nspname = 'genie'" in sql:
            return _Rows([(self.schema_owner,)] if self.schema_owner else [])

        if "c.relkind IN ('r', 'p')" in sql:
            return _Rows(sorted(self.table_owners.items()))

        if "c.relkind = 'S'" in sql:
            return _Rows(sorted(self.sequence_owners.items()))

        if "pg_has_role(CURRENT_USER" in sql:
            role = params[0]
            return _Rows([(role in self.memberships,)])

        match = re.match(r'GRANT "([^"]+)" TO CURRENT_USER', sql)
        if match:
            if not self.can_grant_membership:
                raise RuntimeError("permission denied to grant role")
            self.memberships.add(match.group(1))
            return _Rows([])

        match = re.match(r'REVOKE "([^"]+)" FROM CURRENT_USER', sql)
        if match:
            self.memberships.discard(match.group(1))
            return _Rows([])

        match = re.match(r'ALTER SCHEMA genie OWNER TO "([^"]+)"', sql)
        if match:
            new_owner = match.group(1)
            self._require_membership(self.schema_owner, new_owner)
            self.schema_owner = new_owner
            return _Rows([])

        match = re.match(r'ALTER TABLE genie\."([^"]+)" OWNER TO "([^"]+)"', sql)
        if match:
            table, new_owner = match.groups()
            self._require_membership(self.table_owners[table], new_owner)
            self.table_owners[table] = new_owner
            return _Rows([])

        match = re.match(r'ALTER SEQUENCE genie\."([^"]+)" OWNER TO "([^"]+)"', sql)
        if match:
            sequence, new_owner = match.groups()
            self._require_membership(self.sequence_owners[sequence], new_owner)
            self.sequence_owners[sequence] = new_owner
            return _Rows([])

        if sql.startswith("GRANT ") and " IN SCHEMA genie TO " in sql:
            if not self.allow_object_grants:
                raise RuntimeError("permission denied for schema genie")
            return _Rows([])

        return _Rows([])

    def _require_membership(self, old_owner: str, new_owner: str) -> None:
        if old_owner not in self.memberships or new_owner not in self.memberships:
            raise RuntimeError("must be member of both old and new owner roles")


def test_reconcile_genie_schema_transfers_only_scoped_objects_to_new_sp():
    conn = _FakeLakebaseConn(
        schema_owner="old-sp",
        table_owners={"scan_results": "old-sp", "starred_spaces": "old-sp"},
        sequence_owners={"scan_results_id_seq": "old-sp"},
    )

    ok = setup_lakebase._reconcile_schema_ownership(
        conn, new_sp="new-sp", project_name="project-a",
    )

    assert ok is True
    assert conn.schema_owner == "new-sp"
    assert set(conn.table_owners.values()) == {"new-sp"}
    assert set(conn.sequence_owners.values()) == {"new-sp"}

    joined = "\n".join(conn.statements)
    assert "REASSIGN OWNED" not in joined
    assert 'GRANT "old-sp" TO CURRENT_USER' in joined
    assert 'GRANT "new-sp" TO CURRENT_USER' in joined
    assert 'REVOKE "new-sp" FROM CURRENT_USER' in joined
    assert 'REVOKE "old-sp" FROM CURRENT_USER' in joined


def test_reconcile_genie_schema_falls_back_to_existing_data_grants():
    conn = _FakeLakebaseConn(
        schema_owner="old-sp",
        table_owners={"scan_results": "old-sp"},
        sequence_owners={"scan_results_id_seq": "old-sp"},
        can_grant_membership=False,
    )

    ok = setup_lakebase._reconcile_schema_ownership(
        conn, new_sp="new-sp", project_name="project-a",
    )

    assert ok is True
    assert conn.schema_owner == "old-sp"
    joined = "\n".join(conn.statements)
    assert 'GRANT USAGE, CREATE ON SCHEMA genie TO "new-sp"' in joined
    assert 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA genie TO "new-sp"' in joined
    assert 'GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA genie TO "new-sp"' in joined
    assert "REASSIGN OWNED" not in joined


def test_gso_job_dag_points_to_existing_workspace_notebooks():
    repo_root = Path(__file__).resolve().parents[2]
    jobs_dir = repo_root / "packages/genie-space-optimizer/src/genie_space_optimizer/jobs"
    dag = setup_workbench._gso_job_dag(
        workspace_folder="/Workspace/Users/user/databricks-genie-workbench",
        wheel_path="/Workspace/Users/user/databricks-genie-workbench/.build/gso.whl",
    )

    notebook_paths = [
        task["notebook_task"]["notebook_path"]
        for task in dag["tasks"]
        if "notebook_task" in task
    ]
    assert any(path.endswith("/run_baseline") for path in notebook_paths)
    assert not any(path.endswith("/run_baseline_eval") for path in notebook_paths)

    for notebook_path in notebook_paths:
        notebook_name = notebook_path.rsplit("/", 1)[-1]
        assert (jobs_dir / f"{notebook_name}.py").exists()


def test_patch_app_raises_when_resource_patch_fails():
    class ApiClient:
        def do(self, method, path, body=None):
            if method == "GET":
                return {"resources": []}
            if method == "PATCH":
                raise RuntimeError("patch denied")
            raise AssertionError(method)

    class Workspace:
        api_client = ApiClient()

    with pytest.raises(RuntimeError, match="Could not configure app scopes/resources"):
        setup_workbench._patch_app(
            Workspace(),
            app_name="genie-workbench",
            warehouse_id="warehouse",
            lakebase_db="projects/p/branches/production/databases/db",
            lakebase_branch="projects/p/branches/production",
        )
