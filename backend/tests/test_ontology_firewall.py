"""Read-only firewall (Phase 1 + Phase 2, spec §11 extended).

No Ontology module — backend OR the GSO ``ontology/`` wheel package — contains a
governed-tag WRITE path: no SET/UNSET TAG, no CREATE/ALTER/DROP GOVERNED TAG, and
no ``manage_uc_tags`` import. The only UC writes anywhere are the ``genie_ont_*``
Delta snapshot MERGEs (the batch materializer); the only mutating HTTP verbs are
the Phase-1 settings PUT and the Phase-2 refresh POST (which triggers a job, not a
UC write). Phase-3/4 similarity + web-search tokens are absent."""

from __future__ import annotations

import pathlib

import pytest

_BACKEND_ONTOLOGY = pathlib.Path(__file__).resolve().parents[1] / "ontology"
_WHEEL_ONTOLOGY = (
    pathlib.Path(__file__).resolve().parents[2]
    / "packages" / "genie-space-optimizer" / "src" / "genie_space_optimizer" / "ontology"
)

# Actual DDL/tool write paths the spec names (SET TAG / CREATE GOVERNED TAG /
# manage_uc_tags) + siblings. "APPLY TAG" / "MANAGE DISCOVERY" appear only as
# informational grant COPY in the preflight banner and are not write paths.
_FORBIDDEN = [
    "set tag",
    "unset tag",
    "create governed tag",
    "alter governed tag",
    "drop governed tag",
    "manage_uc_tags",
]

# Phase-3/4 substrate must not be pulled forward anywhere in Phase 2.
_DEFERRED_TOKENS = ["lakebase_vector", "lakebase_text", "web_search"]

_PY_FILES = sorted(_BACKEND_ONTOLOGY.rglob("*.py")) + sorted(_WHEEL_ONTOLOGY.rglob("*.py"))


def test_ontology_packages_have_python_files():
    assert _BACKEND_ONTOLOGY.exists() and any(_BACKEND_ONTOLOGY.rglob("*.py"))
    assert _WHEEL_ONTOLOGY.exists() and any(_WHEEL_ONTOLOGY.rglob("*.py")), (
        "wheel ontology package not found — firewall would not scan it"
    )


@pytest.mark.parametrize("path", _PY_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_write_path_in_ontology_module(path: pathlib.Path):
    text = path.read_text().lower()
    hits = [tok for tok in _FORBIDDEN if tok in text]
    assert not hits, f"{path.name} contains banned governed-tag write token(s): {hits}"


@pytest.mark.parametrize("path", _PY_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_deferred_phase34_tokens(path: pathlib.Path):
    text = path.read_text().lower()
    hits = [tok for tok in _DEFERRED_TOKENS if tok in text]
    assert not hits, f"{path.name} references a deferred Phase-3/4 token: {hits}"


def test_backend_router_verbs_are_read_only_plus_settings_put_and_refresh_post():
    """Only mutating verbs: settings PUT (our config) + refresh POST (job trigger).
    Neither writes Unity Catalog governance."""
    routers_dir = _BACKEND_ONTOLOGY / "routers"
    put_files, post_files = [], []
    for path in sorted(routers_dir.glob("*.py")):
        text = path.read_text()
        if ".put(" in text:
            put_files.append(path.name)
        if ".post(" in text:
            post_files.append(path.name)
        assert ".patch(" not in text, f"{path.name} defines a PATCH route"
        assert ".delete(" not in text, f"{path.name} defines a DELETE route"
    assert put_files == ["settings.py"], f"unexpected PUT routes: {put_files}"
    assert post_files == ["refresh.py"], f"unexpected POST routes: {post_files}"


def test_wheel_writes_only_snapshot_tables_never_phase3():
    """The materializer's MERGE targets are exactly the three snapshot tables;
    the empty Phase-3 tables are created (DDL) but never written."""
    from genie_space_optimizer.ontology import ddl, materialize

    src = (_WHEEL_ONTOLOGY / "materialize.py").read_text()
    # Every Phase-3 table name must be absent from the materializer (never written).
    for t in ddl.PHASE3_TABLES:
        assert t not in src, f"materialize.py references Phase-3 table {t} (must not write it)"
    # The snapshot tables it does write.
    assert "genie_ont_tag_graph" in src and "genie_ont_taxonomy_snapshot" in src
