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

# Phase-4 external-context substrate must not be pulled forward. (Phase 3a
# unlocks the Lakebase Search similarity tokens, but ONLY inside similarity.py —
# see test_lakebase_search_tokens_confined_to_similarity below.)
_DEFERRED_TOKENS = ["web_search"]

# Lakebase Search tokens are allowed in exactly one module (the similarity seam).
_LAKEBASE_SEARCH_TOKENS = ["lakebase_vector", "lakebase_text"]

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
def test_no_deferred_phase4_tokens(path: pathlib.Path):
    text = path.read_text().lower()
    hits = [tok for tok in _DEFERRED_TOKENS if tok in text]
    assert not hits, f"{path.name} references a deferred Phase-4 token: {hits}"


@pytest.mark.parametrize("path", _PY_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_lakebase_search_tokens_confined_to_similarity(path: pathlib.Path):
    """MV-D40/D45: the Lakebase Search tokens live in exactly one module (the
    similarity seam), so enabling/disabling that backend is a scoped change."""
    text = path.read_text().lower()
    hits = [tok for tok in _LAKEBASE_SEARCH_TOKENS if tok in text]
    if path.name == "similarity.py":
        return  # the one place they are allowed
    assert not hits, f"{path.name} references Lakebase Search token(s) outside similarity.py: {hits}"


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


def test_wheel_writes_snapshots_proposals_pages_never_consents_suppressions():
    """Phase 3c: the materializer MERGEs the snapshot tables, the Domain/Member
    PROPOSAL tables, AND the genie_ont_pages PAGE table; only the consent/suppression
    ledger tables are created (DDL) but never written (17g owns them)."""
    from genie_space_optimizer.ontology import ddl, materialize  # noqa: F401

    src = (_WHEEL_ONTOLOGY / "materialize.py").read_text()
    # The still-forbidden ledger tables must be absent from the materializer.
    assert ddl.PHASE3_TABLES == ("genie_ont_consents", "genie_ont_suppressions")
    for t in ddl.PHASE3_TABLES:
        assert t not in src, f"materialize.py references unwritten table {t} (must not write it)"
    # The snapshot tables it writes.
    assert "genie_ont_tag_graph" in src and "genie_ont_taxonomy_snapshot" in src
    # The proposal tables it now writes (referenced via the ddl constants).
    assert ddl.PROPOSAL_TABLES == ("genie_ont_domains", "genie_ont_members")
    assert "TABLE_ONT_DOMAINS" in src and "TABLE_ONT_MEMBERS" in src
    assert "DOMAIN_KEYS" in src and "MEMBER_KEYS" in src
    # Phase 3c: the Page proposal table is now written (concept-anchored, MV-D49).
    assert ddl.PAGE_TABLES == ("genie_ont_pages",)
    assert "TABLE_ONT_PAGES" in src and "PAGE_KEYS" in src


def test_leakage_oracle_has_page_body_scan_not_a_second_scanner():
    """Phase 3c firewall: the LeakageOracle gained a page-body scan (the extended
    oracle, MV-D8 comment-echo transposed) — it delegates to the same corpus matcher,
    not a new scanner class."""
    import inspect

    from genie_space_optimizer.optimization import leakage

    assert hasattr(leakage.LeakageOracle, "contains_page_leak")
    body_src = inspect.getsource(leakage.LeakageOracle.contains_page_leak)
    # Reuses the shared corpus matcher (no second scanner).
    assert "_check_string_against_corpus" in body_src
    # No-op with no corpus (the normal ontology run has no benchmark corpus in scope).
    assert leakage.LeakageOracle().contains_page_leak("any `finance.x.y` body") == (False, "")
