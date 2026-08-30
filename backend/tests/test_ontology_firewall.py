"""Read-only guarantee (spec §11): the Phase-1 firewall. No Ontology route or
service module contains a governed-tag WRITE path — no SET/UNSET TAG, no
CREATE/ALTER/DROP GOVERNED TAG, and no manage_uc_tags import. The ONLY write in
Phase 1 is PUT /api/ontology/settings, which writes our own Lakebase config
(never Unity Catalog)."""

from __future__ import annotations

import pathlib

import pytest

_ONTOLOGY_DIR = pathlib.Path(__file__).resolve().parents[1] / "ontology"

# Write verbs the firewall bans. These are the actual DDL/tool write paths the
# spec names (SET TAG / CREATE GOVERNED TAG / manage_uc_tags) plus their siblings.
# Note: "APPLY TAG" / "MANAGE DISCOVERY" appear only as informational grant COPY
# in the preflight banner (describing the deferred write tier) and are NOT write
# paths, so they are intentionally not banned here.
_FORBIDDEN = [
    "set tag",
    "unset tag",
    "create governed tag",
    "alter governed tag",
    "drop governed tag",
    "manage_uc_tags",
]

_PY_FILES = sorted(_ONTOLOGY_DIR.rglob("*.py"))


def test_ontology_package_has_python_files():
    # Guard against the glob silently matching nothing (which would make the
    # firewall vacuously pass).
    assert _PY_FILES, "no ontology python files found — firewall would be vacuous"


@pytest.mark.parametrize("path", _PY_FILES, ids=lambda p: p.name)
def test_no_write_path_in_ontology_module(path: pathlib.Path):
    text = path.read_text().lower()
    hits = [tok for tok in _FORBIDDEN if tok in text]
    assert not hits, f"{path.name} contains banned write token(s): {hits}"


def test_routers_only_expose_settings_put():
    """The only mutating HTTP verb across ontology routers is the settings PUT."""
    routers_dir = _ONTOLOGY_DIR / "routers"
    put_files = []
    for path in sorted(routers_dir.glob("*.py")):
        text = path.read_text()
        if ".put(" in text or '"PUT"' in text or "'PUT'" in text:
            put_files.append(path.name)
        # No route module issues a POST/PATCH/DELETE in Phase 1.
        assert ".post(" not in text, f"{path.name} defines a POST route (Phase 1 is read-only)"
        assert ".patch(" not in text, f"{path.name} defines a PATCH route (Phase 1 is read-only)"
        assert ".delete(" not in text, f"{path.name} defines a DELETE route (Phase 1 is read-only)"
    assert put_files == ["settings.py"], f"unexpected PUT routes: {put_files}"
