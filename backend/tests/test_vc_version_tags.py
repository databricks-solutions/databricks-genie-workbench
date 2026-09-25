from backend.services.version_control.version_tags import DeltaVersionTagStore


class FakeSql:
    """Minimal in-memory Delta emulator for the tag store: dispatches on the SQL verb and
    applies the store's :name params to a version_id-keyed dict. Serializable upsert/delete
    over a single row per version, exactly like the governed table's 1:1 mapping."""

    def __init__(self):
        self.rows: dict[str, dict] = {}

    def __call__(self, sql: str, params: dict):
        head = sql.strip().split(None, 1)[0].upper()
        if head == "MERGE":
            self.rows[params["version_id"]] = {
                "version_id": params["version_id"], "space_id": params["space_id"],
                "label": params["label"], "note": params["note"], "author": params["author"]}
            return []
        if head == "DELETE":
            row = self.rows.get(params["version_id"])
            if row is not None and row["space_id"] == params["space_id"]:
                del self.rows[params["version_id"]]
            return []
        if head == "SELECT":
            return [dict(r) for r in self.rows.values() if r["space_id"] == params["space_id"]]
        raise AssertionError(f"unexpected SQL: {sql!r}")


def _store():
    return DeltaVersionTagStore(FakeSql(), "`cat`.`ctrl`.genie_space_version_tags")


def test_set_get_delete_version_tag():
    store = _store()
    sid, vid = "space-tags-1", "11111111-1111-1111-1111-111111111111"
    store.set_tag(sid, vid, "Golden baseline", "before rollout", "amy@x.io")
    tags = store.get_tags(sid)
    assert tags[vid] == {"label": "Golden baseline", "note": "before rollout", "author": "amy@x.io"}


def test_get_tags_scoped_by_space():
    store = _store()
    store.set_tag("space-a", "v-a", "A", None, None)
    store.set_tag("space-b", "v-b", "B", None, None)
    assert set(store.get_tags("space-a")) == {"v-a"}
    assert store.get_tags("other-space") == {}


def test_set_tag_upserts():
    store = _store()
    sid, vid = "space-tags-2", "22222222-2222-2222-2222-222222222222"
    store.set_tag(sid, vid, "Champion", "note", "amy@x.io")
    store.set_tag(sid, vid, "Renamed", None, "amy@x.io")
    tag = store.get_tags(sid)[vid]
    assert tag["label"] == "Renamed"
    assert tag["note"] is None


def test_delete_tag():
    store = _store()
    sid, vid = "space-tags-3", "33333333-3333-3333-3333-333333333333"
    store.set_tag(sid, vid, "Temp", None, None)
    store.delete_tag(sid, vid)
    assert vid not in store.get_tags(sid)
    # delete is a no-op when absent
    store.delete_tag(sid, vid)
    assert store.get_tags(sid) == {}


def test_delete_tag_scoped_by_space():
    store = _store()
    store.set_tag("space-x", "shared-vid", "X", None, None)
    # a different space cannot delete another space's tag row
    store.delete_tag("space-y", "shared-vid")
    assert "shared-vid" in store.get_tags("space-x")
