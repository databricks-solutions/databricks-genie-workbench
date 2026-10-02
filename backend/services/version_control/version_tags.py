"""Delta-backed version tags/comments store (mutable UX metadata, not a VC fact).

A version carries AT MOST ONE tag+comment, so ``genie_space_version_tags`` is keyed by
``version_id`` (PRIMARY KEY) with a ``space_id`` scope column — a 1:1/0..1 mapping onto the
version ledger. Unlike the append-only fact tables, this table is written in place: the
observe runtime's executor identity upserts (MERGE) and deletes rows, exactly like the
coordination store. It is deliberately non-authoritative: losing/altering a tag never
affects a version's integrity, provenance, or restorability.

The ``execute`` callable mirrors the coordination store's contract:
``execute(sql, params) -> list[Mapping]`` with ``:name`` bind parameters.
"""


class DeltaVersionTagStore:
    def __init__(self, execute, table):
        self.execute = execute
        self.table = table

    def set_tag(self, space_id: str, version_id: str, label: str,
                note: str | None, author: str | None) -> None:
        """Create or replace the single tag on one version (upsert). ``current_timestamp()``
        stamps ``updated_at`` server-side so no client clock is trusted."""
        self.execute(
            f"""MERGE INTO {self.table} AS t
                USING (SELECT :version_id AS version_id, :space_id AS space_id,
                              :label AS label, :note AS note, :author AS author) AS s
                  ON t.version_id = s.version_id
                WHEN MATCHED THEN UPDATE SET
                  t.space_id = s.space_id, t.label = s.label, t.note = s.note,
                  t.author = s.author, t.updated_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT
                  (version_id, space_id, label, note, author, updated_at)
                  VALUES (s.version_id, s.space_id, s.label, s.note, s.author, current_timestamp())""",
            {"version_id": version_id, "space_id": space_id, "label": label,
             "note": note, "author": author})

    def delete_tag(self, space_id: str, version_id: str) -> None:
        """Remove the tag on one version (no-op when absent). Space-scoped so a mismatched
        space can never delete another space's tag row."""
        self.execute(
            f"DELETE FROM {self.table} WHERE version_id = :version_id AND space_id = :space_id",
            {"version_id": version_id, "space_id": space_id})

    def get_tags(self, space_id: str) -> dict:
        """Return ``{version_id: {label, note, author}}`` for one space."""
        rows = self.execute(
            f"SELECT version_id, label, note, author FROM {self.table} WHERE space_id = :space_id",
            {"space_id": space_id})
        return {row["version_id"]: {"label": row["label"], "note": row["note"],
                                    "author": row["author"]}
                for row in (dict(r) for r in rows)}
