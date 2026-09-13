import pytest

from backend.services import lakebase


@pytest.mark.asyncio
async def test_set_get_delete_version_tag_in_memory():
    sid, vid = "space-tags-1", "11111111-1111-1111-1111-111111111111"
    await lakebase.set_version_tag(sid, vid, "Golden baseline", "before rollout", "amy@x.io")
    tags = await lakebase.get_version_tags(sid)
    assert tags[vid] == {"label": "Golden baseline", "note": "before rollout", "author": "amy@x.io"}
    # scoped by space
    assert await lakebase.get_version_tags("other-space") == {}
    # upsert overwrites
    await lakebase.set_version_tag(sid, vid, "Renamed", None, "amy@x.io")
    assert (await lakebase.get_version_tags(sid))[vid]["label"] == "Renamed"
    # delete
    await lakebase.delete_version_tag(sid, vid)
    assert vid not in await lakebase.get_version_tags(sid)
