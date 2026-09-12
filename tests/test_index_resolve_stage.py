import pytest

pytest.importorskip("yaml")
import yaml  # noqa: E402

from transitio_index import overrides, resolve, store  # noqa: E402


def _feed(feed_id, **kw):
    feed = {
        "feed_id": feed_id,
        "onestop_id": kw.get("onestop_id"),
        "mdb_id": kw.get("mdb_id"),
        "id_minted": kw.get("id_minted", True),
        "source": kw.get("source", "mdb"),
        "spec": kw.get("spec", "gtfs"),
        "name": kw.get("name", feed_id),
        "aliases": kw.get("aliases", []),
    }
    return feed


def _publish(cache, subdir, pointer, artifact, records, manifest=None):
    directory = store.open_subdir(cache, subdir)
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / subdir,
                pointer,
                {artifact: store.jsonl_chunks(records)},
                manifest or {"source": subdir},
                held=directory,
            )
    finally:
        directory.close()


def _crosswalk(cache, feeds):
    _publish(cache, "crosswalk", "feeds.json", "feeds.jsonl", feeds)


def _overrides_dir(tmp_path, entries):
    directory = tmp_path / "overrides"
    directory.mkdir(exist_ok=True)
    (directory / "feeds.yaml").write_text(yaml.safe_dump(entries), encoding="utf-8")
    return directory


def _resolved(cache):
    feeds, manifest = store.read_jsonl(
        cache / "resolve", "feeds_resolved.json", "feeds_resolved.jsonl"
    )
    return {f["feed_id"]: f for f in feeds}, manifest


def test_no_overrides_pass_feeds_through_as_crawlable(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a"), _feed("f-b")])
    resolve.resolve(cache, overrides_dir=None)
    feeds, manifest = _resolved(cache)
    assert feeds["f-a"]["crawlable"] is True
    assert feeds["f-a"]["uncrawlable_reason"] is None
    assert manifest["overridden_feeds"] == 0
    assert manifest["crosswalk_generation"] == (
        store.resolve(cache / "crosswalk", "feeds.json")[1]["generation"]
    )
    assert manifest["uncrawlable"] == 0


def test_only_open_static_gtfs_defaults_to_crawlable(tmp_path):
    # Realtime and GBFS are indexed but never fetched; a GTFS feed whose download
    # URL needs a key is refused up front, judged by the record that supplies
    # the URL (the Atlas static URL first, else the MDB download).
    gated = {"requires_auth": True, "urls": {"direct_download": "https://x/a.zip"}}
    open_atlas = {"requires_auth": False, "urls": {"static_current": "https://y/a.zip"}}
    cases = [
        (_feed("f-rt", spec="gtfs-rt"), False, None),
        (_feed("f-bike", spec="gbfs"), False, None),
        (dict(_feed("f-key"), mdb=gated), False, resolve.AUTH_REASON),
        (dict(_feed("f-both"), atlas=open_atlas, mdb=gated), True, None),
        (
            dict(_feed("f-atlas"), atlas=dict(open_atlas, requires_auth=True)),
            False,
            resolve.AUTH_REASON,
        ),
        (_feed("f-open"), True, None),
        # An upstream decision stands; an override's reason wins over the default.
        (dict(_feed("f-forced"), mdb=gated, crawlable=True), True, None),
        (dict(_feed("f-said"), mdb=gated), False, "closed"),
    ]
    cache = tmp_path / "cache"
    _crosswalk(cache, [feed for feed, _, _ in cases])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-said", "mark_uncrawlable": {"reason": "closed"}}]
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    for feed, crawlable, reason in cases:
        assert feeds[feed["feed_id"]]["crawlable"] is crawlable, feed["feed_id"]
        assert feeds[feed["feed_id"]]["uncrawlable_reason"] == reason, feed["feed_id"]
    assert manifest["uncrawlable"] == 5
    assert manifest["requires_auth"] == 3


def test_set_identity_rewrites_the_named_fields(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a", name="Old", onestop_id="o-old")])
    overrides_dir = _overrides_dir(
        tmp_path,
        [{"feed": "f-a", "set_identity": {"name": "New", "onestop_id": "o-new"}}],
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    assert feeds["f-a"]["name"] == "New"
    assert feeds["f-a"]["onestop_id"] == "o-new"
    assert manifest["overridden_feeds"] == 1


def test_mark_uncrawlable_stops_the_feed_with_a_reason(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a")])
    overrides_dir = _overrides_dir(
        tmp_path,
        [{"feed": "f-a", "mark_uncrawlable": {"reason": "auth-gated"}}],
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    assert feeds["f-a"]["crawlable"] is False
    assert feeds["f-a"]["uncrawlable_reason"] == "auth-gated"
    assert manifest["uncrawlable"] == 1


def test_an_override_matches_a_feed_by_alias(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-new", aliases=["f-gbfs-old"])])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-gbfs-old", "set_identity": {"name": "Renamed"}}]
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    assert feeds["f-new"]["name"] == "Renamed"
    assert manifest["unmatched_overrides"] == []


def test_an_override_for_a_missing_feed_is_recorded_unmatched(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a")])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-ghost", "set_identity": {"name": "X"}}]
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    _, manifest = _resolved(cache)
    assert manifest["unmatched_overrides"] == ["f-ghost"]
    assert manifest["overridden_feeds"] == 0


def test_set_identity_can_rename_the_feed_id_keeping_the_old_in_aliases(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-old")])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-old", "set_identity": {"feed_id": "f-new"}}]
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, _ = _resolved(cache)
    assert "f-new" in feeds
    assert "f-old" not in feeds
    assert "f-old" in feeds["f-new"]["aliases"]


def test_a_rename_that_collides_with_another_feed_is_a_build_error(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a"), _feed("f-b")])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": {"feed_id": "f-b"}}]
    )
    with pytest.raises(overrides.OverrideError, match="share lookup keys"):
        resolve.resolve(cache, overrides_dir=overrides_dir)


def test_an_alias_colliding_with_another_feed_is_a_build_error(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a"), _feed("f-b")])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": {"aliases": ["f-b"]}}]
    )
    with pytest.raises(overrides.OverrideError, match="share lookup keys"):
        resolve.resolve(cache, overrides_dir=overrides_dir)


def test_adopting_a_real_id_clears_id_minted(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-mdb-1", id_minted=True)])
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-mdb-1", "set_identity": {"onestop_id": "o-real"}}]
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, _ = _resolved(cache)
    assert feeds["f-mdb-1"]["id_minted"] is False


def test_an_override_matching_two_feeds_by_alias_is_a_build_error(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(
        cache, [_feed("f-a", aliases=["shared"]), _feed("f-b", aliases=["shared"])]
    )
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "shared", "set_identity": {"name": "X"}}]
    )
    with pytest.raises(overrides.OverrideError, match="several feeds"):
        resolve.resolve(cache, overrides_dir=overrides_dir)


def test_a_malformed_feed_id_value_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": {"feed_id": "../escape"}}]
    )
    with pytest.raises(overrides.OverrideError, match="not a valid feed id"):
        overrides.load_feed_overrides(overrides_dir)


def test_a_rename_with_aliases_still_keeps_the_old_id(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-old")])
    overrides_dir = _overrides_dir(
        tmp_path,
        [{"feed": "f-old", "set_identity": {"feed_id": "f-new", "aliases": ["extra"]}}],
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, _ = _resolved(cache)
    assert set(feeds["f-new"]["aliases"]) == {"extra", "f-old"}


def test_two_overrides_matching_one_feed_is_a_build_error(tmp_path):
    cache = tmp_path / "cache"
    _crosswalk(cache, [_feed("f-a", aliases=["also-a"])])
    overrides_dir = _overrides_dir(
        tmp_path,
        [
            {"feed": "f-a", "set_identity": {"name": "A"}},
            {"feed": "also-a", "mark_uncrawlable": True},
        ],
    )
    with pytest.raises(overrides.OverrideError, match="several overrides"):
        resolve.resolve(cache, overrides_dir=overrides_dir)


def test_a_falsey_set_identity_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(tmp_path, [{"feed": "f-a", "set_identity": []}])
    with pytest.raises(overrides.OverrideError, match="non-empty mapping"):
        overrides.load_feed_overrides(overrides_dir)


def test_an_entry_with_no_operation_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(tmp_path, [{"feed": "f-a", "reason": "note"}])
    with pytest.raises(overrides.OverrideError, match="no operation"):
        overrides.load_feed_overrides(overrides_dir)


def test_an_out_of_enum_static_link_method_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": {"static_link_method": "bogus"}}]
    )
    with pytest.raises(overrides.OverrideError, match="static_link_method"):
        overrides.load_feed_overrides(overrides_dir)


def _write_yaml(tmp_path, text):
    directory = tmp_path / "overrides"
    directory.mkdir(exist_ok=True)
    (directory / "feeds.yaml").write_text(text, encoding="utf-8")
    return directory


def test_a_duplicate_yaml_key_is_a_build_error(tmp_path):
    directory = _write_yaml(
        tmp_path,
        "- feed: f-a\n  set_identity: {name: A}\n  set_identity: {name: B}\n",
    )
    with pytest.raises(overrides.OverrideError, match="duplicate key"):
        overrides.load_feed_overrides(directory)


def test_a_non_list_root_is_a_build_error(tmp_path):
    directory = _write_yaml(tmp_path, "{}\n")
    with pytest.raises(overrides.OverrideError, match="expected a list"):
        overrides.load_feed_overrides(directory)


def test_an_unknown_identity_field_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": {"bogus": "x"}}]
    )
    with pytest.raises(overrides.OverrideError, match="set_identity"):
        overrides.load_feed_overrides(overrides_dir)


def test_a_duplicate_feed_override_is_a_build_error(tmp_path):
    overrides_dir = _overrides_dir(
        tmp_path,
        [
            {"feed": "f-a", "set_identity": {"name": "A"}},
            {"feed": "f-a", "mark_uncrawlable": True},
        ],
    )
    with pytest.raises(overrides.OverrideError, match="duplicate"):
        overrides.load_feed_overrides(overrides_dir)
