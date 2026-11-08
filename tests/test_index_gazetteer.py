import errno
import json

import pytest

pytest.importorskip("pyarrow")
import overture_fixture as fx  # noqa: E402

from transitio_index import overture, store  # noqa: E402

US = ("ov-us", "country", "United States")
NY = ("ov-ny", "region", "New York")

# The skeleton subtypes plus a locality and a rejected finer subtype.
ROWS = [
    fx.division(
        "ov-us",
        "US",
        "country",
        wikidata="Q30",
        name="United States",
        common={"en": "United States", "es": "EE. UU."},
        sources=[fx.osm("relation/148838")],
        hierarchies=fx.chain(US),
    ),
    fx.division(
        "ov-ny",
        "US",
        "region",
        wikidata="Q1384",
        name="New York",
        admin_level=1,
        sources=[fx.osm("relation/61320")],
        hierarchies=fx.chain(US, NY),
    ),
    fx.division(
        "ov-nyc-county",
        "US",
        "county",
        name="New York County",
        admin_level=2,
        sources=[fx.osm("relation/2552450")],
        hierarchies=fx.chain(US, NY, ("ov-nyc-county", "county", "New York County")),
    ),
    fx.division(
        "ov-noqid",
        "US",
        "localadmin",
        name="Nowhere",
        admin_level=3,
        sources=[
            {"dataset": "geoBoundaries", "license": "CC-BY-4.0", "record_id": "X"}
        ],
        hierarchies=fx.chain(US, ("ov-noqid", "localadmin", "Nowhere")),
    ),
    fx.division(
        "ov-loc",
        "US",
        "locality",
        wikidata="Q1",
        name="Sometown",
        admin_level=4,
        sources=[fx.osm("relation/9")],
        hierarchies=fx.chain(US, ("ov-loc", "locality", "Sometown")),
    ),
    fx.division(
        "ov-fi",
        "FI",
        "country",
        wikidata="Q33",
        name="Finland",
        sources=[fx.osm("relation/54224")],
        hierarchies=fx.chain(("ov-fi", "country", "Finland")),
    ),
]


def _read(cache, artifact):
    records, manifest = store.read_jsonl(cache / "gazetteer", "overture.json", artifact)
    return records, manifest


def _resolve(tmp_path, *, mapping=None):
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS)
    wikidata = fx.StubWikidata({"2552450": "Q11299"} if mapping is None else mapping)
    manifest = overture.resolve(cache, dataset=dataset, wikidata=wikidata)
    return cache, manifest, wikidata


def test_resolve_publishes_resolved_and_report(tmp_path):
    cache, manifest, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    report, _ = _read(cache, "place_resolution_report.jsonl")

    by_id = {r["overture_id"]: r for r in resolved}
    assert by_id["ov-us"]["qid"] == "Q30"
    assert by_id["ov-us"]["resolution_method"] == "overture_wikidata"
    assert by_id["ov-ny"]["qid"] == "Q1384"
    # No wikidata property, resolved through the OSM relation's P402 reverse.
    assert by_id["ov-nyc-county"]["qid"] == "Q11299"
    assert by_id["ov-nyc-county"]["resolution_method"] == "osm_p402"
    # The OSM relation ids are kept on the resolved record as a crosswalk.
    assert by_id["ov-nyc-county"]["osm_relation_ids"] == ["2552450"]
    # The whole admin skeleton is read, so FI resolves like US.
    assert by_id["ov-fi"]["qid"] == "Q33"
    # localities and finer subtypes are not part of the skeleton read.
    assert "ov-loc" not in by_id
    # A named division no QID names is a place of its own, identified by
    # its Overture id; only a conflicting or nameless one is reported.
    assert by_id["ov-noqid"]["qid"] is None
    assert by_id["ov-noqid"]["resolution_method"] == "overture_id"
    assert report == []
    assert manifest["resolved"] == 5
    assert manifest["reported"] == 0
    assert manifest["resolved_by_method"] == {
        "overture_wikidata": 3,
        "osm_p402": 1,
        "overture_id": 1,
    }
    assert manifest["countries"] == ["FI", "US"]
    assert manifest["overture_release"] == overture.current_release()


def test_subtype_maps_to_kind_and_keeps_source_subtype(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    kinds = {r["overture_id"]: (r["kind"], r["source_subtype"]) for r in resolved}
    assert kinds["ov-us"] == ("country", "country")
    assert kinds["ov-ny"] == ("region", "region")
    assert kinds["ov-nyc-county"] == ("region", "county")


def test_ancestors_come_from_the_hierarchy_chain(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    county = next(r for r in resolved if r["overture_id"] == "ov-nyc-county")
    assert [(a["overture_id"], a["subtype"]) for a in county["ancestors"]] == [
        ("ov-us", "country"),
        ("ov-ny", "region"),
    ]


def test_names_carry_the_primary_and_multilingual_labels(tmp_path):
    cache, _, _ = _resolve(tmp_path)
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    us = next(r for r in resolved if r["overture_id"] == "ov-us")
    assert us["name"] == "United States"
    assert us["names"]["es"] == "EE. UU."


def test_p402_is_queried_only_for_divisions_without_a_qid(tmp_path):
    _, _, wikidata = _resolve(tmp_path)
    # Among the skeleton only ov-nyc-county lacks a wikidata property and has an
    # OSM relation; ov-noqid's source is OSM-less, so 2552450 is all that's asked.
    assert wikidata.queried == [["2552450"]]


def test_a_conflicting_p402_relation_is_reported_not_minted(tmp_path):
    # The county's relation resolves ambiguously (None), so it is not minted and
    # its report entry says so rather than claiming no evidence at all.
    cache, manifest, _ = _resolve(tmp_path, mapping={"2552450": None})
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    report, _ = _read(cache, "place_resolution_report.jsonl")
    assert "ov-nyc-county" not in {r["overture_id"] for r in resolved}
    entry = next(r for r in report if r["overture_id"] == "ov-nyc-county")
    assert entry["reason"] == "conflicting P402 identities"


def test_a_malformed_direct_wikidata_value_is_not_minted(tmp_path):
    row = fx.division(
        "ov-bad",
        "US",
        "region",
        wikidata="not-a-qid",
        name="Bad",
        admin_level=1,
        hierarchies=fx.chain(US, ("ov-bad", "region", "Bad")),
    )
    record = overture.normalize_division(row)
    assert record["wikidata"] is None
    assert overture.resolve_qid(record, {}) == (
        None,
        "name_country",
        "no wikidata property and no P402 match",
    )


def test_a_clean_qid_beside_an_ambiguous_relation_is_still_a_conflict():
    # One relation resolves cleanly, another is ambiguous: any conflict signal
    # leaves the division unresolved, never minted on the clean one.
    record = {"wikidata": None, "osm_relation_ids": ["100", "200"]}
    assert overture.resolve_qid(record, {"100": "Q5", "200": None}) == (
        None,
        "name_country",
        "conflicting P402 identities",
    )


def test_wikidata_client_parses_and_rejects_ambiguous(monkeypatch):
    import io as _io

    payload = {
        "results": {
            "bindings": [
                {
                    "osm": {"value": "10"},
                    "item": {"value": "http://www.wikidata.org/entity/Q10"},
                },
                # 20 maps to two items: ambiguous, minted for neither.
                {
                    "osm": {"value": "20"},
                    "item": {"value": "http://www.wikidata.org/entity/Q20"},
                },
                {
                    "osm": {"value": "20"},
                    "item": {"value": "http://www.wikidata.org/entity/Q21"},
                },
            ]
        }
    }

    def fake_urlopen(request, timeout=None):
        return _io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(overture.urllib.request, "urlopen", fake_urlopen)
    client = overture.WikidataClient()
    # 10 is unique; 20 is ambiguous (None, present as a conflict); 30 is absent.
    assert client.p402(["10", "20", "30"]) == {"10": "Q10", "20": None}


def test_a_division_is_a_place_without_a_qid_only_when_named_and_unresolved():
    unresolved = {"name": "X", "resolution_reason": overture.UNRESOLVED}
    assert overture.qidless_place(unresolved)
    assert not overture.qidless_place({**unresolved, "name": ""})
    assert not overture.qidless_place(
        {**unresolved, "resolution_reason": "conflicting P402 identities"}
    )


def test_a_nameless_division_is_reported_not_published(tmp_path):
    nameless = fx.division(
        "ov-nameless",
        "US",
        "localadmin",
        name="",
        admin_level=2,
        hierarchies=fx.chain(US, ("ov-nameless", "localadmin", "")),
    )
    cache = tmp_path / "cache"
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", ROWS + [nameless])
    manifest = overture.resolve(
        cache, dataset=dataset, wikidata=fx.StubWikidata({"2552450": "Q11299"})
    )
    resolved, _ = _read(cache, "overture_divisions.jsonl")
    report, _ = _read(cache, "place_resolution_report.jsonl")
    assert "ov-nameless" not in {r["overture_id"] for r in resolved}
    assert [(r["overture_id"], r["reason"]) for r in report] == [
        ("ov-nameless", overture.UNRESOLVED)
    ]
    assert manifest["reported"] == 1


def _bucket(root, releases):
    """A local stand-in for the Overture bucket: a directory per release,
    holding the division table when ``releases`` maps it to True; with no
    releases, not even ``release/`` exists."""
    from pyarrow import fs

    root.mkdir()
    for name, has_table in releases.items():
        path = root / overture.OVERTURE_BUCKET / "release" / name
        (path / "theme=divisions" / "type=division" if has_table else path).mkdir(
            parents=True
        )
    return fs.SubTreeFileSystem(str(root), fs.LocalFileSystem())


def test_the_newest_release_with_a_division_table_is_read(tmp_path, monkeypatch):
    bucket = _bucket(
        tmp_path / "bucket",
        {"2026-09-23.2": True, "2026-09-23.10": True, "2026-10-21.0": False, "a": True},
    )
    monkeypatch.setattr(overture, "s3_filesystem", lambda: bucket)
    monkeypatch.setattr(overture, "_release", None)
    # A day's numbered releases sort by number, not as text.
    assert overture.releases() == ["2026-09-23.2", "2026-09-23.10"]
    assert overture.current_release() == "2026-09-23.10"
    with pytest.raises(overture.GazetteerError, match="2026-08-19.0 is not in"):
        overture.overture_dataset("2026-08-19.0")
    empty = _bucket(tmp_path / "empty", {})
    monkeypatch.setattr(overture, "s3_filesystem", lambda: empty)
    with pytest.raises(overture.GazetteerError, match="no Overture release"):
        overture.latest_release()


def test_a_local_copy_is_downloaded_once_and_read_after(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    release = "2026-10-21.0"
    bucket = _bucket(tmp_path / "bucket", {release: True})
    theme = tmp_path / "bucket" / overture.OVERTURE_BUCKET / "release" / release
    for table, ids in (("division", ["a", "b"]), ("division_area", ["a"])):
        (theme / "theme=divisions" / f"type={table}").mkdir(parents=True, exist_ok=True)
        part = theme / "theme=divisions" / f"type={table}" / "part-0.parquet"
        pq.write_table(pa.table({"id": ids}), part)
    monkeypatch.setattr(overture, "s3_filesystem", lambda: bucket)
    overture.use_local_copy(tmp_path / "copies")
    assert overture.overture_dataset(release).to_table()["id"].to_pylist() == ["a", "b"]
    first = overture._local_copies[release]
    assert first.parent == tmp_path / "copies" / release
    assert (first / overture.LOCAL_COMPLETE).is_file()

    # Later reads, in this process or another, use the copy: the bucket is gone.
    def gone():
        raise AssertionError("the bucket was read again")

    monkeypatch.setattr(overture, "s3_filesystem", gone)
    overture.use_local_copy(tmp_path / "copies")
    area = overture.theme_dataset(
        "release/{release}/theme=divisions/type=division_area", release
    )
    assert area.to_table()["id"].to_pylist() == ["a"]
    with pytest.raises(overture.GazetteerError, match="not a table of the local"):
        overture.theme_dataset("release/{release}/theme=base/type=land", release)
    # A complete copy is found without the lock another build may hold.
    with monkeypatch.context() as patched:
        patched.setattr(overture, "_local_copy_lock", None)
        assert overture.ensure_local_copy(tmp_path / "copies", release) == first

    # A copy is complete only with exactly the files listed, no link below
    # it and a well-formed listing; any other copy is downloaded anew into a
    # folder of its own, the old one left as it was for a build reading it.
    def damage(copy, how):
        tables = copy / "theme=divisions"
        marker = copy / overture.LOCAL_COMPLETE
        if how == "size":
            (tables / "type=division" / "part-0.parquet").write_bytes(b"cut")
        elif how == "extra file":
            (tables / "type=division_area" / "extra.parquet").write_bytes(b"x")
        elif how == "listing without a table":
            listed = json.loads(marker.read_text())
            listed["files"] = {
                k: v for k, v in listed["files"].items() if "division_area" not in k
            }
            marker.write_text(json.dumps(listed))
        elif how == "linked listing":
            (tmp_path / "elsewhere.json").write_text(marker.read_text())
            marker.unlink()
            marker.symlink_to(tmp_path / "elsewhere.json")
        elif how == "listing not an object":
            marker.write_text("null")
        elif how == "deeply nested listing":
            marker.write_text("[" * 100_000 + "]" * 100_000)
        elif how == "link beside the tables":
            (tables / "type=other").symlink_to(tmp_path / "elsewhere.json")
        else:
            tables.rename(tmp_path / "outside")
            tables.symlink_to(tmp_path / "outside")

    try:
        (tmp_path / "probe").symlink_to(tmp_path)
        links = True
    except OSError:  # no symlinks on this platform
        links = False
    copy = first
    for how in (
        "size",
        "extra file",
        "listing without a table",
        "linked listing",
        "listing not an object",
        "deeply nested listing",
        "link beside the tables",
        "linked table folder",
    ):
        if "link" in how and not links:
            continue
        damage(copy, how)
        new = overture.ensure_local_copy(tmp_path / "copies", release, bucket)
        assert new != copy and new.parent == copy.parent, how
        table = new / "theme=divisions" / "type=division" / "part-0.parquet"
        assert pq.read_table(table)["id"].to_pylist() == ["a", "b"], how
        assert copy.exists(), how
        copy = new

    # A failure after current.json names the new copy leaves that copy in place.
    def failing_sync(directory):
        if directory.path == tmp_path / "copies" / release:
            raise OSError("sync failed")

    damage(copy, "size")
    with monkeypatch.context() as patched:
        patched.setattr(store.Directory, "sync", failing_sync)
        with pytest.raises(OSError, match="sync failed"):
            overture.ensure_local_copy(tmp_path / "copies", release, bucket)
    damaged, copy = copy, overture.ensure_local_copy(tmp_path / "copies", release)
    table = copy / "theme=divisions" / "type=division" / "part-0.parquet"
    assert copy != damaged and pq.read_table(table)["id"].to_pylist() == ["a", "b"]
    # A release the bucket lacks, a linked release or copies folder, a listing
    # over the marker bound or a file outside the tables is refused.
    with pytest.raises(overture.GazetteerError, match="2026-08-19.0 is not in"):
        overture.ensure_local_copy(tmp_path / "copies", "2026-08-19.0", bucket)
    if links:
        (tmp_path / "planted").mkdir()
        (tmp_path / "copies" / "2026-08-20.0").symlink_to(tmp_path / "planted")
        with pytest.raises(store.StoreError, match="symlink"):
            overture.ensure_local_copy(tmp_path / "copies", "2026-08-20.0", bucket)
        (tmp_path / "linked copies").symlink_to(tmp_path / "copies")
        with pytest.raises(store.StoreError, match="symlink"):
            overture.ensure_local_copy(tmp_path / "linked copies", release, bucket)
    limit = overture.LOCAL_MARKER_MAX
    monkeypatch.setattr(overture, "LOCAL_MARKER_MAX", 10)
    with pytest.raises(overture.GazetteerError, match="too many files"):
        overture.ensure_local_copy(tmp_path / "copies", release, bucket)
    monkeypatch.setattr(overture, "LOCAL_MARKER_MAX", limit)
    (theme / "theme=divisions" / "type=division" / "nested").mkdir()
    pq.write_table(
        pa.table({"id": ["c"]}),
        theme / "theme=divisions" / "type=division" / "nested" / "p.parquet",
    )
    damage(copy, "size")
    with pytest.raises(overture.GazetteerError, match="unexpected Overture file"):
        overture.ensure_local_copy(tmp_path / "copies", release, bucket)


@pytest.mark.parametrize(
    "case, downloaded",
    [
        ("as held", ["division_area"]),
        ("linked folder", ["division", "division_area"]),
        ("linked table", ["division", "division_area"]),
        ("changed while copied", ["division", "division_area"]),
    ],
)
def test_a_local_copy_takes_the_files_an_earlier_download_holds(
    tmp_path, monkeypatch, case, downloaded
):
    # An earlier download holds the division table at the listed size and the
    # area table at another: the first is copied, the second downloaded; a
    # link on the way or a file that changes while copied is downloaded too.
    import io
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    class Reads:
        """The bucket, recording the tables opened from it."""

        def __init__(self, filesystem):
            self.filesystem, self.opened = filesystem, []

        def get_file_info(self, selector):
            return self.filesystem.get_file_info(selector)

        def open_input_stream(self, path):
            self.opened.append(path.rsplit("type=", 1)[1].split("/")[0])
            return self.filesystem.open_input_stream(path)

    release = "2026-10-21.0"
    bucket = Reads(_bucket(tmp_path / "bucket", {release: True}))
    theme = tmp_path / "bucket" / overture.OVERTURE_BUCKET / "release" / release
    earlier = tmp_path / "earlier"
    for table, ids in (("division", ["a", "b"]), ("division_area", ["a"])):
        folder = f"theme=divisions/type={table}"
        (theme / folder).mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"id": ids}), theme / folder / "part-0.parquet")
        (earlier / folder).mkdir(parents=True)
        shutil.copy(theme / folder / "part-0.parquet", earlier / folder)
    area = earlier / "theme=divisions" / "type=division_area" / "part-0.parquet"
    area.write_bytes(b"another size")
    given = earlier
    try:
        if case == "linked folder":
            given = tmp_path / "linked"
            given.symlink_to(earlier)
        elif case == "linked table":
            table = earlier / "theme=divisions" / "type=division"
            table.rename(tmp_path / "moved")
            table.symlink_to(tmp_path / "moved")
    except OSError:  # no symlinks on this platform
        pytest.skip("this platform cannot create symlinks")
    if case == "changed while copied":
        monkeypatch.setattr(overture, "_earlier_file", lambda *a: io.BytesIO(b"cut"))
    copy = overture.ensure_local_copy(tmp_path / "copies", release, bucket, given)
    assert bucket.opened == downloaded
    table = copy / "theme=divisions" / "type=division" / "part-0.parquet"
    assert pq.read_table(table)["id"].to_pylist() == ["a", "b"]
    assert overture._complete(copy, release)


def test_a_build_waits_while_another_downloads(tmp_path, monkeypatch):
    from transitio_index import store

    real = store.exclusive_writer
    attempts = []

    def busy_once(directory, name):
        attempts.append(name)
        if len(attempts) == 1:
            raise store.LockHeld("another build is publishing to this cache")
        return real(directory, name)

    monkeypatch.setattr(store, "exclusive_writer", busy_once)
    monkeypatch.setattr(overture.time, "sleep", lambda seconds: None)
    with overture._local_copy_lock(tmp_path):
        pass
    assert attempts == [overture.LOCAL_LOCK, overture.LOCAL_LOCK]
    # Any other failure is reported at once: a lock the platform cannot take,
    # or a lock entry that is a link.
    monkeypatch.setattr(store, "exclusive_writer", real)

    def waited(seconds):
        raise AssertionError("waited on a lock that is not held")

    def no_locks(handle):
        raise OSError(errno.ENOLCK, "No locks available")

    monkeypatch.setattr(overture.time, "sleep", waited)
    with monkeypatch.context() as patched:
        patched.setattr(store, "_lock", no_locks)
        with pytest.raises(store.StoreError, match="No locks available"):
            with overture._local_copy_lock(tmp_path):
                pass
    try:
        (tmp_path / "planted" / overture.LOCAL_LOCK).parent.mkdir()
        (tmp_path / "planted" / overture.LOCAL_LOCK).symlink_to(tmp_path)
    except OSError:
        pytest.skip("symlinks unavailable")
    with pytest.raises(store.StoreError, match="symlink"):
        with overture._local_copy_lock(tmp_path / "planted"):
            pass


def test_the_build_reads_the_release_its_option_names(tmp_path, monkeypatch):
    from transitio_index import build

    seen = []
    monkeypatch.setitem(
        build.STAGES,
        "ingest",
        lambda arguments: seen.append(overture.current_release()) or [],
    )
    options = ["--stage", "ingest", "--cache-dir", str(tmp_path), "--overture-release"]
    assert build.main(options + ["2026-10-21.0"]) == 0
    assert seen == ["2026-10-21.0"]
    # Every stage reads a local copy unless the bucket is asked for.
    assert overture._local_root == build.DEFAULT_OVERTURE_DIR
    assert build.main(options + ["2026-10-21.0", "--overture-remote"]) == 0
    assert overture._local_root is None
    for value in ("latest", "2026-10-21", "2026-10-21.0/x"):
        with pytest.raises(SystemExit):
            build.parse_args(options + [value])
