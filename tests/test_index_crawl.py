import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import crawl, fetch, store  # noqa: E402

STOPS = b"stop_id,stop_lat,stop_lon\ns1,60.1,24.9\n"
ROUTES = b"route_id,route_type\nr1,3\n"
AGENCY = b"agency_id,agency_name\na1,Agency\n"
TRIPS = b"trip_id,route_id\nt1,r1\n"
STOP_TIMES = b"trip_id,stop_id,stop_sequence\n" + b"t1,s1,1\n" * 5000

FULL_MEMBERS = {
    "agency.txt": AGENCY,
    "routes.txt": ROUTES,
    "stops.txt": STOPS,
    "trips.txt": TRIPS,
    "stop_times.txt": STOP_TIMES,
}


def _zip_bytes(members=FULL_MEMBERS):
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return sink.getvalue()


def _server(feeds, *, honour_ranges=True):
    """A MockTransport serving ``{path: (data, etag)}`` with Range support."""

    def handler(request):
        entry = feeds.get(request.url.path)
        if entry is None:
            return httpx.Response(404)
        data, etag = entry
        headers = {"Content-Length": str(len(data))}
        if honour_ranges:
            headers["Accept-Ranges"] = "bytes"
        if etag:
            headers["ETag"] = etag
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        if etag and request.headers.get("If-None-Match") == etag:
            return httpx.Response(304)
        wanted = request.headers.get("Range")
        if wanted and honour_ranges:
            span = wanted.split("=", 1)[1]
            start_text, _, end_text = span.partition("-")
            start = int(start_text)
            end = int(end_text) + 1 if end_text else len(data)
            body = data[start:end]
            headers = dict(headers)
            headers["Content-Length"] = str(len(body))
            headers["Content-Range"] = f"bytes {start}-{end - 1}/{len(data)}"
            return httpx.Response(206, headers=headers, content=body)
        return httpx.Response(200, headers=headers, content=data)

    return httpx.MockTransport(handler)


def _feed(feed_id, url, *, crawlable=True, aliases=()):
    return {
        "feed_id": feed_id,
        "spec": "gtfs",
        "crawlable": crawlable,
        "aliases": list(aliases),
        "atlas": {"urls": {"static_current": url}} if url else None,
        "mdb": None,
    }


def _publish_resolved(cache, feeds):
    directory = store.open_subdir(cache, "resolve")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "resolve",
                "feeds_resolved.json",
                {"feeds_resolved.jsonl": store.jsonl_chunks(feeds)},
                {"source": "resolve", "sources": {"atlas": {"commit": "abc"}}},
                held=directory,
            )
    finally:
        directory.close()


def _fetcher(transport):
    return fetch.Fetcher(
        transport=transport, clock=lambda: 0.0, sleeper=lambda wait: None
    )


def _feed_dir(cache, feed_id):
    """The digest-keyed directory the crawl stage uses for a feed."""
    return cache / "crawl" / crawl._dir_name(feed_id)


def _crawl(cache, transport, *, range_threshold=10**9, lookup=None):
    with _fetcher(transport) as fetcher:
        summary = crawl.crawl(
            cache, fetcher=fetcher, range_threshold=range_threshold, lookup=lookup
        )
    log = store.parse_jsonl((cache / "crawl" / "crawl_log.jsonl").read_bytes())
    return summary, {record["feed_id"]: record for record in log}


ROUTES_FIXED = b"route_id,route_type\nr1,0\nr2,715\n"
CITY_RECORDS = [
    {"country": "AA", "kind": "city", "overture_id": "aa-city", "wikidata": "Q1"},
    {"country": "AA", "kind": "region", "overture_id": "aa-reg", "wikidata": "Q2"},
]


class StubLookup:
    """Answers divisions_at from a fixed table, keyed by stop longitude."""

    def __init__(self, by_x):
        self.by_x = by_x

    def ensure(self, boxes):
        return 0

    def divisions_at(self, x, y):
        return self.by_x.get(x, [])


def _members(routes=ROUTES_FIXED, stops=STOPS):
    members = dict(FULL_MEMBERS)
    members["routes.txt"] = routes
    members["stops.txt"] = stops
    return members


@pytest.mark.parametrize("range_threshold", [10**9, 1])
def test_a_single_city_fixed_tier_feed_skips_stop_times(tmp_path, range_threshold):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    data = _zip_bytes(_members())
    summary, log = _crawl(
        cache,
        _server({"/a.zip": (data, '"v1"')}),
        range_threshold=range_threshold,
        lookup=StubLookup({24.9: CITY_RECORDS}),
    )
    assert log["f-a"]["stop_times"] == "skipped"
    assert "stop_times.txt" not in log["f-a"]["members"]
    assert summary["stop_times_skipped"] == 1
    feed_dir = _feed_dir(cache, "f-a")
    assert not (feed_dir / "stop_times.txt").exists()
    state = json.loads((feed_dir / "state.json").read_text())
    assert state["stop_times"] == {"state": "skipped", "reason": None}


TWO_STOPS = b"stop_id,stop_lat,stop_lon\ns1,60.1,24.9\ns2,60.1,25.9\n"
OTHER_CITY = [
    {"country": "AA", "kind": "city", "overture_id": "aa-other", "wikidata": "Q3"}
]
OTHER_COUNTRY = [
    {"country": "BB", "kind": "city", "overture_id": "aa-city", "wikidata": "Q1"}
]


@pytest.mark.parametrize(
    ("members", "lookup", "reason"),
    [
        (
            _members(routes=ROUTES),
            StubLookup({24.9: CITY_RECORDS}),
            "route types need geography",
        ),
        (_members(), StubLookup({24.9: CITY_RECORDS[1:]}), "a stop matches no city"),
        (_members(), StubLookup({}), "a stop matches no division"),
        (
            _members(stops=TWO_STOPS),
            StubLookup({24.9: CITY_RECORDS, 25.9: OTHER_CITY}),
            "stops span cities",
        ),
        (
            _members(),
            StubLookup({24.9: CITY_RECORDS + OTHER_CITY}),
            "a stop matches several cities",
        ),
        (
            _members(stops=STOPS + b"sx,not-a-lat,24.9\n"),
            StubLookup({24.9: CITY_RECORDS}),
            "unparsable stop rows",
        ),
        (
            _members(stops=TWO_STOPS),
            StubLookup({24.9: CITY_RECORDS, 25.9: OTHER_COUNTRY}),
            "stops span countries",
        ),
    ],
)
def test_the_predicate_refuses_and_reads_complete(tmp_path, members, lookup, reason):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    summary, log = _crawl(
        cache, _server({"/a.zip": (_zip_bytes(members), '"v1"')}), lookup=lookup
    )
    assert log["f-a"]["stop_times"] == "complete"
    assert log["f-a"]["stop_times_reason"] == reason
    assert (_feed_dir(cache, "f-a") / "stop_times.txt").exists()
    assert summary["stop_times_skipped"] == 0


def test_without_memo_coverage_the_feed_reads_complete(tmp_path):
    # The default lookup is memo-only; with nothing covered the predicate
    # must fall back to the full read, never fail the feed.
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(_members()), '"v1"')}))
    assert log["f-a"]["stop_times"] == "complete"
    assert log["f-a"]["stop_times_reason"].startswith("predicate error")


def test_a_missing_member_is_absent_and_still_fulfils_a_recrawl(tmp_path):
    # "complete" must mean the member exists AND was read; and a forced read
    # that finds no member has read everything there is to read.
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    members = {k: v for k, v in FULL_MEMBERS.items() if k != "stop_times.txt"}
    server = _server({"/a.zip": (_zip_bytes(members), '"v1"')})
    _, log = _crawl(cache, server)
    assert log["f-a"]["stop_times"] == "absent"
    (cache / "recrawl_requests.jsonl").write_text(json.dumps({"feed_id": "f-a"}) + "\n")
    summary, log = _crawl(cache, server)
    assert log["f-a"]["stop_times"] == "absent"
    assert summary["recrawl_cleared"] == 1


def test_a_cached_skip_is_rejudged_against_the_current_lookup(tmp_path):
    # The archive is unchanged, but the boundary memo now shows a second
    # city: the cached whole-feed skip no longer holds, so the unchanged
    # feed is refetched and the complete member lands.
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    server = _server({"/a.zip": (_zip_bytes(_members()), '"v1"')})
    _crawl(cache, server, lookup=StubLookup({24.9: CITY_RECORDS}))
    assert not (_feed_dir(cache, "f-a") / "stop_times.txt").exists()
    summary, log = _crawl(
        cache, server, lookup=StubLookup({24.9: CITY_RECORDS + OTHER_CITY})
    )
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["stop_times"] == "complete"
    assert (_feed_dir(cache, "f-a") / "stop_times.txt").exists()
    # And a skip that still holds keeps the cheap not_modified path.
    _, log = _crawl(cache, server, lookup=StubLookup({24.9: CITY_RECORDS}))
    assert log["f-a"]["method"] == "not_modified"


def test_a_recrawl_request_under_an_old_id_still_forces_and_clears(tmp_path):
    # An identity override renames a feed but keeps the old id as an alias;
    # a request written before the rename must still force and clear.
    cache = tmp_path / "cache"
    _publish_resolved(
        cache, [_feed("f-new", "https://feeds.example/a.zip", aliases=["f-old"])]
    )
    server = _server({"/a.zip": (_zip_bytes(_members()), '"v1"')})
    lookup = StubLookup({24.9: CITY_RECORDS})
    _crawl(cache, server, lookup=lookup)
    assert not (_feed_dir(cache, "f-new") / "stop_times.txt").exists()
    (cache / "recrawl_requests.jsonl").write_text(
        json.dumps({"feed_id": "f-old"}) + "\n"
    )
    summary, log = _crawl(cache, server, lookup=lookup)
    assert log["f-new"]["stop_times"] == "complete"
    assert summary["recrawl_cleared"] == 1
    assert (cache / "recrawl_requests.jsonl").read_text() == ""


def test_a_recrawl_request_forces_the_complete_read(tmp_path):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    server = _server({"/a.zip": (_zip_bytes(_members()), '"v1"')})
    lookup = StubLookup({24.9: CITY_RECORDS})
    _crawl(cache, server, lookup=lookup)
    assert not (_feed_dir(cache, "f-a") / "stop_times.txt").exists()
    (cache / "recrawl_requests.jsonl").write_text(json.dumps({"feed_id": "f-a"}) + "\n")
    summary, log = _crawl(cache, server, lookup=lookup)
    assert log["f-a"]["stop_times"] == "complete"
    assert log["f-a"]["stop_times_reason"] == "recrawl requested"
    assert (_feed_dir(cache, "f-a") / "stop_times.txt").read_bytes() == STOP_TIMES
    assert summary["recrawl_cleared"] == 1


def test_a_small_feed_downloads_whole_and_extracts(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    summary, log = _crawl(cache, _server({"/a.zip": (data, '"v1"')}))
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["archive_sha256"] == hashlib.sha256(data).hexdigest()
    assert log["f-a"]["members"] == sorted(FULL_MEMBERS)
    feed_dir = _feed_dir(cache, "f-a")
    assert (feed_dir / "stops.txt").read_bytes() == STOPS
    assert (feed_dir / "stop_times.txt").read_bytes() == STOP_TIMES
    assert not (feed_dir / "feed.zip").exists()  # archive removed after extraction
    state = json.loads((feed_dir / "state.json").read_text())
    assert state["etag"] == '"v1"'
    assert summary["by_method"] == {"download": 1}


def test_a_large_feed_reads_members_through_ranges(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    summary, log = _crawl(cache, _server({"/a.zip": (data, '"v1"')}), range_threshold=1)
    assert log["f-a"]["method"] == "range"
    assert log["f-a"]["members"] == sorted(FULL_MEMBERS)
    assert (_feed_dir(cache, "f-a") / "stop_times.txt").read_bytes() == STOP_TIMES
    assert summary["bytes_fetched"] > 0


def test_a_validatorless_large_feed_downloads_rather_than_ranges(tmp_path):
    # Range reads span requests; with no ETag or Last-Modified to pin them,
    # the stage must not risk mixing archive versions.
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (data, None)}), range_threshold=1)
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["fallback_reason"] == "no validator to pin range reads"


def test_a_weak_etag_cannot_pin_range_reads(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (data, 'W/"v1"')}), range_threshold=1)
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["fallback_reason"] == "no validator to pin range reads"


def test_last_modified_alone_cannot_pin_range_reads(tmp_path):
    # A timestamp can stay identical across representations within its
    # one-second granularity, so it never pins multi-request reads.
    cache = tmp_path / "cache"
    data = _zip_bytes()
    stamp = "Mon, 01 Sep 2025 00:00:00 GMT"

    def handler(request):
        headers = {
            "Content-Length": str(len(data)),
            "Accept-Ranges": "bytes",
            "ETag": 'W/"v1"',
            "Last-Modified": stamp,
        }
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        return httpx.Response(200, headers=headers, content=data)

    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, httpx.MockTransport(handler), range_threshold=1)
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["fallback_reason"] == "no validator to pin range reads"


def test_a_url_change_refetches_despite_matching_validators(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, _server({"/a.zip": (data, '"v1"')}))
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/moved.zip")])
    _, log = _crawl(cache, _server({"/moved.zip": (data, '"v1"')}))
    assert log["f-a"]["method"] == "download"  # same ETag, different URL


def test_a_changed_etag_beats_an_unchanged_last_modified(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    stamp = "Mon, 01 Sep 2025 00:00:00 GMT"

    def server(etag):
        def handler(request):
            headers = {
                "Content-Length": str(len(data)),
                "ETag": etag,
                "Last-Modified": stamp,
            }
            if request.method == "HEAD":
                return httpx.Response(200, headers=headers)
            return httpx.Response(200, headers=headers, content=data)

        return httpx.MockTransport(handler)

    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server('"v1"'))
    _, log = _crawl(cache, server('"v2"'))
    assert log["f-a"]["method"] == "download"  # the ETag decides


def test_a_corrupted_cached_member_forces_a_refetch(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    server = _server({"/a.zip": (data, '"v1"')})
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server)
    stops = _feed_dir(cache, "f-a") / "stops.txt"
    stops.write_bytes(b"tampered\n")
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "download"  # digest mismatch, no 304 skip
    assert stops.read_bytes() == STOPS


def test_a_symlinked_cached_member_forces_a_refetch(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    server = _server({"/a.zip": (data, '"v1"')})
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server)
    stops = _feed_dir(cache, "f-a") / "stops.txt"
    aside = tmp_path / "aside.txt"
    aside.write_bytes(STOPS)  # same content: only the symlink is wrong
    stops.unlink()
    try:
        stops.symlink_to(aside)
    except OSError:
        pytest.skip("no symlink support here")
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "download"
    assert not (_feed_dir(cache, "f-a") / "stops.txt").is_symlink()


def test_an_encrypted_archive_fails_the_feed_not_the_run(tmp_path):
    cache = tmp_path / "cache"
    data = bytearray(_zip_bytes({"stops.txt": STOPS}))
    # Flip the encryption flag in both headers; zipfile raises at read time.
    for signature in (b"PK\x03\x04", b"PK\x01\x02"):
        position = data.find(signature)
        offset = 6 if signature == b"PK\x03\x04" else 8
        flags = int.from_bytes(
            data[position + offset : position + offset + 2], "little"
        )
        data[position + offset : position + offset + 2] = (flags | 1).to_bytes(
            2, "little"
        )
    _publish_resolved(
        cache,
        [
            _feed("f-enc", "https://feeds.example/enc.zip"),
            _feed("f-good", "https://feeds.example/a.zip"),
        ],
    )
    summary, log = _crawl(
        cache,
        _server({"/enc.zip": (bytes(data), None), "/a.zip": (_zip_bytes(), None)}),
    )
    assert log["f-enc"]["method"] == "failed"
    assert log["f-good"]["method"] == "download"
    assert not (_feed_dir(cache, "f-enc") / "feed.zip").exists()


def test_an_oversized_member_fails_without_leaving_the_archive(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    monkeypatch.setattr(crawl, "MAX_MEMBER_BYTES", 10)
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(), None)}))
    assert log["f-a"]["method"] == "failed"
    assert "ceiling" in log["f-a"]["fallback_reason"]
    assert not (_feed_dir(cache, "f-a") / "feed.zip").exists()


def test_a_member_dropped_upstream_is_pruned_locally(tmp_path):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, _server({"/a.zip": (_zip_bytes(), '"v1"')}))
    assert (_feed_dir(cache, "f-a") / "stop_times.txt").exists()
    slimmer = {k: v for k, v in FULL_MEMBERS.items() if k != "stop_times.txt"}
    _, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(slimmer), '"v2"')}))
    assert "stop_times.txt" not in log["f-a"]["members"]
    assert not (_feed_dir(cache, "f-a") / "stop_times.txt").exists()


def test_state_records_per_member_digests_on_both_paths(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    for threshold, expected_method in ((10**9, "download"), (1, "range")):
        sub = tmp_path / expected_method
        sub.mkdir()
        cache = sub / "cache"
        _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
        _, log = _crawl(
            cache, _server({"/a.zip": (data, '"v1"')}), range_threshold=threshold
        )
        assert log["f-a"]["method"] == expected_method
        state = json.loads((_feed_dir(cache, "f-a") / "state.json").read_text())
        assert state["member_sha256"]["stops.txt"] == hashlib.sha256(STOPS).hexdigest()


def test_a_range_hostile_server_falls_back_to_download(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(
        cache,
        _server({"/a.zip": (data, None)}, honour_ranges=False),
        range_threshold=1,
    )
    assert log["f-a"]["method"] == "download"
    assert log["f-a"]["fallback_reason"] == "no range support"
    assert (_feed_dir(cache, "f-a") / "stops.txt").read_bytes() == STOPS


def test_an_unchanged_feed_is_skipped_on_rerun(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    server = _server({"/a.zip": (data, '"v1"')})
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server)
    summary, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "not_modified"
    assert log["f-a"]["bytes_fetched"] == 0
    assert (_feed_dir(cache, "f-a") / "stops.txt").read_bytes() == STOPS
    assert summary["by_method"] == {"not_modified": 1}


def test_a_recrawl_request_bypasses_the_skip(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    server = _server({"/a.zip": (data, '"v1"')})
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server)
    (cache / "recrawl_requests.jsonl").write_text(
        json.dumps({"feed_id": "f-a", "reason": "selector needed"}) + "\n"
    )
    summary, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "download"  # fetched despite matching ETag
    assert summary["recrawl_requested"] == 1
    # The complete read fulfilled the request, so it is cleared — and only
    # then: the next run skips again on validators.
    assert summary["recrawl_cleared"] == 1
    assert (cache / "recrawl_requests.jsonl").read_text() == ""
    summary, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "not_modified"


def test_a_feed_directory_conflict_fails_the_feed_not_the_run(tmp_path):
    # A file squatting on the feed's directory name makes open_subdir fail;
    # the per-feed boundary must catch it and continue.
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(
        cache,
        [
            _feed("f-squat", "https://feeds.example/a.zip"),
            _feed("f-good", "https://feeds.example/a.zip"),
        ],
    )
    (cache / "crawl").mkdir(parents=True)
    _feed_dir(cache, "f-squat").write_text("not a directory")
    summary, log = _crawl(cache, _server({"/a.zip": (data, None)}))
    assert log["f-squat"]["method"] == "failed"
    assert log["f-good"]["method"] == "download"


def test_one_failing_feed_does_not_stop_the_run(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(
        cache,
        [
            _feed("f-bad", "https://feeds.example/missing.zip"),
            _feed("f-good", "https://feeds.example/a.zip"),
        ],
    )
    summary, log = _crawl(cache, _server({"/a.zip": (data, None)}))
    assert log["f-bad"]["method"] == "failed"
    assert log["f-good"]["method"] == "download"
    assert summary["by_method"] == {"failed": 1, "download": 1}


def test_uncrawlable_and_urlless_feeds_are_left_out_or_skipped(tmp_path):
    cache = tmp_path / "cache"
    _publish_resolved(
        cache,
        [
            _feed("f-rt", None, crawlable=False),
            _feed("f-nourl", None),
        ],
    )
    summary, log = _crawl(cache, _server({}))
    assert "f-rt" not in log  # not crawlable: never considered
    assert log["f-nourl"]["method"] == "skipped"
    assert log["f-nourl"]["fallback_reason"] == "no download URL"


def test_an_unsafe_feed_id_gets_a_hashed_directory(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    _publish_resolved(cache, [_feed("f/../evil", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (data, None)}))
    assert log["f/../evil"]["directory"].startswith("id-")
    assert (cache / "crawl" / log["f/../evil"]["directory"] / "stops.txt").exists()


def test_a_feed_without_stop_times_still_crawls(tmp_path):
    cache = tmp_path / "cache"
    members = {k: v for k, v in FULL_MEMBERS.items() if k != "stop_times.txt"}
    data = _zip_bytes(members)
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (data, None)}))
    assert log["f-a"]["method"] == "download"
    assert "stop_times.txt" not in log["f-a"]["members"]
    assert (_feed_dir(cache, "f-a") / "stops.txt").exists()


def test_the_mdb_url_is_the_fallback(tmp_path):
    cache = tmp_path / "cache"
    data = _zip_bytes()
    feed = {
        "feed_id": "f-m",
        "spec": "gtfs",
        "crawlable": True,
        "atlas": None,
        "mdb": {"urls": {"direct_download": "https://feeds.example/a.zip"}},
    }
    _publish_resolved(cache, [feed])
    _, log = _crawl(cache, _server({"/a.zip": (data, None)}))
    assert log["f-m"]["method"] == "download"
