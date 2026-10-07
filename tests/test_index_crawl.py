import hashlib
import io
import json
import logging
import os
import sys
import threading
import urllib.parse
import zipfile

import httpx
import pytest
from transitio.index import fingerprint

from transitio_index import crawl, fetch, keys, overrides, store  # noqa: E402

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
    """A MockTransport serving ``{path: (data, etag)}`` with Range support;
    ``data`` may instead be a status to answer or a transport error to raise."""

    def handler(request):
        entry = feeds.get(request.url.path)
        if entry is None:
            return httpx.Response(404)
        data, etag = entry
        if isinstance(data, int):
            return httpx.Response(data)
        if isinstance(data, type):
            raise data("stub failure", request=request)
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


def _publish_resolved(cache, feeds, overrides_dir=None):
    manifest = {"source": "resolve", "sources": {"atlas": {"commit": "abc"}}}
    if overrides_dir is not None:
        digest = overrides.access_providers_digest(overrides_dir)
        manifest["access_providers_sha256"] = digest
    directory = store.open_subdir(cache, "resolve")
    try:
        with store.exclusive_writer(directory):
            store.publish(
                cache / "resolve",
                "feeds_resolved.json",
                {"feeds_resolved.jsonl": store.jsonl_chunks(feeds)},
                manifest,
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


def _crawl(cache, transport, *, range_threshold=10**9, lookup=None, **options):
    with _fetcher(transport) as fetcher:
        summary = crawl.crawl(
            cache,
            fetcher=fetcher,
            range_threshold=range_threshold,
            lookup=lookup,
            **options,
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
        # A tram and a coach are both fixed-tier, but two tiers: per-tier
        # selectors need the complete read.
        (
            _members(routes=b"route_id,route_type\nr1,0\nr2,201\n"),
            StubLookup({24.9: CITY_RECORDS}),
            "route types span tiers",
        ),
        # 750 sits between the bus and trolleybus ranges: unknown to the
        # classifier, so no whole-feed claim may rest on it.
        (
            _members(routes=b"route_id,route_type\nr1,0\nr2,750\n"),
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


def test_malformed_utf8_in_stops_is_a_data_error():
    with pytest.raises(UnicodeDecodeError):
        crawl.stop_rows(b"stop_id,stop_lat,stop_lon\n\xff,1.0,2.0\n")


def test_reading_locks_the_crawl_root_even_before_any_crawl(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    assert crawl.states_digest(cache) is None
    with crawl.reading(cache):
        assert (cache / "crawl").is_dir()
        assert crawl.states_digest(cache) is None  # a directory, but no log
        directory = store.open_subdir(cache, "crawl")
        try:
            with pytest.raises(store.StoreError):
                with store.exclusive_writer(directory):
                    pass
        finally:
            directory.close()


def test_a_corrupt_log_line_or_state_shape_skips_only_that_feed(tmp_path):
    cache = tmp_path / "cache"
    good = cache / "crawl" / crawl._dir_name("f-good")
    bad = cache / "crawl" / crawl._dir_name("f-bad")
    for feed_dir, stop_times in ((good, {"state": "complete"}), (bad, "complete")):
        feed_dir.mkdir(parents=True)
        (feed_dir / "state.json").write_text(
            json.dumps({"members": ["stops.txt"], "stop_times": stop_times})
        )
    (cache / "crawl" / "crawl_log.jsonl").write_text(
        "not json\n"
        + json.dumps({"directory": good.name})
        + "\n"
        + json.dumps({"directory": bad.name})
        + "\n"
    )
    assert [d for d, _ in crawl.crawled_feeds(cache)] == [good]


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
    monkeypatch.setattr(crawl, "DOWNLOAD_MEMBER_BYTES", 10)
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(), None)}))
    assert log["f-a"]["method"] == "failed"
    assert "ceiling" in log["f-a"]["fallback_reason"]
    assert not (_feed_dir(cache, "f-a") / "feed.zip").exists()


def test_a_member_over_the_ranged_buffer_downloads_whole(tmp_path, monkeypatch):
    # The ranged reader buffers a member in memory, so one over its ceiling
    # is left to the whole download, which streams it.
    cache = tmp_path / "cache"
    monkeypatch.setattr(crawl, "RANGED_MEMBER_BYTES", 2000)  # stop_times alone is over
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _, log = _crawl(
        cache, _server({"/a.zip": (_zip_bytes(), '"v1"')}), range_threshold=1
    )
    assert log["f-a"]["method"] == "download"
    assert "stop_times.txt" in log["f-a"]["fallback_reason"]
    assert "ceiling" in log["f-a"]["fallback_reason"]
    assert log["f-a"]["stop_times"] == "complete"


def test_members_under_one_folder_are_read_from_it(tmp_path):
    # GitHub source archives and some publishers put the files under a folder;
    # a fragment names the folder when the archive has several.
    cache = tmp_path / "cache"
    under = {f"gtfs/{name}": data for name, data in FULL_MEMBERS.items()}
    two = {f"a/{name}": data for name, data in FULL_MEMBERS.items()}
    two["b/readme.txt"] = b"x"
    server = _server(
        {
            "/ranged.zip": (_zip_bytes(under), '"v1"'),
            "/whole.zip": (_zip_bytes(under), None),
            "/two.zip": (_zip_bytes(two), None),
        }
    )
    _publish_resolved(
        cache,
        [
            _feed("f-rg", "https://feeds.example/ranged.zip"),
            _feed("f-dl", "https://feeds.example/whole.zip"),
            _feed("f-frag", "https://feeds.example/two.zip#a"),
            _feed("f-enc", "https://feeds.example/two.zip#%61%2F"),  # "a/", decoded
        ],
    )
    _, log = _crawl(cache, server, range_threshold=1)
    assert log["f-rg"]["method"] == "range"
    assert log["f-dl"]["method"] == "download"
    assert log["f-frag"]["method"] == "download"
    for feed_id in ("f-rg", "f-dl", "f-frag", "f-enc"):
        assert log[feed_id]["members"] == sorted(FULL_MEMBERS), feed_id
        assert log[feed_id]["files"] == sorted(FULL_MEMBERS), feed_id
        assert (_feed_dir(cache, feed_id) / "stops.txt").read_bytes() == STOPS


def test_an_archive_fragment_names_the_inner_zip(tmp_path):
    cache = tmp_path / "cache"
    outer = _zip_bytes(
        {
            "7/google_transit.zip": _zip_bytes(),
            "8/other.zip": _zip_bytes({"agency.txt": AGENCY}),
        }
    )
    server = _server({"/gtfs.zip": (outer, '"v1"')})
    _publish_resolved(
        cache,
        [
            _feed("f-in", "https://feeds.example/gtfs.zip#7/google_transit.zip"),
            _feed("f-miss", "https://feeds.example/gtfs.zip#9/none.zip"),
            _feed("f-slash", "https://feeds.example/gtfs.zip#/7/google_transit.zip"),
        ],
    )
    _, log = _crawl(cache, server, range_threshold=1)
    assert log["f-in"]["method"] == "download"  # never ranged: the inner zip is inside
    assert log["f-in"]["fallback_reason"] == "nested archive"
    assert log["f-in"]["members"] == sorted(FULL_MEMBERS)
    left = {p.name for p in _feed_dir(cache, "f-in").iterdir()}
    assert left == set(FULL_MEMBERS) | {"state.json"}  # no inner zip or temporary
    assert log["f-miss"]["method"] == "failed"
    assert "9/none.zip" in log["f-miss"]["fallback_reason"]
    # A fragment with a leading slash is ignored: the outer root has no members.
    assert log["f-slash"]["method"] == "range" and log["f-slash"]["members"] == []


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
        assert state["identity"] == fingerprint.identity(io.BytesIO(data))
        assert state["identity_version"] == fingerprint.IDENTITY_VERSION


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
    # A state written before identities were recorded gets one on the rerun,
    # over its recorded members only: an unreadable leftover table is removed
    # rather than read. One from before the hosted copy was read is the
    # producer's.
    path = _feed_dir(cache, "f-a") / "state.json"
    state = json.loads(path.read_text())
    identity = state.pop("identity")
    for key in ("identity_version", "fetched_from", "producer_failure"):
        del state[key]
    path.write_text(json.dumps(state))
    (_feed_dir(cache, "f-a") / "calendar.txt").write_bytes(b"service_id\n\xff\n")
    try:  # and a broken symlink under a member name, where links can be made
        (_feed_dir(cache, "f-a") / "calendar_dates.txt").symlink_to(tmp_path / "gone")
    except OSError:
        pass
    summary, log = _crawl(cache, server)
    state = json.loads(path.read_text())
    assert state["identity"] == identity
    assert (state["fetched_from"], state["producer_failure"]) == ("producer", None)
    assert log["f-a"]["method"] == "not_modified"
    assert log["f-a"]["bytes_fetched"] == 0
    assert (_feed_dir(cache, "f-a") / "stops.txt").read_bytes() == STOPS
    assert summary["by_method"] == {"not_modified": 1}


@pytest.mark.parametrize(
    "edit",
    [
        # Before the calendar files joined the member set, a state could not
        # say whether the feed lacks them or was never asked.
        pytest.param(lambda s: s.pop("members_requested"), id="smaller-member-set"),
        # Before the sizes were recorded, or with one malformed.
        pytest.param(lambda s: s.pop("member_bytes"), id="no-member-bytes"),
        pytest.param(lambda s: s.update(archive_bytes="12"), id="archive-bytes-text"),
        pytest.param(lambda s: s.update(empty_files=None), id="no-empty-files"),
        pytest.param(lambda s: s["member_bytes"].pop("stops.txt"), id="file-unsized"),
    ],
)
def test_a_legacy_state_is_refetched_once(tmp_path, edit):
    # One refetch settles it, after which validators skip again.
    cache = tmp_path / "cache"
    data = _zip_bytes()
    server = _server({"/a.zip": (data, '"v1"')})
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    _crawl(cache, server)
    state_path = _feed_dir(cache, "f-a") / "state.json"
    state = json.loads(state_path.read_text())
    edit(state)
    state_path.write_text(json.dumps(state))
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "download"
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "not_modified"


def test_a_stale_skip_is_corrected_on_the_next_build(tmp_path):
    # The plan's two-build correction path. Build one: the crawl's memo
    # shows one city, so stop_times is skipped; classification, seeing two
    # cities, finds the whole-feed claim stale and requests a recrawl.
    # Build two: the request bypasses the unchanged validators, the
    # complete read clears it, and classification builds the selectors.
    from test_index_classify import _candidate, _coverage, _records

    from transitio_index import classify

    cache = tmp_path / "cache"
    members = _members(
        routes=b"route_id,route_type\ntram,0\n",
        stops=b"stop_id,stop_lat,stop_lon\ns1,60.1,24.9\ns2,60.1,25.9\n",
    )
    members["trips.txt"] = b"trip_id,route_id\nt,tram\n"
    members["stop_times.txt"] = b"trip_id,stop_id,stop_sequence\nt,s1,1\nt,s2,2\n"
    server = _server({"/a.zip": (_zip_bytes(members), '"v1"')})
    crawl_lookup = StubLookup({24.9: _records("Q-city"), 25.9: _records("Q-city")})
    stage_lookup = StubLookup({24.9: _records("Q-city"), 25.9: _records("Q-other")})
    feeds = [
        {"feed_id": "f-a", "spec": "gtfs", "coverage_source": "crawl", "aliases": []}
    ]
    candidates = [_candidate("Q-city", "f-a"), _candidate("Q-other", "f-a")]
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])

    _, log = _crawl(cache, server, lookup=crawl_lookup)
    assert log["f-a"]["stop_times"] == "skipped"
    _coverage(cache, feeds, candidates)
    manifest = classify.classify(cache, lookup=stage_lookup)
    assert manifest["feeds_by_status"] == {"skip_stale": 1}
    assert manifest["recrawl_requested"] == 1

    summary, log = _crawl(cache, server, lookup=crawl_lookup)
    assert log["f-a"]["method"] == "download"  # past the matching ETag
    assert log["f-a"]["stop_times"] == "complete"
    assert summary["recrawl_cleared"] == 1
    assert (cache / "recrawl_requests.jsonl").read_text() == ""
    _coverage(cache, feeds, candidates)
    manifest = classify.classify(cache, lookup=stage_lookup)
    edges, _ = store.read_jsonl(cache / "classify", "edges.json", "edges.jsonl")
    assert manifest["edges_by_selector_state"] == {"complete": 2}
    assert manifest["recrawl_requested"] == 0
    assert {e["place_id"]: e["selector"] for e in edges} == {
        "Q-city": {"route_id": ["tram"]},
        "Q-other": {"route_id": ["tram"]},
    }


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


HTML = b"\n<!DOCTYPE html>\n<html><body>This file has moved.</body></html>\n"
PRODUCER_URL = "https://producer.example/a.zip"
HOSTED = "https://files.example/mdb-1/latest.zip"


@pytest.mark.parametrize(
    ("access", "producer", "hosted", "method", "fetched_from", "failure"),
    [
        ("open", httpx.ConnectTimeout, HOSTED, "download", "mdb_latest", "stub"),
        ("open", None, None, "failed", None, None),  # a 404 with no hosted copy
        ("open", 403, HOSTED, "download", "mdb_latest", "HTTP 403"),
        ("open", 503, HOSTED, "download", "mdb_latest", "HTTP 503"),
        ("open", HTML, HOSTED, "download", "mdb_latest", "an HTML page"),
        # A body that is neither the archive nor a page fails the read too.
        ("open", b"\x08\x01\x12\x04data", HOSTED, "download", "mdb_latest", "zip"),
        ("open", _zip_bytes(), HOSTED, "download", "producer", None),
        # A feed flagged as needing a key: its URL is read without one first.
        ("key", _zip_bytes(), HOSTED, "download", "producer", None),
    ],
    ids=[
        "timeout",
        "404-no-hosted-copy",
        "403",
        "5xx",
        "html",
        "not-an-archive",
        "producer-ok",
        "key-served-without-one",
    ],
)
def test_a_dead_producer_link_falls_back_to_the_hosted_copy(
    tmp_path, access, producer, hosted, method, fetched_from, failure
):
    cache = tmp_path / "cache"
    feed = dict(_feed("f-a", PRODUCER_URL), access=access)
    feed["mdb"] = {"urls": {"latest": hosted}}
    _publish_resolved(cache, [feed])
    served = {"/mdb-1/latest.zip": (_zip_bytes(), '"h1"')}
    if producer is not None:
        served["/a.zip"] = (producer, '"p1"')
    _, log = _crawl(cache, _server(served))
    record = log["f-a"]
    assert (record["method"], record["fetched_from"]) == (method, fetched_from)
    assert record["url"] == PRODUCER_URL
    assert (record["producer_failure"] is None) == (failure is None)
    assert failure is None or failure in record["producer_failure"]
    # Both attempts' bytes count.
    producer_bytes = len(producer) if isinstance(producer, bytes) else 0
    hosted_bytes = len(_zip_bytes()) if fetched_from == "mdb_latest" else 0
    assert record["bytes_fetched"] == producer_bytes + hosted_bytes
    # The committed state is what coverage reads, whichever copy was read.
    states = {state["feed_id"]: state for _, state in crawl.crawled_feeds(cache)}
    if method == "failed":
        assert states == {}
    else:
        state = states["f-a"]
        assert state["fetched_from"] == fetched_from
        assert state["producer_failure"] == record["producer_failure"]
        assert state["url"] == (
            HOSTED if fetched_from == "mdb_latest" else PRODUCER_URL
        )
    # A rerun reads the same copy, and its validators spare the download.
    _, rerun = _crawl(cache, _server(served))
    expected = "failed" if method == "failed" else "not_modified"
    assert (rerun["f-a"]["method"], rerun["f-a"]["fetched_from"]) == (
        expected,
        fetched_from,
    )


def test_the_crawlers_own_limit_is_not_retried_from_the_hosted_copy(
    tmp_path, monkeypatch
):
    # A member over the crawler's byte ceiling fails the read on this side:
    # the hosted copy would hit the same ceiling, so it is never requested.
    monkeypatch.setattr(crawl, "DOWNLOAD_MEMBER_BYTES", 1)
    cache = tmp_path / "cache"
    feed = _feed("f-a", PRODUCER_URL)
    feed["mdb"] = {"urls": {"latest": HOSTED}}
    _publish_resolved(cache, [feed])
    served = {
        "/a.zip": (_zip_bytes(), '"p1"'),
        "/mdb-1/latest.zip": (_zip_bytes(), '"h1"'),
    }
    inner, paths = _server(served), []
    transport = httpx.MockTransport(
        lambda request: paths.append(request.url.path) or inner.handler(request)
    )
    _, log = _crawl(cache, transport)
    record = log["f-a"]
    assert (record["method"], record["fetched_from"]) == ("failed", None)
    assert "member ceiling" in record["fallback_reason"]
    assert "/mdb-1/latest.zip" not in paths


@pytest.mark.parametrize(
    ("content_type", "body", "html"),
    [
        (None, HTML, True),
        ("text/html; charset=utf-8", b"This file has moved.", True),
        (None, b"\xef\xbb\xbf <!-- portal -->\n<head><title>Moved</title>", True),
        ("application/octet-stream", b"\x08\x01\x12\x04html", False),
        (None, b"<?xml version='1.0'?><Error><Code>NoSuchKey</Code></Error>", False),
    ],
    ids=["doctype", "served-as-html", "no-root-tag", "binary", "xml"],
)
def test_an_html_page_is_told_from_other_non_archives(
    tmp_path, content_type, body, html
):
    feed_dir = store.open_subdir(tmp_path, "crawl")
    try:
        store.write_bytes(feed_dir, crawl.ARCHIVE_FILE, body)
        assert crawl._html_page(feed_dir, content_type) is html
    finally:
        feed_dir.close()


# --- Parallel crawl (workers) -------------------------------------------------


def _freeze_clock(monkeypatch):
    # state.json records a wall-clock retrieved_at; freeze it so a parallel and
    # a sequential build produce byte-identical artifacts.
    dt = crawl.datetime
    fixed = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

    class _Frozen(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(dt, "datetime", _Frozen)


def _crawl_workers(cache, transport, feeds, *, workers):
    _publish_resolved(cache, feeds)
    with _fetcher(transport) as fetcher:
        crawl.crawl(cache, fetcher=fetcher, range_threshold=10**9, workers=workers)


def _tree(root):
    """Every file under ``root`` as ``{relative posix path: bytes}``."""
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_parallel_crawl_is_byte_identical_and_ordered(tmp_path, monkeypatch):
    _freeze_clock(monkeypatch)
    # Feeds across several hosts, two sharing one host so a shared bucket is
    # exercised too.
    urls = {
        "f-0": "https://h0.example/a.zip",
        "f-1": "https://h1.example/b.zip",
        "f-2": "https://h0.example/c.zip",
        "f-3": "https://h2.example/d.zip",
        "f-4": "https://h1.example/e.zip",
    }
    feeds = [_feed(fid, url) for fid, url in urls.items()]
    served = {
        "/" + url.rsplit("/", 1)[1]: (_zip_bytes(), fid) for fid, url in urls.items()
    }
    seq, par = tmp_path / "seq", tmp_path / "par"
    _crawl_workers(seq, _server(served), feeds, workers=1)
    _crawl_workers(par, _server(served), feeds, workers=8)

    # The whole crawl tree — every file, both directions — is byte-identical,
    # so a parallel run can neither drop, add nor alter any persisted artifact.
    assert _tree(seq / "crawl") == _tree(par / "crawl")
    order = [
        record["feed_id"]
        for record in store.parse_jsonl((seq / "crawl" / crawl.LOG_FILE).read_bytes())
    ]
    assert order == [feed["feed_id"] for feed in feeds]  # eligible-input order


@pytest.mark.parametrize("workers", [1, 4])
def test_a_worker_exception_is_contained_at_its_ordinal(tmp_path, monkeypatch, workers):
    feeds = [_feed(f"f-{i}", f"https://h{i}.example/{i}.zip") for i in range(3)]
    served = {f"/{i}.zip": (_zip_bytes(), None) for i in range(3)}
    real = crawl._crawl_one

    def boom(fetcher, cache_dir, feed, **kwargs):
        if feed["feed_id"] == "f-1":
            raise RuntimeError("kaboom")
        return real(fetcher, cache_dir, feed, **kwargs)

    monkeypatch.setattr(crawl, "_crawl_one", boom)
    cache = tmp_path / "c"
    _crawl_workers(cache, _server(served), feeds, workers=workers)

    log = store.parse_jsonl((cache / "crawl" / crawl.LOG_FILE).read_bytes())
    assert [record["feed_id"] for record in log] == ["f-0", "f-1", "f-2"]
    assert log[1]["method"] == "skipped" and "kaboom" in log[1]["fallback_reason"]
    assert log[1]["files"] == []  # the fallback record carries every field
    assert log[0]["method"] in ("range", "download")
    assert log[2]["method"] in ("range", "download")


def test_workers_crawl_feeds_concurrently(tmp_path):
    # Two feeds on different hosts whose HEAD blocks on a shared 2-party barrier:
    # only if both are in flight at once does it release. A serial run trips the
    # timeout, and ``broke`` proves that — the crawler's whole-GET fallback would
    # otherwise let both feeds "succeed" even when they never overlapped.
    barrier = threading.Barrier(2, timeout=5)
    broke = threading.Event()
    data = _zip_bytes()

    def handler(request):
        if request.method == "HEAD":
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                broke.set()
        headers = {"Content-Length": str(len(data)), "Accept-Ranges": "bytes"}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        return httpx.Response(200, headers=headers, content=data)

    feeds = [
        _feed("f-0", "https://h0.example/a.zip"),
        _feed("f-1", "https://h1.example/b.zip"),
    ]
    cache = tmp_path / "c"
    _crawl_workers(cache, httpx.MockTransport(handler), feeds, workers=2)
    assert not broke.is_set()  # both HEADs met the barrier: they ran at once
    log = store.parse_jsonl((cache / "crawl" / crawl.LOG_FILE).read_bytes())
    assert all(record["method"] in ("range", "download") for record in log)


# --- File manifest (schema 5) -------------------------------------------------

EXTRA_MEMBERS = {
    **FULL_MEMBERS,
    "shapes.txt": b"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n",
    "fare_attributes.txt": b"fare_id,price,currency_type,payment_method,transfers\n",
    "sub/": b"",  # an explicit directory marker: excluded
    "sub/nested.txt": b"x\n",  # a subfolder entry: also excluded
}


@pytest.mark.parametrize("range_threshold", [10**9, 1])
def test_the_crawl_records_the_full_file_manifest(tmp_path, range_threshold):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    data = _zip_bytes(EXTRA_MEMBERS)
    _, log = _crawl(
        cache, _server({"/a.zip": (data, '"v1"')}), range_threshold=range_threshold
    )
    files = log["f-a"]["files"]
    # Every root file — including the non-evidence shapes and fares — sorted,
    # with the directory marker and the subfolder entry both excluded.
    assert "shapes.txt" in files and "fare_attributes.txt" in files
    assert "sub/" not in files and "sub/nested.txt" not in files
    assert "sub" not in files and "nested.txt" not in files
    assert files == sorted(files)
    # ``members`` stays the extracted evidence subset; the manifest is a superset.
    assert set(log["f-a"]["members"]) <= set(crawl.MEMBERS)
    assert "shapes.txt" not in log["f-a"]["members"]
    assert set(log["f-a"]["members"]) <= set(files)
    # state.json carries the same manifest.
    state = json.loads((_feed_dir(cache, "f-a") / "state.json").read_text())
    assert state["files"] == files


def test_the_manifest_is_carried_and_backfilled(tmp_path):
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    server = _server({"/a.zip": (_zip_bytes(EXTRA_MEMBERS), '"v1"')})
    _crawl(cache, server)  # first crawl records the manifest
    state_path = _feed_dir(cache, "f-a") / "state.json"

    # An unchanged recrawl reuses the cache and keeps the manifest.
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "not_modified"
    assert "shapes.txt" in log["f-a"]["files"]

    # Legacy state predating the manifest (no ``files`` key) is re-fetched once
    # rather than carrying an empty manifest forward.
    state = json.loads(state_path.read_text())
    del state["files"]
    state_path.write_text(json.dumps(state))
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] in ("range", "download")
    assert "shapes.txt" in log["f-a"]["files"]

    # A genuine empty manifest keeps its key and still reuses the cache.
    state = json.loads(state_path.read_text())
    state["files"] = []
    state_path.write_text(json.dumps(state))
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] == "not_modified"
    assert log["f-a"]["files"] == []

    # A corrupt (non-list) manifest is treated like legacy state and re-fetched,
    # never carried forward as garbage.
    state = json.loads(state_path.read_text())
    state["files"] = "shapes.txt"
    state_path.write_text(json.dumps(state))
    _, log = _crawl(cache, server)
    assert log["f-a"]["method"] in ("range", "download")
    assert "shapes.txt" in log["f-a"]["files"]


def test_root_files_keeps_only_printable_ascii_root_names():
    manifest = crawl._root_files(
        ["shapes.txt", "shapes.txt", "sub/x.txt", "sub/", "café.txt", "b\x00d.txt", ""]
    )
    # Duplicate collapsed; subfolder, marker, non-ascii and NUL all dropped.
    assert manifest == ["shapes.txt"]


def test_manifest_list_rejects_non_list_and_mixed_types():
    assert crawl._manifest_list(["shapes.txt", "sub/x.txt"]) == ["shapes.txt"]
    assert crawl._manifest_list([]) == []
    assert crawl._manifest_list("shapes.txt") is None  # a string is not a manifest
    assert crawl._manifest_list(["shapes.txt", 1]) is None  # mixed types re-fetch
    assert crawl._manifest_list(None) is None


SIZED_MEMBERS = {
    **FULL_MEMBERS,
    "calendar.txt": b"service_id,monday,start_date,end_date\n",  # a member
    "shapes.txt": b"shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n",
    "fare_rules.txt": b"fare_id,route_id\nf1,r1\n",
    "feed_info.txt": b"",
    "translations.txt": b"table_name,field_name," + b"x" * 70 * 1024 + b"\n",
    "attributions.txt": b"attribution_id,organization_name\n",  # unreadable below
    "notes.md": b"# notes\n",
}


@pytest.mark.parametrize("range_threshold", [10**9, 1])
def test_the_crawl_records_the_archive_and_file_sizes(
    tmp_path, monkeypatch, range_threshold
):
    real = crawl._file_record

    def unreadable(names, size_of, read):
        def failing(name):
            if name == "attributions.txt":
                raise NotImplementedError("compression type 99")
            return read(name)

        return real(names, size_of, failing)

    monkeypatch.setattr(crawl, "_file_record", unreadable)
    cache = tmp_path / "cache"
    _publish_resolved(cache, [_feed("f-a", "https://feeds.example/a.zip")])
    data = _zip_bytes(SIZED_MEMBERS)
    _, log = _crawl(
        cache, _server({"/a.zip": (data, '"v1"')}), range_threshold=range_threshold
    )
    assert log["f-a"]["method"] == ("range" if range_threshold == 1 else "download")
    state = json.loads((_feed_dir(cache, "f-a") / "state.json").read_text())
    assert state["archive_bytes"] == len(data)
    assert state["files"] == sorted(SIZED_MEMBERS)
    assert state["member_bytes"] == {k: len(v) for k, v in SIZED_MEMBERS.items()}
    # With a row, over the probe, not a .txt or unreadable: never empty.
    assert state["empty_files"] == ["calendar.txt", "feed_info.txt", "shapes.txt"]


def test_the_crawl_log_records_each_feed_source(tmp_path):
    # A crawled feed carries its catalogue source (mdb/atlas/both/systems_csv)
    # into the crawl log, so provenance — e.g. which feeds come via the
    # Transitland Atlas — is answerable from the log alone.
    cache = tmp_path / "cache"
    feed = _feed("f-a", "https://feeds.example/a.zip")
    feed["source"] = "atlas"
    _publish_resolved(cache, [feed])
    _, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(_members()), '"v1"')}))
    assert log["f-a"]["source"] == "atlas"


def test_the_crawl_log_records_source_even_when_a_feed_errors(tmp_path, monkeypatch):
    # The unexpected-error fallback record must carry source too, so provenance
    # is present on every crawl-log path, not just the successful one.
    cache = tmp_path / "cache"
    feed = _feed("f-a", "https://feeds.example/a.zip")
    feed.update(source="atlas", access="key")
    _publish_resolved(cache, [feed])

    def boom(*args, **kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(crawl, "_crawl_one", boom)
    summary, log = _crawl(cache, _server({"/a.zip": (_zip_bytes(_members()), '"v1"')}))
    assert log["f-a"]["method"] == "skipped" and log["f-a"]["key_crawl"] == "failed"
    assert log["f-a"]["source"] == "atlas" and summary["by_key_crawl"] == {"failed": 1}


@pytest.mark.parametrize(
    "data, rows",
    [
        pytest.param(
            b"id,end_date   \nr1,20261011   \n",
            [{"id": "r1", "end_date": "20261011   "}],
            id="trailing",
        ),
        pytest.param(
            b"id, name, type\nr1,a,3\n",
            [{"id": "r1", "name": "a", "type": "3"}],
            id="leading",
        ),
        pytest.param(b"\xef\xbb\xbf id ,x\n1,2\n", [{"id": "1", "x": "2"}], id="bom"),
        pytest.param(b"a, a\n1,2\n", [{"a": "1", "": "2"}], id="repeated"),
        pytest.param(b"id,name\n", [], id="header-only"),
    ],
)
def test_member_rows_trims_header_names_and_keeps_values(data, rows):
    opened = io.BytesIO(data)
    assert list(crawl.member_rows(opened)) == rows
    opened.seek(0)


@pytest.mark.parametrize(
    "data, has_rows",
    [
        pytest.param(b"shape_id,shape_pt_lat\n", False, id="header"),
        pytest.param(b"shape_id,shape_pt_lat\r\n\r\n", False, id="blank-lines"),
        pytest.param(b"\xef\xbb\xbfshape_id,shape_pt_lat\n", False, id="bom"),
        pytest.param(b"", False, id="zero-bytes"),
        pytest.param(b"shape_id,shape_pt_lat\ns1,60.1\n", True, id="row"),
        pytest.param(b"shape_id\n\xff\n", True, id="invalid-utf8"),
    ],
)
def test_has_rows_reads_a_member_as_the_csv_reader_does(data, has_rows):
    assert crawl._has_rows(data) is has_rows


# --- Crawl with the maintainer's keys -----------------------------------------

SENTINEL = "k3y/S3ntinel"
ENCODED = urllib.parse.quote(SENTINEL, safe="")
A, B = "https://a.example", "https://b.example"
FEED = A + "/feed.zip"
NO_FILE = pytest.mark.skipif(sys.platform == "win32", reason="no file on Windows")
APPROVED = dict(name="P", registration_url="https://r.ex/", credential_fields=["key"])
APPROVED.update(crawl_approved=True, terms_checked="2026-10-02", terms_note="Allowed.")
APPROVED["url_prefixes"] = [A + "/"]


@pytest.fixture(autouse=True)
def _no_maintainer_keys(tmp_path, monkeypatch):
    """No test reads the maintainer's own credentials or request tally."""
    for name in [n for n in os.environ if n.startswith("TRANSITIO_KEY_")]:
        monkeypatch.delenv(name)
    monkeypatch.setenv(keys.USAGE, str(tmp_path / "state" / "key_requests.json"))
    if sys.platform != "win32":
        path = tmp_path / "credentials.toml"
        path.write_text("")
        path.chmod(0o600)
        monkeypatch.setenv(keys.CREDENTIALS, str(path))


def _keyed(feed_id, url=FEED, provider="p", method="query_param", params=None):
    access = {"access": "key", "access_provider": provider, "auth_method": method}
    return _feed(feed_id, url) | access | {"auth_params": params or {"key": "key"}}


def _providers(tmp_path, **entries):
    """An overrides directory approving each provider, with its changes."""
    directory = tmp_path / "overrides"
    directory.mkdir(exist_ok=True)
    rows = [{"provider_id": p, **APPROVED, **changes} for p, changes in entries.items()]
    (directory / overrides.ACCESS_PROVIDERS_FILE).write_text(json.dumps(rows))
    return directory


def _carries_key(request):
    return (
        request.url.params.get("key") == SENTINEL
        or SENTINEL in request.headers.get("x-key", "")
        or "authorization" in request.headers
    )


def _key_server(answers, seen=None):
    """A stub answering ``{URL without query: answer(request)}``, keeping
    ``(method, URL without query, carries the key, request)`` in ``seen``."""

    def handler(request):
        url = str(request.url.copy_with(query=None))
        if seen is not None:
            seen.append((request.method, url, _carries_key(request), request))
        answer = answers.get(url)
        return httpx.Response(404) if answer is None else answer(request)

    return httpx.MockTransport(handler)


def _answer(status=200, keyed=True, etag=None, data=None, **headers):
    """An answer serving ``data`` or the archive (or ``status``), only with
    the key when ``keyed``; an ``etag`` makes it conditional."""

    def answer(request):
        if keyed and not _carries_key(request):
            return httpx.Response(401)
        if etag and request.headers.get("If-None-Match") == etag:
            return httpx.Response(304)
        if status != 200:
            return httpx.Response(status, headers=headers)
        tags = {"ETag": etag} if etag else {}
        content = data or _zip_bytes()
        return httpx.Response(200, headers=tags | headers, content=content)

    return answer


def _moved(location, keyed=True, **headers):
    return _answer(302, keyed=keyed, Location=location, **headers)


# A cookie A sets never comes back to it.
WALK = {
    FEED: _moved(B + "/x", **{"Set-Cookie": "s=1"}),
    B + "/x": _moved(A + "/final.zip", keyed=False),
    A + "/final.zip": _answer(keyed=False),
}
METHODS = {"query_param": {"key": "key"}, "header": {"X-Key": "key"}, "basic_auth": {}}


@pytest.mark.parametrize(
    "method, answers, expected, outcome",
    [
        *[(m, WALK, [FEED, B + "/x", A + "/final.zip"], "read") for m in METHODS],
        # The credential parameter set to another value never reaches B.
        (
            "query_param",
            {FEED: _moved(B + "/x?key=other"), B + "/x": _answer(keyed=False)},
            [FEED, B + "/x"],
            "read",
        ),
        ("query_param", {FEED: _moved(f"{B}/{ENCODED}/x")}, [FEED], "failed"),
        ("query_param", {FEED: _moved("http://a.example/final.zip")}, [FEED], "failed"),
    ],
    ids=["query", "header", "basic", "param", "secret-path", "http"],
)
def test_a_maintainer_key_reaches_only_the_access_origin(
    tmp_path, monkeypatch, method, answers, expected, outcome
):
    cache = tmp_path / "cache"
    fields = ["username", "password"] if method == "basic_auth" else ["key"]
    overrides_dir = _providers(tmp_path, p={"credential_fields": fields})
    for field, value in (("KEY", SENTINEL), ("USERNAME", "u"), ("PASSWORD", SENTINEL)):
        monkeypatch.setenv(f"TRANSITIO_KEY_P__{field}", value)
    feed = _keyed("f", method=method, params=METHODS[method])
    _publish_resolved(cache, [feed], overrides_dir)
    seen = []
    _, log = _crawl(cache, _key_server(answers, seen), overrides_dir=overrides_dir)
    assert log["f"]["key_crawl"] == outcome
    first = next(i for i, (*_, carries, _) in enumerate(seen) if carries)
    walked = seen[first:]
    assert [url for _, url, *_ in walked] == expected
    # Only the access origin's hops before the walk left it carry the key.
    stayed = [
        all(u.startswith(A) for u in expected[: i + 1]) for i in range(len(expected))
    ]
    assert [carries for *_, carries, _ in walked] == stayed
    for verb, _, carries, request in walked:
        assert verb == "GET" and request.headers["accept-encoding"] == "gzip"
        assert carries or "key" not in request.url.params
    assert all("cookie" not in request.headers for *_, request in seen)
    assert SENTINEL not in str(log["f"]["fallback_reason"])


@pytest.mark.parametrize(
    "case, outcome, sent, kept",
    [
        ("keyless", "not_needed", False, "producer"),
        ("hosted", "not_needed", False, "mdb_latest"),
        ("keys-off", "keys_off", False, "p"),
        # Changed data whose ETag reflects the key leave the earlier read whole.
        ("reflected", "failed", True, "p"),
        ("no-url", "not_https", False, None),
        # A key goes only to a URL under its provider's url_prefixes.
        ("unclaimed", "not_claimed", False, None),
        # A cache read without the key is read afresh with it, not reused.
        ("was-keyless", "read", True, "p"),
        # An earlier keyed read is removed once its provider no longer serves
        # the feed, and a rebound feed is read under its new provider.
        ("unbound", "no_provider", False, None),
        ("unapproved", "not_approved", False, None),
        ("rebound", "read", True, "q"),
        ("gone", None, False, None),
        ("no-keys", "no_credentials", False, "p"),
        ("environment", "read", True, "p"),
        pytest.param("file", "read", True, "p", marks=NO_FILE),
        ("http", "not_https", False, None),
        pytest.param("malformed", None, False, None, marks=NO_FILE),
    ],
)
def test_a_key_crawl_needs_an_approved_provider_and_credentials(
    tmp_path, monkeypatch, case, outcome, sent, kept
):
    cache = tmp_path / "cache"
    url = {"http": "http://a.example/feed.zip", "no-url": None}.get(case, FEED)
    feed = _keyed("f", url)
    answers = {FEED: _answer(keyed=case != "keyless", etag='"v1"')}
    if case == "hosted":
        feed["mdb"] = {"urls": {"latest": HOSTED}}
        answers[HOSTED] = _answer(keyed=False)
    entries = {"p": {"url_prefixes": [A + "/other/"]} if case == "unclaimed" else {}}
    monkeypatch.setenv("TRANSITIO_KEY_P__KEY", SENTINEL)
    monkeypatch.setenv("TRANSITIO_KEY_Q__KEY", SENTINEL)
    if case == "was-keyless":
        _publish_resolved(cache, [feed])
        _crawl(cache, _key_server({FEED: _answer(keyed=False, etag='"v1"')}))
    if case in (
        "keys-off",
        "no-keys",
        "reflected",
        "unbound",
        "unapproved",
        "rebound",
        "gone",
    ):
        # An earlier crawl read the feed with p's key.
        overrides_dir = _providers(tmp_path, **entries)
        _publish_resolved(cache, [feed], overrides_dir)
        _crawl(cache, _key_server(answers), overrides_dir=overrides_dir)
    if case == "reflected":
        changed = _zip_bytes(_members(stops=TWO_STOPS))
        answers[FEED] = _answer(etag=f'"{SENTINEL}"', data=changed)
    feed["access_provider"] = {"unbound": None, "rebound": "q"}.get(case, "p")
    if case == "unapproved":
        entries["p"] = {"crawl_approved": False}
    if case == "rebound":
        entries = {"p": {"url_prefixes": [B + "/"]}, "q": {}}
    if case in ("no-keys", "file", "malformed"):
        monkeypatch.delenv("TRANSITIO_KEY_P__KEY")
        text = f'[p]\nkey = "{SENTINEL}"\n' if case == "file" else "[p\n"
        (tmp_path / "credentials.toml").write_text("" if case == "no-keys" else text)
    overrides_dir = None if case == "keys-off" else _providers(tmp_path, **entries)
    _publish_resolved(cache, [] if case == "gone" else [feed], overrides_dir)
    seen = []
    server = _key_server(answers, seen)
    if case == "unapproved":
        # A withdrawal cut short, before any request, keeps the state for the
        # next crawl to finish.
        def busy(*args):
            raise OSError("busy")

        with monkeypatch.context() as patch, pytest.raises(OSError):
            patch.setattr(crawl, "_prune_members", busy)
            _crawl(cache, server, overrides_dir=overrides_dir)
        assert (_feed_dir(cache, "f") / crawl.STATE_FILE).exists() and seen == []
    if case == "malformed":
        with pytest.raises(ValueError, match="credentials.toml"):
            _crawl(cache, server, overrides_dir=overrides_dir)
        assert seen == []
        return
    _, log = _crawl(cache, server, overrides_dir=overrides_dir)
    assert log.get("f", {}).get("key_crawl") == outcome
    assert any(carries for *_, carries, _ in seen) is sent
    path = _feed_dir(cache, "f") / crawl.STATE_FILE
    state = json.loads(path.read_text()) if path.exists() else {}
    assert (state.get("key_provider") or state.get("fetched_from")) == kept
    for name, digest in state.get("member_sha256", {}).items():
        assert hashlib.sha256((path.parent / name).read_bytes()).hexdigest() == digest
    if case == "environment":
        # The next run spends one conditional request, answered 304.
        seen.clear()
        _, log = _crawl(cache, server, overrides_dir=overrides_dir)
        assert (log["f"]["method"], log["f"]["key_crawl"]) == ("not_modified", "read")
        assert [carries for *_, carries, _ in seen].count(True) == 1


class _Overlap:
    """Holds the first keyed request until a second arrives or ``wait``
    seconds pass, and records the most keyed requests in flight at once."""

    def __init__(self, wait):
        self.wait = wait
        self.lock = threading.Lock()
        self.second = threading.Event()
        self.flight = self.most = self.count = 0

    def __call__(self, answer):
        def held(request):
            if not _carries_key(request):
                return answer(request)
            with self.lock:
                self.flight += 1
                self.count += 1
                self.most = max(self.most, self.flight)
                first = self.count == 1
            if first:
                self.second.wait(self.wait)
            else:
                self.second.set()
            try:
                return answer(request)
            finally:
                with self.lock:
                    self.flight -= 1

        return held


THIS_MONTH = crawl.datetime.datetime.now(crawl.datetime.timezone.utc).strftime("%Y-%m")


@pytest.mark.parametrize(
    "budget, before, answer, feeds, sent, outcomes, after",
    [
        # The keyed reads follow feed order at any worker count.
        (1, None, _answer(), 2, 1, ["read", "budget_spent"], 1),
        # A chain of three keyed hops: two are sent, the third is not.
        (2, None, _moved(A + "/hop.zip"), 1, 2, ["budget_spent"], 2),
        # A 503 is retried only while the budget lasts.
        (1, None, _answer(503), 1, 1, ["budget_spent"], 1),
        (1, (THIS_MONTH, 1), _answer(), 1, 0, ["budget_spent"], 1),
        (1, ("2000-01", 5), _answer(), 1, 1, ["read"], 1),
        (1, ("2000-13", 0), _answer(), 1, 0, ["malformed"], None),
        # A 401 counts once and stops the key for the provider's other feeds,
        # whatever their own access.
        (5, (THIS_MONTH, 2), _answer(401), 3, 1, ["failed"] + ["key_refused"] * 2, 3),
    ],
    ids=["two-feeds", "hops", "retry", "spent", "new-month", "malformed", "401"],
)
def test_a_key_budget_caps_the_keyed_requests_a_month(
    tmp_path, monkeypatch, budget, before, answer, feeds, sent, outcomes, after
):
    cache = tmp_path / "cache"
    tally = keys.usage_path()
    if before:
        tally.parent.mkdir()
        month, count = before
        tally.write_text(json.dumps({"month": month, "requests": {"p": count}}))
    overrides_dir = _providers(tmp_path, p={"crawl_budget": budget})
    monkeypatch.setenv("TRANSITIO_KEY_P__KEY", SENTINEL)
    names = [f"f{i}" for i in range(feeds)]
    methods = ["query_param", "query_param", "unsupported"]
    resolved = [_keyed(n, f"{A}/{n}.zip", method=m) for n, m in zip(names, methods)]
    _publish_resolved(cache, resolved, overrides_dir)
    # Two feeds of one provider: a second keyed request while the first is
    # held would mean the provider's keyed reads overlap.
    overlap = _Overlap(0.5 if feeds > 1 else 0)
    answers = {f"{A}/{n}.zip": overlap(answer) for n in names}
    answers.update({A + "/hop.zip": _moved(A + "/end.zip"), A + "/end.zip": _answer()})
    seen = []
    server = _key_server(answers, seen)
    if after is None:
        # A malformed tally stops the crawl before its first request.
        with pytest.raises(ValueError, match="not a tally"):
            _crawl(cache, server, overrides_dir=overrides_dir)
        assert seen == []
        return
    _, log = _crawl(cache, server, overrides_dir=overrides_dir, workers=feeds)
    assert sum(carries for *_, carries, _ in seen) == sent and overlap.most <= 1
    assert [log[name]["key_crawl"] for name in names] == outcomes
    if budget == 5:
        assert any("HTTP 401" in r["fallback_reason"] for r in log.values())
    counts = {"month": THIS_MONTH, "requests": {"p": after}}
    assert json.loads(tally.read_text()) == counts


def test_a_maintainer_key_appears_in_no_output(tmp_path, monkeypatch, caplog):
    cache = tmp_path / "cache"
    C = "https://c.example"
    nsw = {"credential_fields": ["api_key"], "url_prefixes": [C + "/"]}
    overrides_dir = _providers(tmp_path, p={}, nsw=nsw)
    monkeypatch.setenv("TRANSITIO_KEY_P__KEY", SENTINEL)
    monkeypatch.setenv("TRANSITIO_KEY_NSW__API_KEY", "apikey " + SENTINEL)
    trace = logging.getLogger("httpcore.http11")

    def drops(request):
        # A connection lost mid-body, its error naming the credentialed URL.
        def body():
            yield b"PK"
            raise httpx.ReadError(str(request.url))

        if not _carries_key(request):
            return httpx.Response(401)
        return httpx.Response(200, content=body())

    def traced(request):
        location = f"{B}/x?echo={SENTINEL}&again={ENCODED}"
        if trace.isEnabledFor(logging.DEBUG):
            trace.debug("receive_response_headers.complete %s", location)
        return _moved(location)(request)

    answers = {
        A + "/read.zip": _answer(),
        A + "/drop.zip": drops,
        A + "/traced.zip": traced,
        # A TfNSW-shaped "apikey <key>" header whose bare key is reflected.
        C + "/nsw-moved.zip": _moved(f"{B}/{ENCODED}/x"),
        C + "/nsw-etag.zip": _answer(etag=f'"{ENCODED}"'),
    }
    header = dict(provider="nsw", method="header", params={"Authorization": "api_key"})
    feeds = [_keyed(name, f"{A}/{name}.zip") for name in ("read", "drop", "traced")]
    feeds += [_keyed(n, f"{C}/{n}.zip", **header) for n in ("nsw-moved", "nsw-etag")]
    _publish_resolved(cache, feeds, overrides_dir)
    caplog.set_level(logging.DEBUG)
    seen = []
    summary, log = _crawl(
        cache, _key_server(answers, seen), overrides_dir=overrides_dir
    )
    outcomes = {feed_id: record["key_crawl"] for feed_id, record in log.items()}
    assert outcomes == dict.fromkeys(log, "failed") | {"read": "read"}
    assert not trace.filters
    assert not any(B in url for _, url, *_ in seen)
    states = [p.read_text() for p in (cache / "crawl").glob("*/state.json")]
    assert [json.loads(text)["url"] for text in states] == [A + "/read.zip"]
    texts = [*states, json.dumps(summary), caplog.text]
    texts.append((cache / "crawl" / crawl.LOG_FILE).read_text())
    assert not any(form in text for text in texts for form in (SENTINEL, ENCODED))
