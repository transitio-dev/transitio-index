import hashlib
import io
import sys
import threading
import zipfile
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from index_build import fetch, store, ziprange  # noqa: E402

BODY = b"stop_id,stop_name\n" + b"1,Central\n" * 2000


def _zip_bytes(members):
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return sink.getvalue()


def _range_server(data, *, honour_ranges=True, etag=None):
    """A MockTransport serving ``data`` with HEAD/Range/conditional support."""

    def handler(request):
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
            return httpx.Response(206, headers=headers, content=body)
        return httpx.Response(200, headers=headers, content=data)

    return httpx.MockTransport(handler)


def _fetcher(transport, **kw):
    kw.setdefault("clock", lambda: 0.0)
    kw.setdefault("sleeper", lambda wait: None)
    return fetch.Fetcher(transport=transport, **kw)


def test_head_reports_size_ranges_and_validators():
    with _fetcher(_range_server(BODY, etag='"v1"')) as fetcher:
        probe = fetcher.head("https://feeds.example/gtfs.zip")
    assert probe["size"] == len(BODY)
    assert probe["accept_ranges"] is True
    assert probe["etag"] == '"v1"'


def test_an_encoded_head_answer_is_refused():
    def handler(request):
        return httpx.Response(
            200,
            headers={"Content-Length": "100", "Content-Encoding": "gzip"},
        )

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError, match="content-encoding"):
            fetcher.head("https://feeds.example/gtfs.zip")


def test_read_range_returns_exactly_the_slice():
    with _fetcher(_range_server(BODY)) as fetcher:
        chunk = fetcher.read_range("https://feeds.example/gtfs.zip", 5, 10)
        assert chunk == BODY[5:15]
        assert fetcher.bytes_fetched == 10
        assert fetcher.requests == 1


def test_a_server_ignoring_the_range_is_reported():
    with _fetcher(_range_server(BODY, honour_ranges=False)) as fetcher:
        with pytest.raises(fetch.RangeUnsupported):
            fetcher.read_range("https://feeds.example/gtfs.zip", 0, 10)


def test_ziprange_reads_members_over_the_range_reader():
    # An incompressible bulk member the test never reads, so the archive is
    # much larger than the EOCD tail window and the saving is measurable.
    bulk = b"".join(hashlib.sha256(str(i).encode()).digest() for i in range(20_000))
    data = _zip_bytes({"stop_times.txt": bulk, "stops.txt": BODY})
    assert len(data) > 500_000
    with _fetcher(_range_server(data)) as fetcher:
        read = fetcher.range_reader("https://feeds.example/gtfs.zip")
        directory = ziprange.central_directory(read, len(data))
        assert ziprange.read_member(read, directory["stops.txt"]) == BODY
        # The bytes fetched stay far below the archive size: tail + directory
        # + one member, never the bulk one.
        assert fetcher.bytes_fetched < len(data) / 2


def test_download_streams_hashes_and_replaces(tmp_path):
    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(_range_server(BODY, etag='"v1"')) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    assert result["status"] == "fetched"
    assert result["bytes"] == len(BODY)
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert result["etag"] == '"v1"'
    assert (tmp_path / "crawl" / "feed.zip").read_bytes() == BODY


def test_an_unchanged_file_costs_one_304(tmp_path):
    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(_range_server(BODY, etag='"v1"')) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip",
                directory,
                "feed.zip",
                etag='"v1"',
            )
            assert result == {"status": "not_modified"}
            assert fetcher.bytes_fetched == 0
            assert not (tmp_path / "crawl" / "feed.zip").exists()
    finally:
        directory.close()


def test_an_unpinned_drop_restarts_from_zero(tmp_path):
    # The first response carries no validator, so its partial bytes cannot be
    # pinned to a representation: the retry must not send a Range.
    calls = []

    def handler(request):
        calls.append(request.headers.get("Range"))
        if len(calls) == 1:
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(BODY))},
                content=BODY[:4096],
            )
        return httpx.Response(
            200, headers={"Content-Length": str(len(BODY))}, content=BODY
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    assert calls == [None, None]
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert (tmp_path / "crawl" / "feed.zip").read_bytes() == BODY


def test_a_refused_resume_restarts_from_zero(tmp_path):
    # A pinned resume is attempted, but the server answers 200 instead of 206:
    # the partial bytes are discarded and the download starts over.
    calls = []

    def handler(request):
        calls.append(request.headers.get("Range"))
        if len(calls) == 1:
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(BODY)), "ETag": '"v1"'},
                content=BODY[:100],
            )
        return httpx.Response(
            200, headers={"Content-Length": str(len(BODY))}, content=BODY
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    assert calls == [None, "bytes=100-", None]
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert (tmp_path / "crawl" / "feed.zip").read_bytes() == BODY


def test_a_persistently_short_body_fails_after_the_attempts(tmp_path):
    def handler(request):
        return httpx.Response(
            200, headers={"Content-Length": str(len(BODY))}, content=BODY[:10]
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            with pytest.raises(fetch.FetchError):
                fetcher.download(
                    "https://feeds.example/gtfs.zip", directory, "feed.zip"
                )
        assert not (tmp_path / "crawl" / "feed.zip").exists()
    finally:
        directory.close()


def test_a_body_over_the_ceiling_is_refused(tmp_path):
    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(_range_server(BODY)) as fetcher:
            with pytest.raises(fetch.FetchError, match="ceiling"):
                fetcher.download(
                    "https://feeds.example/gtfs.zip",
                    directory,
                    "feed.zip",
                    max_bytes=100,
                )
    finally:
        directory.close()


def test_redirect_hops_are_checked_throttled_and_counted():
    data = b"final body"

    def handler(request):
        if request.url.host == "start.example":
            return httpx.Response(
                302, headers={"Location": "https://cdn.example/real.zip"}
            )
        return httpx.Response(
            200, headers={"Content-Length": str(len(data))}, content=data
        )

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        probe = fetcher.head("https://start.example/gtfs.zip")
        assert probe["size"] == len(data)
        assert fetcher.requests == 2  # both hops counted


def test_a_redirect_loop_is_capped():
    def handler(request):
        return httpx.Response(302, headers={"Location": str(request.url)})

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError, match="redirects"):
            fetcher.head("https://loop.example/gtfs.zip")


def test_a_malformed_redirect_target_is_a_fetch_error():
    def handler(request):
        return httpx.Response(302, headers={"Location": "http://[::1"})

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError):
            fetcher.head("https://feeds.example/gtfs.zip")


def test_a_redirect_into_a_private_address_is_refused():
    def handler(request):
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/steal"})

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError, match="not fetched"):
            fetcher.head("https://feeds.example/gtfs.zip")


@pytest.mark.parametrize(
    "url",
    [
        "ftp://feeds.example/gtfs.zip",
        "http://localhost/gtfs.zip",
        "http://feeds.local/gtfs.zip",
        "http://127.0.0.1/gtfs.zip",
        "http://10.1.2.3/gtfs.zip",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/gtfs.zip",
        "https://user:secret@feeds.example/gtfs.zip",
        "http://127.1/gtfs.zip",
        "http://2130706433/gtfs.zip",
        "http://0x7f000001/gtfs.zip",
        "http://0177.0.0.1/gtfs.zip",
        "http://[::1/gtfs.zip",
        "http://127。0。0。1/gtfs.zip",  # IDNA maps the dots to ASCII "."
    ],
)
def test_urls_the_crawler_must_not_contact_are_refused(url):
    with pytest.raises(fetch.FetchError):
        fetch.check_url(url)


def test_a_range_request_over_the_ceiling_is_refused_before_asking():
    with _fetcher(_range_server(BODY)) as fetcher:
        with pytest.raises(fetch.FetchError, match="ceiling"):
            fetcher.read_range("https://feeds.example/gtfs.zip", 0, 1000, max_bytes=100)
        assert fetcher.requests == 0  # nothing was contacted


def test_an_overlong_range_body_is_cut_off_not_buffered():
    big = BODY * 40  # much larger than one read chunk

    def handler(request):
        # Ignore the requested size; answer 206 with the whole body.
        return httpx.Response(
            206,
            headers={"Content-Range": f"bytes 0-9/{len(big)}"},
            content=big,
        )

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError, match="answered with more"):
            fetcher.read_range("https://feeds.example/gtfs.zip", 0, 10)
        # Reading stopped at the first over-size chunk, far short of the body.
        assert fetcher.bytes_fetched < len(big) / 2


class _RawStream(httpx.SyncByteStream):
    """A deferred body, so response headers are inspected before any decode."""

    def __init__(self, data):
        self._data = data

    def __iter__(self):
        yield self._data


def test_requests_ask_for_identity_and_encoded_answers_are_refused(tmp_path):
    seen = []

    def handler(request):
        seen.append(request.headers.get("Accept-Encoding"))
        return httpx.Response(
            200,
            headers={
                "Content-Length": str(len(BODY)),
                "Content-Encoding": "gzip",
            },
            stream=_RawStream(BODY),
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            with pytest.raises(fetch.FetchError, match="content-encoding"):
                fetcher.download(
                    "https://feeds.example/gtfs.zip", directory, "feed.zip"
                )
    finally:
        directory.close()
    assert set(seen) == {"identity"}


def test_one_bucket_covers_host_spellings():
    slept = []
    with _fetcher(
        _range_server(BODY), rate=1.0, burst=1, sleeper=lambda w: slept.append(w)
    ) as fetcher:
        fetcher.read_range("https://feeds.example/gtfs.zip", 0, 1)
        fetcher.read_range("https://Feeds.Example.:443/gtfs.zip", 1, 1)
    assert slept  # same host, same bucket, so the second request waited


def test_one_bucket_covers_unicode_and_punycode_spellings():
    slept = []
    with _fetcher(
        _range_server(BODY), rate=1.0, burst=1, sleeper=lambda w: slept.append(w)
    ) as fetcher:
        fetcher.read_range("https://bücher.example/gtfs.zip", 0, 1)
        fetcher.read_range("https://xn--bcher-kva.example/gtfs.zip", 1, 1)
    assert slept  # IDNA-equal hosts share one bucket


def test_the_guard_normalises_like_httpx_connects():
    # UTS-46 (httpx's mapping) sends "faß.de" to xn--fa-hia.de, not IDNA-2003's
    # "fass.de"; the guard and bucket must agree with the socket layer.
    _, host = fetch.check_url("https://faß.de/gtfs.zip")
    assert host == "xn--fa-hia.de"


def test_a_resume_answering_a_different_validator_restarts(tmp_path):
    calls = []

    def handler(request):
        calls.append(request.headers.get("Range"))
        if len(calls) == 1:
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(BODY)), "ETag": '"v1"'},
                content=BODY[:4096],
            )
        if calls[-1] is not None:
            start = int(request.headers["Range"].split("=", 1)[1].rstrip("-"))
            rest = BODY[start:]
            # A buggy server resumes but reports a NEW representation.
            return httpx.Response(
                206,
                headers={
                    "Content-Length": str(len(rest)),
                    "Content-Range": f"bytes {start}-{len(BODY) - 1}/{len(BODY)}",
                    "ETag": '"v2"',
                },
                content=rest,
            )
        return httpx.Response(
            200,
            headers={"Content-Length": str(len(BODY)), "ETag": '"v2"'},
            content=BODY,
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    # The spliced resume was rejected; the file was refetched whole.
    assert calls == [None, "bytes=4096-", None]
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert result["etag"] == '"v2"'


def test_a_wrong_content_range_start_is_refused():
    def handler(request):
        return httpx.Response(
            206,
            headers={"Content-Range": f"bytes 99-108/{len(BODY)}"},
            content=BODY[99:109],
        )

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        with pytest.raises(fetch.FetchError, match="answered with"):
            fetcher.read_range("https://feeds.example/gtfs.zip", 5, 10)


def test_a_short_resume_segment_resumes_again_not_publishes(tmp_path):
    # The resuming server answers with an honest but SHORT 206 segment; the
    # declared total must drive completeness, forcing a further resume.
    cut_at, segment = 4096, 4096
    calls = []

    def handler(request):
        calls.append(request.headers.get("Range"))
        if len(calls) == 1:
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(BODY)), "ETag": '"v1"'},
                content=BODY[:cut_at],
            )
        start = int(request.headers["Range"].split("=", 1)[1].rstrip("-"))
        # One short segment, then the full remainder.
        end = min(start + segment, len(BODY)) if len(calls) == 2 else len(BODY)
        body = BODY[start:end]
        return httpx.Response(
            206,
            headers={
                "Content-Length": str(len(body)),
                "Content-Range": f"bytes {start}-{end - 1}/{len(BODY)}",
            },
            content=body,
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    assert calls[0] is None and len(calls) == 3
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert (tmp_path / "crawl" / "feed.zip").read_bytes() == BODY


def test_a_bad_port_is_a_fetch_error():
    with pytest.raises(fetch.FetchError, match="invalid port"):
        fetch.check_url("http://feeds.example:99999/gtfs.zip")
    with _fetcher(_range_server(BODY)) as fetcher:
        with pytest.raises(fetch.FetchError):
            fetcher.head("http://feeds.example:99999/gtfs.zip")


def test_range_reads_pin_the_representation_with_if_range():
    seen = []

    def handler(request):
        seen.append(request.headers.get("If-Range"))
        return httpx.Response(
            206,
            headers={"Content-Range": f"bytes 0-9/{len(BODY)}"},
            content=BODY[:10],
        )

    with _fetcher(httpx.MockTransport(handler)) as fetcher:
        read = fetcher.range_reader("https://feeds.example/gtfs.zip", validator='"v1"')
        read(0, 10)
    assert seen == ['"v1"']


def test_a_resume_is_pinned_by_if_range(tmp_path):
    cut_at = 4096
    calls = []

    def handler(request):
        calls.append((request.headers.get("Range"), request.headers.get("If-Range")))
        if len(calls) == 1:
            return httpx.Response(
                200,
                headers={"Content-Length": str(len(BODY)), "ETag": '"v7"'},
                content=BODY[:cut_at],
            )
        span = request.headers["Range"].split("=", 1)[1]
        start = int(span.rstrip("-"))
        rest = BODY[start:]
        return httpx.Response(
            206,
            headers={
                "Content-Length": str(len(rest)),
                "Content-Range": f"bytes {start}-{len(BODY) - 1}/{len(BODY)}",
            },
            content=rest,
        )

    directory = store.open_subdir(tmp_path, "crawl")
    try:
        with _fetcher(httpx.MockTransport(handler)) as fetcher:
            result = fetcher.download(
                "https://feeds.example/gtfs.zip", directory, "feed.zip"
            )
    finally:
        directory.close()
    assert calls == [(None, None), (f"bytes={cut_at}-", '"v7"')]
    assert result["sha256"] == hashlib.sha256(BODY).hexdigest()
    # The resuming 206 carried no ETag; the pin from the first response is kept
    # so the next crawl can still make a conditional request.
    assert result["etag"] == '"v7"'


def test_the_token_bucket_spaces_requests_per_host():
    waits = []
    now = [0.0]

    def clock():
        return now[0]

    def sleeper(wait):
        waits.append(wait)
        now[0] += wait

    buckets = fetch.HostBuckets(rate=1.0, burst=2, clock=clock, sleeper=sleeper)
    for _ in range(4):
        buckets.acquire("a.example")
    buckets.acquire("b.example")  # a different host has its own bucket
    assert waits == [1.0, 1.0]


def test_requests_are_throttled_by_host():
    slept = []
    with _fetcher(
        _range_server(BODY), rate=1.0, burst=1, sleeper=lambda w: slept.append(w)
    ) as fetcher:
        fetcher.read_range("https://feeds.example/gtfs.zip", 0, 1)
        fetcher.read_range("https://feeds.example/gtfs.zip", 1, 1)
    assert slept  # the second request had to wait


def test_concurrent_acquire_shares_one_bucket_and_serializes():
    # Many workers first-touch the same new host together, then keep acquiring:
    # exactly one bucket is created (atomic get-or-create), no take is lost or
    # deadlocks (the bucket lock), and only `burst` requests get through free.
    sleeps = []
    guard = threading.Lock()

    def sleeper(wait):
        with guard:
            sleeps.append(wait)

    buckets = fetch.HostBuckets(rate=1.0, burst=5, clock=lambda: 0.0, sleeper=sleeper)
    n_threads, per = 16, 10
    start = threading.Barrier(n_threads, timeout=5)

    def worker():
        start.wait()
        for _ in range(per):
            buckets.acquire("newhost:443")

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    total = n_threads * per
    assert not any(thread.is_alive() for thread in threads)  # no deadlock
    assert list(buckets._buckets) == ["newhost:443"]  # one bucket for the host
    assert len(sleeps) == total - 5  # only burst got through free
    # Under the lock every acquire is serialized against the same bucket, so the
    # state is exactly the sequential one: burst free, then each waiter reserves
    # 1/rate later. A lost update (unsynchronized get-or-create or take) would
    # double-grant and leave a smaller timestamp, so this pins both.
    assert buckets._buckets["newhost:443"] == (0.0, float(total - 5))


def test_one_host_shares_a_bucket_across_schemes_and_ports():
    # The limiter is the single per-host authority: a second request to the same
    # host on another scheme/port shares its one bucket and waits, rather than
    # minting a fresh bucket that would double the aggregate rate.
    slept = []
    with _fetcher(
        _range_server(BODY), rate=1.0, burst=1, sleeper=lambda w: slept.append(w)
    ) as fetcher:
        fetcher.head("https://feeds.example/a.zip")
        fetcher.head("http://feeds.example:8080/b.zip")
    assert slept  # the second request shared the host's bucket and waited
