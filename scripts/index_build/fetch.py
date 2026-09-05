"""HTTP fetch layer for the feed crawler.

One :class:`Fetcher` owns an ``httpx`` client, a per-host token bucket and the
per-worker byte/request counters each crawl-log record reads as its own delta
(the run total is the sum over records, so no counter is shared across
threads). It offers three verbs: ``head``
probes a URL for its size and range support; ``read_range`` reads one byte
range (with :meth:`Fetcher.range_reader` adapting it to the ``ziprange`` read
contract); ``download`` streams the whole file into a store directory —
bounded, digest-as-it-arrives, resumed over a range request when a connection
drops mid-body, honouring ``ETag``/``Last-Modified`` conditional requests so an
unchanged feed costs one 304.

Feed URLs are third-party data, so every destination — redirect hops included —
is checked before it is contacted: only http(s), no userinfo (httpx would turn
it into an Authorization header), never a loopback, private, link-local or
otherwise non-global literal address, never ``localhost``-like names.
Redirects are followed manually so each hop pays the token bucket and appears
in the counters. Requests ask for ``identity`` encoding and refuse encoded
answers, because ranges and Content-Length only mean anything over the raw
bytes. Range reads and resumes carry ``If-Range``, and a ``Content-Range``
answer must match what was asked, so a feed republished mid-crawl degrades to a
whole download instead of mixing versions.

Servers lie: a 200 answering a range request, an over-long or short range
body, a body short of its Content-Length, or an over-ceiling stream all raise
:class:`FetchError` (``RangeUnsupported`` where the caller should fall back to
a whole download) rather than being papered over.
"""

import hashlib
import ipaddress
import os
import threading
import time
import urllib.parse

import httpx
import idna

from index_build import overture, store

# The plan's default: range machinery only pays above this size.
RANGE_THRESHOLD = 20 * 1024 * 1024

# Ceiling on one whole-file download; the largest national feeds are far below.
MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024 * 1024

DOWNLOAD_ATTEMPTS = 3
REDIRECT_LIMIT = 5
TIMEOUT = 60.0

_REDIRECTS = (301, 302, 303, 307, 308)

# Hostnames that always mean "not the internet", whatever DNS says. Literal
# addresses are judged by ipaddress below; hostname-based DNS tricks are out of
# scope for a maintainer batch tool that never sends credentials.
_BLOCKED_NAMES = ("localhost",)
_BLOCKED_SUFFIXES = (".localhost", ".local", ".internal")


def _looks_numeric(host):
    """Whether every label of ``host`` is decimal or hex — a real DNS name has
    an alphabetic top-level label, so this only matches address-like forms."""

    def numeric(label):
        if label.isdigit():
            return True
        return label[:2].lower() == "0x" and all(
            c in "0123456789abcdefABCDEF" for c in label[2:]
        )

    labels = host.split(".")
    return bool(labels) and all(label and numeric(label) for label in labels)


class FetchError(RuntimeError):
    """The fetch failed in a way retrying this call cannot fix."""


class RangeUnsupported(FetchError):
    """The server did not honour a range request; download the file whole."""


def check_url(url):
    """Refuse a URL the crawler must not contact; return ``(split, host)``.

    Only http(s), with no userinfo and a hostname that is neither a blocked
    name nor a literal address outside the global ranges (loopback, private,
    link-local, metadata and friends). Applied to every redirect hop as well as
    the original URL. The returned ``host`` is the normalised connect form —
    IDNA-encoded, lowercased, a compressed address for IP literals — so every
    spelling of one host shares it.
    """
    try:
        split = urllib.parse.urlsplit(url)
    except ValueError as error:
        raise FetchError(f"{url}: malformed URL ({error})")
    if split.scheme not in ("http", "https"):
        raise FetchError(f"{url}: scheme {split.scheme!r} is not fetched")
    if split.username is not None or split.password is not None:
        # httpx turns URL userinfo into an Authorization header.
        raise FetchError(f"{url}: userinfo is not fetched")
    host = split.hostname
    if not host:
        raise FetchError(f"{url}: no host")
    try:
        split.port
    except ValueError:
        raise FetchError(f"{url}: invalid port")
    bare = host.lower().rstrip(".")
    if not bare.isascii():
        # Judge the host in the exact form httpx will connect to: the idna
        # package with UTS-46 mapping is what httpx itself applies, so Unicode
        # dot variants map to "." and spellings like "faß.de" normalise the
        # same way here and at the socket.
        try:
            bare = idna.encode(bare, uts46=True).decode("ascii").rstrip(".")
        except idna.IDNAError:
            raise FetchError(f"{url}: host {host!r} does not encode")
    if bare in _BLOCKED_NAMES or bare.endswith(_BLOCKED_SUFFIXES):
        raise FetchError(f"{url}: host {host!r} is not fetched")
    try:
        address = ipaddress.ip_address(bare)
    except ValueError:
        if _looks_numeric(bare):
            # Legacy address forms — "127.1", "2130706433", "0x7f000001" —
            # fail ipaddress but resolve as loopback in common resolvers.
            raise FetchError(f"{url}: numeric address form {host!r} is not fetched")
        return split, bare
    if not address.is_global:
        raise FetchError(f"{url}: address {host!r} is not fetched")
    return split, address.compressed


class HostBuckets:
    """A token bucket per host: ``rate`` requests/second, bursting to ``burst``.

    ``clock`` and ``sleeper`` are injectable so tests measure waits instead of
    taking them.
    """

    def __init__(self, rate=1.0, burst=5, *, clock=time.monotonic, sleeper=time.sleep):
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._sleeper = sleeper
        self._buckets = {}
        # Guards the get-or-create-and-update of one host's bucket so many
        # workers throttle correctly against a shared registry; the sleep is
        # taken outside it, so one host's wait never blocks another host.
        self._lock = threading.Lock()

    def acquire(self, host):
        with self._lock:
            tokens, then = self._buckets.get(host, (float(self._burst), self._clock()))
            now = self._clock()
            tokens = min(self._burst, tokens + (now - then) * self._rate)
            if tokens < 1.0:
                # Reserve this request's slot at the future time before sleeping,
                # so a concurrent waiter on the same host queues behind it.
                wait = (1.0 - tokens) / self._rate
                self._buckets[host] = (0.0, now + wait)
            else:
                wait = 0.0
                self._buckets[host] = (tokens - 1.0, now)
        if wait:
            self._sleeper(wait)


def _content_range(response):
    """The ``(start, end, total)`` a 206 claims, or None when unusable.

    ``end`` and ``total`` are None when the header leaves them out (``*``).
    """
    header = response.headers.get("Content-Range", "")
    if not header.startswith("bytes "):
        return None
    span, _, total_text = header[6:].partition("/")
    start_text, _, end_text = span.partition("-")
    if not start_text.isdigit():
        return None
    return (
        int(start_text),
        int(end_text) if end_text.isdigit() else None,
        int(total_text) if total_text.isdigit() else None,
    )


def _refuse_encoding(response, url):
    """Refuse a content-encoded answer: offsets and lengths mean raw bytes."""
    encoding = response.headers.get("Content-Encoding", "").lower()
    if encoding not in ("", "identity"):
        raise FetchError(f"{url}: content-encoding {encoding!r} is not accepted")


class Fetcher:
    """The crawler's HTTP client: rate-limited, counted, range-capable."""

    def __init__(
        self,
        *,
        transport=None,
        rate=1.0,
        burst=5,
        timeout=TIMEOUT,
        clock=time.monotonic,
        sleeper=time.sleep,
    ):
        # Redirects are followed manually in _hops, so every hop is checked,
        # throttled and counted. Identity encoding keeps byte counts, ranges
        # and Content-Length in one currency: raw bytes on the wire.
        self._client = httpx.Client(
            transport=transport,
            follow_redirects=False,
            timeout=timeout,
            headers={
                "User-Agent": overture.USER_AGENT,
                "Accept-Encoding": "identity",
            },
        )
        self._buckets = HostBuckets(rate, burst, clock=clock, sleeper=sleeper)
        # Per-thread counters: one shared Fetcher serves every crawl worker, and
        # a feed's record reads these as a delta over its own crawl, so each
        # thread must count only the requests and bytes it made.
        self._counters = threading.local()
        self.requests = 0
        self.bytes_fetched = 0

    @property
    def requests(self):
        return getattr(self._counters, "requests", 0)

    @requests.setter
    def requests(self, value):
        self._counters.requests = value

    @property
    def bytes_fetched(self):
        return getattr(self._counters, "bytes_fetched", 0)

    @bytes_fetched.setter
    def bytes_fetched(self, value):
        self._counters.bytes_fetched = value

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def _throttle(self, url):
        # One bucket per real host: the single per-host rate authority, so
        # every scheme and port of one host share it and the aggregate rate to
        # that host cannot double. The key is the normalised host, so a spelling
        # variant (case, trailing dot, Unicode vs punycode, an IPv6 long form)
        # or any port never mints a fresh bucket.
        _, host = check_url(url)
        self._buckets.acquire(host)
        self.requests += 1

    def _hops(self, method, url, headers, *, stream=False):
        """The final response, following redirects one checked hop at a time.

        A streamed response is returned unread (the caller closes it); every
        hop is URL-checked, throttled and counted, and more than
        ``REDIRECT_LIMIT`` hops is an error.
        """
        for _ in range(REDIRECT_LIMIT + 1):
            self._throttle(url)
            try:
                request = self._client.build_request(method, url, headers=headers)
                response = self._client.send(request, stream=stream)
            except (httpx.HTTPError, httpx.InvalidURL, ValueError) as error:
                # ValueError covers malformed third-party URLs (a bad port,
                # say) that httpx surfaces outside its own error tree.
                raise FetchError(f"{method} {url}: {error}")
            if response.status_code in _REDIRECTS:
                location = response.headers.get("Location")
                if stream:
                    response.close()
                if not location:
                    raise FetchError(f"{method} {url}: redirect without Location")
                try:
                    url = urllib.parse.urljoin(url, location)
                except ValueError as error:
                    raise FetchError(
                        f"{method} {url}: malformed redirect target ({error})"
                    )
                continue
            return url, response
        raise FetchError(f"{method} {url}: more than {REDIRECT_LIMIT} redirects")

    def head(self, url):
        """Size, range support and validators, from a ``HEAD`` probe."""
        _, response = self._hops("HEAD", url, {})
        if response.status_code != 200:
            raise FetchError(f"HEAD {url}: HTTP {response.status_code}")
        # An encoded HEAD would describe a different byte representation than
        # the ranges later read.
        _refuse_encoding(response, url)
        declared = response.headers.get("Content-Length")
        return {
            "size": int(declared) if declared and declared.isdigit() else None,
            "accept_ranges": response.headers.get("Accept-Ranges", "").lower()
            == "bytes",
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
        }

    def read_range(
        self, url, start, size, *, validator=None, max_bytes=MAX_DOWNLOAD_BYTES
    ):
        """Exactly ``size`` bytes at ``start``, or raise.

        ``size`` ultimately comes from remote ZIP metadata, so it is capped by
        ``max_bytes`` before anything is asked for. A 200 answer means the
        server ignored the range — or, under ``If-Range``, that the
        representation changed — and the caller must fall back to a whole
        download; the unread body is closed, not buffered. A 206 whose
        ``Content-Range`` disagrees with the request, or whose body is longer
        or shorter than asked, is refused — the body is read at most ``size``
        bytes plus one probe chunk, never whole.
        """
        if size > max_bytes:
            raise FetchError(
                f"{url}: range of {size} bytes is over the {max_bytes}-byte ceiling"
            )
        headers = {"Range": f"bytes={start}-{start + size - 1}"}
        if validator:
            # Pins the representation across the archive's several reads.
            headers["If-Range"] = validator
        _, response = self._hops("GET", url, headers, stream=True)
        try:
            if response.status_code == 200:
                raise RangeUnsupported(f"{url}: server ignored the range request")
            if response.status_code != 206:
                raise FetchError(f"GET {url}: HTTP {response.status_code} for a range")
            _refuse_encoding(response, url)
            claimed = _content_range(response)
            if claimed is not None:
                claimed_start, claimed_end, _total = claimed
                if claimed_start != start or (
                    claimed_end is not None and claimed_end - claimed_start + 1 != size
                ):
                    raise FetchError(
                        f"{url}: asked for {size} bytes at {start}, answered "
                        f"with {response.headers.get('Content-Range')!r}"
                    )
            chunks = []
            got = 0
            try:
                # With non-identity encodings refused above, decoded bytes ARE
                # the wire bytes, so counts and offsets stay in one currency.
                for chunk in response.iter_bytes(65536):
                    got += len(chunk)
                    self.bytes_fetched += len(chunk)
                    if got > size:
                        raise FetchError(
                            f"{url}: range of {size} bytes answered with more"
                        )
                    chunks.append(chunk)
            except httpx.HTTPError as error:
                raise FetchError(f"GET {url}: {error}")
        finally:
            response.close()
        body = b"".join(chunks)
        if len(body) != size:
            raise FetchError(f"{url}: range of {size} bytes answered with {len(body)}")
        return body

    def range_reader(self, url, *, validator=None):
        """A ``read(start, size)`` over ``url``, for :mod:`index_build.ziprange`.

        Pass the ``ETag`` (or ``Last-Modified``) from :meth:`head` as
        ``validator`` so all of an archive's reads pin one representation.
        """

        def read(start, size):
            return self.read_range(url, start, size, validator=validator)

        return read

    def download(
        self,
        url,
        directory,
        name,
        *,
        etag=None,
        last_modified=None,
        max_bytes=MAX_DOWNLOAD_BYTES,
    ):
        """Stream ``url`` into ``name`` under ``directory``; return the outcome.

        With a validator given, the request is conditional and an unchanged
        file returns ``{"status": "not_modified"}`` after one 304. Otherwise
        the body is streamed to an exclusively created temporary file — hashed
        as it arrives, bounded by ``max_bytes``, checked against the declared
        Content-Length — and replaced into place only when complete. A
        connection dropping mid-body is resumed with a range request from the
        bytes already written, pinned by ``If-Range`` to the representation the
        first bytes came from; a refused or unpinnable resume restarts from
        zero. Returns ``status``, ``sha256``, ``bytes`` and the response
        validators (kept from the responses that supplied them, so a resumed
        download still records the validators for the next crawl's
        conditional request).
        """
        conditional = {}
        if etag:
            conditional["If-None-Match"] = etag
        if last_modified:
            conditional["If-Modified-Since"] = last_modified

        handle, partial = store.create_temporary(directory)
        digest = hashlib.sha256()
        written = 0
        validators = {}
        try:
            with os.fdopen(handle, "wb") as opened_file:
                handle = None
                for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
                    resume_pin = validators.get("etag") or validators.get(
                        "last_modified"
                    )
                    if written and not resume_pin:
                        # Nothing to pin the resume to: bytes from a possibly
                        # different representation must not be mixed in.
                        opened_file.seek(0)
                        opened_file.truncate()
                        digest = hashlib.sha256()
                        written = 0
                        validators = {}
                    headers = dict(conditional) if written == 0 else {}
                    if written:
                        headers["Range"] = f"bytes={written}-"
                        headers["If-Range"] = resume_pin
                    try:
                        outcome = self._stream_once(
                            url, headers, opened_file, digest, written, max_bytes
                        )
                    except (httpx.HTTPError, FetchError) as error:
                        if attempt == DOWNLOAD_ATTEMPTS:
                            raise FetchError(f"GET {url}: {error}")
                        continue
                    if outcome["status"] == "not_modified":
                        return {"status": "not_modified"}
                    if outcome["status"] == "restart":
                        # The server would not resume; start over from zero.
                        opened_file.seek(0)
                        opened_file.truncate()
                        digest = hashlib.sha256()
                        written = 0
                        validators = {}
                        continue
                    changed = any(
                        value and validators.get(key) and value != validators[key]
                        for key, value in outcome["validators"].items()
                    )
                    if changed and written:
                        # A resumed 206 answering with a DIFFERENT validator is
                        # a new representation: never splice it onto the old
                        # prefix.
                        opened_file.seek(0)
                        opened_file.truncate()
                        digest = hashlib.sha256()
                        written = 0
                        validators = {}
                        continue
                    written = outcome["written"]
                    for key, value in outcome["validators"].items():
                        # A later 206 that omits a validator must not erase the
                        # one the representation was pinned by.
                        if value:
                            validators[key] = value
                    if outcome["complete"]:
                        opened_file.flush()
                        os.fsync(opened_file.fileno())
                        break
                    if attempt == DOWNLOAD_ATTEMPTS:
                        raise FetchError(f"GET {url}: body ended early")
                else:
                    raise FetchError(f"GET {url}: could not complete the download")
            directory.replace(partial, name)
            return {
                "status": "fetched",
                "sha256": digest.hexdigest(),
                "bytes": written,
                "etag": validators.get("etag"),
                "last_modified": validators.get("last_modified"),
            }
        finally:
            if handle is not None:
                os.close(handle)
            store.unlink(directory, partial)

    def _stream_once(self, url, headers, opened_file, digest, written, max_bytes):
        """One streaming attempt; returns what happened rather than raising.

        Network errors propagate (the caller retries); protocol answers are
        returned so the caller can distinguish 304, a refused resume, and a
        short body.
        """
        expected = None
        _, response = self._hops("GET", url, headers, stream=True)
        try:
            if response.status_code == 304:
                return {"status": "not_modified"}
            if written and response.status_code != 206:
                return {"status": "restart"}
            if response.status_code not in (200, 206):
                raise FetchError(f"HTTP {response.status_code}")
            if response.status_code == 206 and not written:
                raise FetchError("206 answer to a request without a range")
            _refuse_encoding(response, url)
            total = None
            if written:
                claimed = _content_range(response)
                if claimed is None or claimed[0] != written:
                    # An unverifiable or misaligned resume segment must not be
                    # appended; start over instead.
                    return {"status": "restart"}
                total = claimed[2]
            declared = response.headers.get("Content-Length")
            if declared and declared.isdigit():
                expected = written + int(declared)
            if total is not None:
                # A resumed segment is complete only when the whole declared
                # representation is: a short segment must trigger another
                # resume, never publish a truncated file.
                expected = total
            for chunk in response.iter_bytes(1024 * 1024):
                written += len(chunk)
                self.bytes_fetched += len(chunk)
                if written > max_bytes:
                    raise FetchError(f"download exceeds the {max_bytes}-byte ceiling")
                digest.update(chunk)
                opened_file.write(chunk)
            validators = {
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
            }
        finally:
            response.close()
        complete = expected is None or written == expected
        return {
            "status": "streamed",
            "written": written,
            "complete": complete,
            "validators": validators,
        }
