"""Stage 3, resolve half: settle feed identity, access and crawlability.

Applies the ``set_identity``, ``mark_uncrawlable`` and ``set_access``
operations from ``overrides/feeds.yaml`` to the crosswalk feeds and writes
``feeds_resolved.jsonl``. These are settled before any crawl because identity
is the crawl cache key and an uncrawlable feed must never be fetched at all; the
crawl half itself is a later stage, and ``set_coverage`` is left for the coverage
stage. Each feed records its access details (``access``, ``access_provider``,
``auth_method``, ``auth_params``, the catalogue's ``auth_param_name`` and
``registration_url``), from a curated ``set_access`` or else the catalogue
record supplying its URL. A feed that needs a key takes its provider from
``set_access`` or else from the ``overrides/access_providers.yaml`` entry whose
URL prefix covers its URL; ``access_report.jsonl`` lists the protected feeds
no provider claims and the curation errors. A GTFS feed whose download URL
needs a key is uncrawlable from the start, so the crawl never spends a request
to learn the 401. It fetches nothing. An override references a feed by its
``feed_id`` or any of its aliases — the crosswalk keeps superseded ids in
``aliases`` for exactly this — so a correction filed against a pre-crosswalk
id still lands.
"""

import collections
import datetime

from transitio_index import crawl, overrides, store

RESOLVE_POINTER = "feeds_resolved.json"
RESOLVE_ARTIFACT = "feeds_resolved.jsonl"
ACCESS_REPORT = "access_report.jsonl"


def _matching_refs(feed_overrides, feed):
    """Every override reference this feed matches, by feed_id or any alias."""
    keys = [feed["feed_id"], *(feed.get("aliases") or [])]
    return {key for key in keys if key in feed_overrides}


def _check_namespace(feeds):
    """Every lookup key — a feed_id or an alias — must resolve to one feed.

    Overrides can rename ids and add aliases, so after applying them the whole
    lookup namespace is checked, not just the primary keys: a key shared across
    two feeds (a duplicate id, or an alias equal to another feed's id or alias)
    would make the next build's override matching ambiguous.
    """
    namespace = collections.defaultdict(set)
    for index, feed in enumerate(feeds):
        for key in [feed["feed_id"], *(feed.get("aliases") or [])]:
            namespace[key].add(index)
    shared = sorted(key for key, at in namespace.items() if len(at) > 1)
    if shared:
        raise overrides.OverrideError(
            f"resolved feeds share lookup keys (id or alias): {shared}"
        )


AUTH_REASON = "requires authentication"
# The catalogue methods a request can carry as they are; MDB numbers its
# authentication types (0 is open).
_ATLAS_METHODS = ("query_param", "header", "basic_auth")
_MDB_METHODS = {"1": "query_param", "2": "header"}
_OPEN = {
    "access": "open",
    "access_provider": None,
    "auth_method": None,
    "auth_params": None,
    "auth_param_name": None,
    "registration_url": None,
}


def _access_source(feed):
    """``(catalogue, record)`` whose access details apply: the record
    supplying the crawl URL, or, for a feed without one, the Atlas record
    when it needs a key, else the MDB one."""
    atlas = feed.get("atlas") or {}
    url = crawl.feed_url(feed)
    if url is None:
        from_atlas = bool(atlas.get("requires_auth"))
    else:
        from_atlas = url == (atlas.get("urls") or {}).get("static_current")
    return ("atlas", atlas) if from_atlas else ("mdb", feed.get("mdb") or {})


def _registration_page(feed):
    """The Atlas ``info_url``, else the MDB registration page."""
    atlas_auth = (feed.get("atlas") or {}).get("authorization") or {}
    return overrides.web_page(atlas_auth.get("info_url")) or overrides.web_page(
        (feed.get("mdb") or {}).get("authentication_info")
    )


def _catalogue_access(feed):
    """The feed's access details as its catalogues give them.

    ``auth_params`` stays null: binding the catalogue's parameter or header
    name (``auth_param_name``) to a credential field needs the provider.
    """
    catalogue, record = _access_source(feed)
    if not record.get("requires_auth"):
        return dict(_OPEN)
    if catalogue == "atlas":
        block = record.get("authorization") or {}
        method = block.get("type")
        method = method if method in _ATLAS_METHODS else "unsupported"
        name = block.get("param_name")
    else:
        method = _MDB_METHODS.get(record.get("authentication_type"), "unsupported")
        name = record.get("api_key_parameter_name")
    names = overrides.PARAM_NAMES.get(method)
    if names is None or not isinstance(name, str) or not names.fullmatch(name):
        name = None
    return {
        "access": "key",
        "access_provider": None,
        "auth_method": method,
        "auth_params": None,
        "auth_param_name": name,
        "registration_url": _registration_page(feed),
    }


def _apply(feed, entry):
    identity = entry.get("set_identity") or {}
    old_id = feed["feed_id"]
    new_id = identity.get("feed_id")
    if "feed_id" in identity or "onestop_id" in identity:
        # A curator-supplied id is authoritative, not machine-minted.
        feed["id_minted"] = False
    for field, value in identity.items():
        feed[field] = value
    if new_id and new_id != old_id:
        # Preserve the old id in aliases — after any aliases the override itself
        # set — so the override chain and a crawl artifact filed under it still
        # resolve to this feed.
        aliases = feed.setdefault("aliases", [])
        if old_id not in aliases:
            aliases.append(old_id)
    if "set_access" in entry:
        spec = entry["set_access"]
        feed.update(access="key", registration_url=_registration_page(feed))
        if "access_provider" in spec:
            feed["access_provider"] = spec["access_provider"]
        if "auth_method" in spec:
            feed.update(
                auth_method=spec["auth_method"],
                auth_params=dict(spec["auth_params"]),
                auth_param_name=None,
            )
        if feed.get("spec") == "gtfs":
            feed["crawlable"] = False
            feed["uncrawlable_reason"] = AUTH_REASON
    if "mark_uncrawlable" in entry:
        spec = entry["mark_uncrawlable"]
        feed["crawlable"] = False
        feed["uncrawlable_reason"] = (
            spec.get("reason") if isinstance(spec, dict) else None
        )


_BASIC_FIELDS = ["password", "username"]


def _bind_access(feed, providers, report):
    """Settle a protected feed's provider and request details.

    The provider is the curated one, else the one claiming the feed's URL; a
    feed with none stays unresolved. A catalogue parameter or header name is
    bound to a provider's only credential field, and catalogue basic auth
    needs the fields ``username`` and ``password``. A pair that cannot be
    bound, or one naming a field the provider does not declare, is reported
    and the feed left ``unsupported``.
    """
    if feed["access"] != "key":
        return
    url = crawl.feed_url(feed)
    provider_id = feed["access_provider"]

    def error(message):
        report.append(
            {
                "kind": "curation_error",
                "feed_id": feed["feed_id"],
                "access_provider": provider_id,
                "error": message,
            }
        )

    if provider_id is None:
        provider_id = overrides.claiming_provider(url, providers)
    elif provider_id not in providers:
        error("access_provider names no accepted provider")
        provider_id = None
    feed["access_provider"] = provider_id
    if provider_id is None:
        report.append(
            {
                "kind": "unresolved",
                "feed_id": feed["feed_id"],
                "url": url,
                "registration_url": feed["registration_url"],
            }
        )
        return
    fields = providers[provider_id]["credential_fields"]
    method, params = feed["auth_method"], feed["auth_params"]
    if params is None:
        name = feed["auth_param_name"]
        if method in ("basic_auth", "unsupported"):
            params = {}
        elif method is not None and name is not None and len(fields) == 1:
            params = {name: fields[0]}
        else:
            error("needs a curated auth_method and auth_params")
            method = None
    if method == "basic_auth" and sorted(fields) != _BASIC_FIELDS:
        error("basic_auth needs the credential fields username and password")
        method = None
    elif method is not None and not set(params.values()) <= set(fields):
        error("auth_params names a field the provider does not declare")
        method = None
    if method is None:
        method, params = "unsupported", {}
    feed.update(auth_method=method, auth_params=params)


def resolve(cache_dir, *, overrides_dir=None):
    """Resolve feed identity, access and crawlability; publish the
    ``feeds_resolved`` gen.

    Reads the crosswalk feeds, applies matching feed overrides, stamps every feed
    with a ``crawlable`` flag (and any ``uncrawlable_reason``), and republishes
    them. One writer lock spans the read and the publish. Returns the manifest.
    """
    feed_overrides, feeds_digest = overrides.load_feed_overrides(overrides_dir)
    providers, refused, providers_digest = overrides.load_access_providers(
        overrides_dir
    )
    directory = store.open_subdir(cache_dir, "resolve")
    try:
        with store.exclusive_writer(directory):
            feeds, crosswalk_manifest = store.read_jsonl(
                cache_dir / "crosswalk", "feeds.json", "feeds.jsonl"
            )
            # Build the whole feed<->override match graph first, so neither an
            # override matching several feeds nor several overrides matching one
            # feed can slip through a first-match shortcut.
            ref_to_feeds = collections.defaultdict(list)
            for feed in feeds:
                feed.update(_catalogue_access(feed))
                # Only static GTFS is crawled in v1; GTFS-RT and GBFS are
                # indexed but never fetched.
                if "crawlable" not in feed:
                    gated = feed.get("spec") == "gtfs" and feed["access"] == "key"
                    feed["crawlable"] = feed.get("spec") == "gtfs" and not gated
                    feed["uncrawlable_reason"] = AUTH_REASON if gated else None
                feed.setdefault("uncrawlable_reason", None)
                refs = _matching_refs(feed_overrides, feed)
                if len(refs) > 1:
                    raise overrides.OverrideError(
                        f"feed {feed['feed_id']!r} matched by several overrides: "
                        f"{sorted(refs)}"
                    )
                for ref in refs:
                    ref_to_feeds[ref].append(feed)
            ambiguous = sorted(ref for ref, hit in ref_to_feeds.items() if len(hit) > 1)
            if ambiguous:
                raise overrides.OverrideError(
                    f"override matches several feeds: {ambiguous}"
                )
            for ref, hit in ref_to_feeds.items():
                _apply(hit[0], feed_overrides[ref])
            matched = set(ref_to_feeds)
            _check_namespace(feeds)
            report = [{"kind": "refused_provider", **row} for row in refused]
            for feed in feeds:
                _bind_access(feed, providers, report)
            kinds = collections.Counter(row["kind"] for row in report)
            manifest = {
                "source": "resolve",
                # The catalogue versions the feeds were built from, carried
                # forward so later stages label their output with the same ones.
                "sources": crosswalk_manifest.get("sources"),
                # The exact crosswalk generation resolved from: publish
                # refuses these feeds once the crosswalk has moved on.
                "crosswalk_generation": crosswalk_manifest.get("generation"),
                "feeds": len(feeds),
                "overridden_feeds": len(matched),
                "uncrawlable": sum(1 for feed in feeds if not feed["crawlable"]),
                # Refused GTFS feeds whose URL is key-gated, whatever reason an
                # override gave.
                "requires_auth": sum(
                    1
                    for feed in feeds
                    if not feed["crawlable"]
                    and feed.get("spec") == "gtfs"
                    and feed["access"] == "key"
                ),
                "unmatched_overrides": sorted(set(feed_overrides) - matched),
                # Listed in the access report for curation.
                "access_providers": len(providers),
                "access_unresolved": kinds["unresolved"],
                "access_curation_errors": len(report) - kinds["unresolved"],
                "access_providers_sha256": providers_digest,
                # The exact feeds.yaml applied, and its identity and
                # crawlability operations alone: coverage must see the same
                # ones, while a set_coverage edit does not send this stage back.
                "feeds_overrides_sha256": feeds_digest,
                "feeds_resolve_sha256": overrides.phase_digest(
                    feed_overrides, overrides.RESOLVE_OPERATIONS
                ),
                "retrieved_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }
            return store.publish(
                cache_dir / "resolve",
                RESOLVE_POINTER,
                {
                    RESOLVE_ARTIFACT: store.jsonl_chunks(feeds),
                    ACCESS_REPORT: store.jsonl_chunks(report),
                },
                manifest,
                held=directory,
            )
    finally:
        directory.close()
