"""Load the maintainer override files (``overrides/*.yaml``).

Overrides are the durable curation the plan protects: the generated index is
disposable, the override file is the asset, and every build reads them fresh.
This loads the feed-keyed and edge-keyed files and returns their entries for
the stages that own them to apply; staleness detection belongs to those
stages. Place overrides are added with the stage that applies them.
"""

import collections
import datetime
import hashlib
import itertools
import json
import os
import pathlib

from transitio_index import registry as _registry
from transitio_index import store

import re

FEEDS_FILE = "feeds.yaml"
EDGES_FILE = "edges.yaml"
PLACES_FILE = "places.yaml"
CATALOGUE_EXCEPTIONS_FILE = "catalogue_exceptions.yaml"
ACCESS_PROVIDERS_FILE = "access_providers.yaml"
PLACE_KINDS = ("country", "region", "city", "metro")
COVERAGE_LEVELS = ("municipality", "subdivision", "country", "bbox", "geohash")
TIERS = ("local", "regional", "national", "international", "unknown")

# The identity fields ``set_identity`` may correct — ``feed_id`` included, since
# the corrected id is the crawl cache key. Renaming it preserves the old id in
# ``aliases`` (the resolve stage) so a later override filed against it still lands.
IDENTITY_FIELDS = frozenset(
    {
        "feed_id",
        "onestop_id",
        "mdb_id",
        "name",
        "aliases",
        "static_feed_id",
        "static_link_method",
    }
)

# A feed id flows into JSONL, the Parquet index and a crawl-cache path component,
# so reject values that would corrupt those: empty, over-long, control bytes, a
# path separator, or a traversal. Full path-component portability (Windows
# reserved names and the like) is the crawl stage's own concern.
_MAX_FEED_ID = 512
_UNSAFE_ID = re.compile(r"[\x00-\x1f\x7f/\\]")
_NULLABLE_STR = ("onestop_id", "mdb_id", "name", "static_feed_id", "static_link_method")

# static_link_method is a closed set the crosswalk assigns; an override may only
# set it to one of these or null, never an arbitrary string.
_STATIC_LINK_METHODS = frozenset({"declared", "same_file", "same_host", "none"})

# The operations a feed entry may carry. ``add_feed`` is applied by the
# crosswalk and ``set_coverage`` by the coverage stage, not the resolve stage,
# but both are valid keys here.
_OPERATIONS = frozenset(
    {"set_identity", "mark_uncrawlable", "set_coverage", "set_access", "add_feed"}
)
# The operation the crosswalk applies: a curated feed joins the feed set there.
CROSSWALK_OPERATIONS = frozenset({"add_feed"})
# The operations whose effect the resolve stage settles, a curated feed's
# access among them; set_coverage enters at coverage.
RESOLVE_OPERATIONS = frozenset(
    {"set_identity", "mark_uncrawlable", "set_access", "add_feed"}
)
# A curated feed's id: ``f-curated-`` and lower-case words joined by hyphens.
# The prefix is add_feed's alone; no catalogue mint or other override takes it.
_CURATED_PREFIX = "f-curated-"
_CURATED_ID = re.compile(re.escape(_CURATED_PREFIX) + r"[a-z0-9]+(-[a-z0-9]+)*")
_ADD_FEED_REQUIRED = frozenset({"name", "url", "spec", "license", "location"})
_ADD_FEED_ACCESS = frozenset({"access_provider", "auth_method", "auth_params"})
_ADD_FEED_FIELDS = (
    _ADD_FEED_REQUIRED | _ADD_FEED_ACCESS | {"access", "registration_url"}
)
# The licence fields of a Transitland Atlas record, which a curated feed's
# licence is read as: text, and terms answered yes, no or unknown.
_LICENCE_TEXT = frozenset(
    {"spdx_identifier", "url", "attribution_text", "attribution_instructions"}
)
_LICENCE_TERMS = frozenset(
    {
        "use_without_attribution",
        "create_derived_product",
        "redistribution_allowed",
        "commercial_use_allowed",
        "share_alike_optional",
    }
)
_LICENCE_FIELDS = _LICENCE_TEXT | _LICENCE_TERMS
# A declared location in the Mobility Database's shape.
LOCATION_FIELDS = ("country_code", "subdivision_name", "municipality")

# How a request to a key-protected feed carries the credentials; catalogue
# methods transitio does not send (a key in the path, a URL template) are
# ``unsupported``.
AUTH_METHODS = ("query_param", "header", "basic_auth", "unsupported")
# A provider id; a provider's credential field; a header name (an RFC 9110
# token); a query parameter name, sent percent-encoded (``acl:consumerKey``).
PROVIDER_ID = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
_CREDENTIAL_FIELD = re.compile(r"[a-z][a-z0-9]*(_[a-z0-9]+)*")
PARAM_NAMES = {
    "header": re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+"),
    "query_param": re.compile(r"[^\x00-\x20\x7f]+"),
}
_METADATA = frozenset({"feed", "reason", "author", "date", "evidence_hash"})
_EXCEPTION_KEYS = frozenset({"feed", "reason", "author", "date"})


class OverrideError(RuntimeError):
    """An override file is malformed."""


_LOADER = None


def _strict_loader():
    """A YAML loader that rejects duplicate mapping keys (PyYAML keeps the last)."""
    global _LOADER
    if _LOADER is None:
        import yaml

        class _StrictLoader(yaml.SafeLoader):
            pass

        def _no_duplicate_keys(loader, node, deep=False):
            mapping = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node, deep=deep)
                if key in mapping:
                    raise OverrideError(f"duplicate key {key!r} in an override entry")
                mapping[key] = loader.construct_object(value_node, deep=deep)
            return mapping

        _StrictLoader.add_constructor(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
        )
        _LOADER = _StrictLoader
    return _LOADER


def _valid_feed_id(value):
    return (
        isinstance(value, str)
        and 0 < len(value) <= _MAX_FEED_ID
        and value not in (".", "..")
        and _UNSAFE_ID.search(value) is None
    )


def _validate_identity(path, ref, identity):
    """Reject a ``set_identity`` whose fields or values would corrupt the feed."""
    if not isinstance(identity, dict) or not identity:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity must be a non-empty mapping"
        )
    unknown = set(identity) - IDENTITY_FIELDS
    if unknown:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity has unknown fields {sorted(unknown)}"
        )
    if "feed_id" in identity and not _valid_feed_id(identity["feed_id"]):
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity feed_id {identity['feed_id']!r} is "
            f"not a valid feed id"
        )
    for field in _NULLABLE_STR:
        value = identity.get(field)
        if field in identity and value is not None and not isinstance(value, str):
            raise OverrideError(
                f"{path}: feed {ref!r} set_identity {field} must be a string or null"
            )
    if "aliases" in identity:
        aliases = identity["aliases"]
        if not isinstance(aliases, list) or not all(
            isinstance(alias, str) for alias in aliases
        ):
            raise OverrideError(
                f"{path}: feed {ref!r} set_identity aliases must be a list of strings"
            )
    method = identity.get("static_link_method")
    if method is not None and method not in _STATIC_LINK_METHODS:
        raise OverrideError(
            f"{path}: feed {ref!r} set_identity static_link_method {method!r} must "
            f"be one of {sorted(_STATIC_LINK_METHODS)} or null"
        )
    for value in [identity.get("feed_id"), *(identity.get("aliases") or [])]:
        if isinstance(value, str) and value.startswith(_CURATED_PREFIX):
            raise OverrideError(
                f"{path}: feed {ref!r} set_identity cannot take the curated id "
                f"{value!r}"
            )


def _validate_access(where, spec):
    """Reject access fields that are not a provider id, one whole,
    well-formed auth pair, or both: ``auth_params`` maps each query parameter
    (at least one) or the one header to a credential field, and is empty for
    basic auth and unsupported."""
    keys = {"access_provider", "auth_method", "auth_params"}
    if not isinstance(spec, dict) or not spec or set(spec) - keys:
        raise OverrideError(
            f"{where} must be a mapping of access_provider, auth_method and "
            "auth_params"
        )
    if "access_provider" in spec and not (
        isinstance(spec["access_provider"], str)
        and PROVIDER_ID.fullmatch(spec["access_provider"])
    ):
        raise OverrideError(f"{where} access_provider must be a provider id")
    if ("auth_method" in spec) != ("auth_params" in spec):
        raise OverrideError(f"{where} sets auth_method and auth_params together")
    if "auth_method" not in spec:
        return
    method, params = spec["auth_method"], spec["auth_params"]
    if method not in AUTH_METHODS:
        raise OverrideError(f"{where} auth_method must be one of {list(AUTH_METHODS)}")
    if method == "query_param":
        fits = isinstance(params, dict) and bool(params)
    elif method == "header":
        fits = isinstance(params, dict) and len(params) == 1
    else:
        fits = params == {}
    if not fits:
        raise OverrideError(f"{where} auth_params does not fit auth_method {method!r}")
    if not all(
        isinstance(name, str)
        and PARAM_NAMES[method].fullmatch(name)
        and isinstance(field, str)
        and _CREDENTIAL_FIELD.fullmatch(field)
        for name, field in params.items()
    ):
        raise OverrideError(
            f"{where} auth_params must map parameter names to credential fields"
        )


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _validate_add_feed(path, ref, entry):
    """Reject an ``add_feed`` that is not a whole curated feed: an
    ``f-curated-<slug>`` id, a name, an http(s) URL, the spec ``gtfs``, a
    licence in the Atlas fields, a location naming a country and optionally
    its subdivision and municipality, and, for a feed that needs a key
    (``access: key``), optionally its registration page, provider and auth
    pair. Its identity and access are its own: no set_identity or
    set_access."""
    where = f"{path}: feed {ref!r} add_feed"
    spec = entry["add_feed"]
    if not (_CURATED_ID.fullmatch(ref) and _valid_feed_id(ref)):
        raise OverrideError(f"{where} needs an id f-curated-<slug>")
    if (
        not isinstance(spec, dict)
        or _ADD_FEED_REQUIRED - set(spec)
        or set(spec) - _ADD_FEED_FIELDS
    ):
        raise OverrideError(
            f"{where} must be a mapping of {sorted(_ADD_FEED_REQUIRED)} and the "
            "optional access fields"
        )
    if {"set_identity", "set_access"} & set(entry):
        raise OverrideError(f"{where} carries its own identity and access")
    licence, location = spec["license"], spec["location"]
    access = spec.get("access", "open")
    problems = (
        (not _text(spec["name"]), "name must be a non-empty string"),
        (web_page(spec["url"]) is None, "url must be an http(s) URL"),
        (spec["spec"] != "gtfs", "spec must be gtfs"),
        (
            not isinstance(licence, dict)
            or not licence
            or set(licence) - _LICENCE_FIELDS
            or not all(
                (
                    _text(value)
                    if key in _LICENCE_TEXT
                    else isinstance(value, bool) or value in ("yes", "no", "unknown")
                )
                for key, value in licence.items()
            )
            or ("url" in licence and web_page(licence["url"]) is None),
            f"license must be a mapping of {sorted(_LICENCE_FIELDS)}",
        ),
        (
            not isinstance(location, dict)
            or set(location) - set(LOCATION_FIELDS)
            or not re.fullmatch(r"[A-Z]{2}", str(location.get("country_code")))
            or not all(location.get(k) is None or _text(location[k]) for k in location),
            "location must give a two-letter upper-case country_code, and may "
            "name its subdivision_name and municipality",
        ),
        (access not in ("open", "key"), "access must be open or key"),
        (
            access == "open" and set(spec) & (_ADD_FEED_ACCESS | {"registration_url"}),
            "an open feed takes no access fields",
        ),
        (
            "registration_url" in spec and web_page(spec["registration_url"]) is None,
            "registration_url must be an http(s) URL",
        ),
    )
    for broken, message in problems:
        if broken:
            raise OverrideError(f"{where} {message}")
    pair = {key: spec[key] for key in _ADD_FEED_ACCESS if key in spec}
    if pair:
        _validate_access(where, pair)


def _validate_operations(path, ref, entry):
    if not set(entry) & _OPERATIONS:
        raise OverrideError(f"{path}: feed {ref!r} carries no operation")
    if "add_feed" in entry:
        _validate_add_feed(path, ref, entry)
    if "set_identity" in entry:
        _validate_identity(path, ref, entry["set_identity"])
    if "mark_uncrawlable" in entry:
        spec = entry["mark_uncrawlable"]
        if spec is not True and not isinstance(spec, dict):
            raise OverrideError(
                f"{path}: feed {ref!r} mark_uncrawlable must be true or a mapping"
            )
    if "set_access" in entry:
        _validate_access(f"{path}: feed {ref!r} set_access", entry["set_access"])
    if "set_coverage" in entry:
        spec = entry["set_coverage"]
        if not isinstance(spec, dict) or set(spec) != {"level", "place_id"}:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage must be a mapping of level and "
                "place_id"
            )
        if spec["level"] not in COVERAGE_LEVELS:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage level must be one of "
                f"{list(COVERAGE_LEVELS)}"
            )
        if not isinstance(spec["place_id"], str) or not spec["place_id"]:
            raise OverrideError(
                f"{path}: feed {ref!r} set_coverage place_id must be a place id"
            )


def _feed_entries(overrides_dir, name, allowed):
    """``(path, entries, digest)`` of the feed-keyed override file ``name``:
    its entries by ``feed`` reference and the digest of its bytes, with no
    entries and no digest when there is no directory or file. Every entry
    must be a mapping of ``allowed`` keys whose ``feed`` is a non-empty
    string no other entry repeats."""
    if overrides_dir is None:
        return None, {}, None
    path = pathlib.Path(overrides_dir) / name
    data, digest = read_override(overrides_dir, name)
    if data is None:
        return path, {}, None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return path, {}, digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    by_feed = {}
    for entry in raw:
        if not isinstance(entry, dict) or "feed" not in entry:
            raise OverrideError(f"{path}: every entry needs a 'feed' key")
        ref = entry["feed"]
        if not isinstance(ref, str) or not ref:
            raise OverrideError(f"{path}: a 'feed' key must be a non-empty string")
        if ref in by_feed:
            raise OverrideError(f"{path}: duplicate override for feed {ref!r}")
        unknown = set(entry) - allowed
        if unknown:
            raise OverrideError(
                f"{path}: feed {ref!r} has unknown keys {sorted(unknown)}"
            )
        by_feed[ref] = entry
    return path, by_feed, digest


def load_feed_overrides(overrides_dir, *, registry=None):
    """The ``feeds.yaml`` entries keyed by feed reference and the digest of
    the bytes they came from: ``({}, None)`` when absent.

    Returns ``(feed_ref -> entry, digest)``. A duplicate reference, an entry
    with no ``feed`` key or no operation, an unknown operation, or a malformed
    operation value is a build error rather than a silent skip.
    """
    path, by_feed, digest = _feed_entries(
        overrides_dir, FEEDS_FILE, _OPERATIONS | _METADATA
    )
    for ref, entry in by_feed.items():
        _validate_operations(path, ref, entry)
        if registry is not None and "set_coverage" in entry:
            entry["set_coverage"]["place_id"] = _key(
                registry,
                entry["set_coverage"]["place_id"],
                f"{path}: feed {ref!r}",
                strict=True,
            )
    return by_feed, digest


def load_catalogue_exceptions(overrides_dir):
    """``{catalogue id: reason}`` from ``catalogue_exceptions.yaml``: the MDB
    and Atlas GTFS ids and the ``add_feed`` ids a merged index may lack, each
    entry naming the id as ``feed`` with a non-empty ``reason`` (and
    optionally an ``author`` and a ``date``). No file means no exceptions."""
    path, entries, _ = _feed_entries(
        overrides_dir, CATALOGUE_EXCEPTIONS_FILE, _EXCEPTION_KEYS
    )
    for ref, entry in entries.items():
        reason = entry.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise OverrideError(f"{path}: feed {ref!r} needs a reason")
    return {ref: entry["reason"] for ref, entry in entries.items()}


# ---- access_providers.yaml ----

_PROVIDER_REQUIRED = frozenset(
    {"provider_id", "name", "registration_url", "credential_fields", "crawl_approved"}
)
_PROVIDER_OPTIONAL = frozenset(
    {
        "docs_url",
        "terms_url",
        "url_prefixes",
        "free",
        "terms_checked",
        "terms_note",
        "crawl_budget",
    }
)


def web_page(value):
    """``value`` when it is an http(s) URL the crawler would contact, with no
    whitespace or control character, else None."""
    from transitio_index import fetch

    if not isinstance(value, str) or re.search(r"[\x00-\x20\x7f]", value):
        return None
    try:
        fetch.check_url(value)
    except fetch.FetchError:
        return None
    return value


def prefix_scope(value):
    """The :func:`fetch.url_scope` of a provider's URL prefix, without the
    path's trailing empty segment; None unless ``value`` is an https URL with
    no userinfo, query, fragment or empty path segment."""
    from transitio_index import fetch

    if not isinstance(value, str) or "?" in value or "#" in value:
        return None
    scope = fetch.url_scope(value)
    if scope is None or scope[0] != "https":
        return None
    segments = scope[3][:-1] if scope[3][-1:] == ("",) else scope[3]
    return None if "" in segments else scope[:3] + (segments,)


def _covers(prefix, scope):
    """Whether ``prefix`` covers ``scope``: the same scheme, host and port,
    and the prefix's path segments leading the scope's."""
    return prefix[:3] == scope[:3] and scope[3][: len(prefix[3])] == prefix[3]


def claiming_provider(url, providers):
    """The id of the provider with a URL prefix covering ``url``, or None."""
    from transitio_index import fetch

    scope = fetch.url_scope(url)
    if scope is None:
        return None
    for provider_id, provider in providers.items():
        for prefix in provider["url_prefixes"]:
            if _covers(prefix_scope(prefix), scope):
                return provider_id
    return None


def _date(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value.isoformat()
    try:
        return datetime.date.fromisoformat(value).isoformat()
    except (TypeError, ValueError):
        raise OverrideError("terms_checked must be a date") from None


def _provider(entry):
    """``entry`` as an accepted provider, its URL prefixes in canonical form
    (``https://host:port/path/``); :class:`OverrideError` naming the first
    rule it breaks."""
    if not isinstance(entry, dict):
        raise OverrideError("an entry must be a mapping")
    missing = _PROVIDER_REQUIRED - set(entry)
    unknown = set(entry) - _PROVIDER_REQUIRED - _PROVIDER_OPTIONAL
    if missing or unknown:
        raise OverrideError(
            f"missing keys {sorted(missing)}, unknown keys {sorted(unknown, key=str)}"
        )
    if not isinstance(entry["provider_id"], str) or not PROVIDER_ID.fullmatch(
        entry["provider_id"]
    ):
        raise OverrideError("provider_id must be lower-case words joined by hyphens")
    if not isinstance(entry["name"], str) or not entry["name"].strip():
        raise OverrideError("name must be a non-empty string")
    pages = {
        key: entry.get(key) for key in ("registration_url", "docs_url", "terms_url")
    }
    for key, value in pages.items():
        if (key == "registration_url" or value is not None) and not web_page(value):
            raise OverrideError(f"{key} must be an http(s) URL")
    fields = entry["credential_fields"]
    if not (
        isinstance(fields, list)
        and fields
        and all(isinstance(f, str) and _CREDENTIAL_FIELD.fullmatch(f) for f in fields)
        and len(set(fields)) == len(fields)
    ):
        raise OverrideError(
            "credential_fields must be a non-empty list of unique field names"
        )
    prefixes = entry.get("url_prefixes", [])
    scopes = [prefix_scope(p) for p in prefixes] if isinstance(prefixes, list) else []
    if not isinstance(prefixes, list) or None in scopes:
        raise OverrideError(
            "url_prefixes must be https URLs with a host and a path, without "
            "userinfo, query or fragment"
        )
    if not isinstance(entry.get("free"), (bool, type(None))):
        raise OverrideError("free must be true, false or null")
    approved = entry["crawl_approved"]
    if not isinstance(approved, bool):
        raise OverrideError("crawl_approved must be true or false")
    checked = entry.get("terms_checked")
    checked = None if checked is None else _date(checked)
    note = entry.get("terms_note")
    if note is not None and (not isinstance(note, str) or not note.strip()):
        raise OverrideError("terms_note must be a non-empty string")
    if approved and (checked is None or note is None):
        raise OverrideError("crawl_approved needs terms_checked and terms_note")
    budget = entry.get("crawl_budget")
    if budget is not None and (type(budget) is not int or budget < 1):
        raise OverrideError("crawl_budget must be a whole number of at least 1")
    canonical = set()
    for _, host, port, segments in scopes:
        host = f"[{host}]" if ":" in host else host
        canonical.add(f"https://{host}:{port}/" + "".join(f"{s}/" for s in segments))
    return {
        "provider_id": entry["provider_id"],
        "name": entry["name"],
        **pages,
        "credential_fields": list(fields),
        "url_prefixes": sorted(canonical),
        "free": entry.get("free"),
        "crawl_approved": approved,
        "terms_checked": checked,
        "terms_note": note,
        "crawl_budget": budget,
    }


def load_access_providers(overrides_dir):
    """``(providers, refused, digest)``: the accepted ``access_providers.yaml``
    entries by ``provider_id``, a ``{"provider_id", "error"}`` row for each
    entry refused, and the digest of the file's bytes — ``({}, [], None)``
    when there is no file.

    An entry carries its ``provider_id`` (lower-case words joined by hyphens,
    unique in the file), ``name``, ``registration_url``, the
    ``credential_fields`` its accounts issue (``key``, ``client_id``) and
    ``crawl_approved``; optionally ``docs_url``, ``terms_url``, ``free``,
    ``url_prefixes`` (the https URLs under which it claims feeds), the
    ``terms_checked`` date with a ``terms_note``, which an approval needs,
    and ``crawl_budget``, the keyed requests a month the crawl may make.
    An entry breaking these rules is refused; two providers whose prefixes
    cover one URL are a build error.
    """
    if overrides_dir is None:
        return {}, [], None
    path = pathlib.Path(overrides_dir) / ACCESS_PROVIDERS_FILE
    data, digest = read_override(overrides_dir, ACCESS_PROVIDERS_FILE)
    if data is None:
        return {}, [], None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return {}, [], digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of provider entries")
    ids = [entry.get("provider_id") for entry in raw if isinstance(entry, dict)]
    counts = collections.Counter(i for i in ids if isinstance(i, str))
    providers, refused = {}, []
    for entry in raw:
        provider_id = entry.get("provider_id") if isinstance(entry, dict) else None
        try:
            provider = _provider(entry)
            if counts[provider_id] > 1:
                raise OverrideError("provider_id is not unique")
        except OverrideError as error:
            if not isinstance(provider_id, str):
                provider_id = None
            refused.append({"provider_id": provider_id, "error": str(error)})
            continue
        providers[provider_id] = provider
    claims = [
        (prefix_scope(prefix), provider_id)
        for provider_id, provider in providers.items()
        for prefix in provider["url_prefixes"]
    ]
    for (one, owner), (other, rival) in itertools.combinations(claims, 2):
        if owner != rival and (_covers(one, other) or _covers(other, one)):
            raise OverrideError(
                f"{path}: providers {owner!r} and {rival!r} have overlapping "
                "url_prefixes"
            )
    return providers, refused, digest


def read_override(overrides_dir, name):
    """``(bytes, sha256)`` of the override file ``name`` under
    ``overrides_dir``, or ``(None, None)`` when it does not exist.

    The directory is opened refusing a symlink at its own component, and
    the fixed basename relative to that directory descriptor — never
    following a symlink, non-blocking so a FIFO cannot wedge the open, and
    checked on the descriptor to be a regular file — through the store's
    own helpers, so an override file is repository data and never a pointer
    to something outside it. Read once: what is parsed is what is hashed.
    """
    try:
        directory = store.open_directory(pathlib.Path(overrides_dir))
    except store.StoreError as error:
        raise OverrideError(str(error)) from error
    try:
        try:
            handle = store.open_regular(directory, name)
        except store.MissingEntry:
            return None, None
        except store.StoreError as error:
            raise OverrideError(str(error)) from error
        with os.fdopen(handle, "rb") as opened:
            data = opened.read()
    finally:
        directory.close()
    return data, hashlib.sha256(data).hexdigest()


def applied_digest(manifest, overrides_dir):
    """The digest of the current ``edges.yaml`` an edge generation must have
    applied, or :class:`OverrideError`: a curate generation whose recorded
    digest differs from the file was built from another version of it, and
    an ``edges.yaml`` with no curate generation at all is a stage that has
    not run. Returns the current digest (None without a file)."""
    current = edges_digest(overrides_dir)
    if manifest is not None and manifest.get("source") in ("curate", "rank"):
        if manifest.get("overrides_sha256") != current:
            raise OverrideError(
                "edges.yaml changed since the curate stage applied it; "
                "re-run the curate stage"
            )
    elif current is not None:
        raise OverrideError(
            "edge overrides exist but no curate generation applied them; "
            "run the curate stage"
        )
    return current


def edges_digest(overrides_dir):
    """The SHA-256 of the exact ``edges.yaml`` bytes, or None when there is
    no file: what a curate generation records, and what publish checks the
    current file against, so an edited override can never ship through a
    generation built before the edit."""
    return override_digest(overrides_dir, EDGES_FILE)


def override_digest(overrides_dir, name):
    """The SHA-256 of the override file ``name``'s current bytes, or None
    without the file or an overrides directory."""
    if overrides_dir is None:
        return None
    return read_override(overrides_dir, name)[1]


# ---- edges.yaml ----

# One operation per entry. ``set_tiers`` is pair-scoped (no ``tier``); the
# other three name the tier edge they touch.
_EDGE_OPERATIONS = frozenset({"set_tiers", "set_selector", "add_edge", "remove_edge"})
_EDGE_METADATA = frozenset(
    {"feed", "place", "tier", "reason", "author", "date", "evidence_hash"}
)
# Only a tier decision carries a confidence.
_CONFIDENCE_OPERATIONS = frozenset({"set_tiers", "add_edge"})
_SELECTOR_CLAUSES = frozenset({"route_id", "agency_id", "route_type"})


def _validate_selector(path, where, spec):
    """A selector is ``whole_feed``, an explicit ``route_id`` list, or a
    predicate of ``agency_id`` / ``route_type`` lists (ANDed); never both an
    id list and a predicate, which would leave the ids' meaning ambiguous."""
    if spec == "whole_feed":
        return
    if not isinstance(spec, dict) or not spec:
        raise OverrideError(
            f"{path}: {where} selector must be 'whole_feed' or a non-empty mapping"
        )
    unknown = set(spec) - _SELECTOR_CLAUSES
    if unknown:
        raise OverrideError(
            f"{path}: {where} selector has unknown keys {sorted(unknown)}"
        )
    if "route_id" in spec and len(spec) > 1:
        raise OverrideError(
            f"{path}: {where} selector lists route ids and a predicate; pick one"
        )
    for key, kind in (("route_id", str), ("agency_id", str), ("route_type", int)):
        values = spec.get(key)
        if key in spec and (
            not isinstance(values, list)
            or not values
            or any(not isinstance(v, kind) or isinstance(v, bool) for v in values)
        ):
            raise OverrideError(
                f"{path}: {where} selector {key} must be a non-empty list of "
                f"{kind.__name__}"
            )


def _validate_confidence(path, where, value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OverrideError(f"{path}: {where} tier_confidence must be a number")
    if not 0.0 <= float(value) <= 1.0:
        raise OverrideError(f"{path}: {where} tier_confidence must lie in [0, 1]")


def load_edge_overrides(overrides_dir, *, registry=None):
    """``(entries, sha256)``: the ``edges.yaml`` entries in file order and
    the digest of the bytes they were parsed from — ``([], None)`` when
    there is no file.

    Each entry gains an ``operation`` key naming its single operation. Every
    entry needs ``feed`` and ``place`` (``"*"`` for every place the feed
    serves); tier-edge operations need ``tier`` (``"*"`` allowed), pair-scoped
    ``set_tiers`` must not carry one. Two entries with the same keys and
    operation are a duplicate, a build error rather than a silent skip.
    """
    if overrides_dir is None:
        return [], None
    path = pathlib.Path(overrides_dir) / EDGES_FILE
    data, digest = read_override(overrides_dir, EDGES_FILE)
    if data is None:
        return [], None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return [], digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    entries = []
    seen = set()
    for entry in raw:
        if not isinstance(entry, dict):
            raise OverrideError(f"{path}: every entry must be a mapping")
        for key in ("feed", "place"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                raise OverrideError(f"{path}: every entry needs a non-empty '{key}'")
        where = f"{entry['feed']}/{entry['place']}"
        if registry is not None and entry["place"] != "*":
            entry["place"] = _key(
                registry, entry["place"], f"{path}: {where}", strict=True
            )
        unknown = set(entry) - _EDGE_OPERATIONS - _EDGE_METADATA - {"tier_confidence"}
        if unknown:
            raise OverrideError(f"{path}: {where} has unknown keys {sorted(unknown)}")
        operations = sorted(set(entry) & _EDGE_OPERATIONS)
        if len(operations) != 1:
            raise OverrideError(f"{path}: {where} needs exactly one operation")
        (operation,) = operations
        tier = entry.get("tier")
        if operation == "set_tiers":
            if tier is not None:
                raise OverrideError(
                    f"{path}: {where} set_tiers is pair-scoped: no tier"
                )
            tiers = entry["set_tiers"]
            if (
                not isinstance(tiers, list)
                or not tiers
                or any(t not in TIERS for t in tiers)
                or len(set(tiers)) != len(tiers)
            ):
                raise OverrideError(
                    f"{path}: {where} set_tiers must be a non-empty list of tiers"
                )
        else:
            if tier != "*" and tier not in TIERS:
                raise OverrideError(
                    f"{path}: {where} {operation} needs a tier (or '*')"
                )
            where = f"{where}/{tier}"
        if operation == "set_selector":
            _validate_selector(path, where, entry["set_selector"])
        if operation == "add_edge":
            spec = entry["add_edge"]
            if spec is not True and not isinstance(spec, dict):
                raise OverrideError(
                    f"{path}: {where} add_edge must be true or a mapping"
                )
            if isinstance(spec, dict):
                unknown = set(spec) - {"selector", "tier_confidence"}
                if unknown:
                    raise OverrideError(
                        f"{path}: {where} add_edge has unknown keys {sorted(unknown)}"
                    )
                if "selector" in spec:
                    _validate_selector(path, where, spec["selector"])
                if "tier_confidence" in spec:
                    if "tier_confidence" in entry:
                        raise OverrideError(
                            f"{path}: {where} tier_confidence declared twice"
                        )
                    _validate_confidence(path, where, spec["tier_confidence"])
            if tier == "*":
                raise OverrideError(f"{path}: {where} add_edge names one tier")
        if operation == "remove_edge" and entry["remove_edge"] is not True:
            raise OverrideError(f"{path}: {where} remove_edge must be true")
        if "tier_confidence" in entry:
            if operation not in _CONFIDENCE_OPERATIONS:
                raise OverrideError(
                    f"{path}: {where} {operation} takes no tier_confidence"
                )
            _validate_confidence(path, where, entry["tier_confidence"])
        if "evidence_hash" in entry and not isinstance(entry["evidence_hash"], str):
            raise OverrideError(f"{path}: {where} evidence_hash must be a string")
        key = (entry["feed"], entry["place"], tier, operation)
        if key in seen:
            raise OverrideError(f"{path}: duplicate {operation} for {where}")
        seen.add(key)
        entries.append({**entry, "operation": operation})
    return entries, digest


# ---- places.yaml ----

_PLACE_OPERATIONS = frozenset(
    {
        "add_place",
        "set_place_members",
        "set_boundary",
        "set_aliases",
        "resolve_place",
        "set_statistical_area",
    }
)
# The statistical schemes a curated crosswalk may name; a metro published
# under one carries the scheme's code as its ``statistical_area_id``.
STATISTICAL_SCHEMES = frozenset({"eurostat_metro", "eurostat_fua", "fao_city_region"})
_STATISTICAL_AREA_FIELDS = frozenset({"scheme", "code"})
_PLACE_METADATA = frozenset(
    {"place", "source_ref", "reason", "author", "date", "evidence_hash"}
)
_ADD_PLACE_FIELDS = frozenset(
    {"kind", "name", "parent_id", "boundary", "member_ids", "country_code"}
)


def _qid(value):
    return isinstance(value, str) and bool(re.match(r"\AQ[1-9][0-9]*\Z", value))


def is_reference(value):
    """A place reference: a QID, a ``tp_`` id or ``namespace:value``."""
    if not isinstance(value, str) or not value:
        return False
    if _qid(value) or _registry.ID_PATTERN.match(value):
        return True
    namespace, _, rest = value.partition(":")
    return namespace in _registry.NAMESPACES and bool(rest)


def elsewhere(reference, *, own_ids=False):
    """Whether a reference a build does not hold may name another build's
    place: a QID or a concordance may, a string that is no reference never
    does. A registry own id may only in a stage that holds its places under
    their own ids (``own_ids``); the seed and metros stages hold a QID-less or
    derived place under its concordance key until it is identified, so an own
    id they lack may still name one of their places."""
    if not is_reference(reference):
        return False
    return own_ids or not _registry.ID_PATTERN.match(reference)


def _reference_list(value):
    return isinstance(value, list) and bool(value) and all(map(is_reference, value))


def _key(registry, reference, where, **options):
    """``reference`` as the registry's current key for the place it names."""
    try:
        return registry.key_for(reference, **options)
    except _registry.RegistryError as error:
        raise OverrideError(f"{where}: {error}") from None


def _resolve_place_entry(entry, registry, where, internal, pending):
    # Only add_place may name a place no row carries yet, to mint it; the
    # file's other entries may name the places its add_place entries create.
    def key(reference, mint=False):
        return _key(
            registry,
            reference,
            where,
            mint=mint or reference in pending,
            internal=internal,
        )

    entry["place"] = key(entry["place"], mint="add_place" in entry)
    spec = entry.get("add_place")
    if isinstance(spec, dict):
        if "parent_id" in spec:
            spec["parent_id"] = key(spec["parent_id"])
        if "member_ids" in spec:
            spec["member_ids"] = [key(m) for m in spec["member_ids"]]
    if "set_place_members" in entry:
        entry["set_place_members"] = [key(m) for m in entry["set_place_members"]]


def _validate_place_entry(path, entry):
    where = f"place {entry.get('place') or entry.get('source_ref')!r}"
    unknown = set(entry) - _PLACE_OPERATIONS - _PLACE_METADATA
    if unknown:
        raise OverrideError(f"{path}: {where} has unknown keys {sorted(unknown)}")
    operations = sorted(set(entry) & _PLACE_OPERATIONS)
    if len(operations) != 1:
        raise OverrideError(f"{path}: {where} needs exactly one operation")
    (operation,) = operations
    if not isinstance(entry.get("place"), str) or not entry["place"]:
        raise OverrideError(f"{path}: {where} needs a 'place' id")
    if operation in (
        "add_place",
        "resolve_place",
        "set_statistical_area",
    ) and not is_reference(entry["place"]):
        raise OverrideError(
            f"{path}: {where} {operation} needs a place reference as 'place'"
        )
    if operation == "resolve_place":
        if not isinstance(entry.get("source_ref"), str) or not entry["source_ref"]:
            raise OverrideError(f"{path}: {where} resolve_place needs a source_ref")
        if entry["resolve_place"] is not True:
            raise OverrideError(f"{path}: {where} resolve_place must be true")
    elif "source_ref" in entry:
        raise OverrideError(f"{path}: {where} only resolve_place takes a source_ref")
    spec = entry[operation]
    if operation == "add_place":
        if not isinstance(spec, dict):
            raise OverrideError(f"{path}: {where} add_place must be a mapping")
        unknown = set(spec) - _ADD_PLACE_FIELDS
        if unknown:
            raise OverrideError(
                f"{path}: {where} add_place has unknown keys {sorted(unknown)}"
            )
        if spec.get("kind") not in PLACE_KINDS or not (
            isinstance(spec.get("name"), str) and spec["name"].strip()
        ):
            raise OverrideError(f"{path}: {where} add_place needs a kind and a name")
        code = spec.get("country_code")
        if "country_code" in spec and not (
            isinstance(code, str) and re.fullmatch(r"[A-Z]{2}", code)
        ):
            raise OverrideError(
                f"{path}: {where} add_place country_code must be a two-letter "
                "upper-case ISO code"
            )
        if spec["kind"] in ("city", "region") and "parent_id" not in spec:
            raise OverrideError(
                f"{path}: {where} add_place: a {spec['kind']} needs a parent_id"
            )
        if "parent_id" in spec and not is_reference(spec["parent_id"]):
            raise OverrideError(
                f"{path}: {where} add_place parent_id must be a place reference"
            )
        if "boundary" in spec and "member_ids" in spec:
            raise OverrideError(
                f"{path}: {where} add_place takes a boundary or a member list, not both"
            )
        if "boundary" in spec and not isinstance(spec["boundary"], str):
            raise OverrideError(f"{path}: {where} add_place boundary must be WKT")
        if "member_ids" in spec and (
            spec["kind"] != "metro" or not _reference_list(spec["member_ids"])
        ):
            raise OverrideError(
                f"{path}: {where} add_place member_ids belong to a metro, as a list "
                "of place references"
            )
    elif operation == "set_place_members":
        if not _reference_list(spec):
            raise OverrideError(
                f"{path}: {where} set_place_members must be a non-empty list of "
                "place references"
            )
    elif operation == "set_boundary":
        if not isinstance(spec, str) or not spec:
            raise OverrideError(f"{path}: {where} set_boundary must be WKT")
    elif operation == "set_aliases":
        if (
            not isinstance(spec, list)
            or not spec
            or any(not isinstance(a, str) or not a for a in spec)
        ):
            raise OverrideError(
                f"{path}: {where} set_aliases must be a non-empty list of strings"
            )
    elif operation == "set_statistical_area":
        if not isinstance(spec, dict) or set(spec) != _STATISTICAL_AREA_FIELDS:
            raise OverrideError(
                f"{path}: {where} set_statistical_area needs a scheme and a code"
            )
        if spec["scheme"] not in STATISTICAL_SCHEMES:
            raise OverrideError(
                f"{path}: {where} set_statistical_area scheme must be one of "
                f"{sorted(STATISTICAL_SCHEMES)}"
            )
        if not isinstance(spec["code"], str) or not spec["code"].strip():
            raise OverrideError(
                f"{path}: {where} set_statistical_area code must be a non-empty string"
            )
        digest = entry.get("evidence_hash")
        if not isinstance(digest, str) or not store.DIGEST_PATTERN.match(digest):
            raise OverrideError(
                f"{path}: {where} set_statistical_area needs the SHA-256 "
                "evidence_hash of the derived member list it confirms"
            )
    if "evidence_hash" in entry and not isinstance(entry["evidence_hash"], str):
        raise OverrideError(f"{path}: {where} evidence_hash must be a string")
    return operation


def load_place_overrides(overrides_dir, *, registry=None, internal=False):
    """``(entries, sha256)``: the ``places.yaml`` entries in file order, each
    with an ``operation`` key, and the digest of the bytes they were parsed
    from — ``([], None)`` when there is no file. Every entry names the
    ``place`` it concerns by a reference; ``resolve_place`` also names the
    ``source_ref`` (the unresolved candidate's Overture id) it assigns it. With
    ``registry``, every place reference becomes the registry's current key
    for the place it names — its own id, or with ``internal`` the QID the
    seed and metros stages join on."""
    if overrides_dir is None:
        return [], None
    path = pathlib.Path(overrides_dir) / PLACES_FILE
    data, digest = read_override(overrides_dir, PLACES_FILE)
    if data is None:
        return [], None
    import yaml

    raw = yaml.load(data.decode("utf-8"), Loader=_strict_loader())
    if raw is None:
        return [], digest
    if not isinstance(raw, list):
        raise OverrideError(f"{path}: expected a list of override entries")
    entries = []
    seen = set()
    pending = {
        entry.get("place")
        for entry in raw
        if isinstance(entry, dict) and "add_place" in entry
    }
    for entry in raw:
        if not isinstance(entry, dict):
            raise OverrideError(f"{path}: every entry must be a mapping")
        operation = _validate_place_entry(path, entry)
        if registry is not None:
            _resolve_place_entry(
                entry, registry, f"{path}: place {entry['place']!r}", internal, pending
            )
        # resolve_place is keyed by the candidate it resolves: two entries
        # naming one candidate would race for its QID.
        key = (
            (entry["source_ref"], operation)
            if operation == "resolve_place"
            else (entry["place"], operation)
        )
        if key in seen:
            raise OverrideError(f"{path}: duplicate {operation} for {key[0]!r}")
        seen.add(key)
        entries.append({**entry, "operation": operation})
    return entries, digest


def by_operation(entries, operation):
    return [entry for entry in entries if entry["operation"] == operation]


def canonical_digest(payload):
    """The SHA-256 of a payload's canonical JSON — the one way every stage
    hashes the evidence a curator recorded against."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def judge(entry, evidence, report, scope):
    """Whether an entry is stale against ``evidence`` (the derived data the
    curator looked at, hashed canonically); a mismatch is applied anyway and
    reported with the current hash to record. An entry without an
    ``evidence_hash`` is never stale."""
    recorded = entry.get("evidence_hash")
    if recorded is None:
        return False
    current = canonical_digest(evidence)
    if current == recorded:
        return False
    report.append(
        {
            "scope": scope,
            "place": entry.get("place"),
            "feed": entry.get("feed"),
            "source_ref": entry.get("source_ref"),
            "operation": entry["operation"],
            "recorded_evidence_hash": recorded,
            "current_evidence_hash": current,
        }
    )
    return True


def phase_digest(by_feed, operations):
    """The digest of the feed entries' given operations alone, None when no
    entry carries one: an edit to another phase's operations does not send
    this phase's stage back."""
    subset = {}
    for ref, entry in by_feed.items():
        ops = {op: entry[op] for op in sorted(operations) if op in entry}
        if ops:
            subset[ref] = ops
    return canonical_digest(subset) if subset else None


def feeds_digest(overrides_dir):
    """The SHA-256 of the current ``feeds.yaml`` bytes, or None without one."""
    return override_digest(overrides_dir, FEEDS_FILE)


def places_digest(overrides_dir):
    """The SHA-256 of the current ``places.yaml`` bytes, or None without one."""
    return override_digest(overrides_dir, PLACES_FILE)


def access_providers_digest(overrides_dir):
    """The SHA-256 of the current ``access_providers.yaml`` bytes, or None
    without one."""
    return override_digest(overrides_dir, ACCESS_PROVIDERS_FILE)


def expect_digest(recorded, current, what, rerun):
    """An override file read by a later stage must be the one an earlier
    stage applied: a mixed snapshot never ships. ``recorded`` is what the
    earlier manifest carries (None when it predates the file)."""
    if recorded != current:
        raise OverrideError(
            f"{what} changed since the {rerun} stage applied it; re-run the "
            f"{rerun} stage"
        )


def strict_check(strict, report, stage):
    """``--strict-overrides``: a stale override fails the stage once its
    report is preserved in the generation just published."""
    if strict and report:
        raise OverrideError(
            f"{len(report)} stale override(s) in the {stage} stage; see its "
            "override_report.jsonl"
        )
