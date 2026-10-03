"""The maintainer's keys for the key-protected feeds whose providers approve
a crawl.

A provider in ``overrides/access_providers.yaml`` with ``crawl_approved``
lends the crawl its key. Each credential field resolves as transitio's do:
``TRANSITIO_KEY_<PROVIDER>__<FIELD>`` first, then the file
``TRANSITIO_INDEX_CREDENTIALS`` names, else transitio's own credentials file.
Values stay transitio secrets, which only transitio's transport reveals. A
provider's optional ``crawl_budget`` caps its keyed requests per calendar month
(UTC) on this machine, across runs: the tally is ``key_requests.json`` in the
user state directory, or the file ``TRANSITIO_INDEX_KEY_USAGE`` names.
"""

import datetime
import json
import logging
import os
import pathlib
import re
import sys
import urllib.parse

import platformdirs
from transitio._http import locked, replacing
from transitio.catalog._access import _Access, _proxy, _redact, _Secret, _secrets
from transitio.catalog._access import _sends
from transitio.credentials import _resolve
from transitio.index import AccessProvider

from transitio_index import fetch, overrides, store

CREDENTIALS = "TRANSITIO_INDEX_CREDENTIALS"
USAGE = "TRANSITIO_INDEX_KEY_USAGE"
_MONTH = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")
# httpcore traces response headers, a Location among them, at DEBUG.
_TRACERS = ("httpcore", "httpcore.http11", "httpcore.http2", "httpcore.connection")
_TRACERS += ("httpcore.proxy", "httpcore.socks")
_DESCRIBED = ("name", "registration_url", "docs_url", "terms_url", "free")


def _credentials_file():
    """The file ``TRANSITIO_INDEX_CREDENTIALS`` names, else None (transitio's
    own); raises when set on Windows, which reads no file, or naming none."""
    named = os.environ.get(CREDENTIALS)
    if named is None:
        return None
    if sys.platform == "win32":
        raise ValueError(f"{CREDENTIALS} is set, but Windows reads no credentials file")
    if not os.path.isfile(named):
        raise ValueError(f"{CREDENTIALS} names no file: {named}")
    return named


def usage_path():
    """The tally of keyed requests this month."""
    named = os.environ.get(USAGE)
    if named:
        return pathlib.Path(named)
    state = platformdirs.user_state_dir("transitio-index")
    return pathlib.Path(state) / "key_requests.json"


def _month():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m")


def _counts(path, month):
    """The tally's ``{provider_id: requests}`` for ``month``: empty without
    a file or for another month; a malformed tally, a symlink or a
    non-regular file raises."""
    try:
        handle = store.open_regular_path(path)
    except FileNotFoundError:
        return {}
    try:
        data = json.loads(store.read_all(handle, 64 * 1024))
    except ValueError:
        data = None
    finally:
        os.close(handle)
    requests = data.get("requests") if isinstance(data, dict) else None
    if not (
        isinstance(requests, dict)
        and isinstance(data.get("month"), str)
        and _MONTH.fullmatch(data["month"])
        and all(type(n) is int and n >= 0 for n in requests.values())
    ):
        raise ValueError(f"{path}: not a tally of key requests")
    return dict(requests) if data["month"] == month else {}


class _Untraced(logging.Filter):
    """Drops records below INFO: httpcore's traces."""

    def filter(self, record):
        return record.levelno >= logging.INFO


class Keys:
    """The keys the crawl may send, by provider, and the outcome of each
    feed's keyed read: a 401 to one stops the provider's key for the rest of
    the run. Until :meth:`close`, httpcore's tracing loggers drop their
    DEBUG records."""

    def __init__(self, providers, resolved, methods):
        self._providers = providers
        self._resolved = resolved
        secrets = [
            secret
            for provider_id, (fields, _) in resolved.items()
            for method in methods[provider_id]
            if method != "basic_auth" or {"username", "password"} <= set(fields)
            for secret in _secrets(method, fields)
        ]
        # A prefixed header value, such as "apikey <key>", leaks as its token too.
        tokens = [secret.reveal().partition(" ")[2] for secret in secrets]
        self._secrets = secrets + [_Secret(token) for token in tokens if token]
        # The providers a 401 refused, and the month each spent budget is for.
        self._refused, self._spent = set(), {}
        self._untraced = _Untraced()
        for name in _TRACERS:
            logging.getLogger(name).addFilter(self._untraced)

    @classmethod
    def load(cls, overrides_dir, feeds, resolve_manifest):
        """The keys of the approved providers the crawlable key-flagged
        ``feeds`` name. ``access_providers.yaml`` must be the file the
        resolve stage applied; credentials are read only for an approved
        provider, and a malformed credentials file or tally raises."""
        providers, _, digest = overrides.load_access_providers(overrides_dir)
        overrides.expect_digest(
            resolve_manifest.get("access_providers_sha256"),
            digest,
            overrides.ACCESS_PROVIDERS_FILE,
            "resolve",
        )
        methods = {}
        for feed in feeds:
            if feed.get("crawlable") and feed.get("access") == "key":
                bound = methods.setdefault(feed.get("access_provider"), set())
                bound.add(feed.get("auth_method"))
        lent = [p for p in providers if p in methods and providers[p]["crawl_approved"]]
        path = _credentials_file() if lent else None
        resolved = {}
        for provider_id in lent:
            provider = providers[provider_id]
            described = AccessProvider(
                provider_id=provider_id,
                credential_fields=tuple(provider["credential_fields"]),
                **{key: provider[key] for key in _DESCRIBED},
            )
            resolved[provider_id] = _resolve(described, path=path)
        if any(providers[p]["crawl_budget"] for p in lent):
            _counts(usage_path(), _month())
        return cls(providers, resolved, methods)

    def close(self):
        """Let httpcore's tracing loggers through again."""
        for name in _TRACERS:
            logging.getLogger(name).removeFilter(self._untraced)

    def access(self, feed, url):
        """``(transitio _Access, None)`` when the feed may be read from
        ``url`` with a key, else ``(None, outcome)`` saying why not."""
        provider = self._providers.get(feed.get("access_provider"))
        if provider is None:
            return None, "no_provider"
        if not provider["crawl_approved"]:
            return None, "not_approved"
        method, params = feed.get("auth_method"), feed.get("auth_params")
        if not _sends(method, params, provider["credential_fields"]):
            return None, "unsupported"
        if not url or urllib.parse.urlsplit(url).scheme != "https":
            return None, "not_https"
        if overrides.claiming_provider(url, self._providers) != provider["provider_id"]:
            return None, "not_claimed"
        fields, missing = self._resolved[provider["provider_id"]]
        if missing:
            return None, "no_credentials"
        try:
            return _Access(url, method, params or {}, fields), None
        except ValueError:
            return None, "unsendable"

    def serves(self, provider_id, feed):
        """Whether data read with ``provider_id``'s key may stand for the
        feed: the provider still approves the crawl and the feed is still
        its."""
        provider = self._providers.get(provider_id)
        return (
            provider is not None
            and provider["crawl_approved"]
            and feed.get("access_provider") == provider_id
        )

    def settled(self, provider_id):
        """The outcome that settles a feed of the provider without a request:
        ``key_refused`` after a 401 this run, ``budget_spent`` while its
        budget for this month is; else None."""
        if provider_id in self._refused:
            return "key_refused"
        if self._spent.get(provider_id) == _month():
            return "budget_spent"
        return None

    def outcome(self, provider_id, error):
        """The keyed read's outcome from the error that ended it, if any; a
        401 refuses the provider's key for the rest of the run."""
        if error is None:
            return "read"
        if self._spent.get(provider_id) == _month():
            return "budget_spent"
        if getattr(error, "status", None) == 401:
            self._refused.add(provider_id)
        return "failed"

    def session(self, fetcher, provider_id, access):
        """The walk session of one keyed download
        (:meth:`fetch.Fetcher.keyed`) through transitio's proxy rule, each
        request counted against the provider's budget; a URL holding a
        secret is refused unless the credential travels in the query."""
        refuse = None if access.method == "query_param" else self.holds
        return fetcher.keyed(access, _proxy, lambda: self._spend(provider_id), refuse)

    def _spend(self, provider_id):
        """Count one keyed request against the provider's monthly budget,
        under a lock other processes share; refuse it once the budget is
        spent."""
        budget = self._providers[provider_id]["crawl_budget"]
        if budget is None:
            return
        path = usage_path()
        with locked(path.with_name(path.name + ".lock")):
            month = _month()
            counts = _counts(path, month)
            if counts.get(provider_id, 0) >= budget:
                self._spent[provider_id] = month
                raise fetch.FetchError(
                    f"the {budget} keyed requests a month for {provider_id} are spent"
                )
            counts[provider_id] = counts.get(provider_id, 0) + 1
            with replacing(path) as handle:
                handle.write(json.dumps({"month": month, "requests": counts}).encode())

    def holds(self, text):
        """Whether a secret occurs in ``text``, raw or percent-encoded."""
        return any(secret.occurs_in(text) for secret in self._secrets)

    def redact(self, value):
        """``value`` with every secret masked in each string it holds."""
        if isinstance(value, str):
            return _redact(value, self._secrets)
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        if isinstance(value, dict):
            return {key: self.redact(item) for key, item in value.items()}
        return value
