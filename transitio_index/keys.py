"""The maintainer's keys for the key-protected feeds whose providers approve
a crawl.

A provider in ``overrides/access_providers.yaml`` with ``crawl_approved``
lends the crawl its key. Each credential field resolves as transitio's do:
``TRANSITIO_KEY_<PROVIDER>__<FIELD>`` first, then the file
``TRANSITIO_INDEX_CREDENTIALS`` names, else transitio's own credentials file.
Values stay transitio secrets, which only transitio's transport reveals.
"""

import logging
import os
import sys
import urllib.parse

from transitio.catalog._access import _Access, _proxy, _redact, _Secret, _secrets
from transitio.catalog._access import _sends
from transitio.credentials import _resolve
from transitio.index import AccessProvider

from transitio_index import overrides

CREDENTIALS = "TRANSITIO_INDEX_CREDENTIALS"
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


class _Untraced(logging.Filter):
    """Drops records below INFO: httpcore's traces."""

    def filter(self, record):
        return record.levelno >= logging.INFO


class Keys:
    """The keys the crawl may send, by provider. Until :meth:`close`,
    httpcore's tracing loggers drop their DEBUG records."""

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
        self._untraced = _Untraced()
        for name in _TRACERS:
            logging.getLogger(name).addFilter(self._untraced)

    @classmethod
    def load(cls, overrides_dir, feeds, resolve_manifest):
        """The keys of the approved providers the crawlable key-flagged
        ``feeds`` name. ``access_providers.yaml`` must be the file the
        resolve stage applied; credentials are read only for an approved
        provider, and a malformed credentials file raises."""
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

    def session(self, fetcher, access):
        """The walk session of one keyed download
        (:meth:`fetch.Fetcher.keyed`) through transitio's proxy rule; a URL
        holding a secret is refused unless the credential travels in the
        query."""
        refuse = None if access.method == "query_param" else self.holds
        return fetcher.keyed(access, _proxy, refuse)

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
