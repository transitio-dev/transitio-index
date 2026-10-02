import datetime

import pytest

pytest.importorskip("yaml")
import yaml  # noqa: E402

from transitio_index import crawl, overrides, resolve, store  # noqa: E402


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


def test_static_gtfs_defaults_to_crawlable(tmp_path):
    # Realtime and GBFS are indexed but never fetched; a GTFS feed that needs a
    # key is crawled without one, so only one without a URL to try is refused.
    gated = {"requires_auth": True, "urls": {"direct_download": "https://x/a.zip"}}
    open_atlas = {"requires_auth": False, "urls": {"static_current": "https://y/a.zip"}}
    cases = [
        (_feed("f-rt", spec="gtfs-rt"), False, None),
        (_feed("f-bike", spec="gbfs"), False, None),
        (dict(_feed("f-key"), mdb=gated), True, None),
        (dict(_feed("f-both"), atlas=open_atlas, mdb=gated), True, None),
        (
            dict(_feed("f-atlas"), atlas=dict(open_atlas, requires_auth=True)),
            True,
            None,
        ),
        (
            dict(_feed("f-no-url"), mdb=dict(gated, urls={})),
            False,
            resolve.AUTH_REASON,
        ),
        (_feed("f-open"), True, None),
        # An upstream decision stands; an override's reason wins over the default.
        (dict(_feed("f-forced"), mdb=dict(gated, urls={}), crawlable=True), True, None),
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
    assert manifest["uncrawlable"] == 4
    assert manifest["requires_auth"] == 2


def test_access_details_come_from_the_record_supplying_the_url(tmp_path):
    def mdb(kind, name=None, info=None):
        return {
            "requires_auth": kind != "0",
            "authentication_type": kind,
            "api_key_parameter_name": name,
            "authentication_info": info,
            "urls": {"direct_download": "https://m/a.zip"},
        }

    def atlas(block, url="https://a/a.zip"):
        urls = {"static_current": url} if url else {}
        return {"requires_auth": bool(block), "authorization": block, "urls": urls}

    page = "https://register.example/"
    open_ = ("open", None, None, None)
    cases = {
        "f-open": ({"mdb": mdb("0")}, open_),
        "f-q": ({"mdb": mdb("1", "key", page)}, ("key", "query_param", "key", page)),
        "f-h": ({"mdb": mdb("2", "ApiKey")}, ("key", "header", "ApiKey", None)),
        "f-other": ({"mdb": mdb("3", "key")}, ("key", "unsupported", None, None)),
        # The Atlas registration page comes first.
        "f-both": (
            {
                "atlas": atlas(
                    {"type": "header", "param_name": "x-key", "info_url": page}
                ),
                "mdb": mdb("1", "key", "https://elsewhere.example/"),
            },
            ("key", "header", "x-key", page),
        ),
        "f-basic": (
            {"atlas": atlas({"type": "basic_auth"}), "mdb": mdb("1", "k", page)},
            ("key", "basic_auth", None, page),
        ),
        "f-template": (
            {"atlas": atlas({"type": "replace_url", "info_url": "javascript:x()"})},
            ("key", "unsupported", None, None),
        ),
        "f-path": (
            {"atlas": atlas({"type": "path_segment", "param_name": "key"})},
            ("key", "unsupported", None, None),
        ),
        "f-new-type": (
            {"atlas": atlas({"type": "cookie", "param_name": "key"})},
            ("key", "unsupported", None, None),
        ),
        # A name that cannot be sent as a header is dropped.
        "f-bad-name": (
            {"atlas": atlas({"type": "header", "param_name": "X-Key\r\nHost: x"})},
            ("key", "header", None, None),
        ),
        # The open Atlas URL is the one crawled, whatever the MDB row says.
        "f-atlas-open": ({"atlas": atlas({}), "mdb": mdb("1", "key")}, open_),
        # Without a static URL the record that needs a key speaks for the feed.
        "f-rt": (
            {"atlas": atlas({"type": "query_param", "param_name": "t"}, url=None)},
            ("key", "query_param", "t", None),
        ),
        "f-rt-mdb": (
            {"atlas": atlas({}, url=None), "mdb": dict(mdb("2", "h"), urls={})},
            ("key", "header", "h", None),
        ),
    }
    cache = tmp_path / "cache"
    _crosswalk(
        cache, [dict(_feed(ref), **records) for ref, (records, _) in cases.items()]
    )
    resolve.resolve(cache, overrides_dir=None)
    feeds, _ = _resolved(cache)
    for ref, (_, expected) in cases.items():
        fields = ("access", "auth_method", "auth_param_name", "registration_url")
        assert tuple(feeds[ref][field] for field in fields) == expected, ref
        assert feeds[ref]["auth_params"] is None


def test_set_access_records_the_curated_pair(tmp_path):
    page = "https://register.example/"
    gated = {
        "requires_auth": True,
        "authentication_type": "1",
        "api_key_parameter_name": "key",
        "authentication_info": page,
        "urls": {"direct_download": "https://x/a.zip"},
    }
    # The open Atlas URL makes the catalogues call the feed open.
    open_atlas = {"requires_auth": False, "urls": {"static_current": "https://y/a.zip"}}
    cache = tmp_path / "cache"
    _crosswalk(
        cache,
        [
            dict(_feed("f-open"), atlas=open_atlas, mdb=gated),
            dict(_feed("f-key"), mdb=gated),
        ],
    )
    pair = {"client_id": "client_id", "client_secret": "client_secret"}
    overrides_dir = _overrides_dir(
        tmp_path,
        [
            {
                "feed": "f-open",
                "set_access": {"auth_method": "query_param", "auth_params": pair},
            },
            {
                "feed": "f-key",
                "set_access": {
                    "auth_method": "header",
                    "auth_params": {"Authorization": "key"},
                },
            },
        ],
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    assert feeds["f-open"]["access"] == "key"
    assert feeds["f-open"]["auth_params"] == pair
    # Its URL is still crawled, without the key.
    assert feeds["f-open"]["crawlable"] is True
    assert feeds["f-open"]["registration_url"] == page
    assert feeds["f-key"]["auth_method"] == "header"
    assert feeds["f-key"]["auth_params"] == {"Authorization": "key"}
    assert feeds["f-key"]["auth_param_name"] is None
    assert manifest["requires_auth"] == 0


@pytest.mark.parametrize(
    "spec, message",
    [
        ({}, "mapping of access_provider"),
        ({"access_provider": "x", "auth": "key"}, "mapping of access_provider"),
        ({"access_provider": "Trafik_Lab"}, "access_provider must be a provider id"),
        ({"auth_method": "query_param"}, "auth_method and auth_params together"),
        ({"access_provider": "gcba-transporte"}, None),
        ({"auth_method": "cookie", "auth_params": {}}, "auth_method must be one of"),
        ({"auth_method": "query_param", "auth_params": {}}, "does not fit"),
        (
            {"auth_method": "header", "auth_params": {"A": "key", "B": "key"}},
            "does not fit",
        ),
        (
            {"auth_method": "basic_auth", "auth_params": {"u": "username"}},
            "does not fit",
        ),
        ({"auth_method": "query_param", "auth_params": {"key": "Key"}}, "auth_params"),
        ({"auth_method": "query_param", "auth_params": {"a b": "key"}}, "auth_params"),
        ({"auth_method": "header", "auth_params": {"acl:key": "key"}}, "auth_params"),
        # A query parameter name is sent percent-encoded.
        (
            {"auth_method": "query_param", "auth_params": {"acl:consumerKey": "key"}},
            None,
        ),
    ],
)
def test_set_access_validation(tmp_path, spec, message):
    overrides_dir = _overrides_dir(tmp_path, [{"feed": "f-a", "set_access": spec}])
    if message is None:
        assert overrides.load_feed_overrides(overrides_dir)[0]["f-a"]["set_access"]
        return
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_feed_overrides(overrides_dir)


ADD_FEED = {
    "name": "Colectivos",
    "url": "https://api.example.org/feed-gtfs",
    "spec": "gtfs",
    "license": {"url": "https://api.example.org/terms", "spdx_identifier": "x"},
    "location": {"country_code": "AR", "municipality": "Buenos Aires"},
}
DROP = object()
KEY = {
    "access": "key",
    "registration_url": "https://register.example/",
    "access_provider": "gcba",
    "auth_method": "query_param",
    "auth_params": {"client_id": "client_id"},
}
G = "f-curated-gcba"


@pytest.mark.parametrize(
    "ref, change, message",
    [
        (G, KEY, None),
        (G, {"location": {"country_code": "AR"}}, None),
        (
            G,
            {"license": {"spdx_identifier": "x", "redistribution_allowed": "no"}},
            None,
        ),
        ("f-gcba", {}, "needs an id f-curated-<slug>"),
        # A feed id is at most 512 characters.
        ("f-curated-" + "a" * 502, {}, None),
        ("f-curated-" + "a" * 503, {}, "needs an id f-curated-<slug>"),
        (G, {"license": DROP}, "must be a mapping of"),
        (G, {"extra": 1}, "must be a mapping of"),
        (G, {"name": " "}, "name must be"),
        (G, {"url": "ftp://x.example/g.zip"}, "url must be"),
        (G, {"spec": "gtfs-rt"}, "spec must be gtfs"),
        (G, {"license": {}}, "license must be"),
        (G, {"license": {"spdx": "x"}}, "license must be"),
        (G, {"license": {"url": "javascript:x()"}}, "license must be"),
        (G, {"license": {"spdx_identifier": True}}, "license must be"),
        (G, {"license": {"redistribution_allowed": "maybe"}}, "license must be"),
        (G, {"location": {"country_code": "ar"}}, "location must"),
        (G, {"location": {"municipality": "X"}}, "location must"),
        (G, {"location": {"country_code": "AR", "city": "X"}}, "location must"),
        (G, {"access": "free"}, "access must be open or key"),
        (G, {"registration_url": "https://r.example/"}, "an open feed takes no"),
        (G, {**KEY, "registration_url": "r.example"}, "registration_url must be"),
        (G, {**KEY, "auth_params": {"id": "Id"}}, "add_feed auth_params must map"),
    ],
)
def test_add_feed_validation(tmp_path, ref, change, message):
    spec = {key: v for key, v in {**ADD_FEED, **change}.items() if v is not DROP}
    entry = {"feed": ref, "add_feed": spec}
    overrides_dir = _overrides_dir(tmp_path, [entry])
    if message is None:
        assert overrides.load_feed_overrides(overrides_dir)[0][ref]["add_feed"]
        # A curated feed's identity and access are its own entry's.
        for operation in ("set_identity", "set_access"):
            other = {**entry, operation: {"access_provider": "gcba"}}
            with pytest.raises(overrides.OverrideError, match="its own identity"):
                overrides.load_feed_overrides(_overrides_dir(tmp_path, [other]))
        return
    with pytest.raises(overrides.OverrideError, match=message):
        overrides.load_feed_overrides(overrides_dir)


def _provider(provider_id, fields=("key",), prefixes=(), **kw):
    return {
        "provider_id": provider_id,
        "name": provider_id.title(),
        "registration_url": "https://register.example/",
        "credential_fields": list(fields),
        "url_prefixes": list(prefixes),
        "crawl_approved": False,
        **kw,
    }


def _providers_dir(tmp_path, entries):
    directory = tmp_path / "overrides"
    directory.mkdir(exist_ok=True)
    (directory / overrides.ACCESS_PROVIDERS_FILE).write_text(
        yaml.safe_dump(entries, sort_keys=False), encoding="utf-8"
    )
    return directory


@pytest.mark.parametrize(
    "entry, message",
    [
        ("trafiklab", "must be a mapping"),
        (_provider("Trafik_Lab"), "provider_id must be"),
        (dict(_provider("p"), extra=1), "unknown keys ['extra']"),
        ({**_provider("p"), 1: "x", "y": 2}, "unknown keys [1, 'y']"),
        (_provider("p", registration_url="javascript:x()"), "registration_url"),
        (_provider("p", docs_url="https://r.example/\nX: y"), "docs_url"),
        (_provider("p", terms_url="https://r.example:99999/"), "terms_url"),
        (_provider("p", fields=()), "credential_fields"),
        (_provider("p", fields=("key", "key")), "credential_fields"),
        (_provider("p", fields=("Key",)), "credential_fields"),
        (_provider("p", prefixes=["http://x.example/"]), "url_prefixes"),
        (_provider("p", prefixes=["https://x.example/a?b=1"]), "url_prefixes"),
        (_provider("p", prefixes=["https://u@x.example/"]), "url_prefixes"),
        (_provider("p", prefixes=["https://x.example//a/"]), "url_prefixes"),
        (_provider("p", prefixes=["https://./"]), "url_prefixes"),
        (_provider("p", prefixes=["https://x.exa\tmple/"]), "url_prefixes"),
        (_provider("p", free="yes"), "free must be"),
        (_provider("p", crawl_approved=True), "needs terms_checked and terms_note"),
        (_provider("p", terms_checked="last week"), "terms_checked must be a date"),
        *[(_provider("p", crawl_budget=b), "crawl_budget") for b in (0, True, "9")],
        (_provider("dup"), "not unique"),
    ],
)
def test_access_providers_refuse_a_broken_entry(tmp_path, entry, message):
    accepted = _provider(
        "dup",
        prefixes=["HTTPS://Bücher.Example/gtfs", "https://x.example:8443/"],
        crawl_approved=True,
        terms_checked=datetime.date(2026, 10, 1),
        terms_note="Crawling allowed.",
        free=True,
        crawl_budget=50,
    )
    entries = [dict(accepted, provider_id="ok"), entry]
    providers, refused, digest = overrides.load_access_providers(
        _providers_dir(tmp_path, entries)
    )
    assert digest is not None
    if message == "not unique":
        # A repeated id refuses every entry carrying it.
        entries[0] = accepted
        providers, refused, _ = overrides.load_access_providers(
            _providers_dir(tmp_path, entries)
        )
        assert providers == {}
        assert [row["provider_id"] for row in refused] == ["dup", "dup"]
        assert all(message in row["error"] for row in refused)
        return
    assert providers["ok"]["url_prefixes"] == [
        "https://x.example:8443/",
        "https://xn--bcher-kva.example:443/gtfs/",
    ]
    assert providers["ok"]["terms_checked"] == "2026-10-01"
    assert providers["ok"]["crawl_budget"] == 50
    assert len(refused) == 1 and message in refused[0]["error"]


@pytest.mark.parametrize(
    "one, other, overlap",
    [
        ("https://a.example/gtfs/", "https://A.example:443/gtfs", True),
        ("https://a.example/", "https://a.example/gtfs/feed.zip", True),
        ("https://a.example/gtfs/", "https://a.example/%67tfs/x", True),
        ("https://a.example/bücher/", "https://a.example/b%c3%bccher/", True),
        ("https://a.example/gtfs/", "https://a.example/gtfs-other/", False),
        ("https://a.example/gtfs/", "https://a.example:8443/gtfs/", False),
    ],
)
def test_overlapping_provider_prefixes_are_a_build_error(tmp_path, one, other, overlap):
    overrides_dir = _providers_dir(
        tmp_path,
        [_provider("one", prefixes=[one]), _provider("other", prefixes=[other])],
    )
    if overlap:
        with pytest.raises(overrides.OverrideError, match="overlapping url_prefixes"):
            overrides.load_access_providers(overrides_dir)
    else:
        assert len(overrides.load_access_providers(overrides_dir)[0]) == 2


@pytest.mark.parametrize(
    "url, claimed",
    [
        ("https://api.example.com/gtfs/feed.zip?key=1", True),
        ("https://API.example.com:443/gtfs", True),
        ("https://api.example.com.evil/gtfs/feed.zip", False),
        ("https://api.example.com:8443/gtfs/feed.zip", False),
        ("https://api.example.com/gtfs-other/feed.zip", False),
        ("http://api.example.com/gtfs/feed.zip", False),
        ("https://user@api.example.com/gtfs/feed.zip", False),
        ("https://api.example.com/gtfs/../other/feed.zip", False),
        ("https://api.example.com/gtfs/%2E%2E/other/feed.zip", False),
        ("https://api.example.com/gtfs/%252e%252e/other/feed.zip", False),
        ("https://api.example.com/gtfs/%zz", False),
        ("https://api.example.com/%67tfs/feed.zip", True),
        (None, False),
    ],
)
def test_a_provider_claims_urls_by_origin_and_whole_path_segments(url, claimed):
    providers = {"p": {"url_prefixes": ["https://api.example.com:443/gtfs/"]}}
    assert overrides.claiming_provider(url, providers) == ("p" if claimed else None)


def test_the_committed_access_overrides_load():
    from pathlib import Path

    directory = Path(__file__).resolve().parent.parent / "overrides"
    providers, refused, _ = overrides.load_access_providers(directory)
    assert providers and refused == []
    entries, _ = overrides.load_feed_overrides(directory)
    protected = [
        entry["add_feed"]
        for entry in entries.values()
        if entry.get("add_feed", {}).get("access") == "key"
    ]
    assert protected
    # A curated feed names an accepted provider, which also claims its URL.
    for spec in protected:
        provider = spec["access_provider"]
        assert provider in providers
        assert overrides.claiming_provider(spec["url"], providers) == provider


def test_resolve_binds_protected_feeds_to_providers_and_reports_the_rest(tmp_path):
    page = "https://register.example/"

    def mdb(kind, name, url):
        record = {"authentication_type": kind, "api_key_parameter_name": name}
        record.update(requires_auth=kind != "0", authentication_info=page)
        return {"mdb": dict(record, urls={"direct_download": url})}

    def atlas(kind, url, name=None):
        block = {"type": kind, "param_name": name}
        urls = {"static_current": url}
        return {"atlas": {"requires_auth": True, "authorization": block, "urls": urls}}

    one, two, basic = (
        "https://one.example/",
        "https://two.example/",
        "https://b.example/",
    )
    pair = {"client_id": "client_id", "client_secret": "client_secret"}
    unsupported = ("one", "unsupported", {})
    unresolved = (None, "query_param", None)
    cases = {
        "f-q": (
            mdb("1", "token", one + "q.zip"),
            ("one", "query_param", {"token": "key"}),
        ),
        "f-h": (
            atlas("header", one + "h.zip", "x-key"),
            ("one", "header", {"x-key": "key"}),
        ),
        "f-path": (atlas("path_segment", one + "p.zip"), unsupported),
        "f-basic": (atlas("basic_auth", basic + "b.zip"), ("basic", "basic_auth", {})),
        # A curated provider and pair stand whatever the URL.
        "f-curated": (
            mdb("2", "x", "https://c.example/"),
            ("two", "query_param", pair),
        ),
        "f-open": (mdb("0", None, one + "o.zip"), (None, None, None)),
        # An http URL is read as the https form a provider claims, else kept.
        "f-http": (
            mdb("1", "token", "http://one.example/h.zip"),
            ("one", "query_param", {"token": "key"}),
        ),
        "f-none": (mdb("1", "key", "http://n.example/"), unresolved),
    }
    errors = {
        "f-multi": (mdb("1", "client_id", two + "m.zip"), ("two", "unsupported", {})),
        "f-noname": (mdb("2", None, one + "x.zip"), unsupported),
        "f-basic-key": (atlas("basic_auth", one + "b.zip"), unsupported),
        "f-undeclared": (mdb("1", "key", one + "u.zip"), unsupported),
        "f-ghost": (mdb("1", "key", "https://g.example/"), unresolved),
    }
    cases.update(errors)
    cache = tmp_path / "cache"
    _crosswalk(
        cache, [dict(_feed(ref), **records) for ref, (records, _) in cases.items()]
    )
    overrides_dir = _overrides_dir(
        tmp_path,
        [
            {
                "feed": "f-curated",
                "set_access": {
                    "access_provider": "two",
                    "auth_method": "query_param",
                    "auth_params": pair,
                },
            },
            {
                "feed": "f-undeclared",
                "set_access": {"auth_method": "header", "auth_params": {"X": "token"}},
            },
            {"feed": "f-ghost", "set_access": {"access_provider": "ghost"}},
        ],
    )
    _providers_dir(
        tmp_path,
        [
            _provider("one", prefixes=[one]),
            _provider("two", fields=("client_id", "client_secret"), prefixes=[two]),
            _provider("basic", fields=("username", "password"), prefixes=[basic]),
            _provider("broken", fields=()),
        ],
    )
    resolve.resolve(cache, overrides_dir=overrides_dir)
    feeds, manifest = _resolved(cache)
    for ref, (_, expected) in cases.items():
        fields = ("access_provider", "auth_method", "auth_params")
        assert tuple(feeds[ref][field] for field in fields) == expected, ref
    report, _ = store.read_jsonl(
        cache / "resolve", resolve.RESOLVE_POINTER, resolve.ACCESS_REPORT
    )
    rows = {}
    for row in report:
        rows.setdefault(row["kind"], []).append(row)
    assert [row["provider_id"] for row in rows["refused_provider"]] == ["broken"]
    assert sorted(row["feed_id"] for row in rows["curation_error"]) == sorted(errors)
    assert rows["unresolved"] == [
        {"kind": "unresolved", "feed_id": ref, "url": url, "registration_url": page}
        for ref, url in (
            ("f-none", "http://n.example/"),
            ("f-ghost", "https://g.example/"),
        )
    ]
    assert crawl.feed_url(feeds["f-http"]) == one + "h.zip"
    assert "download_url" not in feeds["f-none"]
    assert manifest["access_providers"] == 3
    assert manifest["access_unresolved"] == 2
    assert manifest["access_curation_errors"] == len(errors) + 1


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


@pytest.mark.parametrize(
    "identity, message",
    [
        ({"feed_id": "../escape"}, "not a valid feed id"),
        # The curated prefix is add_feed's alone.
        ({"feed_id": "f-curated-x"}, "cannot take the curated id"),
        ({"aliases": ["f-a-old", "f-curated-x"]}, "cannot take the curated id"),
    ],
)
def test_a_malformed_feed_id_value_is_a_build_error(tmp_path, identity, message):
    overrides_dir = _overrides_dir(
        tmp_path, [{"feed": "f-a", "set_identity": identity}]
    )
    with pytest.raises(overrides.OverrideError, match=message):
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
