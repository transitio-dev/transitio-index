import contextlib
import json

import pytest

pytest.importorskip("pyarrow")

import overture_fixture as fx  # noqa: E402
from transitio_index import store, wikidata_copy  # noqa: E402


def test_copy_keys_are_what_a_build_can_ask(tmp_path):
    rows = [
        fx.division("fi", "FI", "country", wikidata="Q33"),
        fx.division("nyc", "US", "locality", wikidata="Q60"),
        fx.division("hel", "FI", "locality", wikidata="Q1757"),
        fx.division("x", "FI", "locality", sources=[fx.osm("relation/123")]),
        fx.division(
            "bad", "FI", "county", wikidata="nope", sources=[fx.osm("relation/456")]
        ),
        fx.division("hood", "FI", "neighborhood", wikidata="Q999"),
    ]
    dataset = fx.write_dataset(tmp_path / "divisions.parquet", rows)
    keys = wikidata_copy.copy_keys(dataset, {"35620": ["36061", "36047"]})
    assert keys == {
        "p402": ["123", "456"],
        "statistical_metros": ["Q60"],
        "metro_candidates": ["Q1757", "Q60"],
        "fips_items": ["36047", "36061"],
        "labels_and_aliases": ["Q1757", "Q33", "Q60"],
        "coordinates": ["Q1757", "Q33", "Q60"],
    }


class Stub(fx.StubWikidata):
    """The fixture's stub with FIPS codes, recording every call's keys and
    failing once at the call ``fail_at`` when given."""

    def __init__(self, fail_at=None, **maps):
        super().__init__(**maps)
        self.calls, self.fail_at = [], fail_at

    def __getattribute__(self, name):
        attribute = super().__getattribute__(name)
        if name not in wikidata_copy.LOOKUPS:
            return attribute

        def recorded(keys):
            self.calls.append((name, list(keys)))
            if len(self.calls) == self.fail_at:
                raise RuntimeError("connection dropped")
            return attribute(keys)

        return recorded

    def fips_items(self, codes):
        return {code: ["Q11"] for code in codes if code == "36061"}


def test_a_copy_answers_as_the_lookups_did_and_resumes(tmp_path, monkeypatch):
    monkeypatch.setattr(wikidata_copy, "CHUNK", 2)
    maps = {
        "mapping": {"123": "Q5"},
        "metros": {"Q60": [{"qid": "Q1", "name": "New York metro", "cbsa": "35620"}]},
        "candidates": {"Q1757": [{"qid": "Q2", "name": "Helsinki FUA"}]},
        "labels": {
            q: {"labels": {"en": q}, "aliases": {}} for q in ("Q60", "Q1", "Q5")
        },
        "points": {"Q60": [(-74.0, 40.7)]},
    }
    keys = {
        "p402": ["123", "456"],
        "statistical_metros": ["Q60"],
        "metro_candidates": ["Q1757", "Q60"],
        "fips_items": ["36047", "36061"],
        "labels_and_aliases": ["Q1757", "Q33", "Q60"],
        "coordinates": ["Q1757", "Q33", "Q60"],
    }
    root = tmp_path / "wiki?data#%"
    # A job stopped midway leaves no current copy and resumes where it stopped.
    stopped = Stub(fail_at=5, **maps)
    with pytest.raises(RuntimeError, match="dropped"):
        wikidata_copy.make_copy(root, keys, stopped, day="2026-11-08")
    assert wikidata_copy.current_copy(root) is None
    resumed = Stub(**maps)
    folder = wikidata_copy.make_copy(root, keys, resumed)
    assert folder.name == "copy-2026-11-08"
    asked = [call for call in stopped.calls[:4] + resumed.calls]
    for lookup in wikidata_copy.LOOKUPS:
        seen = [key for name, chunk in asked if name == lookup for key in chunk]
        assert len(seen) == len(set(seen)), lookup
    # The names and coordinates cover the items the other lookups found too.
    names = {k for n, c in asked if n == "labels_and_aliases" for k in c}
    assert names == {"Q1757", "Q33", "Q60", "Q5", "Q1", "Q2"}
    assert json.loads((root / "current.json").read_text()) == {"copy": folder.name}

    copy = wikidata_copy.current_copy(root)
    assert copy.day == "2026-11-08"
    assert copy.answers("p402", ["123", "456", "789"]) == ({"123": "Q5"}, ["789"])
    assert copy.answers("coordinates", ["Q60"]) == ({"Q60": [(-74.0, 40.7)]}, [])
    found, uncovered = copy.answers("labels_and_aliases", ["Q1", "Q33"])
    assert found == {"Q1": {"labels": {"en": "Q1"}, "aliases": {}}} and uncovered == []
    assert copy.answers("fips_items", ["36061"]) == ({"36061": ["Q11"]}, [])
    with pytest.raises(ValueError, match="not a Wikidata lookup"):
        copy.answers("areas", ["Q1"])
    copy.close()
    # A finished copy is never changed: named again when it covers the keys,
    # refused when it lacks some; a day is an ISO date.
    untouched = Stub(fail_at=1, **maps)
    assert wikidata_copy.make_copy(root, keys, untouched, day="2026-11-08") == folder
    assert untouched.calls == []
    more = {**keys, "p402": ["123", "456", "789"]}
    with pytest.raises(wikidata_copy.WikidataCopyError, match="lacks 1 p402 keys"):
        wikidata_copy.make_copy(root, more, untouched, day="2026-11-08")
    for day in ("../2026-11-08", ""):
        with pytest.raises(wikidata_copy.WikidataCopyError, match="not a copy day"):
            wikidata_copy.make_copy(root, keys, untouched, day=day)
    # An impossible day's folder is no unfinished copy; a linked copy is not
    # read; a second job beside a running one is refused.
    (root / "copy-2026-99-99").mkdir()
    assert wikidata_copy._unfinished_day(root) is None
    try:
        (tmp_path / "linked").symlink_to(folder)
    except OSError:  # no symlinks on this platform
        pass
    else:
        with pytest.raises(wikidata_copy.WikidataCopyError, match="no Wikidata copy"):
            wikidata_copy.WikidataCopy(tmp_path / "linked")

    @contextlib.contextmanager
    def held(directory, name):
        raise store.LockHeld("held")
        yield

    monkeypatch.setattr(store, "exclusive_writer", held)
    with pytest.raises(wikidata_copy.WikidataCopyError, match="another copy job"):
        wikidata_copy.make_copy(root, keys, untouched, day="2026-11-08")


def test_a_failed_call_is_asked_again(tmp_path, monkeypatch):
    import http.client

    monkeypatch.setattr(wikidata_copy, "CALL_WAIT", 0)

    class CutOnce(Stub):
        def p402(self, osm_relation_ids):
            if not getattr(self, "cut", False):
                self.cut = True
                raise http.client.IncompleteRead(b"")
            return super().p402(osm_relation_ids)

    client = CutOnce(mapping={"123": "Q5"})
    folder = wikidata_copy.make_copy(
        tmp_path, {"p402": ["123"]}, client, day="2026-11-08"
    )
    copy = wikidata_copy.WikidataCopy(folder)
    assert copy.answers("p402", ["123"]) == ({"123": "Q5"}, [])
    copy.close()


def test_a_client_answers_from_the_copy_and_asks_the_rest_live(
    tmp_path, monkeypatch, caplog
):
    from transitio_index import overture

    root = tmp_path / "wikidata"
    keys = {"p402": ["123", "456"]}
    wikidata_copy.make_copy(root, keys, Stub(mapping={"123": "Q5"}), day="2026-11-08")

    def live(query):
        """Wikidata's answer for relation 789 only."""
        assert '"789"' in query and '"123"' not in query
        return [{"osm": {"value": "789"}, "item": {"value": "http://x/entity/Q7"}}]

    # Relation 456 is covered without an item; only 789 is asked live.
    overture.use_wikidata_copy(root)
    client = overture.WikidataClient()
    monkeypatch.setattr(client, "_get", live)
    assert client.p402(["789", "456", "123"]) == {"123": "Q5", "789": "Q7"}
    assert client.provenance() == {"copy": "2026-11-08", "live": {"p402": 1}}
    # Without a copy every key is asked live, with one warning.
    overture.use_wikidata_copy(tmp_path / "none")
    client = overture.WikidataClient()
    monkeypatch.setattr(client, "_get", live)
    assert client.p402(["789"]) == {"789": "Q7"}
    assert client.provenance() == {"copy": None, "live": {"p402": 1}}
    assert "no Wikidata copy" in caplog.text
