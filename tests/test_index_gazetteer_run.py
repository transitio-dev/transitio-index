import hashlib

import pytest

from transitio_index import build as build_index  # noqa: E402

from transitio_index import expand, registry, store  # noqa: E402

HEADER = '{"next_id": 1, "registry": 1}\n'


def _stub_stage(
    cache, pointer, text, identify=None, pointer_publish=False, manifest=None
):
    """A stub stage: one generation (staged, or behind a live pointer with
    ``pointer_publish``), and an identification when asked."""

    def run_stage(places, run):
        if identify:
            places.identify(
                identify, kind="city", minted_from="test", minted_in="test 1"
            )
        directory = store.open_subdir(cache, "gazetteer")
        try:
            with store.exclusive_writer(directory):
                published = store.publish(
                    cache / "gazetteer",
                    pointer,
                    {"places.jsonl": store.jsonl_chunks([{"place_id": text}])},
                    {"stage": pointer, **(manifest or {})},
                    held=directory,
                    staged=not pointer_publish,
                )
        finally:
            directory.close()
        run[pointer] = published["generation"]
        return published

    return run_stage


def _failing(places, run):
    raise RuntimeError("boom")


def _registry(tmp_path):
    path = tmp_path / "places_registry.jsonl"
    path.write_text(HEADER)
    return path


def _consume(cache, path):
    """The registry the consumers' guard yields."""
    with build_index.check_registry_state(cache, path) as places:
        return places


def _committed(cache, path):
    """A first run that minted Q1: the set a failing rerun must leave current."""
    stages = [_stub_stage(cache, "seed.json", "first", identify={"wikidata": ["Q1"]})]
    build_index.commit_run(cache, path, read_only=False, stages=stages)
    return store.run_generations(cache / "gazetteer"), path.read_bytes()


def _assert_first_run_current(cache, path, first, *, registry_unchanged=True):
    generations, registry_bytes = first
    assert store.run_generations(cache / "gazetteer") == generations
    rows, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places.jsonl")
    assert rows == [{"place_id": "first"}]
    assert registry_unchanged == (path.read_bytes() == registry_bytes)


def test_a_run_commits_its_set_and_registry_together(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    stages = [
        _stub_stage(cache, "seed.json", "s", identify={"wikidata": ["Q1"]}),
        _stub_stage(cache, "metros.json", "m"),
    ]
    manifest = build_index.commit_run(cache, path, read_only=False, stages=stages)[-1]
    assert set(manifest["generations"]) == {
        "seed.json",
        "metros.json",
        expand.SNAPSHOT_POINTER,
    }
    assert manifest["minted"] == 1 and manifest["next_id"] == 2
    assert manifest["registry_digest"] == hashlib.sha256(path.read_bytes()).hexdigest()
    # No per-stage pointer exists; consumers resolve through the run manifest.
    assert not (cache / "gazetteer" / "seed.json").exists()
    rows, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places.jsonl")
    assert rows == [{"place_id": "s"}]
    assert (
        store.current_generation(cache / "gazetteer", "metros.json")
        == manifest["generations"]["metros.json"]
    )
    assert _consume(cache, path).digest == manifest["registry_digest"]
    # Another build's places and values are no reason to rerun: consumers
    # read the registry this run saved. A place the run used that lost a
    # value is.
    with registry.session(path) as other:
        other.identify(
            {"wikidata": ["Q1"], "cbsa": ["1"]},
            kind="city",
            minted_from="o",
            minted_in="o",
        )
        other.identify(
            {"wikidata": ["Q2"]}, kind="city", minted_from="o", minted_in="o"
        )
        other.save()
    consumed = _consume(cache, path)
    assert consumed.digest == manifest["registry_digest"] and consumed.next_id == 2
    path.write_text(path.read_text().replace('"Q1"', '"Q9"'))
    with pytest.raises(registry.RegistryError, match="tp_1 lost wikidata:Q1; rerun"):
        _consume(cache, path)


def test_a_failure_before_the_save_leaves_the_previous_run_current(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    first = _committed(cache, path)
    stages = [
        _stub_stage(cache, "seed.json", "second", identify={"wikidata": ["Q2"]}),
        _failing,
    ]
    with pytest.raises(RuntimeError, match="boom"):
        build_index.commit_run(cache, path, read_only=False, stages=stages)
    _assert_first_run_current(cache, path, first)
    _consume(cache, path)
    # The rerun mints the place once and commits.
    manifest = build_index.commit_run(cache, path, read_only=False, stages=stages[:1])[
        -1
    ]
    assert manifest["minted"] == 1
    rows, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places.jsonl")
    assert rows == [{"place_id": "second"}]


def test_a_crash_after_the_save_is_recovered_by_a_rerun(tmp_path, monkeypatch):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    first = _committed(cache, path)
    real = store.publish

    def crash(directory, pointer, *args, **kwargs):
        if pointer == store.RUN_POINTER:
            raise RuntimeError("crash")
        return real(directory, pointer, *args, **kwargs)

    monkeypatch.setattr(store, "publish", crash)
    stages = [_stub_stage(cache, "seed.json", "second", identify={"wikidata": ["Q2"]})]
    with pytest.raises(RuntimeError, match="crash"):
        build_index.commit_run(cache, path, read_only=False, stages=stages)
    # The registry holds the mint, valid rows a rerun reproduces; the first
    # run stays current, and its consumers read the registry it saved.
    assert registry.load(path).resolve("Q2") == "tp_2"
    _assert_first_run_current(cache, path, first, registry_unchanged=False)
    assert _consume(cache, path).next_id == 2
    monkeypatch.undo()
    manifest = build_index.commit_run(cache, path, read_only=False, stages=stages)[-1]
    assert manifest["minted"] == 0 and manifest["generations"]["seed.json"]
    _consume(cache, path)


@pytest.mark.parametrize(
    "identify, staged_first",
    [
        ({"wikidata": ["Q2"]}, False),
        ({"wikidata": ["Q1"], "cbsa": ["1"]}, False),
        ({"wikidata": ["Q1"], "cbsa": ["1"]}, True),
    ],
    ids=["mint", "enrich", "enrich-after-a-staged-stage"],
)
def test_read_only_refuses_a_change_and_keeps_the_previous_run(
    tmp_path, identify, staged_first
):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    first = _committed(cache, path)
    # A change the metros stage is first to discover, after seed has
    # already staged its output, is refused the same way.
    stages = [_stub_stage(cache, "seed.json", "second")] if staged_first else []
    stages.append(_stub_stage(cache, "metros.json", "m", identify=identify))
    with pytest.raises(registry.RegistryError, match="read-only"):
        build_index.commit_run(cache, path, read_only=True, stages=stages)
    _assert_first_run_current(cache, path, first)


@pytest.mark.parametrize(
    "pointer, text, message",
    [
        ("run.json", "{}", "names no generation"),
        ("seed.json", "not json", "not JSON"),
        ("seed.json", '{"generation": 5}', "names no generation"),
    ],
)
def test_a_corrupt_pointer_is_an_error_not_an_absence(tmp_path, pointer, text, message):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    gazetteer = cache / "gazetteer"
    assert store.run_generations(gazetteer) is None
    assert store.current_generation(gazetteer, pointer) is None
    gazetteer.mkdir(parents=True)
    (gazetteer / pointer).write_text(text)
    with pytest.raises(store.StoreError, match=message):
        store.current_generation(gazetteer, pointer)
    if pointer == store.RUN_POINTER:
        with pytest.raises(store.StoreError, match=message):
            _consume(cache, path)


def test_runs_on_one_cache_do_not_interleave(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory, store.RUN_LOCK):
            with pytest.raises(store.StoreError, match="another build"):
                build_index.commit_run(
                    cache,
                    path,
                    read_only=False,
                    stages=[_stub_stage(cache, "seed.json", "s")],
                )
    finally:
        directory.close()
    assert store.run_generations(cache / "gazetteer") is None


def test_the_run_manifest_outranks_a_stale_pointer(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    old = _stub_stage(cache, "seed.json", "old", pointer_publish=True)(None, {})
    build_index.commit_run(
        cache, path, read_only=False, stages=[_stub_stage(cache, "seed.json", "new")]
    )
    rows, _ = store.read_jsonl(cache / "gazetteer", "seed.json", "places.jsonl")
    assert rows == [{"place_id": "new"}]
    # An explicit map, as a stage reading its predecessor inside a run, wins.
    rows, _ = store.read_jsonl(
        cache / "gazetteer",
        "seed.json",
        "places.jsonl",
        generations={"seed.json": old["generation"]},
    )
    assert rows == [{"place_id": "old"}]


def test_a_consumer_holds_its_cache_but_not_the_registry(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    _committed(cache, path)
    other = tmp_path / "other"
    with build_index.check_registry_state(cache, path) as places:
        with pytest.raises(store.StoreError, match="another build"):
            build_index.commit_run(
                cache,
                path,
                read_only=False,
                stages=[_stub_stage(cache, "seed.json", "s")],
            )
        # Another build's run writes the registry meanwhile.
        stages = [_stub_stage(other, "seed.json", "o", identify={"wikidata": ["Q2"]})]
        build_index.commit_run(other, path, read_only=False, stages=stages)
    assert registry.load(path).next_id == 3 and places.next_id == 2
    # Without a registry file the cache is held too.
    fresh = tmp_path / "fresh"
    with build_index.check_registry_state(fresh, tmp_path / "none.jsonl") as none:
        assert none is None
        with pytest.raises(store.StoreError, match="another build"):
            build_index.commit_run(fresh, path, read_only=False, stages=[])


def test_consumers_follow_the_registry_the_expand_stage_saved(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    _committed(cache, path)
    run_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    # Expand identified a discovery on that registry and saved it.
    with registry.session(path) as reg:
        reg.identify(
            {"wikidata": ["Q2"]}, kind="city", minted_from="t", minted_in="t 1"
        )
        saved = reg.save()

    run_generation = store.run_manifest(cache / "gazetteer")["generation"]

    def expanded(base, generation=run_generation):
        _stub_stage(
            cache,
            "expanded.json",
            "x",
            pointer_publish=True,
            manifest={
                "registry_base": base,
                "registry_digest": saved,
                "run_generation": generation,
            },
        )(None, {})

    expanded(run_digest)
    _consume(cache, path)
    # A registry changed after an expansion that kept no copy of it, and
    # expanded places built on another registry than the run's, are both
    # refused.
    path.write_text(path.read_text().replace('"next_id": 3', '"next_id": 4'))
    with pytest.raises(registry.RegistryError, match="no copy of the .*rerun expand"):
        _consume(cache, path)
    path.write_text(path.read_text().replace('"next_id": 4', '"next_id": 3'))
    for base, generation in (
        (run_digest, "gen-00000000-0000000000000000"),
        ("0" * 64, run_generation),
    ):
        expanded(base, generation)
        with pytest.raises(registry.RegistryError, match="rerun expand"):
            _consume(cache, path)
    # A run manifest that records no digest is refused, never skipped.
    _stub_stage(
        cache,
        store.RUN_POINTER,
        "r",
        pointer_publish=True,
        manifest={"generations": {}},
    )(None, {})
    with pytest.raises(registry.RegistryError, match="no registry digest"):
        _consume(cache, path)


@pytest.mark.parametrize("used, readable", [(["tp_1"], True), (["no-id"], False)])
def test_a_copy_names_its_used_places_by_id(tmp_path, used, readable):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    directory = store.open_subdir(cache, "gazetteer")
    try:
        with store.exclusive_writer(directory), registry.session(path) as reg:
            published = expand.publish_snapshot(
                cache / "gazetteer",
                reg,
                expand.SNAPSHOT_POINTER,
                held=directory,
                used=used,
                staged=True,
            )
    finally:
        directory.close()
    copy = expand._registry_copy(
        cache / "gazetteer", published["generation"], reg.digest
    )
    assert (copy is not None) == readable


def test_prepare_stages_run_before_the_registry_is_taken(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)

    def prepare(run):
        with registry.session(path):  # free: the run holds no session yet
            pass
        return _stub_stage(cache, "overture.json", "o")(None, run)

    stages = [_stub_stage(cache, "seed.json", "s")]
    manifest = build_index.commit_run(
        cache, path, read_only=False, stages=stages, prepare=[prepare]
    )[-1]
    assert set(manifest["generations"]) == {
        "overture.json",
        "seed.json",
        expand.SNAPSHOT_POINTER,
    }


def test_the_gazetteer_reads_overture_before_its_registry_turn(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        build_index, "commit_run", lambda *args, **kwargs: seen.update(kwargs) or []
    )
    build_index.run_gazetteer(
        build_index.parse_args(["--stage", "gazetteer", "--wait-for-registry"])
    )
    assert seen["wait"] is True
    assert (len(seen["prepare"]), len(seen["stages"])) == (1, 5)
