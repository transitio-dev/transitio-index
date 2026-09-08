import hashlib

import pytest

from transitio_index import build as build_index  # noqa: E402

from transitio_index import registry, store  # noqa: E402

HEADER = '{"next_id": 1, "registry": 1}\n'


def _stage(cache, pointer, text, identify=None, pointer_publish=False, manifest=None):
    """A stub stage: one staged generation, and an identification when asked."""

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
    """Enter the consumers' guard, which holds the registry read-only."""
    with build_index.check_registry_state(cache, path):
        pass


def _committed(cache, path):
    """A first run that minted Q1: the set a failing rerun must leave current."""
    stages = [_stage(cache, "seed.json", "first", identify={"wikidata": ["Q1"]})]
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
        _stage(cache, "seed.json", "s", identify={"wikidata": ["Q1"]}),
        _stage(cache, "metros.json", "m"),
    ]
    manifest = build_index.commit_run(cache, path, read_only=False, stages=stages)[-1]
    assert set(manifest["generations"]) == {"seed.json", "metros.json"}
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
    _consume(cache, path)
    # A registry changed after the commit: consumers refuse until a rerun.
    path.write_text(path.read_text().replace('"next_id": 2', '"next_id": 3'))
    with pytest.raises(registry.RegistryError, match="rerun the gazetteer"):
        _consume(cache, path)


def test_a_failure_before_the_save_leaves_the_previous_run_current(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    first = _committed(cache, path)
    stages = [
        _stage(cache, "seed.json", "second", identify={"wikidata": ["Q2"]}),
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
    stages = [_stage(cache, "seed.json", "second", identify={"wikidata": ["Q2"]})]
    with pytest.raises(RuntimeError, match="crash"):
        build_index.commit_run(cache, path, read_only=False, stages=stages)
    # The registry holds the mint, valid rows a rerun reproduces; the first
    # run stays current, and its consumers refuse until the rerun.
    assert registry.load(path).resolve("Q2") == "tp_2"
    _assert_first_run_current(cache, path, first, registry_unchanged=False)
    with pytest.raises(registry.RegistryError, match="rerun the gazetteer"):
        _consume(cache, path)
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
    stages = [_stage(cache, "seed.json", "second")] if staged_first else []
    stages.append(_stage(cache, "metros.json", "m", identify=identify))
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
                    stages=[_stage(cache, "seed.json", "s")],
                )
    finally:
        directory.close()
    assert store.run_generations(cache / "gazetteer") is None


def test_the_run_manifest_outranks_a_stale_pointer(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    old = _stage(cache, "seed.json", "old", pointer_publish=True)(None, {})
    build_index.commit_run(
        cache, path, read_only=False, stages=[_stage(cache, "seed.json", "new")]
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


def test_a_consumer_holds_the_registry_while_it_reads_the_run(tmp_path):
    cache, path = tmp_path / "cache", _registry(tmp_path)
    _committed(cache, path)
    with build_index.check_registry_state(cache, path):
        with pytest.raises(registry.RegistryError, match="another build"):
            build_index.commit_run(
                cache, path, read_only=False, stages=[_stage(cache, "seed.json", "s")]
            )


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
        _stage(
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
    # A registry changed after expand, and expanded places built on another
    # registry than the run's, are both refused.
    path.write_text(path.read_text().replace('"next_id": 3', '"next_id": 4'))
    with pytest.raises(registry.RegistryError, match="rerun the gazetteer"):
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
    _stage(
        cache,
        store.RUN_POINTER,
        "r",
        pointer_publish=True,
        manifest={"generations": {}},
    )(None, {})
    with pytest.raises(registry.RegistryError, match="no registry digest"):
        _consume(cache, path)
