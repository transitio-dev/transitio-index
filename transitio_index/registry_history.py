"""Check a change to the place registry against its contract.

The registry is a current-state file, so the rules of its history are
enforced here, on the diff between the base branch's file and the pull
request's: rows are only ever added; a live row may only gain concordance
values or detached entries, or become merged or retired; a merged row's
successor changes only when that successor itself was merged (flattening);
a retired row never changes; the counter never decreases. Run in CI as
``python -m transitio_index.registry_history BASE HEAD``, where an absent
BASE stands for the header-only registry the series started from.

Exit status 1 lists every violation.
"""

import argparse
import pathlib
import sys

from transitio_index import registry  # noqa: E402

EMPTY = b'{"next_id": 1, "registry": 1}\n'


def _load(path, *, missing_ok=False):
    """The parsed registry at ``path``, read as a regular file — never through
    a symlink; only the base may be absent, standing for the header-only
    registry the series started from."""
    if missing_ok and not path.is_symlink() and not path.exists():
        raw = EMPTY
    else:
        raw = registry._read(path)
    return registry.parse(raw, str(path))


def _live(row):
    return row.get("status") is None


def violations(base, head):
    """The contract violations between two parsed registries."""
    base_header, base_rows = base
    head_header, head_rows = head
    found = []
    if head_header["next_id"] < base_header["next_id"]:
        found.append(
            f"next_id decreased from {base_header['next_id']} to {head_header['next_id']}"
        )
    for place_id in head_rows:
        if (
            place_id not in base_rows
            and registry._number(place_id) < base_header["next_id"]
        ):
            found.append(f"{place_id}: added below the base counter")
    for place_id, old in base_rows.items():
        new = head_rows.get(place_id)
        if new is None:
            found.append(f"{place_id}: row removed")
            continue
        if _live(old):
            if _live(new):
                found.extend(_only_gained(place_id, old, new))
                for field in (
                    "kind",
                    "name",
                    "country_code",
                    "minted_from",
                    "minted_in",
                ):
                    if old[field] != new[field]:
                        found.append(f"{place_id}: {field} changed")
            elif new["status"] in ("merged", "retired"):
                found.extend(
                    _only_gained(
                        place_id, old, new, allow_drop=new["status"] == "retired"
                    )
                )
            continue
        if old["status"] == "retired":
            if new != old:
                found.append(f"{place_id}: retired row changed")
            continue
        # merged
        if new.get("status") != "merged":
            found.append(f"{place_id}: merged row changed status")
            continue
        found.extend(_only_gained(place_id, old, new))
        for field in ("at", "reason"):
            if old[field] != new[field]:
                found.append(f"{place_id}: {field} changed")
        if new["into"] != old["into"]:
            target = head_rows.get(old["into"])
            if (
                target is None
                or target.get("status") != "merged"
                or target["into"] != new["into"]
            ):
                found.append(
                    f"{place_id}: into retargeted from {old['into']} to "
                    f"{new['into']} without a flattening"
                )
    return found


def _only_gained(place_id, old, new, allow_drop=False):
    found = []
    old_c = old.get("concordances", {})
    new_c = new.get("concordances", {})
    for namespace, values in old_c.items():
        kept = new_c.get(namespace, [])
        if allow_drop and not kept:
            continue
        if kept[: len(values)] != values:
            found.append(f"{place_id}: {namespace} concordances lost or reordered")
    old_d = old.get("detached", {})
    new_d = new.get("detached", {})
    for namespace, entries in old_d.items():
        kept = new_d.get(namespace, [])
        if allow_drop and not kept:
            continue
        if kept[: len(entries)] != entries:
            found.append(f"{place_id}: {namespace} detachments lost or reordered")
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("base", type=pathlib.Path)
    parser.add_argument("head", type=pathlib.Path)
    arguments = parser.parse_args(argv)
    try:
        found = violations(
            _load(arguments.base, missing_ok=True), _load(arguments.head)
        )
    except registry.RegistryError as error:
        print(f"invalid registry: {error}", file=sys.stderr)
        return 1
    for line in found:
        print(line, file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
