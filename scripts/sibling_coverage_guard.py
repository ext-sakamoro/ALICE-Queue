#!/usr/bin/env python3
"""Check that the advisory scan covers the real dependency graph, path siblings included.

`cargo audit` and `cargo deny check` pass on an empty or partial graph, so before trusting them
this compares:

  - the fresh resolve (Cargo.lock): not empty, holds this crate and every path sibling (each
    dependency in any table of the manifest whose `path` leaves the repository, i.e. starts with
    `../`; its package name is read from that directory's Cargo.toml);
  - with `--graph`, cargo deny's crate graph (`cargo deny --all-features list --format json
    --layout crate`): holds this crate and every path sibling, and is no larger than the resolve
    it came from (it keeps only the features and targets that are built, so it is smaller).

An empty graph cannot hold this crate, and an empty resolve is smaller than any graph that does,
so both fail through those checks. Prints the counts; exit 1 on any failed check.

usage: sibling_coverage_guard.py --manifest Cargo.toml --lock Cargo.lock [--graph graph.json]
"""
import argparse
import json
import pathlib
import re
import sys
import tomllib


def path_siblings(manifest: pathlib.Path) -> list[str]:
    data = tomllib.loads(manifest.read_text(encoding="utf-8"))
    tables = []
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        tables.append(data.get(key, {}))
    for target in data.get("target", {}).values():
        for key in ("dependencies", "dev-dependencies", "build-dependencies"):
            tables.append(target.get(key, {}))
    names = []
    for table in tables:
        for spec in table.values():
            if isinstance(spec, dict) and str(spec.get("path", "")).startswith("../"):
                sib = (manifest.parent / spec["path"] / "Cargo.toml").resolve()
                pkg = tomllib.loads(sib.read_text(encoding="utf-8")).get("package", {}).get("name")
                if not pkg:
                    sys.exit(f"{sib}: no [package] name")
                names.append(pkg)
    return sorted(set(names))


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--lock", required=True)
    ap.add_argument("--graph")
    a = ap.parse_args(argv)

    manifest = pathlib.Path(a.manifest)
    me = tomllib.loads(manifest.read_text(encoding="utf-8"))["package"]["name"]
    siblings = path_siblings(manifest)
    resolved = set(re.findall(r'^name = "([^"]+)"', pathlib.Path(a.lock).read_text(encoding="utf-8"), flags=re.M))
    n_resolved = len(re.findall(r'^name = "', pathlib.Path(a.lock).read_text(encoding="utf-8"), flags=re.M))

    errors = []
    for name in [me, *siblings]:
        if name not in resolved:
            errors.append(f"{name} is not in the resolve (Cargo.lock): the scan would not cover it")
    report = f"sibling_coverage_guard: resolve {n_resolved} packages, {len(siblings)} path siblings ({', '.join(siblings) or 'none'})"

    if a.graph:
        graph = json.loads(pathlib.Path(a.graph).read_text(encoding="utf-8"))
        in_graph = {key.split(" ", 1)[0] for key in graph}
        for name in [me, *siblings]:
            if name not in in_graph:
                errors.append(f"{name} is not in cargo deny's graph (an empty graph compared nothing)")
        if len(graph) > n_resolved:
            errors.append(f"cargo deny's graph has {len(graph)} crates, more than the {n_resolved} resolved packages")
        report += f", cargo deny graph {len(graph)} crates"

    print(report)
    for e in errors:
        print(f"::error::{e}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
