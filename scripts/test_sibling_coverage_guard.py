#!/usr/bin/env python3
"""Tests of scripts/sibling_coverage_guard.py.

The guard must fail whenever the advisory scan could not have covered the real dependency graph:
an empty resolve, this crate or one of its path siblings missing from the resolve, and (with
`--graph`) a cargo deny crate graph that is empty, misses this crate or a path sibling, or is
larger than the resolve it came from. A complete resolve and graph pass.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

GUARD = pathlib.Path(__file__).with_name("sibling_coverage_guard.py")


def entry(name, version="1.0.0", src="registry+https://github.com/rust-lang/crates.io-index"):
    return f"{name} {version} {src}", {"licenses": ["MIT"]}


def run(lock_names, graph=None, siblings=("alice-lol",)):
    with tempfile.TemporaryDirectory() as d:
        d = pathlib.Path(d)
        me = d / "ALICE-Me"
        me.mkdir()
        deps = "".join(
            f'{s} = {{ path = "../ALICE-{s.split("-", 1)[1].capitalize()}/{s}", optional = true }}\n'
            for s in siblings
        )
        (me / "Cargo.toml").write_text(
            f'[package]\nname = "alice-me"\nversion = "0.1.0"\n\n[dependencies]\n{deps}'
            'serde = { version = "1" }\n\n[dev-dependencies]\nlocal-helper = { path = "helper" }\n',
            encoding="utf-8",
        )
        (me / "helper").mkdir()
        (me / "helper" / "Cargo.toml").write_text('[package]\nname = "local-helper"\n', encoding="utf-8")
        for s in siblings:
            crate = d / f"ALICE-{s.split('-', 1)[1].capitalize()}" / s
            crate.mkdir(parents=True)
            (crate / "Cargo.toml").write_text(f'[package]\nname = "{s}"\nversion = "0.5.0"\n', encoding="utf-8")
        lock = "".join(f'[[package]]\nname = "{n}"\nversion = "1.0.0"\n\n' for n in lock_names)
        (me / "Cargo.lock").write_text(lock, encoding="utf-8")
        args = [sys.executable, str(GUARD), "--manifest", str(me / "Cargo.toml"), "--lock", str(me / "Cargo.lock")]
        if graph is not None:
            (me / "graph.json").write_text(json.dumps(dict(graph)), encoding="utf-8")
            args += ["--graph", str(me / "graph.json")]
        r = subprocess.run(args, capture_output=True, text=True)
        return r.returncode, r.stdout + r.stderr


def main():
    lock = ["alice-me", "alice-lol", "serde", "only-on-windows"]
    graph = [entry("alice-me", "0.1.0", "path+file:///a"), entry("alice-lol", "0.5.0", "path+file:///b"), entry("serde")]
    cases = [
        ("a full resolve passes", (lock,), 0),
        ("a full resolve and graph pass", (lock, graph), 0),
        ("an empty resolve fails", ([],), 1),
        ("a resolve without this crate fails", (lock[1:],), 1),
        ("a resolve without a path sibling fails", ([n for n in lock if n != "alice-lol"],), 1),
        ("an empty graph fails", (lock, []), 1),
        ("a graph without this crate fails", (lock, graph[1:]), 1),
        ("a graph without a path sibling fails", (lock, [g for g in graph if not g[0].startswith("alice-lol")]), 1),
        ("a graph larger than the resolve fails", (["alice-me", "alice-lol"], graph), 1),
        ("a crate with no path sibling is still checked", (["alice-me", "serde"], None, ()), 0),
        ("no path sibling, empty resolve fails", ([], None, ()), 1),
    ]
    failures = []
    for name, args, want in cases:
        rc, out = run(*args)
        if rc != want:
            failures.append(f"{name}: rc {rc}, want {want}\n{out}")
    if failures:
        print("test_sibling_coverage_guard FAIL:", *failures, sep="\n  ")
        return 1
    print(f"test_sibling_coverage_guard: {len(cases)}/{len(cases)} cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
