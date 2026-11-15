#!/usr/bin/env python3
r"""Tests of scripts/advisory_hold_guard.py on small throwaway trees (no network, no cargo).

  - valid hold                                         -> 0
  - held version gone from Cargo.lock (stale)          -> 1
  - a new package depends on the held version          -> 1
  - a recorded dependent is gone                       -> 1
  - a bare dependency name (the crate's only version)  -> 0 (counted as that version)
  - expired (today is the day after `expires`)        -> 1
  - today is `expires`                                 -> 0 (the last valid day)
  - `expires` 21 days after today                      -> 0 (the longest hold)
  - `expires` 22 days after today                      -> 1 (a hold is renewed, not set far ahead)
  - another version in a pre-release gap of `fixed`    -> 1 (0.3.0-alpha.3 with the
    advisory's ranges ">= 0.2.0, < 0.3.0-alpha.1" / ">= 0.3.0-alpha.8")
  - another version at the pre-release bound           -> 0 (0.3.0-alpha.8)
  - a release above a pre-release upper bound          -> 1 (0.3.0 with "< 0.3.0-alpha.1":
    a release sorts after its pre-releases)
  - numeric pre-release identifiers                    -> 0 (alpha.10 is after alpha.8)
  - a pre-release with no pre-release bound            -> 1 (0.3.0-alpha.1 with ">= 0.2.0":
    a pre-release is fixed only when a range names that release's pre-releases)
  - another version below `fixed`                      -> 1
  - another version at `fixed`                         -> 0
  - another version when no version is safe            -> 1
  - id not ignored in deny.toml                        -> 1
  - vulnerability id not ignored in .cargo/audit.toml  -> 1
  - unmaintained id not in .cargo/audit.toml           -> 0 (cargo audit only warns)
  - no holds file / no Cargo.lock / Cargo.lock empty   -> 2 (nothing compared)
  - no holds, and no ignore anywhere                   -> 0
  - no holds, an id ignored in deny.toml / audit.toml / a cargo audit command line -> 1 (an ignore
    without a hold)
  - no holds, deny.toml without `unused-ignored-advisory = "deny"` -> 1
  - deny.toml without `unused-ignored-advisory = "deny"` -> 1 (an ignore of an advisory that no longer
    matches would pass silently)
  - `unused-ignored-advisory = "warn"`                   -> 1
  - `cargo audit --ignore <id>` in a workflow, held     -> 0 (both `--ignore ID` and `--ignore=ID`)
  - `--ignore` of an id that is not held, in a workflow -> 1
  - `--ignore` of an id that is not held, in preflight  -> 1
  - `--ignore` of a held id whose crate left Cargo.lock -> 1
  - holds, and no ignore read anywhere                  -> 1 (the lists were not read)
  - a quoted id (`--ignore "ID"`, `--ignore='ID'`), held -> 0 (read like an unquoted one)
  - a quoted id that is not held                        -> 1
  - an `--ignore` whose argument is not an id (a variable, an array, a typo, nothing) -> 1 (an
    ignore that cannot be read is not taken for none)
  - `--ignore-source` and other flags that only start with `--ignore` -> 0 (not an ignore of an id)
  - `--ignore` of another tool (pytest, ruff) or in a comment line -> 0 (only cargo audit's count)
  - a cargo audit continued over lines with `\`, the id on the next line: held -> 0, not held -> 1
  - until_release (fixture sparse index, no network):
    - the index has a newer release                    -> 1 ("upstream released")
    - the index has nothing newer                      -> 0
    - the only newer version is yanked                 -> 0 (ignored)
    - the only newer version is a pre-release          -> 0 (printed, not counted)
    - a newer release in a second until_release crate  -> 1
    - until_release missing / empty / malformed        -> 1
    - reason missing / empty                           -> 1
    - index unreachable, CI unset, not expired         -> 0 (falls back to the date, says so)
    - index unreachable, CI unset, expired             -> 1 (no extension)
    - index unreachable under CI=true                  -> 1
  - the sparse-index path of 1-, 2-, 3- and 4+-letter names
  - scope = "lock-only" (in Cargo.lock, not compiled): accepted with a cargo audit ignore and no deny.toml
    ignore -> 0, also through a `cargo audit --ignore` -> 0, an unmaintained one with no ignore at all
    -> 0; ignored in deny.toml -> 1; a vulnerability without a cargo audit ignore -> 1; a reason
    without the `cargo tree` command -> 1; a scope other than graph / lock-only -> 1
  - --require-index: an unreadable index fails from the command line without CI=true
  - every hold of the cases above carries `reason` and `until_release` (the fixture index has
    nothing newer unless a case says so)
"""

import contextlib
import datetime
import io
import json
import os
import pathlib
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import advisory_hold_guard as guard  # noqa: E402

TODAY = datetime.date(2026, 10, 23)


def pkg(name, version, deps=()):
    lines = [f'[[package]]\nname = "{name}"\nversion = "{version}"\n']
    if deps:
        lines.append("dependencies = [\n" + "".join(f' "{d}",\n' for d in deps) + "]\n")
    return "".join(lines)


HOLD = '''[[hold]]
id = "RUSTSEC-0000-0001"
kind = "{kind}"
crate = "leaf"
version = "0.1.0"
dependents = [{dependents}]
fixed = [{fixed}]
expires = "{expires}"
until = "an upgrade"
{reason}{until_release}'''

REASON = 'reason = "leaf 0.1.0 <- mid 1.0.0 <- app 1.0.0"\n'
UNTIL = 'until_release = [{crate = "up", newer_than = "0.6.0"}]\n'



def index_line(name, vers, yanked=False):
    return json.dumps({"name": name, "vers": vers, "deps": [], "cksum": "0" * 64, "features": {}, "yanked": yanked})


def write_index(d, index):
    """index: {crate: [(vers, yanked), ...]}; a crate absent from it has no file (unreachable)"""
    root = d / "index"
    root.mkdir()
    for name, versions in index.items():
        f = root / guard.index_path(name)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("\n".join(index_line(name, v, y) for v, y in versions) + "\n", encoding="utf-8")
    return root


def tree(tmp, name, packages, holds=None, deny_ids=("RUSTSEC-0000-0001",), audit_ids=("RUSTSEC-0000-0001",),
         unused='"deny"', workflow=None, preflight=None):
    d = pathlib.Path(tmp) / name
    (d / "scripts").mkdir(parents=True)
    (d / ".cargo").mkdir()
    (d / ".github" / "workflows").mkdir(parents=True)
    if workflow is not None:
        (d / ".github" / "workflows" / "audit.yml").write_text(workflow, encoding="utf-8")
    if preflight is not None:
        (d / "scripts" / "preflight.sh").write_text(preflight, encoding="utf-8")
    if packages is not None:
        (d / "Cargo.lock").write_text("version = 4\n\n" + "\n".join(packages), encoding="utf-8")
    if holds is not None:
        (d / "scripts" / "advisory_holds.toml").write_text(holds, encoding="utf-8")
    deny = ", ".join(f'{{ id = "{i}", reason = "test" }}' for i in deny_ids)
    setting = "" if unused is None else f"unused-ignored-advisory = {unused}\n"
    (d / "deny.toml").write_text(f"[advisories]\n{setting}ignore = [{deny}]\n", encoding="utf-8")
    audit = ", ".join(f'"{i}"' for i in audit_ids)
    (d / ".cargo" / "audit.toml").write_text(f"[advisories]\nignore = [{audit}]\n", encoding="utf-8")
    return d


def hold(kind="vulnerability", dependents=('"mid 1.0.0"',), fixed=('>= 0.2.0',), expires="2026-11-13",
         reason=REASON, until_release=UNTIL):
    return HOLD.format(kind=kind, dependents=", ".join(dependents),
                       fixed=", ".join(f'"{r}"' for r in fixed), expires=expires,
                       reason=reason, until_release=until_release)


LOCK = 'scope = "lock-only"\n'
LOCK_REASON = ('reason = "leaf 0.1.0 <- mid 1.0.0 in Cargo.lock only: `cargo tree -e normal -i leaf@0.1.0 --target all '
               '--all-features` prints nothing"\n')
NO_NEWER = {"up": [("0.5.0", False), ("0.6.0", False)]}
TWO = 'until_release = [{crate = "up", newer_than = "0.6.0"}, {crate = "core", newer_than = "0.0.7"}]\n'


BASE = [pkg("app", "1.0.0", ["mid"]), pkg("mid", "1.0.0", ["leaf 0.1.0"]), pkg("leaf", "0.1.0")]


def main():
    failures = []
    with tempfile.TemporaryDirectory() as tmp:
        cases = [
            # (name, packages, holds, deny ids, audit ids, want rc, a word the message must contain)
            ("valid", BASE, hold(), None, None, 0, "held until"),
            ("stale", [pkg("app", "1.0.0", ["mid"]), pkg("mid", "1.0.0")], hold(), None, None, 1, "stale"),
            ("new dependent", BASE + [pkg("other", "2.0.0", ["leaf 0.1.0"])], hold(), None, None, 1, "new ['other 2.0.0']"),
            ("dependent gone", [pkg("app", "1.0.0", ["leaf"]), pkg("leaf", "0.1.0")], hold(), None, None, 1, "gone ['mid 1.0.0']"),
            ("bare name", [pkg("app", "1.0.0", ["mid"]), pkg("mid", "1.0.0", ["leaf"]), pkg("leaf", "0.1.0")],
             hold(), None, None, 0, "held until"),
            ("expired", BASE, hold(expires="2026-10-22"), None, None, 1, "expired"),
            ("expires today", BASE, hold(expires="2026-10-23"), None, None, 0, "held until"),
            ("expires in 21 days", BASE, hold(expires="2026-11-13"), None, None, 0, "held until"),
            ("expires in 22 days", BASE, hold(expires="2026-11-14"), None, None, 1, "more than 21 days"),
            ("pre-release in the gap", BASE + [pkg("x", "1.0.0", ["leaf 0.3.0-alpha.3"]), pkg("leaf", "0.3.0-alpha.3")],
             hold(fixed=(">= 0.2.0, < 0.3.0-alpha.1", ">= 0.3.0-alpha.8")), None, None, 1, "not held and not fixed"),
            ("pre-release at the bound", BASE + [pkg("x", "1.0.0", ["leaf 0.3.0-alpha.8"]), pkg("leaf", "0.3.0-alpha.8")],
             hold(fixed=(">= 0.2.0, < 0.3.0-alpha.1", ">= 0.3.0-alpha.8")), None, None, 0, "held until"),
            ("release past the gap", BASE + [pkg("x", "1.0.0", ["leaf 0.3.1"]), pkg("leaf", "0.3.1")],
             hold(fixed=(">= 0.2.0, < 0.3.0-alpha.1", ">= 0.3.0-alpha.8")), None, None, 0, "held until"),
            ("release above a pre-release upper bound", BASE + [pkg("x", "1.0.0", ["leaf 0.3.0"]), pkg("leaf", "0.3.0")],
             hold(fixed=(">= 0.2.0, < 0.3.0-alpha.1",)), None, None, 1, "not held and not fixed"),
            ("numeric pre-release identifiers", BASE + [pkg("x", "1.0.0", ["leaf 0.3.0-alpha.10"]), pkg("leaf", "0.3.0-alpha.10")],
             hold(fixed=(">= 0.3.0-alpha.8",)), None, None, 0, "held until"),
            ("pre-release, no pre-release bound", BASE + [pkg("x", "1.0.0", ["leaf 0.3.0-alpha.1"]), pkg("leaf", "0.3.0-alpha.1")],
             hold(), None, None, 1, "not held and not fixed"),
            ("other below fixed", BASE + [pkg("x", "1.0.0", ["leaf 0.1.5"]), pkg("leaf", "0.1.5")],
             hold(), None, None, 1, "not held and not fixed"),
            ("other at fixed", BASE + [pkg("x", "1.0.0", ["leaf 0.2.0"]), pkg("leaf", "0.2.0")],
             hold(), None, None, 0, "held until"),
            ("other, none safe", BASE + [pkg("x", "1.0.0", ["leaf 0.2.0"]), pkg("leaf", "0.2.0")],
             hold(kind="unmaintained", fixed=()), None, None, 1, "not held and not fixed"),
            ("not in deny.toml", BASE, hold(), (), None, 1, "not ignored in deny.toml"),
            ("vulnerability not in audit.toml", BASE, hold(), None, (), 1, "not ignored in .cargo/audit.toml"),
            ("unmaintained not in audit.toml", BASE, hold(kind="unmaintained"), None, (), 0, "held until"),
            ("no holds, no ignores", BASE, "", (), (), 0, "ignore nothing"),
            ("no holds, deny.toml ignores an id", BASE, "", ("RUSTSEC-0000-0001",), (), 1, "deny.toml ignores RUSTSEC-0000-0001 without a hold"),
            ("no holds, audit.toml ignores an id", BASE, "", (), ("RUSTSEC-0000-0001",), 1, ".cargo/audit.toml ignores RUSTSEC-0000-0001 without a hold"),
            ("no holds file", BASE, None, None, None, 2, "not found"),
            ("no Cargo.lock", None, hold(), None, None, 2, "not found"),
            ("empty Cargo.lock", [], hold(), None, None, 2, "no packages"),
            # until_release against a fixture index; extra: (index, ci)
            ("upstream newer", BASE, hold(), None, None, 1, "upstream released up 0.6.1",
             ({"up": [("0.6.0", False), ("0.6.1", False)]}, False)),
            ("upstream not newer", BASE, hold(), None, None, 0, "up has no release newer than 0.6.0",
             (NO_NEWER, False)),
            ("upstream newer is yanked", BASE, hold(), None, None, 0, "up has no release newer than 0.6.0",
             ({"up": [("0.6.0", False), ("0.7.0", True)]}, False)),
            ("upstream newer is a pre-release", BASE, hold(), None, None, 0, "pre-releases, not counted: 0.7.0-pre.1",
             ({"up": [("0.6.0", False), ("0.7.0-pre.1", False)]}, False)),
            ("second crate newer", BASE, hold(until_release=TWO), None, None, 1, "upstream released core 0.0.8",
             ({"up": [("0.6.0", False)], "core": [("0.0.7", False), ("0.0.8", False)]}, False)),
            ("second crate not newer", BASE, hold(until_release=TWO), None, None, 0, "core has no release newer than 0.0.7",
             ({"up": [("0.6.0", False)], "core": [("0.0.7", False)]}, False)),
            ("until_release missing", BASE, hold(until_release=""), None, None, 1, "no `until_release`", (NO_NEWER, False)),
            ("until_release empty", BASE, hold(until_release="until_release = []\n"), None, None, 1,
             "no `until_release`", (NO_NEWER, False)),
            ("until_release without newer_than", BASE, hold(until_release='until_release = [{crate = "up"}]\n'),
             None, None, 1, "needs crate and newer_than", (NO_NEWER, False)),
            ("until_release bad version", BASE, hold(until_release='until_release = [{crate = "up", newer_than = "x"}]\n'),
             None, None, 1, "is not a version", (NO_NEWER, False)),
            ("reason missing", BASE, hold(reason=""), None, None, 1, "no `reason`", (NO_NEWER, False)),
            ("reason empty", BASE, hold(reason='reason = "  "\n'), None, None, 1, "no `reason`", (NO_NEWER, False)),
            ("unreachable, not CI", BASE, hold(), None, None, 0, "falling back to the expiry date 2026-11-13",
             ({}, False)),
            ("unreachable, not CI, expired", BASE, hold(expires="2026-10-22"), None, None, 1, "expired on 2026-10-22",
             ({}, False)),
            ("unreachable under CI", BASE, hold(), None, None, 1, "CI=true, so this fails", ({}, True)),
            # scope = "lock-only": in Cargo.lock, not compiled (cargo deny never sees it)
            ("lock-only vulnerability, audit ignore only", BASE, hold(reason=LOCK_REASON, until_release=UNTIL + LOCK),
             (), None, 0, "held until"),
            ("lock-only, ignored in deny.toml", BASE, hold(reason=LOCK_REASON, until_release=UNTIL + LOCK),
             None, None, 1, "but ignored in deny.toml"),
            ("lock-only vulnerability, no cargo audit ignore", BASE, hold(reason=LOCK_REASON, until_release=UNTIL + LOCK),
             (), (), 1, "not ignored for cargo audit"),
            ("lock-only vulnerability, cargo audit --ignore only", BASE, hold(reason=LOCK_REASON, until_release=UNTIL + LOCK),
             (), (), 0, "held until", None, {"workflow": "run: cargo audit --ignore RUSTSEC-0000-0001\n"}),
            ("lock-only unmaintained, no ignore anywhere", BASE,
             hold(kind="unmaintained", reason=LOCK_REASON, until_release=UNTIL + LOCK), (), (), 0, "held until"),
            ("lock-only without the cargo tree evidence", BASE, hold(until_release=UNTIL + LOCK),
             (), None, 1, "does not carry the `cargo tree` command"),
            ("unknown scope", BASE, hold(until_release=UNTIL + 'scope = "feature"\n'), None, None, 1,
             "is not \"graph\" or \"lock-only\""),
        ]
        # (name, packages, holds, tree keywords, want rc, a word the message must contain)
        more = [
            ("no unused-ignored-advisory", BASE, hold(), {"unused": None}, 1, "unused-ignored-advisory"),
            ("unused-ignored-advisory warn", BASE, hold(), {"unused": '"warn"'}, 1, "unused-ignored-advisory"),
            ("cli ignore held", BASE, hold(),
             {"workflow": "run: cargo audit --deny yanked --ignore RUSTSEC-0000-0001 --ignore=RUSTSEC-0000-0001\n"},
             0, "2 cargo audit --ignore"),
            ("cli ignore not held, workflow", BASE, hold(),
             {"workflow": "run: cargo audit --ignore RUSTSEC-0000-0009\n"}, 1, "RUSTSEC-0000-0009"),
            ("cli ignore not held, preflight", BASE, hold(),
             {"preflight": "cargo audit --ignore=RUSTSEC-0000-0009\n"}, 1, "scripts/preflight.sh"),
            ("cli ignore of a crate gone from Cargo.lock", [pkg("app", "1.0.0", ["mid"]), pkg("mid", "1.0.0")], hold(),
             {"workflow": "run: cargo audit --ignore RUSTSEC-0000-0001\n"}, 1, "--ignore RUSTSEC-0000-0001: leaf 0.1.0 is not in Cargo.lock"),
            ("holds, no ignore read", BASE, hold(kind="unmaintained"), {"deny_ids": (), "audit_ids": ()}, 1, "0 ignores"),
            ("no holds, a cargo audit --ignore", BASE, "",
             {"deny_ids": (), "audit_ids": (), "workflow": "run: cargo audit --ignore RUSTSEC-0000-0001\n"}, 1, "without a hold"),
            ("no holds, unused ignores not denied", BASE, "", {"deny_ids": (), "audit_ids": (), "unused": None}, 1,
             "unused-ignored-advisory"),
            ("quoted ids held", BASE, hold(),
             {"workflow": "run: cargo audit --ignore \"RUSTSEC-0000-0001\" --ignore='RUSTSEC-0000-0001'\n"},
             0, "2 cargo audit --ignore"),
            ("quoted id not held", BASE, hold(),
             {"workflow": "run: cargo audit --ignore \"RUSTSEC-0000-0009\"\n"}, 1, "RUSTSEC-0000-0009"),
            ("ignore of a variable", BASE, hold(),
             {"workflow": "run: cargo audit --ignore \"${ID}\"\n"}, 1, "cannot be read"),
            ("ignore of an array", BASE, hold(),
             {"preflight": "cargo audit \"${IGNORES[@]}\" --ignore $X\n"}, 1, "cannot be read"),
            ("ignore of a typo", BASE, hold(),
             {"workflow": "run: cargo audit --ignore RUSTSEC-2026-35\n"}, 1, "cannot be read"),
            ("ignore with nothing after it", BASE, hold(),
             {"workflow": "run: cargo audit --ignore\n"}, 1, "cannot be read"),
            ("other flags that start with --ignore", BASE, hold(),
             {"workflow": "run: cargo audit --ignore-source --ignore-yanked\n"}, 0, "0 cargo audit --ignore"),
            ("another tool's --ignore", BASE, hold(),
             {"workflow": "run: pytest --ignore=tests/slow\n      - run: ruff check --ignore E501 .\n",
              "preflight": "python3 -m pytest --ignore tests/slow\n"}, 0, "0 cargo audit --ignore"),
            ("--ignore in a comment line", BASE, hold(),
             {"workflow": "      # cargo audit --ignore RUSTSEC-0000-0009 was the old form\n",
              "preflight": "  # --ignore $X\n"}, 0, "0 cargo audit --ignore"),
            ("continued cargo audit, held id on the next line", BASE, hold(),
             {"workflow": "        run: |\n          cargo audit --deny yanked \\\n            --ignore RUSTSEC-0000-0001\n"},
             0, "1 cargo audit --ignore"),
            ("continued cargo audit, unheld id on the next line", BASE, hold(),
             {"preflight": "cargo audit \\\n  --ignore=\"RUSTSEC-0000-0009\"\n"}, 1, "RUSTSEC-0000-0009"),
        ]
        cases += [(n, p, h, None, None, w, word, None, kw) for n, p, h, kw, w, word in more]
        for i, case in enumerate(cases):
            name, packages, holds, deny_ids, audit_ids, want, word = case[:7]
            index, ci = case[7] if len(case) > 7 and case[7] is not None else (NO_NEWER, False)
            kw = dict(case[8]) if len(case) > 8 else {}
            if deny_ids is not None:
                kw["deny_ids"] = deny_ids
            if audit_ids is not None:
                kw["audit_ids"] = audit_ids
            root = tree(tmp, f"case{i}", packages, holds, **kw)
            fetch = guard.dir_fetch(write_index(root, index))
            rc, lines = guard.check(root, TODAY, fetch=fetch, ci=ci)
            text = "\n".join(lines)
            if rc != want or word not in text:
                failures.append(f"{name}: rc {rc} (want {want}, message with {word!r}):\n{text}")
        # --require-index: an unreadable index fails from the command line too, without CI=true (the local
        # receipt runs without CI, so the release check must not silently fall back to the date there)
        saved_ci = os.environ.pop("CI", None)
        try:
            root = tree(tmp, "require-index", BASE, hold())
            empty = pathlib.Path(tmp) / "empty-index"
            empty.mkdir()
            argv = ["--dir", str(root), "--today", TODAY.isoformat(), "--index-dir", str(empty)]
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                plain = guard.main(argv)
                required = guard.main(argv + ["--require-index"])
            if plain != 0:
                failures.append(f"empty index without --require-index: rc {plain} (want 0, the date fallback)")
            if required != 1:
                failures.append(f"empty index with --require-index: rc {required} (want 1)")
        finally:
            if saved_ci is not None:
                os.environ["CI"] = saved_ci
    # an until_release crate the index answers 404 for fails (a typo in the crate name is not "nothing newer")
    def not_in_index(name):
        raise guard.NotInIndex(f"{name}: HTTP 404")
    with tempfile.TemporaryDirectory() as tmp2:
        root = tree(tmp2, "not-in-index", BASE, hold())
        rc, lines = guard.check(root, TODAY, fetch=not_in_index, ci=False)
        text = "\n".join(lines)
        if rc != 1 or "which the index does not have" not in text:
            failures.append(f"until_release crate not in the index: rc {rc} (want 1):\n{text}")
    paths = {"a": "1/a", "ab": "2/ab", "abc": "3/a/abc", "openmls": "op/en/openmls",
             "Core-Models": "co/re/core-models"}
    for crate, want in paths.items():
        if guard.index_path(crate) != want:
            failures.append(f"index_path({crate!r}) = {guard.index_path(crate)!r}, want {want!r}")
    total = len(cases) + len(paths) + 3
    for f in failures:
        print(f"FAIL {f}", file=sys.stderr)
    print(f"test_advisory_hold_guard: {total - len(failures)}/{total} cases passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
