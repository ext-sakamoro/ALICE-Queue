#!/usr/bin/env python3
"""Tests of scripts/advisory_hold_guard.py on small throwaway trees (no network, no cargo).

  - valid hold                                         -> 0
  - held version gone from Cargo.lock (stale)          -> 1
  - a new package depends on the held version          -> 1
  - a recorded dependent is gone                       -> 1
  - a bare dependency name (the crate's only version)  -> 0 (counted as that version)
  - expired (today is the day after `expires`)        -> 1
  - today is `expires`                                 -> 0 (the last valid day)
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
  - no holds / no Cargo.lock / Cargo.lock empty        -> 2 (nothing compared)
"""

import datetime
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
'''


def tree(tmp, name, packages, holds=None, deny_ids=("RUSTSEC-0000-0001",), audit_ids=("RUSTSEC-0000-0001",)):
    d = pathlib.Path(tmp) / name
    (d / "scripts").mkdir(parents=True)
    (d / ".cargo").mkdir()
    if packages is not None:
        (d / "Cargo.lock").write_text("version = 4\n\n" + "\n".join(packages), encoding="utf-8")
    if holds is not None:
        (d / "scripts" / "advisory_holds.toml").write_text(holds, encoding="utf-8")
    deny = ", ".join(f'{{ id = "{i}", reason = "test" }}' for i in deny_ids)
    (d / "deny.toml").write_text(f"[advisories]\nignore = [{deny}]\n", encoding="utf-8")
    audit = ", ".join(f'"{i}"' for i in audit_ids)
    (d / ".cargo" / "audit.toml").write_text(f"[advisories]\nignore = [{audit}]\n", encoding="utf-8")
    return d


def hold(kind="vulnerability", dependents=('"mid 1.0.0"',), fixed=('>= 0.2.0',), expires="2026-12-31"):
    return HOLD.format(kind=kind, dependents=", ".join(dependents),
                       fixed=", ".join(f'"{r}"' for r in fixed), expires=expires)


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
            ("no holds", BASE, "", None, None, 2, "holds nothing"),
            ("no holds file", BASE, None, None, None, 2, "not found"),
            ("no Cargo.lock", None, hold(), None, None, 2, "not found"),
            ("empty Cargo.lock", [], hold(), None, None, 2, "no packages"),
        ]
        for i, (name, packages, holds, deny_ids, audit_ids, want, word) in enumerate(cases):
            kw = {}
            if deny_ids is not None:
                kw["deny_ids"] = deny_ids
            if audit_ids is not None:
                kw["audit_ids"] = audit_ids
            root = tree(tmp, f"case{i}", packages, holds, **kw)
            rc, lines = guard.check(root, TODAY)
            text = "\n".join(lines)
            if rc != want or word not in text:
                failures.append(f"{name}: rc {rc} (want {want}, message with {word!r}):\n{text}")
    for f in failures:
        print(f"FAIL {f}", file=sys.stderr)
    print(f"test_advisory_hold_guard: {len(cases) - len(failures)}/{len(cases)} cases passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
