#!/usr/bin/env python3
"""Keep each dated advisory hold (scripts/advisory_holds.toml) exact.

A hold lets one RustSec advisory id through the cargo deny / cargo audit steps
for one crate version that cannot be fixed without a dependency upgrade. The
ignore by id would also hide the advisory on any other version or path, so each
hold is checked against Cargo.lock:

  - stale: the held crate version is no longer in Cargo.lock -> fail (remove the hold
    and the ignore)
  - path: the packages that directly depend on the held version differ from `dependents`
    -> fail (a new path reaches it, or the recorded one is gone). Only direct dependents
    are compared: a new consumer of a recorded dependent reaches the held version
    through the same code, and passes
  - other version: another version of the crate is in Cargo.lock, not held, and not in
    one of the `fixed` ranges (the advisory's patched ranges, semver precedence; a
    pre-release is fixed only when a range names a pre-release of that release) -> fail
    (the ignore would hide it)
  - expired: today is after `expires` -> fail (the upgrade is overdue)
  - too far: `expires` is more than MAX_HOLD_DAYS (21) after today -> fail (a hold is
    renewed with its reason when it runs out, not set far ahead)
    (kept with `until_release`: an upstream that never publishes a newer release, or is not on the
    index at all, never trips the release check, so the expiry cap is what bounds a renewal)
  - reason: each hold carries `reason`, the measured required-by chain that keeps the
    held version in the lock (and whether it is compiled) -> fail when missing or empty
  - upstream: each hold names in `until_release` the upstream releases that remove it
    ([{crate, newer_than}, ...]); the crates.io sparse index is read for each and a
    non-yanked release (not a pre-release) newer than `newer_than` fails ("upstream
    released X: bump instead of holding"), so a hold cannot be renewed past the release
    that removes it. A missing or empty `until_release` fails. When the index cannot be
    read, the expiry date alone decides (nothing is extended) and the guard says it could
    not query; under CI=true an unreadable index fails instead. A crate the index does not
    know (404) fails. Pre-releases newer than the bound are printed, not failed (cargo
    does not select them without an explicit requirement)
  - lists: each held id must be ignored in deny.toml, and a held vulnerability also in
    .cargo/audit.toml (an ignore without a hold is not this guard's: the ignore lists
    carry their own reasons)
  - lock-only: a hold with `scope = "lock-only"` is for a version that is in Cargo.lock but not
    compiled by any feature (cargo audit scans the lock, cargo deny reads the feature graph). It
    must NOT be ignored in deny.toml (cargo deny never sees it, so with unused-ignored-advisory =
    "deny" the ignore itself fails), a lock-only vulnerability must be ignored for cargo audit
    (.cargo/audit.toml or a `cargo audit --ignore`), and its `reason` must carry the `cargo tree`
    command that showed it is not compiled. cargo deny checks the claim: if the version is in the
    graph after all, cargo deny reports the advisory it no longer ignores. `scope` is "graph"
    (the default) or "lock-only"; any other value fails
  - unused ignores: deny.toml's [advisories] must set `unused-ignored-advisory = "deny"`,
    so cargo deny fails on an ignore that no longer matches anything in the graph
  - command-line ignores: every `--ignore <id>` (or `--ignore=<id>`, the id bare or in quotes) of a
    `cargo audit` command in the workflows and scripts/preflight.sh (a command continued over lines with
    a trailing backslash is one command; comment lines and other tools' `--ignore` are not read) must be held, and its held crate version must be in Cargo.lock;
    an `--ignore` whose argument is not an advisory id (a variable, an array, a typo, nothing) fails,
    since an ignore that cannot be read must not count as none
  - read nothing: with holds, the ignore lists and command lines together must give at
    least one ignore (otherwise they were not read)
  - no holds: an empty advisory_holds.toml passes only when deny.toml, .cargo/audit.toml and the
    cargo audit command lines ignore nothing (and deny.toml denies unused ignores); an ignore with
    no hold fails, so an empty hold list cannot hide one

It fails when it could not compare anything (no Cargo.lock, no packages, no holds file),
so it never passes vacuously.

    python3 scripts/advisory_hold_guard.py [--dir .] [--today YYYY-MM-DD] [--index-dir DIR] [--require-index]

--index-dir reads a sparse-index tree (DIR/<prefix>/<name>, the crates.io layout) instead
of https://index.crates.io, so the tests use no network. --require-index makes an unreadable
index fail as under CI=true (the workflow and the local receipt pass it).
"""

import argparse
import datetime
import json
import os
import pathlib
import re
import sys
import tomllib
import urllib.error
import urllib.request

# the longest a hold may run from today
MAX_HOLD_DAYS = 21

INDEX_URL = "https://index.crates.io"
USER_AGENT = "advisory-hold-guard (CI dependency check)"


class Unreachable(Exception):
    """the index could not be read (network, timeout, server error, missing fixture file)"""


class NotInIndex(Exception):
    """the index answered that the crate does not exist"""


def index_path(name):
    """the sparse-index path of a crate: 1/a, 2/ab, 3/a/abc, ab/cd/abcd..."""
    n = name.lower()
    if len(n) <= 2:
        return f"{len(n)}/{n}"
    if len(n) == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def http_fetch(name):
    url = f"{INDEX_URL}/{index_path(name)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            raise NotInIndex(f"{url}: HTTP {e.code}") from e
        raise Unreachable(f"{url}: HTTP {e.code}") from e
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise Unreachable(f"{url}: {e}") from e


def dir_fetch(index_dir):
    def fetch(name):
        path = pathlib.Path(index_dir) / index_path(name)
        try:
            return path.read_text(encoding="utf-8")
        except OSError as e:
            raise Unreachable(f"{path}: {e}") from e
    return fetch


def newer_releases(index_text, newer_than):
    """(non-yanked releases above `newer_than`, non-yanked pre-releases above it), by precedence"""
    bound = precedence(parse_version(newer_than))
    releases, pres = [], []
    for line in index_text.splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("yanked"):
            continue
        v = parse_version(entry["vers"])
        if precedence(v) <= bound:
            continue
        (pres if v[3] else releases).append(entry["vers"])
    key = lambda s: precedence(parse_version(s))  # noqa: E731
    return sorted(releases, key=key), sorted(pres, key=key)




def parse_version(v):
    """(major, minor, patch, pre) with pre a tuple of identifiers, () for a release"""
    core, _, pre = v.split("+")[0].partition("-")
    major, minor, patch = (int(x) for x in core.split("."))
    return major, minor, patch, tuple(pre.split(".")) if pre else ()


def precedence(v):
    """semver precedence: a pre-release sorts before its release, identifiers compare
    numerically when both are numeric, numeric before alphanumeric, shorter first"""
    major, minor, patch, pre = v
    ids = tuple((0, int(i), "") if i.isdigit() else (1, 0, i) for i in pre)
    return (major, minor, patch, 1 if not pre else 0, ids)


def in_range(version, rng):
    """`version` matches the comma-separated comparators of `rng` (">= a, < b"); like cargo,
    a pre-release matches only when one comparator names a pre-release of the same release"""
    v = parse_version(version)
    comparators = []
    for part in rng.split(","):
        part = part.strip()
        op = next(o for o in (">=", "<=", ">", "<", "=") if part.startswith(o))
        comparators.append((op, parse_version(part[len(op):].strip())))
    if v[3] and not any(c[3] and c[:3] == v[:3] for _, c in comparators):
        return False
    pv = precedence(v)
    ok = {">=": lambda b: pv >= b, "<=": lambda b: pv <= b, ">": lambda b: pv > b,
          "<": lambda b: pv < b, "=": lambda b: pv == b}
    return all(ok[op](precedence(c)) for op, c in comparators)


def ignored_ids(table):
    out = set()
    for entry in (table.get("advisories") or {}).get("ignore") or []:
        out.add(entry["id"] if isinstance(entry, dict) else entry)
    return out


# `--ignore` as a whole flag (not `--ignore-source` and the like), then its argument up to the next blank
CLI_IGNORE = re.compile(r"--ignore(?![\w-])(?:=|[ \t]+)?(\S*)")
ADVISORY_ID = re.compile(r"RUSTSEC-\d{4}-\d{4,}")


def commands(text):
    """The logical commands of a script or workflow: lines continued with a trailing backslash joined, comment
    lines dropped"""
    out, cur = [], ""
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        if line.rstrip().endswith("\\"):
            cur += line.rstrip()[:-1] + " "
            continue
        out.append(cur + line)
        cur = ""
    if cur:
        out.append(cur)
    return out


def cli_ignores(root):
    """(file relative to root, advisory id or None, the argument as written) for each `--ignore` of a
    `cargo audit` command in the workflows and preflight; the id is None when the argument is not one
    (unquoted or in one pair of quotes)"""
    files = sorted((root / ".github" / "workflows").glob("*.y*ml"))
    files.append(root / "scripts" / "preflight.sh")
    out = []
    for f in files:
        if not f.is_file():
            continue
        for command in commands(f.read_text(encoding="utf-8")):
            if "cargo audit" not in command:
                continue
            for m in CLI_IGNORE.finditer(command):
                arg = m.group(1)
                bare = arg[1:-1] if len(arg) >= 2 and arg[0] == arg[-1] and arg[0] in "\"'" else arg
                aid = bare if ADVISORY_ID.fullmatch(bare) else None
                out.append((f.relative_to(root).as_posix(), aid, arg))
    return out


def dependents_of(packages, name, version):
    versions = [p["version"] for p in packages if p["name"] == name]
    found = set()
    for p in packages:
        for dep in p.get("dependencies", []):
            parts = dep.split(" ")
            if parts[0] != name:
                continue
            # a bare name in Cargo.lock means the only version of that crate
            if (len(parts) > 1 and parts[1] == version) or (len(parts) == 1 and versions == [version]):
                found.add(f"{p['name']} {p['version']}")
    return found


def check_upstream(holds, fetch, ci):
    """errors, notes for the `until_release` of every hold; each crate is read once"""
    errors, notes, texts = [], [], {}
    for h in holds:
        label = f"{h['id']} {h['crate']} {h['version']}"
        for rel in h["until_release"]:
            crate, bound = rel["crate"], rel["newer_than"]
            if crate not in texts:
                try:
                    texts[crate] = ("ok", fetch(crate))
                except NotInIndex as e:
                    texts[crate] = ("missing", str(e))
                except Unreachable as e:
                    texts[crate] = ("unreachable", str(e))
            state, text = texts[crate]
            if state == "missing":
                errors.append(f"{label}: until_release names {crate}, which the index does not have ({text})")
            elif state == "unreachable":
                if ci:
                    errors.append(f"{label}: could not query the index for {crate} ({text}); CI=true, so this fails")
                else:
                    notes.append(f"{label}: could not query the index for {crate} ({text}); "
                                 f"falling back to the expiry date {h['expires']} (not extended)")
            else:
                newer, pres = newer_releases(text, bound)
                if newer:
                    errors.append(f"{label}: upstream released {crate} {newer[-1]} (newer than {bound}; "
                                  f"all: {', '.join(newer)}): bump instead of holding")
                else:
                    notes.append(f"{label}: {crate} has no release newer than {bound}"
                                 + (f" (pre-releases, not counted: {', '.join(pres)})" if pres else ""))
    return errors, notes


def check_rows(holds):
    """reason and until_release present and well formed: (errors, holds whose until_release
    can be queried)"""
    errors, queryable = [], []
    for h in holds:
        label = f"{h.get('id')} {h.get('crate')} {h.get('version')}"
        reason = h.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            errors.append(f"{label}: no `reason` (the measured required-by chain)")
        scope = h.get("scope", "graph")
        if scope not in ("graph", "lock-only"):
            errors.append(f"{label}: scope {scope!r} is not \"graph\" or \"lock-only\"")
        elif scope == "lock-only" and (not isinstance(reason, str) or "cargo tree" not in reason):
            errors.append(f"{label}: scope lock-only, but `reason` does not carry the `cargo tree` command "
                          "that shows the version is not compiled")
        rels = h.get("until_release")
        if not isinstance(rels, list) or not rels:
            errors.append(f"{label}: no `until_release` (the upstream release that removes the hold)")
            continue
        ok = True
        for rel in rels:
            if (not isinstance(rel, dict) or not isinstance(rel.get("crate"), str) or not rel["crate"]
                    or not isinstance(rel.get("newer_than"), str)):
                errors.append(f"{label}: until_release entry {rel!r} needs crate and newer_than")
                ok = False
                continue
            try:
                parse_version(rel["newer_than"])
            except ValueError:
                errors.append(f"{label}: until_release {rel['crate']} newer_than {rel['newer_than']!r} is not a version")
                ok = False
        if ok:
            queryable.append(h)
    return errors, queryable


def check(root, today, fetch=None, ci=None):
    if fetch is None:
        fetch = http_fetch
    if ci is None:
        ci = os.environ.get("CI", "").lower() == "true"
    lock = root / "Cargo.lock"
    holds_file = root / "scripts" / "advisory_holds.toml"
    if not lock.is_file():
        return 2, [f"{lock} not found (nothing to compare)"]
    packages = tomllib.loads(lock.read_text(encoding="utf-8")).get("package", [])
    if not packages:
        return 2, ["Cargo.lock lists no packages (nothing to compare)"]
    if not holds_file.is_file():
        return 2, [f"{holds_file} not found (nothing to compare)"]
    holds = tomllib.loads(holds_file.read_text(encoding="utf-8")).get("hold", [])
    deny = root / "deny.toml"
    audit = root / ".cargo" / "audit.toml"
    deny_table = tomllib.loads(deny.read_text(encoding="utf-8")) if deny.is_file() else {}
    deny_ids = ignored_ids(deny_table)
    audit_ids = ignored_ids(tomllib.loads(audit.read_text(encoding="utf-8"))) if audit.is_file() else set()
    if not holds:
        # nothing is held: the ignore lists and the command lines must ignore nothing either
        errors = [f"deny.toml ignores {i} without a hold" for i in sorted(deny_ids)]
        errors += [f".cargo/audit.toml ignores {i} without a hold" for i in sorted(audit_ids)]
        errors += [f"{where}: cargo audit --ignore {arg} without a hold" for where, _, arg in cli_ignores(root)]
        if (deny_table.get("advisories") or {}).get("unused-ignored-advisory") != "deny":
            errors.append('deny.toml [advisories] does not set unused-ignored-advisory = "deny"')
        if errors:
            return 1, errors
        return 0, [f"0 holds, and deny.toml, .cargo/audit.toml and the cargo audit command lines ignore "
                   f"nothing ({len(packages)} packages in Cargo.lock)"]

    errors, queryable = check_rows(holds)
    notes = []
    held = {(h["crate"], h["version"]) for h in holds}
    if (deny_table.get("advisories") or {}).get("unused-ignored-advisory") != "deny":
        errors.append('deny.toml [advisories] does not set unused-ignored-advisory = "deny"')
    cli = cli_ignores(root)
    by_id = {h["id"]: h for h in holds}
    for where, aid, arg in cli:
        if aid is None:
            errors.append(f"{where}: cargo audit --ignore {arg!r} cannot be read as an advisory id")
            continue
        h = by_id.get(aid)
        if h is None:
            errors.append(f"{where}: cargo audit --ignore {aid} is not held in advisory_holds.toml")
        elif not any(p["name"] == h["crate"] and p["version"] == h["version"] for p in packages):
            errors.append(f"{where}: --ignore {aid}: {h['crate']} {h['version']} is not in Cargo.lock")
    # a lock-only unmaintained hold needs no ignore anywhere (cargo deny does not see it, cargo audit warns)
    needs_ignore = any(h.get("scope", "graph") != "lock-only" or h["kind"] == "vulnerability" for h in holds)
    cli_ids = {aid for _, aid, _ in cli if aid}
    if needs_ignore and not (deny_ids or audit_ids or cli):
        errors.append(f"{len(holds)} holds, but 0 ignores read from deny.toml, .cargo/audit.toml and the command lines")
    for h in holds:
        label = f"{h['id']} {h['crate']} {h['version']}"
        expires = datetime.date.fromisoformat(h["expires"])
        if today > expires:
            errors.append(f"{label}: expired on {h['expires']} ({h['until']})")
        elif (expires - today).days > MAX_HOLD_DAYS:
            errors.append(f"{label}: expires {h['expires']}, more than {MAX_HOLD_DAYS} days after {today}")
        if h.get("scope", "graph") == "lock-only":
            if h["id"] in deny_ids:
                errors.append(f"{label}: scope lock-only, but ignored in deny.toml (cargo deny does not see a "
                              "version that is not compiled; the ignore fails unused-ignored-advisory = \"deny\")")
            if h["kind"] == "vulnerability" and h["id"] not in audit_ids and h["id"] not in cli_ids:
                errors.append(f"{label}: scope lock-only, not ignored for cargo audit "
                              "(.cargo/audit.toml or a cargo audit --ignore)")
        else:
            if h["id"] not in deny_ids:
                errors.append(f"{label}: not ignored in deny.toml")
            if h["kind"] == "vulnerability" and h["id"] not in audit_ids:
                errors.append(f"{label}: not ignored in .cargo/audit.toml")
        if not any(p["name"] == h["crate"] and p["version"] == h["version"] for p in packages):
            errors.append(f"{label}: no longer in Cargo.lock, the hold is stale: remove it and its ignore")
            continue
        got, want = dependents_of(packages, h["crate"], h["version"]), set(h["dependents"])
        if got != want:
            errors.append(f"{label}: dependents changed: new {sorted(got - want)}, gone {sorted(want - got)}")
        notes.append(f"{label}: held until {h['expires']} ({len(got)} dependents)")
    for crate in sorted({h["crate"] for h in holds}):
        # the advisory's own patched ranges, the same for every hold of the crate
        ranges = {r for h in holds if h["crate"] == crate for r in h["fixed"]}
        for p in packages:
            if p["name"] != crate or (crate, p["version"]) in held:
                continue
            if not any(in_range(p["version"], r) for r in ranges):
                errors.append(f"{crate} {p['version']}: in Cargo.lock, not held and not fixed: the ignore would hide it")
    # rows whose until_release is malformed are already reported; the rest are queried
    if queryable:
        up_errors, up_notes = check_upstream(queryable, fetch, ci)
        errors += up_errors
        notes += up_notes
    if errors:
        return 1, errors
    return 0, notes + [
        f"{len(holds)} holds compared against {len(packages)} packages, {len(cli)} cargo audit --ignore"
    ]


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".")
    ap.add_argument("--today", default=None)
    ap.add_argument("--index-dir", default=None, help="read a sparse-index tree instead of the network")
    ap.add_argument(
        "--require-index",
        action="store_true",
        help="an unreadable index fails, as under CI=true (the workflow and the local receipt pass it)",
    )
    args = ap.parse_args(argv)
    today = datetime.date.fromisoformat(args.today) if args.today else datetime.date.today()
    fetch = dir_fetch(args.index_dir) if args.index_dir else http_fetch
    ci = True if args.require_index else None
    rc, lines = check(pathlib.Path(args.dir).resolve(), today, fetch=fetch, ci=ci)
    for line in lines:
        print(f"advisory_hold_guard: {line}", file=sys.stderr if rc else sys.stdout)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
