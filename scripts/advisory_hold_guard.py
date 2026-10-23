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
  - lists: each held id must be ignored in deny.toml, and a held vulnerability also in
    .cargo/audit.toml (an ignore without a hold is not this guard's: the ignore lists
    carry their own reasons)

It fails when it could not compare anything (no Cargo.lock, no packages, no holds),
so it never passes vacuously.

    python3 scripts/advisory_hold_guard.py [--dir .] [--today YYYY-MM-DD]
"""

import argparse
import datetime
import pathlib
import sys
import tomllib


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


def check(root, today):
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
    if not holds:
        return 2, ["advisory_holds.toml holds nothing (nothing to compare)"]
    deny = root / "deny.toml"
    audit = root / ".cargo" / "audit.toml"
    deny_ids = ignored_ids(tomllib.loads(deny.read_text(encoding="utf-8"))) if deny.is_file() else set()
    audit_ids = ignored_ids(tomllib.loads(audit.read_text(encoding="utf-8"))) if audit.is_file() else set()

    errors, notes = [], []
    held = {(h["crate"], h["version"]) for h in holds}
    for h in holds:
        label = f"{h['id']} {h['crate']} {h['version']}"
        if today > datetime.date.fromisoformat(h["expires"]):
            errors.append(f"{label}: expired on {h['expires']} ({h['until']})")
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
    if errors:
        return 1, errors
    return 0, notes + [f"{len(holds)} holds compared against {len(packages)} packages"]


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".")
    ap.add_argument("--today", default=None)
    args = ap.parse_args(argv)
    today = datetime.date.fromisoformat(args.today) if args.today else datetime.date.today()
    rc, lines = check(pathlib.Path(args.dir).resolve(), today)
    for line in lines:
        print(f"advisory_hold_guard: {line}", file=sys.stderr if rc else sys.stdout)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
