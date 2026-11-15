#!/usr/bin/env bash
# bincode (RUSTSEC-2025-0141, unmaintained) must not come back into alice-queue's
# dependency graph: alice-text 3 (the `text` feature) reads and writes its payloads
# with alice-serial's bincode1. Lists every package of the graph (the lock is resolved as CI
# resolves it) with all features, all targets and every edge kind, and fails
# when bincode is one of them; a listing of 0 packages compared nothing and fails.
set -euo pipefail
cd "$(dirname "$0")/.."

pkgs=$(cargo tree --all-features --target all -e normal,build,dev \
  --prefix none --format '{p}' | awk '{print $1}' | sort -u)
n=$(printf '%s\n' "$pkgs" | grep -c . || true)
if [ "$n" -eq 0 ]; then
  echo "cargo tree listed no packages: the check compared nothing" >&2
  exit 1
fi
if printf '%s\n' "$pkgs" | grep -qx 'bincode'; then
  echo "bincode is in alice-queue's dependency graph:" >&2
  cargo tree --all-features --target all -e normal,build,dev -i bincode >&2
  exit 1
fi
echo "bincode absent: checked $n packages"
