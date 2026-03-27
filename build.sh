#!/usr/bin/env bash
# build.sh — clean and rebuild all TurboCable release binaries inside WSL.
#
# Usage (from project root on Windows or inside WSL):
#   bash build.sh
#
# What it does:
#   1. Deletes target/release (binaries + incremental artefacts)
#   2. Runs `cargo build --release` for all workspace members
#   3. Prints the paths of the produced binaries

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Windows host: relay everything into WSL so asdf / cargo are on PATH.
if [[ "$(uname -s)" == MINGW* ]] || [[ "$(uname -s)" == CYGWIN* ]] || grep -qi microsoft /proc/version 2>/dev/null; then
    # Running natively on Windows (Git Bash / CMD via wsl.exe) — delegate to WSL.
    : # fall through; the script is designed to be invoked via wsl bash -ic already
fi

PROJECT_ROOT="$SCRIPT_DIR"

echo "================================================================"
echo "  TurboCable — clean release build"
echo "  Project: $PROJECT_ROOT"
echo "================================================================"

# 1. Remove old release artefacts so the build starts from scratch.
RELEASE_DIR="$PROJECT_ROOT/target/release"
if [[ -d "$RELEASE_DIR" ]]; then
    echo "--> Deleting $RELEASE_DIR ..."
    rm -rf "$RELEASE_DIR"
    echo "    Done."
else
    echo "--> No existing release build found — skipping clean."
fi

# 2. Build all workspace binaries in release mode.
echo "--> Running: cargo build --release"
cd "$PROJECT_ROOT"
cargo build --release

# 3. Report produced binaries (executables only, ignore .d / .rlib / build scripts).
echo ""
echo "================================================================"
echo "  Build complete. Binaries:"
find "$RELEASE_DIR" -maxdepth 1 -type f -executable \
    ! -name '*.d' \
    ! -name '*.rlib' \
    ! -name '*.rmeta' \
    | sort \
    | while read -r bin; do
        size=$(du -sh "$bin" 2>/dev/null | cut -f1)
        echo "    $bin  ($size)"
      done
echo "================================================================"
