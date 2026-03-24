#!/usr/bin/env bash
# Builds and tests the Hardwood native binary.
#
# Usage:
#   ./native-image-tests/test.sh [--local] [--skip-build] [TESTDATA_DIR=/path]
#
#   (default)     Build Linux binary via Quarkus container-build, run tests in Docker.
#   --local       Build macOS binary locally, run tests directly (no Docker).
#   --skip-build  Skip Maven build; reuse the most recent dist archive in cli/target/.
#   TESTDATA_DIR  Optional directory of extra .parquet files mounted at /testdata.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE=linux
SKIP_BUILD=false

for arg in "$@"; do
    case "$arg" in
        --local)      MODE=local ;;
        --skip-build) SKIP_BUILD=true ;;
    esac
done

# ---------------------------------------------------------------------------
# Step 1: Build
# ---------------------------------------------------------------------------
if [ "$SKIP_BUILD" = false ]; then
    cd "$REPO_ROOT"
    if [ "$MODE" = "linux" ]; then
        echo "==> Building Linux native binary (native-image runs inside Docker via Quarkus)..."
        ./mvnw -Dnative -Dquarkus.native.container-build=true package -pl cli -am -DskipTests
    else
        echo "==> Building macOS native binary..."
        ./mvnw -Dnative package -pl cli -am -DskipTests
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: Locate dist archive
# ---------------------------------------------------------------------------
DIST_ARCHIVE=$(find "$REPO_ROOT/cli/target" -maxdepth 1 -name "*.tar.gz" | head -1)
if [ -z "$DIST_ARCHIVE" ]; then
    echo "ERROR: No dist archive found in cli/target/. Run without --skip-build." >&2
    exit 1
fi
echo "==> Using dist archive: $(basename "$DIST_ARCHIVE")"

STAGING="$REPO_ROOT/cli/target/native-test-staging"
rm -rf "$STAGING" && mkdir -p "$STAGING"

# ---------------------------------------------------------------------------
# Step 3: Run tests
# ---------------------------------------------------------------------------
if [ "$MODE" = "local" ]; then
    # Extract locally — fine here because the macOS binary needs macOS libs anyway.
    tar -xzf "$DIST_ARCHIVE" -C "$STAGING"
    HARDWOOD_BIN="$STAGING/bin/hardwood"
    chmod +x "$HARDWOOD_BIN"

    echo "==> Running tests directly..."
    echo
    HARDWOOD_BIN="$HARDWOOD_BIN" REPO_ROOT="$REPO_ROOT" bash "$SCRIPT_DIR/run-tests.sh"
else
    # Copy the archive unextracted so Docker extracts it on Linux's case-sensitive FS,
    # keeping lib/Linux/ (snappy) and lib/linux/ (zstd/lz4) as distinct directories.
    cp "$DIST_ARCHIVE" "$STAGING/hardwood.tar.gz"
    cp "$SCRIPT_DIR/run-tests.sh" "$STAGING/run-tests.sh"

    echo "==> Building test Docker image..."
    docker build -f "$SCRIPT_DIR/Dockerfile.test" -t hardwood-native-test "$STAGING"

    echo "==> Running tests in Docker..."
    echo
    DOCKER_OPTS=(-v "$REPO_ROOT:/repo:ro")
    if [ -n "${TESTDATA_DIR:-}" ]; then
        DOCKER_OPTS+=(-v "$TESTDATA_DIR:/testdata:ro")
    fi
    docker run --rm "${DOCKER_OPTS[@]}" hardwood-native-test
fi