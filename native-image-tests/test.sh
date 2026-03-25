#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Builds and tests the Hardwood native binary.
#
# Usage:
#   ./native-image-tests/test.sh [--local] [--skip-build] [--test-s3] [TESTDATA_DIR=/path]
#
#   (default)     Build Linux binary via Quarkus container-build, run tests in Docker.
#   --local       Build macOS binary locally, run tests directly (no Docker).
#   --skip-build  Skip Maven build; reuse the most recent dist archive in cli/target/.
#   --test-s3     Also run S3 tests against a temporary s3mock container.
#   TESTDATA_DIR  Optional directory of extra .parquet files mounted at /testdata.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE=linux
SKIP_BUILD=false
TEST_S3=false

for arg in "$@"; do
    case "$arg" in
        --local)      MODE=local ;;
        --skip-build) SKIP_BUILD=true ;;
        --test-s3)    TEST_S3=true ;;
    esac
done

# ---------------------------------------------------------------------------
# S3 mock lifecycle (used only when --test-s3 is passed)
# ---------------------------------------------------------------------------
S3_CONTAINER=""
S3_NETWORK=""
S3_ENDPOINT=""

cleanup_s3mock() {
    [ -n "$S3_CONTAINER" ] && docker rm -f "$S3_CONTAINER" 2>/dev/null || true
    [ -n "$S3_NETWORK"   ] && docker network rm "$S3_NETWORK" 2>/dev/null || true
}

wait_for_s3mock() {
    echo -n "==> Waiting for s3mock to be ready"
    for _ in $(seq 1 30); do
        if curl -sf "http://localhost:9090" > /dev/null 2>&1; then
            echo " ready."
            return 0
        fi
        sleep 1
        echo -n "."
    done
    echo " timed out." >&2
    return 1
}

if [ "$TEST_S3" = true ]; then
    trap cleanup_s3mock EXIT
    S3_CONTAINER="s3mock-hardwood-$$"

    if [ "$MODE" = "linux" ]; then
        S3_NETWORK="hardwood-test-$$"
        docker network create "$S3_NETWORK"
        echo "==> Starting s3mock on Docker network $S3_NETWORK..."
        docker run -d --name "$S3_CONTAINER" --network "$S3_NETWORK" -p 9090:9090 adobe/s3mock
        S3_ENDPOINT="http://${S3_CONTAINER}:9090"
    else
        echo "==> Starting s3mock..."
        docker run -d --name "$S3_CONTAINER" -p 9090:9090 adobe/s3mock
        S3_ENDPOINT="http://localhost:9090"
    fi

    wait_for_s3mock
fi

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
    S3_ENV=()
    if [ "$TEST_S3" = true ]; then
        S3_ENV=(TEST_S3=true AWS_ENDPOINT_URL="$S3_ENDPOINT" AWS_ACCESS_KEY_ID=foo AWS_SECRET_ACCESS_KEY=bar AWS_REGION=us-east-1)
    fi
    env "${S3_ENV[@]}" HARDWOOD_BIN="$HARDWOOD_BIN" REPO_ROOT="$REPO_ROOT" bash "$SCRIPT_DIR/run-tests.sh"
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
    if [ "$TEST_S3" = true ]; then
        DOCKER_OPTS+=(
            --network "$S3_NETWORK"
            -e TEST_S3=true
            -e AWS_ENDPOINT_URL="$S3_ENDPOINT"
            -e AWS_ACCESS_KEY_ID=foo
            -e AWS_SECRET_ACCESS_KEY=bar
            -e AWS_REGION=us-east-1
        )
    fi
    docker run --rm "${DOCKER_OPTS[@]}" hardwood-native-test
fi