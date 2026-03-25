#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Platform-agnostic test runner for the Hardwood native binary.
#
# Environment variables (set by test.sh; defaults suit Docker):
#   HARDWOOD_BIN   path to the hardwood executable  (default: 'hardwood' from PATH)
#   REPO_ROOT      repo root for locating parquet files (default: /repo)
set -euo pipefail

HW="${HARDWOOD_BIN:-hardwood}"
REPO="${REPO_ROOT:-/repo}"

# Resolve the binary to an absolute path so dirname works correctly.
HW_PATH=$(command -v "$HW" 2>/dev/null || echo "$HW")
LIB_DIR="$(cd "$(dirname "$HW_PATH")/.." && pwd)/lib"

PASS=0
FAIL=0
SKIP=0

green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[0;33m%s\033[0m\n' "$*"; }

run_s3_test() {
    local label="$1"
    local url="$2"
    shift 2
    local cmd=("$@")

    printf '  %-60s' "$label"
    if output=$("${cmd[@]}" -f "$url" 2>&1); then
        green "PASS"
        PASS=$((PASS + 1))
    else
        red "FAIL"
        echo "         Output: $output"
        FAIL=$((FAIL + 1))
    fi
}

run_test() {
    local label="$1"
    local file="$2"
    shift 2
    local cmd=("$@")

    if [ ! -f "$file" ]; then
        yellow "  SKIP  $label ($file not found)"
        SKIP=$((SKIP + 1))
        return
    fi

    printf '  %-60s' "$label"
    if output=$("${cmd[@]}" -f "$file" 2>&1); then
        green "PASS"
        PASS=$((PASS + 1))
    else
        red "FAIL"
        echo "         Output: $output"
        FAIL=$((FAIL + 1))
    fi
}

echo "=== Hardwood Native Binary Test ==="
echo
echo "Binary info:"
file "$HW_PATH"
echo
echo "Lib directory contents:"
find "$LIB_DIR" -type f 2>/dev/null | sort || echo "  (empty or missing)"
echo

echo "--- Smoke test ---"
"$HW" --version

echo
echo "--- Compression codec tests ---"

run_test "uncompressed (schema)" "$REPO/core/src/test/resources/plain_uncompressed.parquet"                    "$HW" schema
run_test "uncompressed (head)"   "$REPO/core/src/test/resources/plain_uncompressed.parquet"                    "$HW" head
run_test "snappy (schema)"       "$REPO/core/src/test/resources/plain_snappy.parquet"                          "$HW" schema
run_test "snappy (head)"         "$REPO/core/src/test/resources/plain_snappy.parquet"                          "$HW" head
run_test "gzip (schema)"         "$REPO/integration-test/src/test/resources/gzip_compressed.parquet"           "$HW" schema
run_test "gzip (head)"           "$REPO/integration-test/src/test/resources/gzip_compressed.parquet"           "$HW" head
run_test "zstd (schema)"         "$REPO/performance-testing/end-to-end/src/test/resources/profiling_zstd_plain.parquet" "$HW" schema
run_test "zstd (head)"           "$REPO/performance-testing/end-to-end/src/test/resources/profiling_zstd_plain.parquet" "$HW" head

echo
echo "--- Performance test data ---"

run_test "tlc yellow tripdata 2016-01 (schema)" \
    "$REPO/performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2016-01.parquet" \
    "$HW" schema
run_test "tlc yellow tripdata 2016-01 (head)" \
    "$REPO/performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2016-01.parquet" \
    "$HW" head
run_test "overture places zstd (schema)" \
    "$REPO/performance-testing/test-data-setup/target/overture-maps-data/overture_places.zstd.parquet" \
    "$HW" schema
run_test "overture places zstd (head)" \
    "$REPO/performance-testing/test-data-setup/target/overture-maps-data/overture_places.zstd.parquet" \
    "$HW" head

echo
echo "--- HARDWOOD_LIB_PATH override test ---"

run_test "zstd via HARDWOOD_LIB_PATH" \
    "$REPO/performance-testing/end-to-end/src/test/resources/profiling_zstd_plain.parquet" \
    env HARDWOOD_LIB_PATH="$LIB_DIR" "$HW" schema

echo
echo "--- Misc parquet files (from /testdata if mounted) ---"

for f in /testdata/*.parquet; do
    [ -f "$f" ] || continue
    run_test "$(basename "$f")" "$f" "$HW" schema
done

if [ "${TEST_S3:-false}" = "true" ]; then
    echo
    echo "--- S3 tests ---"

    S3_PARQUET="$REPO/performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2016-01.parquet"
    S3_ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:9090}"

    if [ ! -f "$S3_PARQUET" ]; then
        yellow "  SKIP  S3 tests ($S3_PARQUET not found)"
        SKIP=$((SKIP + 1))
    else
        curl -sf -X PUT "$S3_ENDPOINT/test-bucket" > /dev/null
        curl -sf -X PUT --data-binary @"$S3_PARQUET" \
            "$S3_ENDPOINT/test-bucket/yellow_tripdata_2025-01.parquet" > /dev/null

        run_s3_test "S3 info (yellow tripdata 2025-01)" \
            "s3://test-bucket/yellow_tripdata_2025-01.parquet" \
            "$HW" info
    fi
fi

echo
echo "=== Results: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped ==="
[ "$FAIL" -eq 0 ]
