#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# ----------------------------------------------------------------------------
# tools/release/cli-smoke-test.sh – run a CLI distribution as its users do
#
# Usage: tools/release/cli-smoke-test.sh <VERSION> <ARCHIVE>
#
# Unpacks ARCHIVE (hardwood-cli-<VERSION>-<os>-<arch>.tar.gz or .zip) into a temporary
# directory and runs its binary: --version must name VERSION, and info, schema and print
# must report what tools/release/smoke-test/smoke.parquet holds (100 rows in two row
# groups, ZSTD-compressed, written by PyArrow). Builds nothing and needs no credentials.
# ----------------------------------------------------------------------------

if [[ $# -ne 2 ]]; then
  sed -n '/^# Usage/,/^# ---/p' "$0" | sed '$d; s/^# \{0,1\}//'
  exit 1
fi
VERSION="$1"
ARCHIVE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
FIXTURE="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/tools/release/smoke-test/smoke.parquet"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

case "$(basename "${ARCHIVE}")" in
  "hardwood-cli-${VERSION}-"*.tar.gz) tar -xzf "${ARCHIVE}" -C "${WORK}" ;;
  "hardwood-cli-${VERSION}-"*.zip)    unzip -q "${ARCHIVE}" -d "${WORK}" ;;
  *) echo "Error: $(basename "${ARCHIVE}") is not a CLI archive of ${VERSION}"; exit 1 ;;
esac
BINARY="$(find "${WORK}" -path '*/bin/hardwood' -o -path '*/bin/hardwood.exe' | head -n 1)"
if [[ -z "${BINARY}" ]]; then
  echo "Error: $(basename "${ARCHIVE}") holds no bin/hardwood"
  exit 1
fi

# Runs the binary and fails unless its output holds each of the given lines, as
# extended regular expressions matched against whole lines.
expect() {
  local args="$1" output pattern
  shift
  # shellcheck disable=SC2086 # args is a list of plain words
  if ! output="$("${BINARY}" ${args} 2>&1)"; then
    printf 'Error: hardwood %s failed:\n%s\n' "${args}" "${output}"
    exit 1
  fi
  for pattern in "$@"; do
    if ! grep -Eqx -- "${pattern}" <<< "${output//$'\r'/}"; then
      printf 'Error: the output of hardwood %s has no line matching %s:\n%s\n' "${args}" "${pattern}" "${output}"
      exit 1
    fi
  done
}

expect "--version" "hardwood ${VERSION//./\\.} \([0-9a-f]+\)"
expect "info -f ${FIXTURE}" \
  'Row Groups: +2' \
  'Total Rows: +100'
expect "schema -f ${FIXTURE}" \
  ' +optional int64 id;' \
  ' +optional byte_array station \(STRING\);' \
  ' +optional int64 pickup \(TIMESTAMP\(MICROS, local\)\);'
# The first rows, in the first row group, and the last ones, in the second.
expect "print -n 3 -f ${FIXTURE}" \
  '\| 2 +\| station-2 +\| 0\.5 +\| 2026-01-01T00:02 +\|'
expect "print -n -2 -f ${FIXTURE}" \
  '\| 99 +\| station-1 +\| 24\.75 +\| 2026-01-01T01:39 +\|'

echo "$(basename "${ARCHIVE}"): the CLI reports ${VERSION} and reads the fixture"
