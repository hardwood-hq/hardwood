#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Generate an API change report laid out for publishing under /api-changes/<version>/.
#
# Resolves the comparison endpoints from the repo's git tags and delegates to
# tools/api-report.sh, then stages the unified report in <outputDir> with the
# combined HTML exposed as index.html so the published URL serves cleanly.
#
# Usage: tools/publish-api-report.sh <version> <outputDir>
#   <version>    Maven release version (e.g. 1.0.0.CR1) or "dev"
#   <outputDir>  directory to receive index.html + the per-module reports
#
# For "dev", the local HEAD snapshot is the new side and the latest tagged
# release is the old side. For a release version, both sides are the published
# JARs — the release and its immediate predecessor — so the report is correct
# regardless of which ref the repository is checked out at. A version with no
# predecessor (the very first release) produces no report; the script prints a
# notice and exits 0.

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <version> <outputDir>" >&2
  echo "Examples:" >&2
  echo "  $0 dev /tmp/api-changes-dev" >&2
  echo "  $0 1.0.0.CR1 /tmp/api-changes-cr1" >&2
  exit 2
fi

VERSION="$1"
OUT="$2"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Released versions, oldest first. Release tags are `v<maven-version>`; the
# `-docs` re-tags and the rolling `1.0-early-access` tag are not releases. The
# release branches are tagged as siblings (not ancestors of one another), so the
# predecessor cannot be derived from git ancestry — only from version order.
RELEASES="$(git tag --list 'v[0-9]*' \
  | grep -vE -- '-(docs|early-access)$' \
  | sed 's/^v//' \
  | sort -V)"

# `sort -V` is a lexical sort, not a Maven version comparator. It yields the
# correct order *only* because the project restricts release qualifiers to
# Alpha<N>/Beta<N>/CR<N>/Final<N>, which already sort alphabetically into Maven's
# precedence. Fail loudly if a tag ever uses a qualifier outside that set (e.g.
# RC, which Maven orders before Final but which sorts after it lexically): its
# position would no longer match Maven's ordering and the predecessor could be
# silently wrong. Extend this list and re-check the ordering before releasing
# any other qualifier.
BAD="$(printf '%s\n' "$RELEASES" \
  | grep -vE '^[0-9]+\.[0-9]+\.[0-9]+\.(Alpha|Beta|CR|Final)[0-9]*$' || true)"
if [ -n "$BAD" ]; then
  echo "Release tag(s) with an unrecognized version qualifier:" >&2
  printf '  %s\n' $BAD >&2
  echo "Only Alpha/Beta/CR/Final sort lexically into Maven order — extend" >&2
  echo "tools/publish-api-report.sh before releasing other qualifiers." >&2
  exit 1
fi

if [ "$VERSION" = "dev" ]; then
  OLD="$(printf '%s\n' "$RELEASES" | tail -1)"
  if [ -z "$OLD" ]; then
    echo "No tagged release to compare 'dev' against — skipping report." >&2
    exit 0
  fi
  echo "Generating dev report: HEAD vs $OLD"
  tools/api-report.sh "$OLD"
else
  OLD="$(printf '%s\n' "$RELEASES" \
    | awk -v v="$VERSION" '$0 == v { print prev } { prev = $0 }')"
  if [ -z "$OLD" ]; then
    echo "No predecessor release for $VERSION — skipping report." >&2
    exit 0
  fi
  echo "Generating release report: $VERSION vs $OLD"
  tools/api-report.sh "$OLD" "$VERSION"
fi

REPORT_DIR="$ROOT/target/japicmp"
if [ ! -f "$REPORT_DIR/api-report.html" ]; then
  echo "Expected $REPORT_DIR/api-report.html was not produced." >&2
  exit 1
fi

mkdir -p "$OUT"
cp -r "$REPORT_DIR/." "$OUT/"
cp "$OUT/api-report.html" "$OUT/index.html"

echo "Staged report for /api-changes/$VERSION/ in $OUT"
