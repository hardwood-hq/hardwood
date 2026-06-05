#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Run japicmp across the four published modules and concatenate the per-module
# reports into a unified text diff and a unified HTML.
#
# Usage: tools/api-report.sh <oldVersion> [<newVersion>]
#   e.g. tools/api-report.sh 1.0.0.CR1               # HEAD (snapshot) vs 1.0.0.CR1
#        tools/api-report.sh 1.0.0.Beta2 1.0.0.CR1   # compare two published versions
#
# When <newVersion> is omitted, the local snapshot is the new side. When it is
# supplied, both sides are resolved from the Maven repository. Either way the
# local modules are installed first — japicmp's shared classpath needs the
# project's own artifact present (see below).
#
# Outputs:
#   target/japicmp/api-report.diff           — concatenated per-module diffs
#   target/japicmp/api-report.html           — single HTML with TOC + module sections
#   target/japicmp/<artifactId>/*.{html,diff,md,xml}  — per-module reports

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <oldVersion> [<newVersion>]" >&2
  echo "Examples:" >&2
  echo "  $0 1.0.0.CR1" >&2
  echo "  $0 1.0.0.Beta2 1.0.0.CR1" >&2
  exit 2
fi

OLD="$1"
NEW="${2:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODULES=":hardwood-core,:hardwood-avro,:hardwood-s3,:hardwood-aws-auth"
ARTIFACTS=(hardwood-core hardwood-avro hardwood-s3 hardwood-aws-auth)

if [ -z "$NEW" ]; then
  # Record which commit HEAD was so a downloaded report names its exact source.
  # Honor CI's ${GITHUB_SHA} when set, otherwise resolve it from the local repo.
  HEAD_SHA="$(git rev-parse --short "${GITHUB_SHA:-HEAD}")"
  CAPTION="HEAD ($HEAD_SHA) vs $OLD"
else
  CAPTION="$NEW vs $OLD"
fi

# The local modules are always installed, even when both compared versions are
# published. japicmp resolves with ONE_COMMON_CLASSPATH, which pulls the current
# project's own artifact (the local snapshot) onto the shared classpath; without
# it the goal fails to resolve that snapshot regardless of the compared
# coordinates. The installed snapshot only backs the classpath — the diff is
# still computed strictly between $OLD and the new side.
#
# error-prone-checks is wired as an annotationProcessorPath, not a project
# dependency, so it stays invisible to the reactor and must be installed into
# the local repo first. The four modules are then installed (not just packaged)
# so japicmp can resolve their snapshot dependencies cross-invocation.
echo "Installing error-prone-checks and module JARs..."
./mvnw -ntp -pl :hardwood-error-prone-checks -DskipTests install -q
./mvnw -ntp -pl "$MODULES" -am -DskipTests install -q

echo "Running japicmp..."
if [ -z "$NEW" ]; then
  ./mvnw -ntp -pl "$MODULES" japicmp:cmp -Djapicmp.oldVersion="$OLD" -q
else
  ./mvnw -ntp -pl "$MODULES" japicmp:cmp \
    -Djapicmp.oldVersion="$OLD" -Djapicmp.newVersion="$NEW" -q
fi

REPORT_DIR="$ROOT/target/japicmp"
mkdir -p "$REPORT_DIR"
UNIFIED_DIFF="$REPORT_DIR/api-report.diff"
UNIFIED_HTML="$REPORT_DIR/api-report.html"
GENERATED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Per-module file paths. The plugin names report files after the execution id
# (default-cli when invoked from the command line). Glob to stay robust if the
# goal ever gets bound to a phase with a different id.
diff_for() { ls "$REPORT_DIR/$1"/*.diff 2>/dev/null | head -1; }
html_for() { ls "$REPORT_DIR/$1"/*.html 2>/dev/null | head -1; }

# --- text diff ----------------------------------------------------------------
{
  echo "Hardwood API report"
  echo "  $CAPTION"
  echo "  generated $GENERATED"
  for artifact in "${ARTIFACTS[@]}"; do
    diff_file=$(diff_for "$artifact")
    echo
    echo "=================================================="
    echo " $artifact"
    echo "=================================================="
    if [ -n "$diff_file" ] && [ -f "$diff_file" ]; then
      cat "$diff_file"
    else
      echo "(no diff produced — japicmp goal did not run for this module)"
    fi
  done
} > "$UNIFIED_DIFF"

# --- HTML ---------------------------------------------------------------------
# Reuse one per-module HTML's <head> as a style template (japicmp embeds its
# CSS there); add a table of contents under the page heading, then splice each
# module's <body> content under an <h1 id="<artifact>"> banner.
template=$(html_for "${ARTIFACTS[0]}")
if [ -n "$template" ] && [ -f "$template" ]; then
  {
    sed -n '1,/<\/head>/p' "$template" \
      | sed "s|<title>.*</title>|<title>Hardwood API report — $CAPTION</title>|"
    echo "<body>"
    echo "<h1>Hardwood API report</h1>"
    echo "<p>$CAPTION &mdash; generated $GENERATED</p>"
    echo "<ul>"
    for artifact in "${ARTIFACTS[@]}"; do
      echo "  <li><a href=\"#$artifact\">$artifact</a></li>"
    done
    echo "</ul>"
    for artifact in "${ARTIFACTS[@]}"; do
      html_file=$(html_for "$artifact")
      echo "<hr>"
      echo "<h1 id=\"$artifact\">$artifact</h1>"
      if [ -n "$html_file" ] && [ -f "$html_file" ]; then
        # Body content, without the wrapping <body>/</body> tags.
        sed -n '/<body>/,/<\/body>/p' "$html_file" | sed '1d;$d'
      else
        echo "<p>(no HTML report produced)</p>"
      fi
    done
    echo "</body>"
    echo "</html>"
  } > "$UNIFIED_HTML"
fi

echo
echo "Wrote unified reports:"
echo "  $UNIFIED_DIFF"
[ -f "$UNIFIED_HTML" ] && echo "  $UNIFIED_HTML"
echo "Per-module reports:   $REPORT_DIR/<artifact>/*.{html,diff,md,xml}"
