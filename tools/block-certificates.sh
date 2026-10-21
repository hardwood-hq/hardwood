#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# block-certificates.sh — add the entries of cli/src/main/trust/blocked.certs to a JDK's
# lib/security/blocked.certs, so that a native CLI built with that JDK rejects them.
#
# Usage: tools/block-certificates.sh [java-home]   (default: $GRAALVM_HOME, else $JAVA_HOME,
# the JDK native-maven-plugin takes native-image from)
#
# Entries the JDK already lists are skipped, so running it again changes nothing.

set -euo pipefail

java_home="${1:-${GRAALVM_HOME:-${JAVA_HOME:?set GRAALVM_HOME or JAVA_HOME, or pass the JDK directory}}}"
if command -v cygpath >/dev/null; then
  java_home="$(cygpath -u "$java_home")"
fi
target="$java_home/lib/security/blocked.certs"
source="$(dirname "$0")/../cli/src/main/trust/blocked.certs"

[ -f "$target" ] || { echo "No blocked.certs in $java_home" >&2; exit 1; }
# An appended entry must start on a line of its own.
[ -z "$(tail -c 1 "$target")" ] || echo >> "$target"

added=0
comment=""
while IFS= read -r line; do
  # A Windows checkout may have converted the list to CRLF line endings.
  line="${line%$'\r'}"
  case "$line" in
    '#'*) comment="$line" ;;
    [0-9A-F]*)
      if ! grep -qx "$line" "$target"; then
        printf '%s\n%s\n' "$comment" "$line" >> "$target"
        added=$((added + 1))
      fi
      ;;
  esac
done < "$source"

echo "Added $added entries to $target"
