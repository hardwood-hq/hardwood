#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Injects a noindex robots meta tag into HTML files of older versioned
# directories on the gh-pages branch. Idempotent — files that already
# contain the tag are skipped.
#
# Usage: patch-noindex.sh <current-version>
#   Run from the root of a gh-pages checkout.

CURRENT_VERSION="${1:?Usage: patch-noindex.sh <current-version>}"

patched=0
for dir in [0-9]*/; do
  [ -d "$dir" ] || continue
  version=$(basename "$dir")
  [ "$version" = "$CURRENT_VERSION" ] && continue

  while IFS= read -r file; do
    sed -i 's|</head>|<meta name="robots" content="noindex, follow">\n</head>|' "$file"
    patched=$((patched + 1))
  done < <(grep -rL 'name="robots"' "$dir" --include="*.html" || true)
done

echo "patch-noindex: patched $patched file(s)"
