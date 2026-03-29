#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Patches HTML files on the gh-pages branch for SEO:
#
# - Versioned doc directories ([0-9]*/, latest/): inject a <link rel="canonical">
#   pointing to the equivalent page under /latest/, so Google indexes only the
#   current release.
# - dev/: inject <meta name="robots" content="noindex, follow"> since dev docs
#   are unstable and should not appear in search results.
# - Versioned API directories (api/[0-9]*/, api/latest/): inject a canonical
#   pointing to /api/latest/.
# - api/dev/: inject noindex.
#
# Idempotent — files that already contain the relevant tag are skipped.
#
# Usage: patch-seo.sh <site-url>
#   Run from the root of a gh-pages checkout.
#   Example: patch-seo.sh https://hardwood.dev

SITE_URL="${1:?Usage: patch-seo.sh <site-url>}"
SITE_URL="${SITE_URL%/}"  # strip trailing slash

patched=0

inject_canonical() {
  local dir="$1" base_path="$2"

  while IFS= read -r file; do
    # Derive the relative path within the version directory
    rel_path="${file#"$dir"}"
    canonical="${SITE_URL}/${base_path}${rel_path}"
    sed -i "s|</head>|<link rel=\"canonical\" href=\"${canonical}\" />\n</head>|" "$file"
    patched=$((patched + 1))
  done < <(grep -rL 'rel="canonical"' "$dir" --include="*.html" || true)
}

inject_noindex() {
  local dir="$1"

  while IFS= read -r file; do
    sed -i 's|</head>|<meta name="robots" content="noindex, follow">\n</head>|' "$file"
    patched=$((patched + 1))
  done < <(grep -rL 'name="robots"' "$dir" --include="*.html" || true)
}

# Prose docs: canonical for versioned + latest, noindex for dev
for dir in [0-9]*/ latest/; do
  [ -d "$dir" ] || continue
  inject_canonical "$dir" "latest/"
done

if [ -d "dev/" ]; then
  inject_noindex "dev/"
fi

# API docs: canonical for versioned + latest, noindex for dev
for dir in api/[0-9]*/ api/latest/; do
  [ -d "$dir" ] || continue
  inject_canonical "$dir" "api/latest/"
done

if [ -d "api/dev/" ]; then
  inject_noindex "api/dev/"
fi

echo "patch-seo: patched $patched file(s)"
