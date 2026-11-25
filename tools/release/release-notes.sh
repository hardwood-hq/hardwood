#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Usage: tools/release/release-notes.sh <RELEASE_VERSION>
#
# Prints the version's section of docs/content/release-notes.md, without its heading, for
# the GitHub release: links into the site are made absolute and pinned to the version.
# Fails when there is no such section.

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <RELEASE_VERSION>" >&2
  exit 1
fi
RELEASE_VERSION="$1"
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

notes="$(awk -v version="${RELEASE_VERSION}" '/^## / { in_section = ($2 == version); next } in_section { print }' \
    docs/content/release-notes.md \
  | sed -E \
      -e "s#\]\(/(api|api-changes)/latest/#](/\1/${RELEASE_VERSION}/#g" \
      -e 's|\]\(/|](https://hardwood.dev/|g' \
      -e "s|\]\(([a-z0-9][^):]*)\.md(#[^)]*)?\)|](https://hardwood.dev/${RELEASE_VERSION}/\1/\2)|g" \
      -e 's|/index/([#)])|/\1|g')"
if [[ -z "${notes//[[:space:]]/}" ]]; then
  echo "Error: docs/content/release-notes.md has no section for ${RELEASE_VERSION}" >&2
  exit 1
fi
printf '%s\n' "${notes}"
