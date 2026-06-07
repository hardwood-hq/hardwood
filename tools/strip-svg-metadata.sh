#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Strip PlantUML metadata processing instructions from generated JavaDoc SVGs.
#
# UMLDoclet renders each class diagram with PlantUML, which embeds a copy of the
# diagram source into the SVG as a `<?plantuml-src ...?>` processing instruction
# (preceded by a `<?plantuml <version>?>` header). That embedded source carries a
# wall-clock `' Generated <timestamp>` comment, so every diagram's bytes change on
# every build even when the rendered graphic is byte-for-byte identical — churning
# every class-diagram SVG on the published site on each republish. The embedded
# source only matters for re-importing a diagram into PlantUML; the site never
# uses it. Removing both PIs makes the SVGs reproducible (and smaller) while
# leaving the rendered `<svg>` untouched.
#
# Usage: tools/strip-svg-metadata.sh <dir>
#   <dir>  directory tree to scan for *.svg (e.g. target/reports/apidocs)

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <dir>" >&2
  exit 2
fi

DIR="$1"
if [ ! -d "$DIR" ]; then
  echo "Not a directory: $DIR" >&2
  exit 1
fi

count=0
while IFS= read -r -d '' svg; do
  # PlantUML writes single-line SVGs; the PIs sit inline. Drop the version header
  # and the embedded-source blob, both of which match `<?plantuml...?>`.
  perl -0pi -e 's{<\?plantuml(?:-src)?\b[^>]*\?>}{}g' "$svg"
  count=$((count + 1))
done < <(find "$DIR" -type f -name '*.svg' -print0)

echo "Stripped PlantUML metadata from $count SVG(s) under $DIR"
