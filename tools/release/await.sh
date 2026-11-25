#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Usage: tools/release/await.sh <VERSION> --latest true|false
#
# Follows the Release run that the tag v<VERSION> started (tools/release/release.sh prints
# this command) and, once it has passed, prints the next steps: running the smoke test
# locally on what the run resolved, publishing in the Central Portal, and releasing the
# CLI. Needs an authenticated `gh`.

usage() {
  echo "Usage: $0 <VERSION> --latest true|false" >&2
  exit 1
}

[[ $# -eq 3 && "$2" == --latest && ( "$3" == true || "$3" == false ) ]] || usage
VERSION="$1"
LATEST="$3"
REPO=hardwood-hq/hardwood

# The run appears shortly after the tag push.
RUN=""
for _ in $(seq 1 24); do
  RUN="$(gh run list -R "${REPO}" --workflow release.yml --event push --branch "v${VERSION}" --limit 1 \
    --json databaseId --jq '.[0].databaseId // empty')"
  [[ -z "${RUN}" ]] || break
  sleep 5
done
if [[ -z "${RUN}" ]]; then
  echo "Error: no Release run for v${VERSION}; check https://github.com/${REPO}/actions/workflows/release.yml" >&2
  exit 1
fi

echo "Following https://github.com/${REPO}/actions/runs/${RUN} ..."
if ! gh run watch "${RUN}" -R "${REPO}" --exit-status --interval 30 > /dev/null; then
  echo "Error: the Release run failed; see https://github.com/${REPO}/actions/runs/${RUN}" >&2
  echo "Drop the deployment in the Central Portal if it was uploaded; RELEASING.md describes how to redo the release." >&2
  exit 1
fi

cat <<EOF
The Release run passed: v${VERSION} is staged on the Central Portal and smoke-tested.

To run the smoke test locally on what the run resolved:

  gh run download ${RUN} -R ${REPO} -n staged-classpath -D target/staged-classpath
  java --enable-native-access=ALL-UNNAMED -cp 'target/staged-classpath/lib/*' tools/release/smoke-test/SmokeTest.java ${VERSION}

Then publish the deployment (or drop it) at https://central.sonatype.com/publishing/deployments
and release the CLI with:

  gh workflow run release-cli.yml -R ${REPO} --ref v${VERSION} -f latest=${LATEST}
EOF
