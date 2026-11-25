#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

set -euo pipefail

# Usage: tools/release/smoke-test/resolve.sh <RELEASE_VERSION> <OUTPUT_DIR>
#
# Resolves a release staged on the Central Portal as a user's project does (pom.xml next
# to this script imports hardwood-bom and depends on hardwood-core, hardwood-s3 and
# snappy-java), with a fresh local repository, and copies those JARs, which SmokeTest.java
# runs on, to OUTPUT_DIR/lib.
# Needs MAVEN_CENTRAL_BEARER_TOKEN, base64("<username>:<token>") of a Central Portal user
# token. Runs only Maven and the dependency plugin; nothing resolved
# is executed. Maven asks the Portal for dev.hardwood artifacts only and Maven Central for
# everything else (repository-filter/).

if [[ $# -ne 2 ]]; then
  sed -n 's/^# \{0,1\}//; /^Usage/,/^$/p' "$0"
  exit 1
fi
RELEASE_VERSION="$1"
mkdir -p "$2"
OUTPUT_DIR="$(cd "$2" && pwd)"
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

LOCAL_REPOSITORY="$(mktemp -d)"
trap 'rm -rf "${LOCAL_REPOSITORY}"' EXIT
./mvnw -ntp -B dependency:build-classpath \
  -f tools/release/smoke-test/pom.xml \
  -s tools/release/settings.xml \
  -Pcentral.staging \
  -Dhardwood.version="${RELEASE_VERSION}" \
  -Dmaven.repo.local="${LOCAL_REPOSITORY}" \
  -Daether.remoteRepositoryFilter.groupId=true \
  -Daether.remoteRepositoryFilter.groupId.basedir="$(pwd)/tools/release/smoke-test/repository-filter" \
  -DincludeScope=runtime \
  -DincludeArtifactIds=hardwood-core,hardwood-s3,snappy-java \
  -Dmdep.outputFile="${LOCAL_REPOSITORY}/classpath.txt"

mkdir -p "${OUTPUT_DIR}/lib"
# The classpath file has no trailing newline, which read reports as end of file.
IFS=: read -r -a JARS < <(cat "${LOCAL_REPOSITORY}/classpath.txt"; echo)
cp "${JARS[@]}" "${OUTPUT_DIR}/lib/"
if [[ "$(find "${OUTPUT_DIR}/lib" -name '*.jar' | wc -l)" -ne ${#JARS[@]} ]]; then
  echo "Error: two JARs on the resolved classpath have the same file name" >&2
  exit 1
fi
echo "Resolved ${#JARS[@]} JARs of ${RELEASE_VERSION} into ${OUTPUT_DIR}/lib"
