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
# release.sh – standalone release script for hardwood
#
# Usage: ./release.sh <RELEASE_VERSION> <DEVELOPMENT_VERSION> <STAGE>
#
#   RELEASE_VERSION      e.g. 1.0.0
#   DEVELOPMENT_VERSION  e.g. 1.1.0-SNAPSHOT
#   STAGE                UPLOAD (staging) or FULL (immediate publish)
#
# Required environment variables:
#   JRELEASER_MAVENCENTRAL_USERNAME
#   JRELEASER_MAVENCENTRAL_TOKEN
#   JRELEASER_GPG_PASSPHRASE
#   JRELEASER_GPG_PUBLIC_KEY
#   JRELEASER_GPG_SECRET_KEY
#   JRELEASER_GITHUB_TOKEN
#   MAVEN_CENTRAL_BEARER_TOKEN  (only when STAGE=UPLOAD)
# ----------------------------------------------------------------------------

# -- Validate arguments ------------------------------------------------------

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <RELEASE_VERSION> <DEVELOPMENT_VERSION> <STAGE>"
  exit 1
fi

RELEASE_VERSION="$1"
DEVELOPMENT_VERSION="$2"
STAGE="$3"

if [[ "$STAGE" != "UPLOAD" && "$STAGE" != "FULL" ]]; then
  echo "Error: STAGE must be UPLOAD or FULL (got '$STAGE')"
  exit 1
fi

# -- Validate environment variables ------------------------------------------

REQUIRED_VARS=(
  JRELEASER_MAVENCENTRAL_USERNAME
  JRELEASER_MAVENCENTRAL_TOKEN
  JRELEASER_GPG_PASSPHRASE
  JRELEASER_GPG_PUBLIC_KEY
  JRELEASER_GPG_SECRET_KEY
  JRELEASER_GITHUB_TOKEN
)

MISSING=()
for VAR in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!VAR:-}" ]]; then
    MISSING+=("$VAR")
  fi
done

if [[ "$STAGE" == "UPLOAD" && -z "${MAVEN_CENTRAL_BEARER_TOKEN:-}" ]]; then
  MISSING+=("MAVEN_CENTRAL_BEARER_TOKEN")
fi

if [[ ${#MISSING[@]} -gt 0 ]]; then
  echo "Error: the following required environment variables are not set:"
  printf '  %s\n' "${MISSING[@]}"
  exit 1
fi

# -- Validate tag does not already exist -------------------------------------

RELEASE_TAG="v${RELEASE_VERSION}"
if git rev-parse "${RELEASE_TAG}" &>/dev/null; then
  echo "Error: tag '${RELEASE_TAG}' already exists locally"
  exit 1
fi
if git ls-remote --tags origin "${RELEASE_TAG}" | grep -q "${RELEASE_TAG}"; then
  echo "Error: tag '${RELEASE_TAG}' already exists on remote"
  exit 1
fi

# -- Capture the base branch before switching --------------------------------

START_TIME="$(date +%s)"
BASE_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# -- Create release branch ---------------------------------------------------

echo "Creating release branch release/${RELEASE_VERSION} from ${BASE_BRANCH}..."
git checkout -b "release/${RELEASE_VERSION}"

# -- Generate API change report ----------------------------------------------

echo "Generating API change report..."
JAPICMP_OLD_VERSION="$(sed -n 's/^Latest version: \([^,]*\),.*/\1/p' README.md)"
./mvnw -ntp -B package japicmp:cmp -pl :hardwood-core -DskipTests -Djapicmp.oldVersion="${JAPICMP_OLD_VERSION}"

# -- Update README versions and date -----------------------------------------

echo "Updating README.md and docs versions..."
RELEASE_DATE="$(date +%Y-%m-%d)"
OLD_VERSION="$(sed -n 's/^Latest version: \([^,]*\),.*/\1/p' README.md)"
sed "s/${OLD_VERSION}/${RELEASE_VERSION}/g" README.md > README.md.tmp && mv README.md.tmp README.md
sed "s/^Latest version: .*/Latest version: ${RELEASE_VERSION}, ${RELEASE_DATE}/" README.md > README.md.tmp && mv README.md.tmp README.md
sed "s/${OLD_VERSION}/${RELEASE_VERSION}/g" docs/content/getting-started.md > docs/content/getting-started.md.tmp && mv docs/content/getting-started.md.tmp docs/content/getting-started.md
sed "s/${OLD_VERSION}/${RELEASE_VERSION}/g" docs/content/index.md > docs/content/index.md.tmp && mv docs/content/index.md.tmp docs/content/index.md
sed -i "s/^Latest version: .*/Latest version: ${RELEASE_VERSION}, ${RELEASE_DATE}/" docs/content/index.md
git add README.md docs/content/getting-started.md docs/content/index.md
git commit -m "[release] Update versions for ${RELEASE_VERSION}"

# -- Prepare and perform release ---------------------------------------------

echo "Running Maven release:prepare release:perform..."
./mvnw -ntp -B -Prelease release:prepare release:perform \
  -DreleaseVersion="${RELEASE_VERSION}" \
  -DdevelopmentVersion="${DEVELOPMENT_VERSION}" \
  -Dresume=false \
  -DpushChanges=false \
  -DlocalCheckout=true \
  -DpreparationGoals="clean verify -DskipTests" \
  -Darguments="-DskipTests"
git push -u origin "release/${RELEASE_VERSION}"
# Push the tag created by release:prepare so JReleaser finds it on the release
# commit. Without this, JReleaser doesn't see a tag on GitHub and creates one
# at the release branch's HEAD (the dev-bump commit), which would land the tag
# on the wrong commit.
git push origin "${RELEASE_TAG}"

# -- Publish to Maven Central ------------------------------------------------

echo "Publishing to Maven Central (stage=${STAGE})..."
export JRELEASER_DEPLOY_MAVEN_MAVENCENTRAL_STAGE="${STAGE}"
export JRELEASER_BRANCH="release/${RELEASE_VERSION}"
# maven-release-plugin's local checkout (target/checkout) clones from the local
# filesystem, so its remote points to a local path instead of GitHub. JReleaser
# needs an 'origin' remote with the GitHub URL to create the release.
ORIGIN_URL="$(git remote get-url origin 2>/dev/null || echo "")"
pushd target/checkout > /dev/null
if [[ -n "$ORIGIN_URL" ]]; then
  git remote remove origin 2>/dev/null || true
  git remote add origin "$ORIGIN_URL"
fi
./mvnw -ntp -B -N -Ppublication jreleaser:release
popd > /dev/null

# -- Verify staged release (UPLOAD only) -------------------------------------

if [[ "$STAGE" == "UPLOAD" ]]; then
  echo "Verifying staged release (using temporary local repo)..."
  STAGING_LOCAL_REPO="$(mktemp -d)"

  # The staging repo only serves dev.hardwood:* — pre-warm the temp local repo
  # with every external dep from the home cache so those don't round-trip to
  # staging and get 404s before falling back to Central. Only dev.hardwood:*
  # is actually fetched from staging.
  rsync -a "${HOME}/.m2/repository/" "${STAGING_LOCAL_REPO}/" --exclude='dev/hardwood'

  # Run from target/checkout so ${project.version} resolves to RELEASE_VERSION;
  # release-test-settings.xml lives in the project root.
  pushd target/checkout > /dev/null
  ../../mvnw -B clean verify \
    -pl :hardwood-integration-test \
    -Pcentral.manual.testing \
    -s ../../release-test-settings.xml \
    -Dmaven.repo.local="${STAGING_LOCAL_REPO}"
  popd > /dev/null

  rm -rf "${STAGING_LOCAL_REPO}"
fi

# -- Merge release branch back and clean up ----------------------------------

echo "Merging release branch back into ${BASE_BRANCH}..."
git checkout "${BASE_BRANCH}"
git merge --ff-only "release/${RELEASE_VERSION}"
git push origin "${BASE_BRANCH}"
git push origin --delete "release/${RELEASE_VERSION}" # tag already pushed above
git branch -d "release/${RELEASE_VERSION}"

ELAPSED=$(( $(date +%s) - START_TIME ))
echo "Release ${RELEASE_VERSION} complete in $((ELAPSED / 60))m $((ELAPSED % 60))s 🥳!"
