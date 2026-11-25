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
# tools/release/release.sh – cut a release: tag it and push the tag
#
#   tools/release/release.sh <VERSION> [--base <BRANCH>] [--next <NEXT>-SNAPSHOT]
#       Checks that the release notes of origin/<BRANCH> (default: main) have a section
#       for VERSION, that the tag does not exist yet and, for main, that the Main Build
#       of origin/main passed. Then:
#       - makes the commit "[release] Release VERSION" on top of origin/<BRANCH>, which
#         sets the POMs, README.md and docs/mkdocs.yml to VERSION, and tags it
#         v<VERSION>;
#       - pushes the tag, which starts the Release workflow: it builds the release,
#         stages it on the Central Portal and smoke-tests it there;
#       - opens a pull request against main that names VERSION in README.md and
#         docs/mkdocs.yml (if it is the latest release) and, after a Final release from
#         main, moves the POMs on to NEXT, which --next then has to give.
#
# BRANCH itself is not changed: the release commit is reachable from the tag only.
# Needs a clean working tree, push access to origin and an authenticated `gh`.
# ----------------------------------------------------------------------------

VERSION_PATTERN='^[0-9]+\.[0-9]+\.[0-9]+\.((Alpha|Beta|CR)[0-9]+|Final)$'
REPO=hardwood-hq/hardwood
SNAPSHOT_PATTERN='^[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT$'

usage() {
  sed -n '/^#   tools\/release\/release.sh </,/^# Needs/p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

fail() {
  echo "Error: $*" >&2
  exit 1
}

# Applies a sed expression to a file in place; `sed -i` takes different arguments in GNU
# and BSD (macOS) sed.
edit_file() {
  local expression="$1" file="$2"
  sed "${expression}" "${file}" > "${file}.tmp" && mv "${file}.tmp" "${file}"
}

# Prints a key per version that sorts as the versions do: major, minor and patch, then the
# qualifier in release order (Alpha < Beta < CR < Final) and its number.
version_key() {
  awk -F. '{
    q = $4; n = q; sub(/^[A-Za-z]+/, "", n); sub(/[0-9]+$/, "", q)
    rank = (q == "Alpha") ? 1 : (q == "Beta") ? 2 : (q == "CR") ? 3 : 4
    printf "%06d.%06d.%06d.%d.%06d %s\n", $1, $2, $3, rank, n + 0, $0
  }'
}

# Lists the versions of the release tags on origin.
release_versions() {
  git ls-remote --tags --refs origin 'v*' | sed -n 's|.*refs/tags/v||p' \
    | grep -E "${VERSION_PATTERN}" || true
}

# Sets the version of every POM, and the <scm> tag of every POM that has one, in the
# working tree (versions:set-scm-tag would change the root POM's only).
set_pom_version() {
  local version="$1" scm_tag="$2"
  ./mvnw -ntp -B -q -Pperformance-test versions:set \
    -DnewVersion="${version}" -DgenerateBackupPoms=false -DprocessAllModules=true
  local pom
  while IFS= read -r pom; do
    edit_file "s|<tag>[^<]*</tag>|<tag>${scm_tag}</tag>|" "${pom}"
  done < <(git ls-files '*pom.xml' | xargs grep -l '<tag>')
}

# Names the version as the latest release in README.md (every mention of the previous
# one, the dependency snippets among them) and docs/mkdocs.yml.
name_release_in_docs() {
  local version="$1" previous
  previous="$(sed -n 's/^Latest version: \([^,]*\),.*/\1/p' README.md)"
  [[ -n "${previous}" ]] || fail "README.md has no 'Latest version:' line"
  local previous_pattern
  previous_pattern="$(printf '%s' "${previous}" | sed 's/\./\\./g')"
  edit_file "s/${previous_pattern}/${version}/g" README.md
  edit_file "s/^Latest version: .*/Latest version: ${version}, $(date +%Y-%m-%d)/" README.md
  edit_file "s|^  hardwood_version: .*|  hardwood_version: ${version}|" docs/mkdocs.yml
}

# Fails unless the working tree changes only POMs, README.md and docs/mkdocs.yml.
check_only_versions_changed() {
  local changed
  changed="$(git status --porcelain | awk '{ print $2 }' \
    | grep -vE '(^|/)pom\.xml$|^README\.md$|^docs/mkdocs\.yml$' || true)"
  [[ -z "${changed}" ]] || fail "setting the version changed other files: ${changed}"
}

[[ $# -ge 1 ]] || usage
VERSION="$1"
shift
BASE=main
NEXT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)  [[ $# -ge 2 ]] || usage; BASE="$2"; shift 2 ;;
    --next)  [[ $# -ge 2 ]] || usage; NEXT="$2"; shift 2 ;;
    *)       usage ;;
  esac
done

# The paths below are relative to the repository root.
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# -- pre-flight checks -------------------------------------------------------

[[ "${VERSION}" =~ ${VERSION_PATTERN} ]] || fail "'${VERSION}' is not a release version such as 1.1.0.CR2 or 1.1.0.Final"
# Main moves on to the next development version after a Final release from it, and only then.
if [[ "${BASE}" == main && "${VERSION}" == *.Final ]]; then
  [[ "${NEXT}" =~ ${SNAPSHOT_PATTERN} ]] || fail "after a Final release, pass the next development version of main with --next, such as --next 1.2.0-SNAPSHOT"
else
  [[ -z "${NEXT}" ]] || fail "--next is for a Final release from main only"
fi
[[ -z "$(git status --porcelain)" ]] || fail "the working tree has local changes"
git fetch -q origin "refs/heads/${BASE}:refs/remotes/origin/${BASE}" "refs/heads/main:refs/remotes/origin/main" \
  || fail "cannot fetch ${BASE} and main from origin"
if git rev-parse -q --verify "refs/tags/v${VERSION}" > /dev/null \
    || [[ -n "$(git ls-remote --tags origin "refs/tags/v${VERSION}")" ]]; then
  fail "the tag v${VERSION} already exists"
fi
if git rev-parse -q --verify "refs/heads/release-${VERSION}" > /dev/null \
    || [[ -n "$(git ls-remote --heads origin "refs/heads/release-${VERSION}")" ]]; then
  fail "the branch release-${VERSION} already exists; delete it (and close its pull request) first"
fi
# The tag runs the workflows of its own commit: a branch cut from a tag made before the
# tag-triggered release needs this tooling cherry-picked first (RELEASING.md).
git cat-file -e "origin/${BASE}:tools/release/jreleaser.yml" 2> /dev/null \
  || fail "${BASE} lacks the release tooling (tools/release/, the release workflows); see RELEASING.md"
git show "origin/${BASE}:docs/content/release-notes.md" \
  | awk -v version="${VERSION}" '/^## / && $2 == version { found = 1 } END { exit !found }' \
  || fail "docs/content/release-notes.md on ${BASE} has no section '## ${VERSION}'; land the release notes first"

BASE_COMMIT="$(git rev-parse "origin/${BASE}")"
if [[ "${BASE}" == main ]]; then
  conclusion="$(gh run list -R "${REPO}" --workflow main-build.yml --commit "${BASE_COMMIT}" --limit 1 --json status,conclusion \
    --jq '.[0] | if . == null then "none" elif .status != "completed" then .status else .conclusion end')"
  [[ "${conclusion}" == success ]] \
    || fail "the Main Build of ${BASE_COMMIT} is '${conclusion}', not 'success'; wait for it or fix it"
else
  echo "Note: ${BASE} gets no CI build; make sure it was built and tested locally (see RELEASING.md)."
fi

# A release is the latest unless a higher one exists; the qualifiers sort in release
# order (Alpha < Beta < CR < Final).
LATEST=true
HIGHEST="$({ release_versions; echo "${VERSION}"; } | version_key | sort | tail -n 1 | cut -d' ' -f2)"
[[ "${HIGHEST}" == "${VERSION}" ]] || LATEST=false

ORIGINAL_REF="$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)"
# Back to where the script started, also after a failure; the working tree was clean, so
# what is left in it is the script's own change.
trap 'git switch -q -f "${ORIGINAL_REF}" 2> /dev/null || git switch -q -f --detach "${ORIGINAL_REF}"' EXIT

# -- the release commit ------------------------------------------------------

# On a detached HEAD: the tag holds the commit.
git switch -q --detach "${BASE_COMMIT}"
set_pom_version "${VERSION}" "v${VERSION}"
# The timestamp that makes a rebuild of the tag byte-identical.
edit_file "s|<project.build.outputTimestamp>[^<]*</project.build.outputTimestamp>|<project.build.outputTimestamp>$(date -u +%Y-%m-%dT%H:%M:%SZ)</project.build.outputTimestamp>|" pom.xml
name_release_in_docs "${VERSION}"
check_only_versions_changed
git commit -q -a -m "[release] Release ${VERSION}"
git tag -a "v${VERSION}" -m "Release ${VERSION}"
git --no-pager log --oneline -1

# -- push and release --------------------------------------------------------

git push -q origin "refs/tags/v${VERSION}"
echo "Pushed v${VERSION}, which starts the Release workflow."

# -- the pull request against main -------------------------------------------

if [[ "${LATEST}" == true || -n "${NEXT}" ]]; then
  git switch -q -c "release-${VERSION}" origin/main
  changes=()
  if [[ "${LATEST}" == true ]]; then
    name_release_in_docs "${VERSION}"
    changes+=("names ${VERSION} as the latest release in README.md and docs/mkdocs.yml")
  fi
  if [[ -n "${NEXT}" ]]; then
    set_pom_version "${NEXT}" HEAD
    changes+=("moves the POMs on to ${NEXT}")
  fi
  check_only_versions_changed
  git commit -q -a -m "[release] Update main for ${VERSION}"
  git push -q -u origin "release-${VERSION}"
  body="Released ${VERSION} from tag v${VERSION}; this ${changes[0]}"
  [[ ${#changes[@]} -lt 2 ]] || body+=", and ${changes[1]}"
  body+="."
  gh pr create -R "${REPO}" --base main --head "release-${VERSION}" --title "[release] Update main for ${VERSION}" --body "${body}"
  git switch -q --detach
  git branch -q -D "release-${VERSION}"
fi

echo "Follow the Release run, which prints the next steps once it has passed, with:"
echo "  tools/release/await.sh ${VERSION} --latest ${LATEST}"
