#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# prune-branches.sh — interactively delete worktrees and branches that have been
# merged into main.
#
# Four categories are handled, each with a different (and increasingly safe) delete:
#
#   0. WORKTREES whose checkout is merged          -> git worktree remove <path>
#         Runs first: while a branch is checked out it cannot be deleted, so
#         removing the worktree is what lets category 1 offer the branch.
#   1. LOCAL branches that are merged              -> git branch -D <b>
#   2. Branches you OWN on 'origin' (merged)       -> git push origin --delete <b>
#         "own" = tip commit authored by your git user.email.
#         DESTRUCTIVE: this removes the branch from the shared hardwood-hq repo.
#   3. Every other ref under refs/remotes/ that    -> git update-ref -d <ref>
#      is merged: contributor fork tracking refs, and caches such as
#      refs/remotes/pr/<N> left by `git fetch origin pull/<N>/head:...`
#         Local cleanup only — never touches anyone's fork on GitHub.
#
# WHAT COUNTS AS MERGED: either of two proofs, both about content.
#   A. Patch containment. The ref has commits outside the trunk, and for every one
#      of them the trunk has a commit, outside the ref's own history, with a
#      byte-identical change (verbatim patch id: whitespace counts). This needs no
#      GitHub: it covers rebase-merged PRs, PRs closed because another PR carried
#      their commits, and cherry-picks.
#   B. Tree containment at a merged PR. A squash merge lands several commits as
#      one, so no single trunk commit matches their patches. GitHub names the PR
#      (by the ref's tip commit, else by branch name, else by the N of pr/<N>),
#      and merging the ref into that PR's merge commit S yields S's own tree, with
#      S reachable from the trunk. The PR lookup only picks S; the proof is the
#      tree comparison.
#   A ref passing neither is kept. One that GitHub ties to a merged PR is reported.
#
# OPT-IN (--superseded): every commit the ref has outside the trunk has a twin in
#   the trunk with the same author, author date and subject — the ref is an older
#   revision of commits that were amended or rebased before they landed. This is
#   evidence, not proof: the older revision may hold something review dropped.
#   Without the flag these refs are only reported.
#
# OPEN PRs: a branch on origin that is the head or the base of an open PR is never
#   offered in category 2, however merged it is. Deleting it would close that PR.
#
# Nothing is deleted without a per-branch y/N confirmation. Use --dry-run to preview.
#
# Portability: written for bash 3.2 (the macOS system bash) — no associative arrays.

set -euo pipefail

DRY_RUN=false
DO_FETCH=true
DO_WORKTREES=true
DO_LOCAL=true
DO_ORIGIN=true
DO_OTHERS=true
OFFER_SUPERSEDED=false

usage() {
  cat <<'EOF'
Usage: prune-branches.sh [options]

  --dry-run       Show what would be deleted; never prompt, never delete.
  --no-fetch      Skip the initial `git fetch origin --prune`.
  --superseded    Also offer refs that are older revisions of commits in main
                  (same author, author date and subject). Not proven merged.
  --no-worktrees  Skip category 0 (worktrees on merged checkouts).
  --no-local      Skip category 1 (local branches).
  --no-origin     Skip category 2 (your branches on origin).
  --no-others     Skip category 3 (fork tracking refs and fetched PR refs).
  -h, --help      This help.

A ref is merged when every commit it has outside main landed with a
byte-identical change, or when it adds nothing to the merge commit of its
merged GitHub PR.
Requires `gh` (authenticated).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true ;;
    --no-fetch) DO_FETCH=false ;;
    --superseded) OFFER_SUPERSEDED=true ;;
    --no-worktrees) DO_WORKTREES=false ;;
    --no-local) DO_LOCAL=false ;;
    --no-origin) DO_ORIGIN=false ;;
    --no-others|--no-forks) DO_OTHERS=false ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# --- preconditions ----------------------------------------------------------

command -v git >/dev/null || { echo "git not found" >&2; exit 1; }
command -v gh  >/dev/null || { echo "gh (GitHub CLI) not found — needed for merge detection" >&2; exit 1; }
git rev-parse --git-dir >/dev/null 2>&1 || { echo "not inside a git repository" >&2; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "gh is not authenticated — run 'gh auth login'" >&2; exit 1; }
git remote get-url origin >/dev/null 2>&1 || { echo "no 'origin' remote" >&2; exit 1; }

MY_EMAIL=$(git config user.email || true)
[[ -n "$MY_EMAIL" ]] || { echo "git user.email is not set" >&2; exit 1; }

# hardwood-hq/hardwood — derived from origin's URL (handles ssh and https forms).
ORIGIN_URL=$(git remote get-url origin)
REPO=${ORIGIN_URL#*github.com[:/]}
REPO=${REPO%.git}
REPO_OWNER=${REPO%%/*}

# --- refresh remote state ----------------------------------------------------

# A dry run must be side-effect-free, and `git fetch --prune` mutates local
# tracking refs — so skip it (results may be as stale as your last fetch).
if $DRY_RUN; then
  DO_FETCH=false
  echo ">> --dry-run: skipping fetch (tracking refs may be stale)"
fi

# Fetch ONLY origin — enough to make categories 1/2 act on current state.
# Deliberately NOT `--all`: fetching contributor forks would re-create the very
# tracking refs category 3 then deletes (and re-create them again next run,
# since the branches still exist on those forks). Category 3 works purely on
# whatever refs you have already accumulated locally.
if $DO_FETCH; then
  echo ">> git fetch origin --prune"
  git fetch origin --prune --quiet
fi

TRUNK=origin/main
git rev-parse --verify --quiet "$TRUNK" >/dev/null || TRUNK=main

# --- lookups (one API call, one log walk) ------------------------------------
# MERGED_FILE: "headOid<TAB>prNumber<TAB>owner/headRef<TAB>mergeOid", one line per
# merged PR, newest first. Looked up with awk (exact field match — no regex/glob
# surprises with slashes or dots in branch names).
# OPEN_FILE: "owner/headRef<TAB>baseRef<TAB>prNumber", one line per open PR.
# TWINS_FILE: "authorEmail<TAB>authorDate<TAB>subject" of every trunk commit, the
# identity a commit keeps through amend and rebase (the committer date does not).
# PATCHES_FILE: "patchId commit" of every non-merge trunk commit, newest first.

MERGED_FILE=$(mktemp)
OPEN_FILE=$(mktemp)
TWINS_FILE=$(mktemp)
PATCHES_FILE=$(mktemp)
trap 'rm -f "$MERGED_FILE" "$OPEN_FILE" "$TWINS_FILE" "$PATCHES_FILE"' EXIT

echo ">> querying merged PRs from $REPO"
gh pr list --repo "$REPO" --state merged --limit 1000 \
  --json number,headRefName,headRefOid,headRepositoryOwner,mergeCommit \
  --jq '.[] | "\(.headRefOid)\t\(.number)\t\(.headRepositoryOwner.login)/\(.headRefName)\t\(.mergeCommit.oid // "-")"' > "$MERGED_FILE"
echo ">> $(wc -l < "$MERGED_FILE" | tr -d ' ') merged PRs known"

gh pr list --repo "$REPO" --state open --limit 1000 \
  --json number,headRefName,baseRefName,headRepositoryOwner \
  --jq '.[] | "\(.headRepositoryOwner.login)/\(.headRefName)\t\(.baseRefName)\t\(.number)"' > "$OPEN_FILE"

git log --format='%ae%x09%ad%x09%s' --date=raw "$TRUNK" | sort -u > "$TWINS_FILE"

# --verbatim: the default patch id strips all whitespace, so it equates changes
# that differ only in indentation, which is content in YAML, Python or Makefiles.
patch_ids() { # $1 = revision range -> "patchId commit" per non-merge commit with a diff
  git log -p --no-merges --no-color --no-ext-diff --format='commit %H' "$1" | git patch-id --verbatim
}
patch_ids "$TRUNK" > "$PATCHES_FILE"

# Each prints "<number>\t<owner>/<headRef>\t<mergeOid>", or nothing.
merged_pr_at() { # $1 = commit oid (the PR's head commit)
  awk -F'\t' -v o="$1" '$1==o { print $2 "\t" $3 "\t" $4; exit }' "$MERGED_FILE"
}
merged_pr_named() { # $1 = owner/headRef; a reused name finds its latest merged PR
  awk -F'\t' -v n="$1" '$3==n { print $2 "\t" $3 "\t" $4; exit }' "$MERGED_FILE"
}
merged_pr_numbered() { # $1 = PR number
  awk -F'\t' -v n="$1" '$2==n { print $2 "\t" $3 "\t" $4; exit }' "$MERGED_FILE"
}

# Prints "head of open PR #N" or "base of open PR #N" for a branch on origin, or nothing.
open_pr_on() { # $1 = bare branch name on origin
  awk -F'\t' -v h="$REPO_OWNER/$1" -v b="$1" '
    $1==h { print "head of open PR #" $3; exit }
    $2==b { print "base of open PR #" $3; exit }' "$OPEN_FILE"
}

# --- proofs ------------------------------------------------------------------

# Commits the ref has outside the trunk, marked like `git cherry` marks them: "-"
# when a trunk commit carries a byte-identical change, "+" when none does. The
# trunk commit must lie outside the ref's history: a change the ref re-applies
# after the trunk reverted it matches the commit that was reverted, which is in
# the ref's history too. Of several trunk commits with one patch id only the
# newest is tried, which can keep a merged ref but never offer an unmerged one.
# Merge commits, whose conflict resolutions can carry changes of their own, and
# commits without a diff get no line, so a ref containing one passes neither
# proof A nor the twin check.
cherry_of() { # $1 = ref
  local ref=$1 sign c landed rc
  patch_ids "$TRUNK..$ref" |
    awk 'NR == FNR { if (!($1 in at)) at[$1] = $2; next }
         { print (($1 in at) ? "- " $2 " " at[$1] : "+ " $2) }' "$PATCHES_FILE" - |
    while read -r sign c landed; do
      if [[ "$sign" == "-" ]]; then
        # Exit status 1 means "not an ancestor"; an error must not read as that.
        git merge-base --is-ancestor "$landed" "$ref" && rc=0 || rc=$?
        [[ $rc -eq 1 ]] || sign="+"
      fi
      echo "$sign $c"
    done
}

covers_all_commits() { # $1 = ref, $2 = cherry output -> success if no commit was skipped
  local outside
  outside=$(git rev-list --count "$TRUNK..$1")
  [[ "$(grep -c . <<<"$2")" -eq "$outside" ]]
}

# A. Patch containment. At least one commit outside the trunk is required: a ref
# with none is either an ancestor of the trunk or a branch just created and not
# committed to yet, and the latter must survive. Ancestors that a merged PR
# names are still proven by B.
patches_in_trunk() { # $1 = ref, $2 = cherry output
  [[ -n "$2" ]] || return 1
  covers_all_commits "$1" "$2" || return 1
  ! grep -q '^+' <<<"$2"
}

# B. Tree containment anchored at the merge commit S: if merging the ref into S
# yields exactly S's own tree, every change the ref carries was already present
# the moment the PR merged. Anchoring at S and not at the trunk tip is essential:
# against a trunk that has since evolved the same lines, the merge conflicts and
# proves nothing. S must then be reachable from the trunk (a PR merged into some
# other base that was later squashed away is not, and correctly fails).
merged_into_trunk() { # $1 = ref, $2 = merge commit oid -> success if ref adds nothing to it
  local ref=$1 merge=$2 want got
  [[ "$merge" == "-" ]] && return 1
  git cat-file -e "$merge^{commit}" 2>/dev/null || return 1   # merge commit not fetched
  git merge-base --is-ancestor "$merge" "$TRUNK" 2>/dev/null || return 1
  want=$(git rev-parse "$merge^{tree}")
  got=$(git merge-tree --write-tree "$merge" "$ref" 2>/dev/null | head -1) || got=
  [[ -n "$got" && "$got" == "$want" ]]
}

# Opt-in evidence: every "+" commit has a twin in the trunk.
revisions_in_trunk() { # $1 = ref, $2 = cherry output
  local sign c found=false
  covers_all_commits "$1" "$2" || return 1
  while read -r sign c; do
    [[ "$sign" == "+" ]] || continue
    found=true
    grep -Fxq "$(git log -1 --format='%ae%x09%ad%x09%s' --date=raw "$c")" "$TWINS_FILE" || return 1
  done <<<"$2"
  $found
}

# Sets VERDICT to proven | superseded | kept | none, and REASON to a short label.
# $2 names the PR lookup to fall back on when the tip is no merged PR's head:
# "name:<owner>/<headRef>", "number:<N>", or "" for none (a fork remote's name is
# a local nickname, so pairing it with a branch name proves nothing).
VERDICT=none
REASON=''
assess() { # $1 = ref or commit oid, $2 = PR lookup
  local ref=$1 lookup=$2 tip hit="" via=head pr headref merge cherry
  tip=$(git rev-parse "$ref^{commit}")
  hit=$(merged_pr_at "$tip")
  if [[ -z "$hit" ]]; then
    via=${lookup%%:*}
    case "$lookup" in
      name:*)   hit=$(merged_pr_named "${lookup#name:}") ;;
      number:*) hit=$(merged_pr_numbered "${lookup#number:}") ;;
    esac
  fi
  pr='' headref='' merge='-'
  [[ -n "$hit" ]] && IFS=$'\t' read -r pr headref merge <<<"$hit"

  cherry=$(cherry_of "$ref")
  if patches_in_trunk "$ref" "$cherry"; then
    VERDICT=proven; REASON="every commit landed${pr:+, PR #$pr}"
  elif [[ -n "$pr" ]] && merged_into_trunk "$ref" "$merge"; then
    VERDICT=proven; REASON="contained in PR #$pr's merge"
  elif revisions_in_trunk "$ref" "$cherry"; then
    VERDICT=superseded; REASON="superseded${pr:+, PR #$pr}"
  elif [[ -n "$pr" && "$via" == head ]]; then
    VERDICT=kept; REASON="tip is PR #$pr's head, but its content is not in $TRUNK"
  elif [[ -n "$pr" && "$via" == number ]]; then
    VERDICT=kept; REASON="fetched from PR #$pr, but not its merged head and not contained"
  elif [[ -n "$pr" ]]; then
    VERDICT=kept; REASON="named after PR #$pr, but not at its head and not contained"
  else
    VERDICT=none; REASON=''
  fi
  return 0
}

# Refs left in place, reported at the end.
KEPT=()
SUPERSEDED=()
KEPT_OPEN=()

# Success when the ref may be offered for deletion; otherwise records it in the
# matching report. Sets REASON for the caller's listing.
eligible() { # $1 = ref or commit oid, $2 = display name, $3 = PR lookup
  assess "$1" "$3"
  case "$VERDICT" in
    proven) return 0 ;;
    superseded)
      $OFFER_SUPERSEDED && return 0
      SUPERSEDED+=("$2 ($REASON) — $(tip_line "$1")")
      return 1 ;;
    kept)
      KEPT+=("$2 — $REASON")
      return 1 ;;
  esac
  return 1
}

# --- protected branches ------------------------------------------------------
# These guards are NOT about losing commits — the containment proofs already
# settle that. They protect refs whose existence is itself the point:
#
#   main/master/HEAD  A rebase-merged PR can leave headRefOid equal to its own
#                     merge commit (PR #789 did), so the trunk tip can legitimately
#                     match a merged PR head and pass every content check. Deleting
#                     the trunk would be "safe" and catastrophic. Also covers the
#                     origin/HEAD symref, which resolves to the trunk tip.
#   release docs      docs-publish.yml is dispatched against these by ref. Their
#                     content is merged; their job is to keep existing.
#
# Release docs branches are matched by their full shape (1.0.0.Beta2-docs,
# v1.1.0.Beta1-docs). A glob such as `*-docs` also matches feature branches like
# 1082-decimal-predicate-docs and silently keeps them. Releases themselves are
# tags, which this script never touches.
RELEASE_DOCS_REF='^v?[0-9]+\.[0-9]+\.[0-9]+\.[A-Za-z0-9]+-docs$'

is_protected() { # $1 = bare branch name (no remote prefix)
  local b=$1
  case "$b" in
    main|master|HEAD|'') return 0 ;;
  esac
  [[ "$b" =~ $RELEASE_DOCS_REF ]]
}

# Being checked out blocks `git branch -D` and nothing else, so it is a category 1
# concern only — deliberately NOT folded into is_protected(). Doing so also
# shielded origin/<b> and <fork>/<b> from categories 2 and 3, where whether a
# local worktree happens to sit on the branch has no bearing on the remote ref.
#
# Recomputed after every worktree removal: freeing a branch in category 0 is
# precisely what makes it deletable in category 1.
CHECKED_OUT=''
refresh_checked_out() {
  CHECKED_OUT=$(git worktree list --porcelain | sed -n 's#^branch refs/heads/##p')
}
refresh_checked_out

is_checked_out() { # $1 = bare branch name
  grep -Fxq "$1" <<<"$CHECKED_OUT"
}

# --- confirmation + counters -------------------------------------------------

deleted=0
skipped=0

confirm() { # $1 = human description of the action
  if $DRY_RUN; then
    printf '    [dry-run] would %s\n' "$1"
    return 1
  fi
  local ans
  read -r -p "    Delete ($1)? [y/N] " ans </dev/tty
  [[ "$ans" == y || "$ans" == Y ]]
}

tip_line() { # $1 = a committish; prints "<short-sha> <subject>"
  git log -1 --format='%h %s' "$1" 2>/dev/null || echo '<unknown>'
}

# ============================================================================
# 0. WORKTREES whose checkout is merged
# ============================================================================
# Keyed on the worktree's HEAD commit rather than on its branch, so a detached
# worktree parked on a merged PR head is caught the same way a branch is.
#
# Two states mean someone is still using the tree. Both are reported and kept:
#   locked  `git worktree add` under a claude session locks the worktree for the
#           lifetime of that session. Removing it would pull the tree out from
#           under a running agent, so a lock is a hard no — this script never
#           unlocks and never passes --force.
#   dirty   Uncommitted work exists nowhere else. Ignored build output (target/,
#           ...) does not count: `git status --porcelain` omits it.
KEPT_WORKTREES=()

if $DO_WORKTREES; then
  echo
  echo "== worktrees on merged checkouts =="
  # The main worktree holds the repository itself and is always listed first.
  # Nor can a worktree be removed from inside itself.
  MAIN_WORKTREE=$(git worktree list --porcelain | sed -n '1s/^worktree //p')
  HERE=$(git rev-parse --show-toplevel)
  while IFS=$'\t' read -r path head branch locked; do
    [[ "$path" == "$MAIN_WORKTREE" || "$path" == "$HERE" ]] && continue
    is_protected "$branch" && continue
    label=${branch:-'(detached)'}
    lookup=''
    [[ -n "$branch" ]] && lookup="name:$REPO_OWNER/$branch"
    eligible "$head" "$path [$label]" "$lookup" || continue
    if [[ -n "$locked" ]]; then
      KEPT_WORKTREES+=("$path [$label] ($REASON) — locked: $locked")
      continue
    fi
    if [[ -n "$(git -C "$path" status --porcelain 2>/dev/null)" ]]; then
      KEPT_WORKTREES+=("$path [$label] ($REASON) — uncommitted changes")
      continue
    fi
    printf '  %s  [%s]  (%s)  %s\n' "$path" "$label" "$REASON" "$(tip_line "$head")"
    if confirm "git worktree remove $path"; then
      # Freeing the branch here is what lets category 1 offer it below.
      git worktree remove "$path" && { deleted=$((deleted + 1)); refresh_checked_out; }
    elif $DRY_RUN; then
      # A dry run removes nothing, so the branch would still read as checked out
      # and category 1 would show an empty list — the opposite of what a real run
      # does. Drop it from the cache instead to preview the full cascade.
      CHECKED_OUT=$(grep -Fxv "$branch" <<<"$CHECKED_OUT" || true)
    else
      skipped=$((skipped + 1))
    fi
  done < <(git worktree list --porcelain | awk '
      /^worktree /  { path = substr($0, 10) }
      /^HEAD /      { head = $2 }
      /^branch /    { branch = substr($0, 19) }          # strip "branch refs/heads/"
      /^locked/     { locked = (length($0) > 7) ? substr($0, 8) : "held by another process" }
      /^$/          { if (path != "") print path "\t" head "\t" branch "\t" locked
                      path = head = branch = locked = "" }
      END           { if (path != "") print path "\t" head "\t" branch "\t" locked }
    ')
fi

# ============================================================================
# 1. LOCAL branches
# ============================================================================
if $DO_LOCAL; then
  echo
  echo "== Local branches (merged) =="
  # Local review checkouts (pr-880, pr497-rework, …) need no special casing: their
  # commits are judged like any other branch's. A branch carrying extra local
  # commits on top passes no proof and is kept — those commits exist nowhere else.
  while IFS= read -r b; do
    is_protected "$b" && continue
    is_checked_out "$b" && continue   # `git branch -D` would only error out
    eligible "refs/heads/$b" "$b" "name:$REPO_OWNER/$b" || continue
    printf '  %s  (%s)  %s\n' "$b" "$REASON" "$(tip_line "refs/heads/$b")"
    if confirm "git branch -D $b"; then
      git branch -D "$b" && deleted=$((deleted + 1))
    else
      skipped=$((skipped + 1))
    fi
  done < <(git for-each-ref --format='%(refname:short)' refs/heads/)
fi

# ============================================================================
# 2. Branches you own on origin  (DESTRUCTIVE: git push origin --delete)
# ============================================================================
if $DO_ORIGIN; then
  echo
  echo "== origin branches you own (merged) =="
  while IFS= read -r ref; do
    b=${ref#refs/remotes/origin/}
    [[ "$b" == "$ref" ]] && continue      # not under origin/
    is_protected "$b" && continue
    owner_email=$(git log -1 --format='%ae' "$ref" 2>/dev/null || true)
    [[ "$owner_email" == "$MY_EMAIL" ]] || continue   # only branches you authored
    eligible "$ref" "origin/$b" "name:$REPO_OWNER/$b" || continue
    open_pr=$(open_pr_on "$b")
    if [[ -n "$open_pr" ]]; then
      KEPT_OPEN+=("origin/$b ($REASON) — $open_pr")
      continue
    fi
    printf '  origin/%s  (%s)  %s\n' "$b" "$REASON" "$(tip_line "$ref")"
    if confirm "git push origin --delete $b  [removes it from the shared repo]"; then
      git push origin --delete "$b" && deleted=$((deleted + 1))
    else
      skipped=$((skipped + 1))
    fi
  done < <(git for-each-ref --format='%(refname)' refs/remotes/origin/)
fi

# ============================================================================
# 3. Every other ref under refs/remotes/ (local cleanup only)
# ============================================================================
# Not only configured remotes: `git fetch origin pull/<N>/head:refs/remotes/pr/<N>`
# files refs under a namespace no remote owns, and looping over `git remote`
# never visits them. A ref's namespace is the longest configured remote name it
# starts with, else its first path component.
REMOTES=$(git remote)
namespace_of() { # $1 = full refname under refs/remotes/
  local rest=${1#refs/remotes/} r best=''
  while IFS= read -r r; do
    [[ -n "$r" && "$rest" == "$r"/* && ${#r} -gt ${#best} ]] && best=$r
  done <<<"$REMOTES"
  [[ -n "$best" ]] && echo "$best" || echo "${rest%%/*}"
}

if $DO_OTHERS; then
  echo
  echo "== other remote-tracking refs (merged) =="
  echo "   (removes the LOCAL ref only. A fork's ref returns on the next fetch of"
  echo "    that fork if the branch still exists there; to stop tracking a"
  echo "    contributor for good, remove the remote: git remote remove <name>.)"
  while IFS= read -r ref; do
    ns=$(namespace_of "$ref")
    [[ "$ns" == origin ]] && continue
    b=${ref#refs/remotes/$ns/}
    is_protected "$b" && continue
    # A configured remote's name is a local nickname ('arnab' for the fork of
    # GitHub user 'arnabnandy7'), so only the tip commit can name its PR. A
    # fetched PR ref names it outright: pr/<N>.
    lookup=''
    if ! grep -Fxq "$ns" <<<"$REMOTES" && [[ "$b" =~ ^[0-9]+$ ]]; then
      lookup="number:$b"
    fi
    eligible "$ref" "$ns/$b" "$lookup" || continue
    printf '  %s/%s  (%s)  %s\n' "$ns" "$b" "$REASON" "$(tip_line "$ref")"
    if confirm "git update-ref -d $ref  [local ref only]"; then
      # The expected old value makes the delete refuse a ref that moved meanwhile.
      git update-ref -d "$ref" "$(git rev-parse "$ref")" && deleted=$((deleted + 1))
    else
      skipped=$((skipped + 1))
    fi
  done < <(git for-each-ref --format='%(refname)' refs/remotes/)
fi

echo
# `${arr[*]+x}` rather than `${#arr[@]}`: under `set -u`, bash 3.2 rejects the
# length of an empty array as an unbound variable. The `+` form never does.
if [[ -n "${KEPT[*]+x}" ]]; then
  echo "== kept: tied to a merged PR, but not proven merged =="
  echo "   (commits pushed after the merge, or an older revision of the PR that"
  echo "    review changed. Compare against the PR by hand before removing the ref.)"
  for k in "${KEPT[@]}"; do echo "  $k"; done
  echo
fi

if [[ -n "${SUPERSEDED[*]+x}" ]]; then
  echo "== kept: older revisions of commits in $TRUNK (pass --superseded to offer) =="
  echo "   (every commit outside $TRUNK has a twin there with the same author,"
  echo "    author date and subject, but a different change.)"
  for s in "${SUPERSEDED[@]}"; do echo "  $s"; done
  echo
fi

if [[ -n "${KEPT_OPEN[*]+x}" ]]; then
  echo "== kept: merged origin branches an open PR depends on =="
  echo "   (deleting the head or base branch of a PR closes that PR. Delete it"
  echo "    once the PR is merged, closed or retargeted.)"
  for o in "${KEPT_OPEN[@]}"; do echo "  $o"; done
  echo
fi

if [[ -n "${KEPT_WORKTREES[*]+x}" ]]; then
  echo "== kept: merged worktrees still in use =="
  echo "   (unlock by ending the claude session holding it, or commit/discard the"
  echo "    changes, then re-run.)"
  for w in "${KEPT_WORKTREES[@]}"; do echo "  $w"; done
  echo
fi

if $DRY_RUN; then
  echo "Dry run complete — nothing deleted."
else
  echo "Done. Deleted: $deleted, kept: $skipped."
fi
