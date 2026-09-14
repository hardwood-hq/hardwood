---
name: hardwood-pr-git
description: Rebase, rewrite, squash, reword and push Hardwood PR history — including contributor PRs whose author and committer identity must survive the rewrite. Use whenever the user asks to "rebase", "rebase onto main", "fix the commit message", "prefix with the issue key", "squash the commits", "force push (with lease)", "push to the PR", "amend", "bring the PR into the workspace", or "retain the original author/committership". Covers the `#`-comment-char trap that silently eats Hardwood commit subjects, the bind-mount stat cache that makes the whole tree look dirty when nothing changed, fetching PRs from forks, choosing the right push target, and verifying a rewrite changed nothing but the message.
---

# Hardwood PR git surgery

History rewriting on `hardwood-hq/hardwood` PRs. The mechanics are mechanical but unforgiving: a
careless `--amend` silently replaces a contributor's committer identity with the maintainer's, and
git's default comment character silently deletes the issue key every Hardwood commit subject starts
with.

## 0. The `#` rule — read this before running any git command that commits

**Every Hardwood commit subject starts with `#`, and `#` is git's comment character.** Any git
operation that runs a commit message through *editor cleanup* deletes every line beginning with `#`
— which is the entire subject line.

What that does, in the two shapes it takes:

| Message shape | What git does | What you see |
|---|---|---|
| `#9 Subject` + body | Subject deleted, body's first paragraph is promoted to subject | `01d5773 The write path's measurement stopped at the shape stage 17…` |
| `#9 Subject`, no body | Message becomes empty | `Aborting commit due to empty commit message.` → `error: could not commit staged changes` → the todo is *rescheduled*, and `git rebase --continue` loops forever without advancing |

The second shape is the "the rebase aborts for no reason" symptom. A `--continue` loop that never
terminates is not a stuck rebase — it is git refusing to write an empty commit, over and over.

**The fix, applied once per clone.** All worktrees of a clone share it:

```bash
git config core.commentChar ';'
```

This is one of two clone-local settings this repo needs; the other is in
[The phantom dirty tree](#the-phantom-dirty-tree--the-bind-mount-not-another-writer). Both live in
`.git/config`, which is **not** version-controlled — a fresh clone starts without them. Both are
documented for contributors under *Local git configuration* in
[CONTRIBUTING.md](../../../CONTRIBUTING.md); keep the two in sync.

**Also pass it explicitly** on every rewrite command, so the recipes still work in a fresh clone or a
CI checkout where the config has not been set:

```bash
git -c core.commentChar=';' rebase …
git -c core.commentChar=';' cherry-pick --continue
```

Do **not** use `core.commentChar=auto`: git 2.55 warns it is deprecated and removes it in Git 3.0.

Which commands are affected — memorize the split, it is the whole game:

| Safe (no editor → `--cleanup=whitespace`) | Eats the subject (editor → `--cleanup=default`) |
|---|---|
| `git commit -m …` | `git commit` (message from `MERGE_MSG`) |
| `git commit -F file` / `-F -` | `git commit --amend` (no `-m`/`-F`/`--no-edit`) |
| `git commit --amend --no-edit` | `git commit -c <sha>` |
| `git commit -C <sha>` | `git rebase --continue` after a conflict |
| `git commit-tree -F file` | `git cherry-pick --continue`, `git revert --continue`, `git merge --continue` |
| a rebase pick that applies cleanly | `reword` / `squash` steps in `rebase -i` |

Note the asymmetry: a rebase whose picks all apply cleanly keeps the subjects. The damage only
starts at the first conflict — which is why this looks intermittent and why it always shows up on
exactly the rebases that were already painful.

**Verify after every rewrite** (see §7); a stripped subject is invisible in `git log --oneline -1`
if you only look at the top commit.

## The phantom dirty tree — the bind mount, not another writer

Where the checkout is edited from two places at once — typically a dev container or VM with the
repository bind-mounted from the host, with an editor or git GUI still open on the host side — the
same files stat differently from each side:

| `stat` field | inside the container | on the host |
|---|---|---|
| `st_uid` / `st_gid` | remapped by the mount, often `0` / `0` | the host user's real uid / gid |
| `st_dev`, `st_ino` | the mount's values | the host filesystem's values |
| sub-second `mtime` / `ctime` | translated | native |

Confirm the shape before assuming it applies: `findmnt -T .` names the mount, and
`git ls-files --debug -- <file>` prints the uid/gid/dev/ino the index has cached for it.

Git's index caches those fields per file, and the default `core.checkStat` compares **all** of them,
so the stat cache can only be valid for one side at a time. Whichever side ran git last rewrote it
in its own terms and invalidated it for the other.

**Symptom.** gitk / git gui's history view shows a `Local uncommitted changes, not checked in to
index` node listing *every file in the repo*, with no content change behind any of them. That view
runs `git diff-index` **without** refreshing, so it reports the stale stat cache verbatim. git gui's
**Rescan** runs `update-index --refresh`, which opens each file, finds the content identical, and
rewrites the cache with host values — clearing the node, and dirtying it again for the container.

**The fix, applied once per clone** (`.git/config` is shared by both sides, so setting it anywhere
fixes both):

```bash
git config core.checkStat minimal     # drop uid, gid, ino, dev, sub-second times from the comparison
git config core.trustctime false      # drop ctime, which any host-side metadata touch bumps
```

`minimal` leaves whole-second `mtime` + size, which agree across the mount. The documented
trade-off: a modification made within the same second that leaves the file the same size can be
missed.

**What this is *not* the cause of.** A stat-only mismatch does **not** break a rebase. The sequencer
refreshes the index before each step, finds the content identical, and proceeds — verified by
rewriting the stat cache with host values from an `exec` step *between two picks*, which the rebase
then completed without complaint. So it is not behind `Your local changes … would be overwritten by
merge`, and not behind `cannot rebase: You have unstaged changes` either. Both of those mean the
tree is *genuinely* dirty; keep looking for the writer.

The one real link is second-order: a permanently red git gui invites a **Rescan**, and that Rescan
takes `.git/index.lock`. Lock contention is what stops a rebase — see the table in §1.

**Triage — is the dirt real?**

```bash
git diff --stat                       # refreshes: empty here means no content changed
git --no-optional-locks diff-index --name-status HEAD   # does not refresh: what gitk sees
```

Files in the second and not the first are phantom. Clear them with `git update-index --really-refresh`
— never with `git checkout --` or a stash, which risk destroying someone's real edit.

## Working rules

- **Run history surgery without an interactive terminal.** `rebase -i` does not need one — it needs
  non-interactive editors, and both hooks are honoured:
  ```bash
  GIT_SEQUENCE_EDITOR="sed -i '2s/^pick/reword/'"   # edits the todo list
  GIT_EDITOR=true                                    # or "cp /path/to/msg", accepts/replaces messages
  ```
  Reach for this before hand-rolling `commit-tree` surgery (§6). Only a step that waits on a TTY
  with no editor override is genuinely unavailable.
- **Assume the working tree has other writers.** An editor with the repo open, a running build, or a
  second agent session will write to tracked files while a rebase is replaying. This is the second
  cause of "the rebase aborted": a file goes dirty *mid-rebase* and git reports `Your local changes
  to the following files would be overwritten by merge / Aborting / Could not execute the todo
  command … It has been rescheduled`. See the preflight in §1. Before treating a dirty tree as
  another writer, confirm the dirt is real — on this bind mount most of it is not; see
  [The phantom dirty tree](#the-phantom-dirty-tree--the-bind-mount-not-another-writer).
- **Know what rerere does to a conflict.** Where `rerere.enabled` is on and `rerere.autoUpdate` is
  not, a recorded resolution is replayed into the working tree but **not** staged. Signature:
  `git rebase --continue` says `You must edit all merge conflicts and then mark them as resolved
  using git add`, yet `grep -c '^<<<<<<<' <file>` returns `0`. The file is resolved, just unstaged.
  Read the diff before `git add` — a recorded resolution can be stale and re-applies silently.
- **`origin` is `hardwood-hq/hardwood`** (HTTPS). Contributor forks are added as separate remotes
  named after the GitHub login (`stalep`, `iifawzi`, `kogupta`, `arnab`, `rionmonster`, …).
- **Fetch PRs over the HTTPS pull ref** (§2). `gh pr checkout` and SSH refs need credentials the
  checkout may not have; the pull ref works wherever `origin` was cloned from over HTTPS.
- **Rewriting published history and pushing it are separate decisions.** Do the rewrite, verify it
  (§7), and stop. Push only when asked.

## 1. Rebase a branch onto main

### Preflight — do not skip

```bash
git rev-parse HEAD > /tmp/old-tip                       # the escape hatch, and the §7 baseline
git status --porcelain                                  # must be empty except '??' you own
git fetch origin main
```

If `git status --porcelain` lists a large number of modified files, check whether any of it is real
before reacting — `git diff --stat` empty against a long `status` means the stat cache went stale,
not that anyone wrote anything. Refresh it (`git update-index --really-refresh`) and re-check.

If the tree is dirty with changes that are not yours, they belong to whoever else has the repo open.
Do **not** stash them and hope — a stashed file can come back dirty mid-rebase, because the process
that wrote it is still running. Either surface it and wait, or take the tree out of the race by
rebasing in a worktree of your own:

```bash
git worktree add .claude/worktrees/<issue>-rebase <branch>
```

`.claude/worktrees/` is gitignored and is the conventional place for these. Remove it when done:
`git worktree remove .claude/worktrees/<issue>-rebase`.

### The rebase

```bash
git -c core.commentChar=';' rebase --autostash origin/main
```

`--autostash` covers your own in-flight edits. Two caveats: it does not protect against a
*concurrent* writer — only a private worktree does — and the pop at the end can itself conflict,
leaving `UU` entries in the tree while the rebase prints `Successfully rebased`. Always finish with
`git status --porcelain` and treat a `U` line as unfinished work, not as noise.

### When it stops

Read the actual output. Never write `for i in $(seq 1 12); do git rebase --continue >/dev/null; done` —
that loop was invented to paper over the empty-message reschedule (§0) and it hides the one line
that says what is wrong. Each stop has one correct answer, and they are not interchangeable:

| Output | Meaning | Do |
|---|---|---|
| `Aborting commit due to empty commit message` | §0 — you dropped `-c core.commentChar` | `git rebase --abort`, redo with the flag |
| `CONFLICT` / `could not apply` | a real conflict | resolve, `git add`, `git -c core.commentChar=';' rebase --continue` |
| `You must edit all merge conflicts…` with no `<<<<<<<` in the file | rerere resolved it, unstaged | review the diff, `git add`, continue |
| `Your local changes … would be overwritten by merge` + `It has been rescheduled` | someone else wrote to the tree | `git rebase --abort`, move to a private worktree, restart |
| `Unable to create '…/.git/index.lock': File exists` + `Another git process seems to be running` | a concurrent git holds the index — a git gui **Rescan** on the host does exactly this | wait for it to finish, `git rebase --abort`, restart; do not delete the lock unless you have confirmed no git is running |
| `cannot rebase: You have unstaged changes` / `Your index contains uncommitted changes` | genuinely dirty tree — a stat-only mismatch does *not* produce this, rebase refreshes past that | `git diff --stat` to see whose changes they are, then commit, stash, or use a worktree |

Each `--continue` should advance the `Rebasing (n/N)` counter. If N does not move, stop and read —
looping harder will not fix it.

## 2. Bring a PR into the workspace

```bash
git fetch https://github.com/hardwood-hq/hardwood.git refs/pull/<N>/head:pr-<N> -f
git checkout pr-<N>
git log --format='%h %an <%ae> | %cn <%ce> | %s' main..pr-<N>
```

`-f` matters on a re-fetch: the contributor may have force-pushed since the last fetch, and without
it the update is rejected and you silently review stale code.

Before touching history, establish where it will go back to:

```bash
gh pr view <N> --json headRepositoryOwner,headRepository,headRefName,isCrossRepository,maintainerCanModify
```

- `isCrossRepository: false` → push to `origin <headRefName>`.
- `isCrossRepository: true` + `maintainerCanModify: true` → push to the contributor's fork; add the
  remote if missing:
  ```bash
  git remote add <login> https://github.com/<login>/hardwood.git 2>/dev/null || true
  git push --force-with-lease=<headRefName>:<remote-sha> <login> HEAD:<headRefName>
  ```
  On a fork remote the local remote-tracking ref is often absent or stale, so bare
  `--force-with-lease` either refuses or — worse — has nothing to compare against. Pass the
  **explicit** `=<branch>:<sha>` form, with `<remote-sha>` read from `gh pr view <N> --json headRefOid`.
- `maintainerCanModify: false` → you cannot push. Post the patch as a PR comment instead and say so.

## 3. Message rules (apply to every commit you create)

- First line begins with the issue key: `#853 Separate the S3 network fetcher from range-cache dispatch`.
- Body explains **why**, not what. A short paragraph, not a bullet list. Drop ephemeral detail —
  slips fixed within the branch, interim states, how the change was developed.
- Always pass multi-paragraph messages via `-F -` and a heredoc. `-m` mangles blank lines:
  ```bash
  git commit -q --amend -F - <<'MSG'
  #855 Document the S3 range-backing options

  `rangeBacking` and `tempDir` have been public builder options since #373, but a
  user reading the S3 reference had no way to learn that repeat reads can be
  served from a local mmap-backed cache.
  MSG
  ```

## 4. Amend your own top commit

```bash
git add -A && git commit -q --amend -F - <<'MSG'
#<issue> <subject>

<why>
MSG
```

`--amend --no-edit` when only the tree changes — both forms are `#`-safe. A bare `--amend` is not.
This is safe only when **you** authored the commit — amend rewrites the committer, and on a
contributor's commit that erases their committership.

## 5. Reword or squash your own commits deeper in the branch

Use `rebase -i` with scripted editors. This preserves author identity (rebase rewrites the
committer, which is correct for your own commits).

**Reword the Nth todo line** — line 1 of the todo is the oldest commit:

```bash
cat > /tmp/msg.txt <<'MSG'
#<issue> <subject>

<why>
MSG
GIT_SEQUENCE_EDITOR="sed -i '2s/^pick/reword/'" GIT_EDITOR="cp /tmp/msg.txt" \
  git -c core.commentChar=';' rebase -i <base-sha>
```

**Squash everything onto the first commit:**

```bash
GIT_SEQUENCE_EDITOR="sed -i '2,\$s/^pick/fixup/'" GIT_EDITOR=true \
  git -c core.commentChar=';' rebase -i <base-sha>
```

**Autosquash** — when the fixups were made with `git commit --fixup :/<subject-substring>`:

```bash
GIT_SEQUENCE_EDITOR=true git -c core.commentChar=';' rebase -i --autosquash <base-sha>
```

For the simple case — all of your own commits into one, contributor commits staying below `<base>`
— `reset --soft` is still shorter and cannot go wrong, since it never touches the working tree:

```bash
git reset --soft <base-sha> && git commit -q -F - <<'MSG'
#<issue> <subject>

<why>
MSG
```

Selective — collapse N commits, then restage only part of the tree as the first of two:

```bash
git reset --soft HEAD~<N> && git restore --staged .
git add <paths-for-commit-1> && git commit -q -F - <<'MSG'
…
MSG
git add -A && git commit -q -F - <<'MSG'
…
MSG
```

## 6. Reword a *contributor's* commit, preserving author *and* committer

`rebase -i` cannot do this: every commit it replays gets **you** as committer. Only `commit-tree`
rebuilds the commit under the original identities.

The recurring case: a contributor's commit is missing the issue key, and your own commits sit on top
of it.

```bash
set -e
BASE=<contributor-sha>                       # the commit to reword
PARENT=$(git rev-parse $BASE^)
TREE=$(git rev-parse $BASE^{tree})

AN=$(git show -s --format=%an $BASE); AE=$(git show -s --format=%ae $BASE); AD=$(git show -s --format=%aI $BASE)
CN=$(git show -s --format=%cn $BASE); CE=$(git show -s --format=%ce $BASE); CD=$(git show -s --format=%cI $BASE)

# Byte-exact body, issue key prefixed. `git cat-file commit` avoids the trailing-newline
# normalisation `git log --format=%B` applies.
{ printf '#<issue> '; git cat-file commit $BASE | sed '1,/^$/d'; } > /tmp/msg.txt

NEW=$(GIT_AUTHOR_NAME="$AN" GIT_AUTHOR_EMAIL="$AE" GIT_AUTHOR_DATE="$AD" \
      GIT_COMMITTER_NAME="$CN" GIT_COMMITTER_EMAIL="$CE" GIT_COMMITTER_DATE="$CD" \
      git commit-tree "$TREE" -p "$PARENT" -F /tmp/msg.txt)

git -c core.commentChar=';' rebase --onto "$NEW" $BASE   # replays your commits; you become their committer
git log --format='%h  %an <%ae>  |  %cn <%ce>  |  %s' -3
```

Set the `GIT_*` env vars **only** on the `commit-tree` call. Exporting them would stamp the
contributor's identity onto your own replayed commits too.

If the contributor's commit is the only one and you have exactly one commit of your own,
`git reset --hard "$NEW" && git cherry-pick <your-sha>` is the shorter equivalent.

## 7. Verify before pushing

A reword, squash or rebase must change exactly what you intended and nothing else. Prove it:

```bash
OLD=$(cat /tmp/old-tip)

# 1. Every subject still starts with '#'. This is the §0 check — run it every time.
git log --format='%s' $(git merge-base $OLD origin/main)..HEAD | grep -vn '^#' && echo 'SUBJECT STRIPPED' || echo 'subjects ok'

# 2. Commit-by-commit equivalence across the rebase. '=' rows mean an untouched patch.
git range-diff $(git merge-base $OLD origin/main)..$OLD origin/main..HEAD

# 3. For a reword/squash-only rewrite the tree must be byte-identical.
git diff $OLD HEAD --stat        # must be empty

# 4. Identity, when a contributor commit was involved — invisible in `git log --oneline`.
git log --format='%h  %an <%ae>  |  %cn <%ce>  |  %s' origin/main..HEAD
```

`range-diff` is the one that catches a rebase that quietly dropped or mangled a commit; a `git log`
comparison will not.

Then, once asked:

```bash
git push --force-with-lease origin <branch>
```

Never force-push `main`. If `--force-with-lease` is rejected, the remote moved: re-fetch, look at
what landed, and reconcile — do not escalate to `--force`.

If a rewrite went wrong at any point, `/tmp/old-tip` from §1 is the way back:
`git reset --hard $(cat /tmp/old-tip)`.
