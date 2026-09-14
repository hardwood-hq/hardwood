---
name: hardwood-address-review
description: Work through a `_reviews/pr-<N>-review.md` findings file against a Hardwood PR — check out the PR branch, make the fixes, surface each Decision with its options + recommendation for the maintainer to pick, tick off addressed items in the review file, and commit (do not push). Use whenever the user says "address review for #<N>", "apply review feedback for PR <N>", "work through the review", or otherwise asks to execute on the items in a review file.
---

# Hardwood address-review

Companion to the `hardwood-review` skill. The review skill produces `_reviews/pr-<N>-review.md`; this skill consumes it — implements the fixes, ticks the checkboxes, and commits.

## Workflow

### 1. Identify the PR

Parse the user's request for a PR number (e.g. "449", `#449`, a PR URL). If they didn't supply one, look for the most recently modified file matching `_reviews/pr-*-review.md` and confirm with the user.

### 2. Sanity checks

Before touching the working tree:

- **Working tree state.** Run `git status --porcelain`.
  - **Clean:** proceed.
  - **Dirty, but the only entries are under `.claude/` or `_reviews/`** (Claude meta-state — session settings, in-flight skill drafts, review files): proceed without asking. These paths don't touch hardwood project source and will travel safely with `git checkout`.
  - **Anything else modified or untracked:** stop and tell the user. Don't risk their in-progress work.
- **Review file exists.** `_reviews/pr-<N>-review.md` must be readable. If missing, suggest running `/hardwood-review <N>` first.
- **Find the issue number.** `gh pr view <N> --json title -q .title` — the PR title should start with `#<issue> …` per the project's commit convention. Extract that number for the commit prefix. If the title doesn't have one, ask the user which issue number to use; don't invent one.

### 3. Check out the PR branch

Preferred: `gh pr checkout <N>`. This creates or updates a local branch tracking the PR.

**Fallback when `gh pr checkout` fails** (typically `Host key verification failed` in sandboxed environments where SSH to GitHub isn't set up): fetch the fork directly via HTTPS.

```bash
gh pr view <N> --json headRepositoryOwner,headRepository,headRefName \
    -q '"https://github.com/" + .headRepositoryOwner.login + "/" + .headRepository.name + ".git " + .headRefName'
# → e.g. https://github.com/muhannd2004/hardwood.git 386-fix-text-cut-off
git fetch <https-url> <head-ref>:pr-<N>
git checkout pr-<N>
```

The HTTPS-fetched branch has no upstream, which is safer for a fork PR — there is no way to accidentally `git push` back to the contributor's fork. The local branch name (`pr-<N>`) also differs from the head ref, so any later push needs an explicit refspec.

Add `isCrossRepository` to that `gh pr view` call, because it decides where a push would go — and most Hardwood PRs are the maintainer's own same-repo branches, not forks:

- `isCrossRepository: false` → `origin` is already `hardwood-hq/hardwood`; the push is `git push origin pr-<N>:<head-ref>`. No remote needs adding.
- `isCrossRepository: true` → the fork is not a configured remote; see the push-target section of the `hardwood-pr-git` skill.

Never assert a remote is missing without checking `git remote -v` first.

After checkout, confirm you're on the PR branch with `git branch --show-current` so a later git command can't accidentally commit to `main`.

### 4. Read the review file

Read `_reviews/pr-<N>-review.md` and partition the items:

- **Already addressed** (`[x]`) — skip.
- **Open Decisions** — surface to the user first (next step).
- **Open findings** under `Blockers`, `API semantics`, `Implementation semantics`, `Documentation`, `Tests` — work items.
- **Open Nits** — work items, but ask the user up front whether to batch them all or skip; nits are often left for a follow-up.

### 5. Resolve Decisions before code changes

For each open Decision, use `AskUserQuestion` with:

- **Question:** the `**Q:**` line from the file.
- **Options:** the A/B/C bullets verbatim.
- Surface the `**Rec:**` line in the question prose so the user sees it.

After the user picks:

- In the review file, change `- [ ] **A.** …` (or whichever was picked) to `- [x] **A.** …`. Leave the others `[ ]`.
- Record the answer in the working set so later steps know which branch to implement.

Decisions can have cross-cutting consequences (e.g. option A merges two helpers and resolves three findings). Note any findings under tier sections that the decision will subsume — those should get ticked when the decision's implementation lands, not separately.

**When a tier finding is resolved as a side-effect of a Decision**, tick the finding *and* append a short italicised parenthetical naming the responsible Decision:

```markdown
- [x] `wrapValue` keeps a `value.split("\n", -1)` outer loop … _(Resolved via Decision A: extracted to `Strings.wordWrap`; the `\n` branch is now part of the shared helper's contract.)_
```

The annotation preserves the audit trail — a future reader sees both that the item was addressed and that the fix came from the Decision rather than a separate change. Keep it to one sentence; the Decision section above carries the detail.

### 6. Make the code changes

Walk the open findings in pyramid order (Blockers → API → Impl → Docs → Tests → Nits). For each:

- Read the relevant file(s) to ground the change in the actual code, not the review's description (the review may have stale line numbers).
- Make the smallest change that addresses the finding. Don't expand scope.
- If the finding is a **bug report** (rare in review feedback, but happens — e.g. "narrow-terminal case clips the modal"), CLAUDE.md says: write the failing test first, then fix it. Apply the same here.
- After the change, tick the item in the review file (`[ ]` → `[x]`).

If a finding turns out to be wrong (e.g. the reviewer misread the code), do **not** silently tick it. Add a one-line note next to the item explaining why, and surface to the user at the end of the run — they decide whether to keep, edit, or drop.

### 7. Verify

Run the build:

```bash
timeout 180 ./mvnw verify
```

Per CLAUDE.md: 180s timeout to detect deadlocks early; `verify` covers compile + test. If the change touched `hardwood-core`, install it first (`timeout 180 ./mvnw -pl core install -DskipTests`) before running module-scoped tests.

If the build fails, stop and report the failure. Do not commit. The user decides whether to fix forward in this session or hand back.

### 8. Commit

Single commit, no push.

```bash
git commit -m "#<issue> Addressing findings from code review"
```

`<issue>` is the number extracted in step 2 (typically the issue the PR closes, not the PR number itself).

Body: optional. If multiple distinct findings were addressed, a short paragraph in the body listing the *whys* (not the whats) is helpful. Don't restate the diff.

**Do not push.** The user reviews the commit locally and pushes themselves.

### 9. Hand back

Report:

- The commit SHA.
- A 3-bullet summary: decisions picked, findings addressed, anything skipped or contested.
- If any findings were flagged as wrong-by-reviewer, list them so the user can adjudicate.
- A reminder that nothing was pushed, giving the exact command from step 3 rather than a generic `git push` — e.g. "Not pushed. `git push origin pr-1054:686-stale-quarkus-references` when you're ready."

## When NOT to use this skill

- The review file doesn't exist yet — run `/hardwood-review <N>` first.
- The working tree has uncommitted changes to hardwood project source (see step 2 carve-out for the `.claude/` / `_reviews/` meta-state exception).
- The user wants to push as part of the action — out of scope; this skill stops at the commit.

## Output discipline

- Tick checkboxes only after the corresponding change is in the working tree.
- Don't reorder, reformat, or rephrase items in the review file beyond ticking and the occasional "(see decision above)" note. The file is the maintainer's record; minimal edits preserve diff readability.
- Each user-facing question (Decisions) is a single `AskUserQuestion` call with the verbatim Q and options. Don't paraphrase — the reviewer already did the work of phrasing them well.
- If a finding spans multiple files (e.g. "consolidate two `wrapValue` helpers into `Strings`"), describe the planned change in one or two sentences to the user before doing it, especially when the option requires touching code outside the PR's existing diff.
