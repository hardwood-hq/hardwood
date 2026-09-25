---
name: hardwood-address-review
description: Work through a `_reviews/pr-<N>-review.md` findings file against a Hardwood PR or branch — check it out in its own worktree, make the fixes, surface each Decision with its options + recommendation for the maintainer to pick, tick off addressed items in the review file, and commit the fixes as one separate commit (do not push). Runs automatically at the end of every `hardwood-review` run; also use whenever the user says "address review for #<N>", "apply review feedback for PR <N>", "work through the review", or otherwise asks to execute on the items in a review file.
---

# Hardwood address-review

Companion to the `hardwood-review` skill. The review skill produces `_reviews/pr-<N>-review.md` and then invokes this skill, which consumes the file — implements the fixes, ticks the checkboxes, and commits them as one commit separate from the reviewed work.

## Workflow

### 1. Identify the target

When invoked by `hardwood-review`, the target is handed over: a PR number, a branch name, or `working-tree`. The review file is `_reviews/pr-<N>-review.md` for a PR and `_reviews/branch-<name>-review.md` otherwise.

When the user invokes this skill directly, parse the request for a PR number (e.g. "449", `#449`, a PR URL). If they didn't supply one, look for the most recently modified file matching `_reviews/*-review.md` and confirm with the user.

### 2. Sanity checks

- **Review file exists.** It lives in the main checkout's `_reviews/` (gitignored), not in the worktree created below; keep editing it there. If missing, run `hardwood-review` first.
- **Find the issue number.** For a PR: `gh pr view <N> --json title -q .title` — the title should start with `#<issue> …`. For a branch: the branch name's leading number, or the subject prefix of its commits (`git log main..<branch> --format=%s`). If neither yields one, ask the user; don't invent one.

### 3. Get a worktree on the target

Per CLAUDE.md, commits go into a worktree of their own under `.claude/worktrees/`, never into `/workspace`, which stays on `main` and may carry other sessions' uncommitted files. The state of `/workspace`'s working tree is therefore no reason to stop.

First run `git worktree list`. If a worktree already has the branch checked out, work there, provided its `git status --porcelain` lists nothing outside `.claude/` and `_reviews/`; if it does, stop and tell the user rather than mixing your fixes into someone's in-progress work.

Otherwise create one. For a PR:

```bash
gh pr view <N> --json headRefName,isCrossRepository,headRepositoryOwner,headRepository
git fetch https://github.com/hardwood-hq/hardwood.git pull/<N>/head
git worktree add -b <branch> .claude/worktrees/<branch> FETCH_HEAD
```

If `<branch>` already exists locally without a worktree, drop `-b` and check it out as is when it points at `FETCH_HEAD`; if it has diverged, ask the user.

`<branch>` is the head ref for a same-repo PR (`isCrossRepository: false`), so the push is `git push origin <branch>`. For a fork PR use `pr-<N>`: the local branch then has no upstream and differs from the head ref, so nothing can be pushed back to the contributor's fork by accident; see the push-target section of the `hardwood-pr-git` skill. Fetch over HTTPS as shown; `gh pr checkout` goes over SSH, which is not set up in this container.

For a local branch: `git worktree add .claude/worktrees/<branch> <branch>`.

For `working-tree`: the reviewed changes are uncommitted, so there is no reviewed commit to put a separate fix commit on top of. Make the fixes in place and do not commit; say so in the hand-back.

Run every later step inside the worktree, and confirm with `git branch --show-current` that it is the expected branch.

### 4. Read the review file

Read `_reviews/pr-<N>-review.md` and partition the items:

- **Already addressed** (`[x]`) — skip.
- **Open Premise items** — surface to the user before anything else. A premise that does not hold can make the PR, or part of it, moot; don't start code changes until the user has said how to proceed.
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

Run the build in the worktree:

```bash
timeout 180 ./mvnw verify -DskipITs
```

Per CLAUDE.md: 180s timeout to detect deadlocks early. Run the full `./mvnw verify` instead when the fixes touch what the integration tests cover (the S3 read path, the CLI's S3 commands, parquet-java compatibility, the packaged JAR). If the change touched `hardwood-core`, install it first (`timeout 180 ./mvnw -pl core install -DskipTests`) before running module-scoped tests.

If the build fails, stop and report the failure. Do not commit. The user decides whether to fix forward in this session or hand back.

### 8. Commit

One commit holding all the fixes, separate from the reviewed commits, no push. Stage the touched paths explicitly (never `git add -A`); the review file is gitignored and stays out.

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
- The worktree path, and a reminder that nothing was pushed, giving the exact command from step 3 rather than a generic `git push` — e.g. "Not pushed. `git push origin 686-stale-quarkus-references` from `.claude/worktrees/686-stale-quarkus-references` when you're ready." Remove the worktree once the branch is pushed.

## When NOT to use this skill

- The review file doesn't exist yet — run `/hardwood-review <N>` first.
- The user wants to push as part of the action — out of scope; this skill stops at the commit.

## Output discipline

- Tick checkboxes only after the corresponding change is in the working tree.
- Don't reorder, reformat, or rephrase items in the review file beyond ticking and the occasional "(see decision above)" note. The file is the maintainer's record; minimal edits preserve diff readability.
- Each user-facing question (Decisions) is a single `AskUserQuestion` call with the verbatim Q and options. Don't paraphrase — the reviewer already did the work of phrasing them well.
- If a finding spans multiple files (e.g. "consolidate two `wrapValue` helpers into `Strings`"), describe the planned change in one or two sentences to the user before doing it, especially when the option requires touching code outside the PR's existing diff.
