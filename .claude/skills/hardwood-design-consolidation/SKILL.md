---
name: hardwood-design-consolidation
description: Consolidate one area of Hardwood's legacy design documents (#1290) into current design documents under _designs/, from survey to merged-ready PR. Use when the user says "next design area", "continue #1290", "consolidate the <area> design docs", or invokes /hardwood-design-consolidation. Run it in a fresh session, one area per session.
---

# Design doc consolidation (#1290), one area per session

The state of the whole effort lives in `_plans/DESIGN_DOC_CONSOLIDATION.md` on `main`: target documents, the disposition of every legacy document, the method, and the Execution checklist of areas. That file is the hand-over point between sessions. Read it first; do not re-derive anything it records.

An area in progress (between drafting and its PR) has a local working file `_reviews/1290-<area>.md` (gitignored), plus `_reviews/1290-<area>-review.md` once step 7 has run. Both live in the main checkout (`/workspace/_reviews/`), not in the area worktree. The working file records sources, targets, branch names, divergences and the maintainer's answers, so a later session can resume. If it exists, resume from it instead of starting over.

Keep this session's context small: subagents read the legacy documents and the code; this session holds their reports, reviews each target once, and talks to the maintainer.

## 1. Pick the area

Take the first unticked area of the Execution checklist, unless the user named one. From the plan, collect: the target rows for that area (file, scope, "Fed by"), the disposition rows of its sources, the rows of "Facts from deleted sources" whose target is in the area, the root files it extends, and the open items of "Code/doc drift found" that fall in the area.

Create `_reviews/1290-<area>.md` with those, a `## Divergences` section and a `## State` line. Create the worktree: `git fetch -q origin && git worktree add -b 1290-<area>-design-docs .claude/worktrees/1290-<area>-design-docs origin/main`.

## 2. Draft the targets

Launch one `general-purpose` agent per target document, in parallel. Each prompt is [references/writer-brief.md](references/writer-brief.md) (tell the agent to read it, with the worktree path substituted) plus:
- the target's file name, scope and suggested outline (derive the outline from the scope and the sources; 5–9 sections);
- its source documents under `_designs-legacy/`, plus its rows of "Facts from deleted sources" (each fact must land in the target; the full source text is at the commit that table names);
- the sibling targets of the area and the already-consolidated documents in `_designs/`, to link rather than duplicate;
- user-facing pages under `docs/content/` that cover the same ground, to link rather than duplicate;
- facts the plan's disposition rows record as stale ("verify, don't trust").

Root-file extensions (`TESTING.md`, `PERFORMANCE.md`, `ARCHITECTURE.md`) are written by the same agents or one extra agent, under the same brief.

## 3. Review the drafts

Read each target once. Check: every section states current behaviour; no process, status, benchmark numbers or delta words; tests named at most once per section; untested invariants marked; links to siblings instead of restated content; the doc would survive a class rename better than its sources did. Fix small issues directly; send larger ones back to the agent with `SendMessage`.

Record each agent's divergences, untested invariants, dropped content and inbound links in the working file.

## 4. Resolve every divergence with the maintainer before any PR

A divergence is a claim of a source document the code contradicts with nothing recording the change as deliberate. Also collect: dropped content that may matter, stale GitHub issues, code or JavaDoc drift found along the way.

Put them to the maintainer with `AskUserQuestion`, at most four per call, each with a recommended option first and a **concrete example** (inputs, the call, what happens today, what the doc claimed). Where a question is abstract, the example is mandatory. Record every answer in the working file.

Apply the answers:
- **Doc wording**: edit the target.
- **Code fix**: file an issue (the maintainer's answer is the agreement), fix it test-first in its own worktree and PR, run the full `./mvnw verify`. A self-contained fix can go to an agent. The design doc states the fixed behaviour, and the doc PR says "merge after #…".
- **Issue housekeeping** (close, edit): only what the maintainer approved.

A PR never carries an open decision. If a question is still open at the end of the session, stop before the PR and leave it in the working file.

## 5. Consolidate

In the area worktree:
1. `python3 .claude/skills/hardwood-design-consolidation/scripts/repoint.py SRC=TARGET ...` for every source of the area (names without `.md`). It checks that every source and target exists, removes the sources from `_designs-legacy/` and rewrites every reference, including code comments and legacy documents. It prints lines where one target is now named twice and links whose `#anchor` pointed into a source; fix those by hand.
2. Apply the root-file extensions and the fixes of adjacent inaccuracies found during the area (ROADMAP entries, stale comments, user-doc sentences). Do not defer a small fix found in the surface being changed.
3. Tick the area in the plan's Execution checklist, and tick any "Code/doc drift found" items it resolved.
4. `python3 .claude/skills/hardwood-design-consolidation/scripts/check.py`: no reference to a removed document remains, relative links resolve (links to targets of later areas are reported, not failed), the area's Java changes touch comments only.
5. `python3 tools/docs-prose-check.py` if `docs/content/` changed. Build only if code changed beyond comments.

## 6. Commit, without pushing

One commit, message starting `#1290 Consolidate the <area> design docs`, body on why (what was stale and scattered, what the new documents cover), `Fixes #…` for issues it closes. Stage explicit paths; verify the branch first. Do not push yet. Record the commit SHA in the working file (`State: committed <sha>, in review`).

## 7. Review by a sub-agent

Launch one `general-purpose` agent in the area worktree with this task:

1. Invoke the `hardwood-review` skill on the local branch (`git diff origin/main...HEAD`), writing the findings to `_reviews/1290-<area>-review.md` instead of a `pr-<N>` file.
2. Address the findings as the `hardwood-address-review` skill does from its step 4 on (partition, fix, tick the checkboxes), skipping its PR identification and checkout: the branch is already checked out and has no PR.
3. Do not ask the maintainer anything directly. Fix every finding that the design documents, the code and `CLAUDE.md` settle. For each Decision (a finding that needs the maintainer), stop before committing it and return: the finding, the options, a recommendation, and a concrete example.
4. Commit the fixes as one commit `#1290 Address review of the <area> design docs`, without pushing, and return a summary: findings fixed (one line each), findings rejected as not applicable (with the reason), and open Decisions.

If Decisions come back, first check each against the project's recorded stances (`CLAUDE.md`, the design documents, your memory); drop or settle one those already answer. Put the rest to the maintainer with `AskUserQuestion` (recommendation first, concrete example included), then send the answers to the same agent with `SendMessage`; it applies them and amends its commit. Repeat until no Decision is open.

## 8. Assess, fold and open the PR

Read the review commit (`git show HEAD`) and the review file. For each change, check that it is correct against the code and consistent with the method and the brief; a reviewer can be wrong too. Revert any hunk you disagree with (`git checkout HEAD~1 -- <path>` for a whole file, or an edit) and amend the review commit, noting why in the working file. Rerun `check.py`.

Fold the review commit into the area commit: `git reset --soft HEAD~2` and recommit with the area message (the fact that a review happened is not history worth keeping). Then push and `gh pr create --label documentation` with a body listing the target documents with a one-line scope each, the method sentence ("written from the intent of its sources and checked against the code; divergences were resolved before this PR"), the adjacent fixes, and "Merge after #…" if code PRs came out of step 4. No decisions section.

Comment once on #1290: `<Area>: #<PR>`, and enter the PR (plus any code PRs it must merge after) in the area's "PR" cell of the "Status" table in #1290's body. Once the PR is merged, set the area's row to ✅ (`gh issue view 1290 --json body`, edit, `gh issue edit 1290 --body-file`). Delete `_reviews/1290-<area>.md` and `_reviews/1290-<area>-review.md`, remove the worktree, and tell the user the area is ready for review. Suggest a fresh `claude` session for the next area.

## Last area

When the final area is done: add the `ARCHITECTURE.md` pointers, delete `_designs-legacy/` (by then only its README), `_plans/DESIGN_DOC_CONSOLIDATION.md`, this skill, and the `_designs-legacy` sentences in `CLAUDE.md` and the review checklist; close #1290.
