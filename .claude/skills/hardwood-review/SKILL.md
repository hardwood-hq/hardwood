---
name: hardwood-review
description: Review a Hardwood PR, local branch, or staged diff against the project's CLAUDE.md conventions and recurring failure modes. Use whenever the user asks to "review", "look over", "check", "audit", or "assess" a PR number, a branch, or pending changes in the hardwood-hq/hardwood repo. Also use when the user pastes a PR URL or says they want feedback before merging. Produces a checkbox markdown file with findings grouped by priority.
---

# Hardwood code review

Project-specific code review. Encodes the conventions in `/workspace/CLAUDE.md` and the recurring failure modes the maintainer has surfaced in past reviews, so the review covers them by construction instead of by memory.

## Priority frame: the Code Review Pyramid

Review effort is weighted by **how expensive the issue is to fix after merge**. Width = weight: the wide base is the load-bearing tier, the narrow apex is the least important.

```
       ┌────────────────────┐
       │     Code style     │   ← apex. Automate it; don't burn review cycles here.
      ┌┴────────────────────┴┐
      │        Tests         │
     ┌┴──────────────────────┴┐
     │     Documentation      │   ← focus your review effort
    ┌┴────────────────────────┴┐
    │     Implementation       │   ← focus your review effort
   ┌┴──────────────────────────┴┐
   │       API semantics        │   ← base. Hardest to change later — spend the most thought here.
   └────────────────────────────┘
```

When writing the findings file, sort by pyramid tier, not by checklist section. A small API-shape concern outranks a big style nit. Style items (no `var`, JavaDoc syntax, formatting) belong in a "Nits" footer — flag them so the author can fix in passing, but don't elevate them to "Blockers".

Anchor reference: https://www.morling.dev/images/code_review_pyramid.svg

## Workflow

### 1. Identify the target

Parse the user's request for one of:
- **PR number** (e.g. "418") → `gh pr view <n>` and `gh pr diff <n>`
- **PR URL** → extract number, same as above
- **Branch name** → `git diff main...<branch>` (or whatever the main branch is)
- **No argument** → `git diff` (working tree) plus `git diff --staged`; if both empty, `gh pr list` and ask which one

For PR diffs, persist large output to a file and read it in chunks rather than letting it land in the conversation as one blob. The diff for a non-trivial PR can run 5k+ lines.

### 2. Read the design context

If the PR touches a new design area, look in `_designs/` for a matching markdown file. Hardwood requires non-trivial changes to land a design doc; if one is expected but missing, that's a finding. If one exists, skim it before reading code so the review can flag drift between intent and implementation.

### 3. Run the checklist

Walk every item in [references/checklist.md](references/checklist.md). Each item has a brief rationale and the failure mode it catches. Skip items that don't apply to the diff (e.g. Dive TUI rules on a core-only change). Do not skip items just because they look unlikely — the checklist exists because each one has been missed at least once.

### 4. Cross-check tests

For every new behaviour or matcher, check that test coverage is breadth-first across the type/op axes, not just the one the author exercised. Common gap: an oracle test that proves parity for two types when the implementation supports six. List explicitly which `(type, op)` pairs are *not* covered.

### 5. Check doc/code drift

When a PR adds a design doc, JavaDoc, or user-facing `docs/content/*.md`, read at least the load-bearing claims (gate descriptions, eligibility rules, supported types) and grep the code to confirm the wording matches. Mismatches between "≥ 2 distinct columns" in the doc and `leaves.size() < 2` in the code are the dominant doc-bug class.

### 6. Write the findings file

Write to `_reviews/pr-<N>-review.md` (or `_reviews/branch-<name>-review.md` for non-PR runs). The `_reviews/` directory already exists; do not write findings to the repo root. Format:

```markdown
# PR #<N> — <title> — Review actions

<https://github.com/hardwood-hq/hardwood/pull/<N>>

## Summary

**What:** <1–2 sentences. The actual change, in your own words. Not a copy of the PR description — what the diff does, structurally.>

**Why:** <1 sentence. The motivator, from the PR description / linked issue / design doc. If you can't tell, say "Motivator not stated in the PR or linked issue." — don't invent one.>

**Assessment:** <1–2 sentences. Headline verdict: ready to merge / ready with nits / needs work / not ready, and the single most load-bearing reason. The detail lives in the sections below; this is the elevator pitch.>

## Blockers
Issues that must be addressed before merge. Pulls from any pyramid tier.
- [ ] <item> — one-sentence why

## API semantics
Public surface, breaking changes (classes / config / metrics / log formats), one-way-to-do-one-thing, least-surprise, internal-vs-public split.
- [ ] ...

## Implementation semantics
Correctness, concurrency, error handling, performance, observability, dependency weight + license.
- [ ] ...

## Documentation
Design docs, JavaDoc on public API, `docs/content/*.md` user docs, doc/code drift.
- [ ] ...

## Tests
Coverage breadth across (type, op) axes, boundary values, NFR / performance tests, oracle equivalence.
- [ ] ...

## Nits
Style and convention items. Author can address in passing; do not block on these.
- [ ] ...
```

Use `[ ]` not `[x]` — the maintainer checks items off as they're addressed (per CLAUDE.md "Code Reviews" section).

### 7. Hand back a short summary

After writing the file, give the user a 3–5 sentence summary: what the PR does, the highest-priority finding(s), and the path to the findings file. Do not repeat the whole list inline — they can read the file.

## When NOT to use this skill

- Reviewing a PR in a different repo. The checklist is hardwood-specific.
- Asked to apply a fix or implement a feature. This skill produces findings, not edits.
- Asked for a one-line opinion ("does this look OK?"). Just answer.

## Output discipline

- Findings file uses `[ ]` checkboxes per CLAUDE.md.
- Group by priority, not by file or by checklist section. The maintainer wants to know what to address first.
- Each item is one sentence stating the issue and one sentence (or clause) on why it matters / what fails if ignored. No multi-paragraph justifications in the file.
- Reference specific files / line ranges where useful. Don't fabricate line numbers from memory — if you're not sure, drop the number.
- If a finding is judgment-dependent (e.g. "consider A/B-ing this duplication"), say so. Don't dress up an opinion as a defect.
