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

### 6. Prune to signal

Before writing the file, walk every candidate finding and apply the **inclusion bar**:

> A finding describes something that is *wrong*: code that breaks, will regress under a plausible future change, violates a stated CLAUDE.md / design rule, or has a missing safety property the project relies on elsewhere.

Cut anything that fails this bar. Common cuts:

- **Non-findings.** "No public API change here, theme usage looks fine." If a pyramid tier has no findings, **omit the section entirely** — do not write a section header followed by a reassurance bullet.
- **Taste calls without a stated rule.** "Consider renaming `kv` to `kvLines`", "prefer `Optional` over null here." If CLAUDE.md or a design doc doesn't mandate it and the existing code is correct, drop it.
- **Micro-optimisations on cold paths.** Render-tier UI code, one-shot init, test fixtures. Flag allocations / branches only on paths that run thousands of times per second or are documented hot.
- **Test-style polish when the test is correct.** A brittle-but-correct assertion is not a finding; a *missing* test for a real edge case is. **Worked cut:** a test that asserts exact substrings depending on a width / indent constant. The test passes today. "It will break if anyone touches the constant" is not a defect, it's a property of every assertion. Don't flag. (A *missing* test that would catch the height-overflow bug at a different breakpoint *is* a defect — that survives.)
- **PR-process / description hygiene.** "The 'docs updated' checkbox is unchecked, please confirm." That belongs in a PR comment, not the review file.

Self-edit prompt for each candidate: *"If the author shipped this exact line tomorrow, what specifically breaks, regresses, or fails a CI check / convention?"* If you can't answer concretely in one sentence, delete the finding.

**Lift decisions out of the findings.** Some surviving items aren't pure fix-it work — they're forks the maintainer needs to answer. Move these to the `## Decisions` section instead of leaving them mixed in with defects. The tell-tale shapes:

- **"Either X or Y" phrasing** ("drop the dead branch or move the helper", "document the `-1` or remove it").
- **Trade-offs the maintainer owns** ("keep the per-class duplication for JIT specialisation or extract a shared helper" — project-wide pattern, not a local fix).
- **Sub-pattern changes** ("constructor-inject the filter or keep the setter" — both work; the choice is policy).

Decisions should be **selectable** — the maintainer can tick the option they pick and the file becomes a record of the answer. Format each as:

```markdown
- **Q:** <question, one line>
  - [ ] **A.** <option, one clause — say what *changes* if this is picked>
  - [ ] **B.** <option>
  - [ ] **C.** <"keep as-is" — include this when the question implies a change, so disagreement has an explicit checkbox>
  - **Rec:** <A/B/C> — <one-line justification>
```

Always include a `Rec:` line, even if it's "no strong opinion — A reads cleaner". Posing a decision without doing the analysis homework just defers the work. Two options are typical; three when "keep as-is" is plausible; rarely more.

Worked example:

```markdown
- **Q:** The `split("\n", -1)` branch in `wrapValue` is unreachable — no description contains a newline. Drop it or move the helper somewhere multi-line is exercised?
  - [ ] **A.** Drop the branch; `wrapValue` stays in `HelpOverlay`.
  - [ ] **B.** Move `wrapValue` to a shared `Strings` helper and exercise the multi-line path there (also resolves the duplication with `DataPreviewScreen`).
  - [ ] **C.** Keep as-is.
  - **Rec:** B — collapses two findings into one fix.
```

Items that are unambiguous fixes (one right answer, just hasn't been done) stay in their tier section, not in Decisions.

**A decision must have at least two defensible branches.** Some findings carry "either X or Y" phrasing but only one branch is a real fix — the other is just "document the magic / explain the constant", which is the lazy non-fix. These are **not** decisions. They stay under their tier (usually Implementation) as a defect.

**Worked example:**

> `wrapValue(description, Math.max(1, descBudget - 1))` — the `-1` is unexplained. Either drop it or add a one-liner why.

The two branches are *fix it* vs *paper over it with a comment*. Only the first is a real answer. **Stays under Implementation semantics as a defect**, not a decision.

Compare to a real decision:

> Drop the dead `split("\n")` branch, or move `wrapValue` to a shared helper where the multi-line case is actually exercised?

Both branches are defensible end-states — one shrinks the surface, the other widens reuse. **Goes to Decisions.**

If lifting a finding to Decisions would make the underlying defect disappear (because the question is "should we fix this or not"), keep the defect under its tier and only raise a Decision for genuine forks in *how* to fix.

Better to ship a short review with 5 real defects than a long one where the maintainer has to filter the signal out.

### 7. Write the findings file

Write to `_reviews/pr-<N>-review.md` (or `_reviews/branch-<name>-review.md` for non-PR runs). The `_reviews/` directory already exists; do not write findings to the repo root. Format:

```markdown
# PR #<N> — <title> — Review actions

<https://github.com/hardwood-hq/hardwood/pull/<N>>

## Summary

**What:** <1–2 sentences. The actual change, in your own words. Not a copy of the PR description — what the diff does, structurally.>

**Why:** <1 sentence. The motivator, from the PR description / linked issue / design doc. If you can't tell, say "Motivator not stated in the PR or linked issue." — don't invent one.>

**Assessment:** <1–2 sentences. Headline verdict: ready to merge / ready with nits / needs work / not ready, and the single most load-bearing reason. The detail lives in the sections below; this is the elevator pitch.>

## Decisions
- **Q:** <question or fork — one line>
  - [ ] **A.** <option 1 — what changes if you pick this>
  - [ ] **B.** <option 2>
  - [ ] **C.** <option 3 — typically "keep as-is" if a change is on the table>
  - **Rec:** <A/B/C> — <one-line justification>

## Blockers
- [ ] <item> — one-sentence why

## API semantics
- [ ] ...

## Implementation semantics
- [ ] ...

## Documentation
- [ ] ...

## Tests
- [ ] ...

## Nits
- [ ] ...
```

**Emit only sections that have at least one entry.** A pyramid tier (or `Decisions` / `Blockers`) with nothing in it gets no section header — silence is the signal. The template above lists all seven possible sections; a real review usually has two or three.

Use `[ ]` not `[x]` — the maintainer checks items off as they're addressed (per CLAUDE.md "Code Reviews" section).

### 8. Hand back a short summary

After writing the file, give the user a 3–5 sentence summary: what the PR does, the highest-priority finding(s), and the path to the findings file. Do not repeat the whole list inline — they can read the file.

## When NOT to use this skill

- Reviewing a PR in a different repo. The checklist is hardwood-specific.
- Asked to apply a fix or implement a feature. This skill produces findings, not edits.
- Asked for a one-line opinion ("does this look OK?"). Just answer.

## Output discipline

- Findings file uses `[ ]` checkboxes per CLAUDE.md.
- Group by pyramid tier (see template). Sections with no findings are omitted entirely — do not write a section header followed by "looks fine" or a non-finding bullet.
- Each item is one sentence stating the issue and one sentence (or clause) on why it matters / what fails if ignored. No multi-paragraph justifications.
- Reference specific files / line ranges where useful. Don't fabricate line numbers from memory — if you're not sure, drop the number.
- If a finding is judgment-dependent (e.g. "consider A/B-ing this duplication"), say so. Don't dress up an opinion as a defect.
- A short review with five real defects beats a long one where the maintainer has to filter the signal out. When you find yourself reaching for noise to "look thorough", stop.
