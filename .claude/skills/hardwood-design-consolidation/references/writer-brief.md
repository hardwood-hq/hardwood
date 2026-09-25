# Brief: write one consolidated design document

You write one design document under `_designs/` of the Hardwood repository (a Java Parquet reader and writer), in the worktree the orchestrator names. Work only there. Write only your target file (and a root file only if you were assigned one). Do not delete, stage or commit anything; do not run Maven.

Read first: the `# Design` section of `CLAUDE.md`, and in `_plans/DESIGN_DOC_CONSOLIDATION.md` the sections "Method", "Target design docs" and the disposition rows of your sources.

## What the document is

- It describes the subsystem as it is in the code today: structure, contracts, invariants, ownership, and the reasoning a maintainer needs to keep an invariant intact.
- It states what would need a design discussion to change. Tuning constants stay out unless they are part of a contract; class internals stay out. Class and method names appear as pointers to where something lives (backticked simple names, with the package where ambiguous).
- Tests: a section may end with one line `Tests: FooTest, BarTest.` naming the test classes (not methods) that cover it; only where a test is outside `core`, add the module in parentheses. An invariant no test enforces ends with "Untested." Verify that a test you name exists and covers the section.
- No status field, phases, steps, file-change lists, benchmark numbers, rejected alternatives, delta words ("previously", "now", "still", "no longer"), history, or PR references. Issue numbers appear only for open known gaps, in a final "Boundaries" section, each verified open with `gh issue view <n> --repo hardwood-hq/hardwood --json state`.
- Prose: plain declarative sentences. No praise, no "not X, but Y" framing except for a likely misreading (at most twice per document), no aphorisms, no rhetorical questions, no announcements ("Two points matter here."). Tables where they compress. One line per paragraph (no hard wraps). Target 200–400 lines; cut whole sections rather than squeezing sentences.
- Opening: `# Title`, then a 2–4 sentence scope paragraph that says what the document covers, what it does not, and links the documents that cover the rest.
- Link sibling documents and already-consolidated ones in `_designs/` by file name; do not restate their content. Link user-facing pages under `docs/content/` for user-facing semantics instead of repeating them.

## Method (mandatory)

Source documents are the source of intent; the code is the source of fact. Start from the contracts and invariants of your sources. For each claim you keep, check the code:

1. Source and code agree: state it.
2. Code differs, and something records the change as deliberate (a later source document, a closed issue, a commit message; search `git log -S<symbol>` and `git log --grep`): state the current behaviour.
3. Code differs, and nothing records it: a **divergence**. Do not resolve it silently. If the difference is clearly harmless, write the code's behaviour; otherwise write the source's intended behaviour. Report it either way.

## Final message

1. The file written and its line count.
2. **Divergences**: for each, the source's claim (quoted), what the code does (`file:line`), what you searched for a deliberate change, your recommendation (fix code / accept code / needs maintainer), and a concrete example: inputs, the call, what happens, what the source claimed.
3. **Untested invariants.**
4. **Dropped content** worth a second look, one line each.
5. **Stale outside the docs**: GitHub issues, JavaDoc, comments, user docs, ROADMAP entries you found wrong (`file:line`, what is wrong). Do not edit them.
6. **Inbound links**: files outside `_designs-legacy/` that reference your sources (`git grep -n '<SOURCE>.md'`), with the section of your document each should point to.
