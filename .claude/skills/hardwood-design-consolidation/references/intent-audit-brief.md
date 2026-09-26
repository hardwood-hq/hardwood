# Brief: check that no documented intent was lost in one consolidated design doc

Hardwood's #1290 consolidation replaces legacy design docs with current ones. The orchestrator names your target, its sources and the area worktree. Read only; do not edit, commit or push anything. Read a source with `git fetch -q origin` and `git show origin/main:<path>` (sources exist on main until the area PR merges); read the target, its sibling targets and every other current document (`_designs/*.md`, `docs/content/**`, `TESTING.md`, `PERFORMANCE.md`, `_plans/*.md`) in the area worktree. Check code on origin/main, or on the code-fix branches the orchestrator names.

Rules the consolidation follows (from CLAUDE.md `# Design` and `.claude/skills/hardwood-design-consolidation/references/writer-brief.md`): a design doc states current contracts, invariants, ownership and the reasoning needed to keep an invariant; it drops process, status, phases, file lists, benchmark numbers, rejected alternatives, history and tuning constants; a fact has one home (a sibling or user doc may hold it). Legitimate drops therefore exist. Your job is to find the illegitimate ones.

For each source, walk it section by section and list every claim of these kinds: contract (what an API/component promises), invariant, ownership/lifecycle rule, failure/edge-case behaviour, the *reason* behind a design choice a maintainer needs to keep it, a known limitation or open gap. For each, classify:
- LANDED: where (file#section).
- LANDED ELSEWHERE: in which other doc (sibling, already-consolidated doc, user doc, plan).
- DROPPED-OK: which rule allows the drop (stale per code; process, history, benchmark, constant, rejected alternative).
- LOST: still true in the code (verify at file:line) and still something a maintainer needs, but in no current doc. These are your findings.
- CHANGED: the new doc states it differently in a way that alters the meaning — say whether the new wording matches the code.

Report: the LOST and CHANGED items only, each with the source quote, the code evidence (file:line on origin/main), a proposed sentence and the target section it belongs in (keep sentences plain, one line per paragraph style, no delta words). Then a one-line count of LANDED / LANDED ELSEWHERE / DROPPED-OK. Keep the report under ~80 lines.
