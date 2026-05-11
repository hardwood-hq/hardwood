# Hardwood review checklist

Each item has a **rule**, a **why** (failure mode it catches), and a **how to check** (concrete grep or read). Apply only those that touch the diff.

Organized along the **Code Review Pyramid** — categories are roughly ordered from highest-leverage (hardest to fix later) to lowest. When reporting findings, weight upper-tier hits more heavily than lower-tier ones.

| Section | Pyramid tier |
|---|---|
| A. Implementation semantics | Implementation Semantics |
| B. API semantics | API Semantics (top — heaviest weight) |
| C. Design discipline | (meta — supports upper tiers) |
| D. Testing | Tests |
| E. Documentation | Documentation |
| F. Coding conventions (nits) | Code Style (automate where possible) |
| G. Maven / build | Code Style / project hygiene |
| H. Dive TUI | (cross-cutting on cli/dive changes) |
| I. Commit messages | (meta — affects history hygiene) |

---

## A. Implementation semantics

### A1. Fail-early invariants
- **Rule:** Inputs are validated, misuse throws, silent fallbacks are not acceptable.
- **Why:** CLAUDE.md treats silent failures as never an option. Defensive `default -> null` arms in switches, swallowed exceptions, and "return null on unexpected shape" all violate this.
- **How:** Search the diff for `default ->`, `catch (`, `return null`. For each, ask: can this branch ever fire in production? If no, it's dead — flip to `throw new IllegalStateException`. If yes, the caller must handle it explicitly, not blindly.

### A2. Unsafe primitive downcasts
- **Rule:** Never narrow `long → int` (etc.) without `Math.toIntExact` or an explicit range check.
- **Why:** A 5 GB Parquet file with a value count above `Integer.MAX_VALUE` silently truncates.
- **How:** Grep `\(int\)` and `\(short\)` in the diff. Each cast must be justified by a bounds invariant or guarded by `Math.toIntExact`.

### A3. NULL semantics in predicate / filter / aggregation code
- **Rule:** Three-valued logic must round-trip. `NULL ∧ false` is `false`; `NULL ∨ true` is `true`; bitmap encodings that conflate "no match" with "null" cannot represent OR correctly.
- **Why:** AND-only bitmap intersect masks the bug; OR or `notNull` follow-ups expose it. Easier to flag at design time than after.
- **How:** If the diff touches matching/filtering, look for an explicit statement of how NULL is represented and whether the operations applied (AND/OR/NOT) preserve it.

### A4. Concurrency: happens-before and field finality
- **Rule:** Fields read by a thread other than the writer must be `final` (set in constructor) or `volatile`, or there must be a documented happens-before edge (e.g. `Thread.start()`, `BlockingQueue`).
- **Why:** Setters called "before start()" are an unenforced contract; one refactor away from a JMM bug.
- **How:** For any new setter or non-final mutable field touched by a drain/worker/executor, ask whether constructor injection would work. Prefer it.

### A5. Iterator contracts
- **Rule:** `next()` without prior `hasNext()` must throw or assert, not silently return a poisoned value.
- **Why:** Drain-side iteration in `FlatRowReader` used a `pendingRowIndex = -1` sentinel that became `rowIndex = -1` if invoked out of order — silent index error.
- **How:** New `RowReader` paths: read both `hasNext()` and `next()` and trace what happens when only one is called.

### A6. Observability
- **Rule:** Code on hot paths or failure paths should be observable. Exceptions should carry enough context (file name, row index, column name) to diagnose without a debugger.
- **Why:** Hardwood already has the "include file name in all exceptions" pattern (#90). New code that throws bare `IllegalStateException("bad value")` regresses it.
- **How:** Read new `throw` sites. Each one's message should answer "what was the input that triggered this?" — and at the `ExceptionContext` boundary, include file/page/row context.

### A7. Dependency weight + license
- **Rule:** New runtime dependencies must pull their weight. License must be compatible with Apache 2.0.
- **Why:** Every dep is a transitive supply-chain risk and a compatibility constraint.
- **How:** Diff for `<dependency>` additions in any `pom.xml`. For each: is it used in more than one place? Is it really shorter/clearer than rolling it? Check the license on Maven Central.

### A8. Performance regressions
- **Rule:** Hot-path code changes need before/after numbers, not just a "should be faster" claim. NFR-style perf tests are part of the test gate.
- **Why:** Easy to regress on `nextSetBit` density or branch prediction by adding an innocent-looking check.
- **How:** PR description claims performance improvement → look for end-to-end and JMH numbers in `performance-testing/`. PR description silent on perf but touches `core/src/main/java/dev/hardwood/internal/reader/` or `internal/predicate/` → ask for them.

---

## B. API semantics

### B1. Internal vs public split
- **Rule:** Anything not strictly user-facing belongs in `dev.hardwood.internal.*`. Public API additions require `docs/content/` updates.
- **Why:** CLAUDE.md is explicit. Public surface is hard to shrink later.
- **How:** Look at every `public` class/method added outside `internal.*`. Ask: does an external library user need this? If "maybe", default to internal.

### B2. Smallest workable surface
- **Rule:** API as small as possible, as large as needed. Don't expose helpers, builders, or overloads that aren't required by a real caller.
- **Why:** Once public, removal is a breaking change.
- **How:** For each new public method, find the caller(s) that need it. If the only caller is a test, it's probably internal.

### B3. One way to do one thing
- **Rule:** A given task should have one canonical entry point. Don't ship a builder + a factory + a constructor that all do the same thing.
- **Why:** Multiple paths fragment usage, double the test surface, and create silent divergence ("does the builder default `compression`? does the factory?").
- **How:** Search for the new entry point's name in `docs/content/` and existing source. If there's already a way to do this, the new one needs a justification or should replace the old one.

### B4. Principle of least surprise
- **Rule:** Methods named like existing ones should behave like existing ones. Parameter order should match nearby APIs. Defaults should match the documented norms.
- **How:** If the new method is on an existing class, compare its signature shape to the rest of the class. A `getFoo(int idx)` that throws on out-of-range when every other `getFoo` returns null is a surprise.

### B5. Breaking changes — broader than API classes
- **Rule:** Backwards-incompatible changes include public class/method shape, configuration option names/defaults, CLI flag names, **metric names/types, log format/keys, file format on disk**. All of these can break downstream users without a compiler error.
- **Why:** Easy to forget the non-code parts of the API.
- **How:** For each renamed or removed identifier in the diff, ask: was this name ever in a release? In `docs/content/`? In a benchmark file users may have copied? In a CLI example?

### B6. CLI surface
- **Rule:** Only expose configuration options truly needed in the CLI. No flag for an internal toggle.
- **How:** Diff `cli/src/main/java/...` for new `@Option` / argument-parser entries. Each one needs a user-facing reason.

### B7. API is generic enough to be reused
- **Rule:** New public API should be useful beyond the specific call site that motivated it. If it's only useful in one workflow, it might belong inside that workflow's class, not in the public surface.
- **How:** Imagine three plausible callers. If you can only find one, the abstraction is premature.

---

## C. Design discipline

### C1. Design doc exists for non-trivial changes
- **Rule:** New features / refactorings need a design markdown under `_designs/` before implementation.
- **How:** Large PR (> 1k LOC) with no `_designs/*.md` added or updated → flag it. Small PRs are fine without.

### C2. Design doc is end-state, not process
- **Rule:** Design docs describe the intended end state. No references to prior approaches that were rejected, evolution commentary, or "we used to do X".
- **How:** Read added/modified `_designs/*.md` for phrases like "previously", "v1", "originally we", "considered but rejected". These belong in commit messages or PR descriptions, not the design doc.

### C3. Doc/code drift on load-bearing claims
- **Rule:** If a design doc states a gate condition, eligibility rule, or supported set, the code must match.
- **Why:** The PR #418 design said "≥ 2 distinct projected columns" but the code was `leaves.size() < 2`. Both shipped together. Easy to catch if you grep.
- **How:** Read the design doc's gate descriptions. For each numeric or boolean claim, find the code that implements it. Quote them side-by-side if they differ.

### C4. ROADMAP / status update
- **Rule:** When a feature ships, mark the design doc done and update ROADMAP.md.
- **How:** Diff includes a completed design but no `ROADMAP.md` change → flag.

---

## D. Testing

### D1. Type/op coverage breadth
- **Rule:** If an implementation supports N (type, op) pairs, the test must cover all N — or document which are deferred and why.
- **Why:** Most common testing gap. Author writes code for `long`/`double`/`int`/`float`/`boolean`/`IntIn`/`LongIn`/`IsNull`/`IsNotNull` and writes oracle tests for `long`/`double` only.
- **How:** Count the supported pairs in the new code. Count the tested pairs in the new tests. Diff. If the test doesn't enumerate everything, list explicitly what's missing.

### D2. Corner cases and boundary values
- **Rule:** NaN, ±∞, ±0.0, `MIN_VALUE`/`MAX_VALUE`, empty input, single-element input, exactly-at-boundary input, batch-size boundaries (word boundary at 64 for bitmap code), null-only / all-null columns.
- **How:** For numeric work, grep tests for `NaN`, `Infinity`, `MIN_VALUE`, `MAX_VALUE`. If none of those appear and the diff is numeric, flag it. Pyramid framing: "are corner cases tested?".

### D3. Unit-vs-integration balance
- **Rule:** Unit tests where possible, integration tests where necessary. A new behaviour should be covered by a unit test that locates the failure precisely; an end-to-end test verifies the wiring.
- **How:** If the only new test is `*BenchmarkTest` or end-to-end, ask for the unit test. If the only new tests are mock-heavy unit tests for a wire-level behaviour, ask for an integration test.

### D4. Oracle / equivalence tests
- **Rule:** New paths that replicate behaviour from an existing path should have a three-way oracle: legacy ↔ new ↔ reference.
- **How:** PR #418 has `DrainSideOracleTest`. This is the model.

### D5. Bug-fix PRs start with a failing test
- **Rule:** CLAUDE.md: "When fixing issues which are bug reports, start with a test case which is failing."
- **How:** Bug-fix PR's diff should show a test added or modified before the production change (or at minimum in the same commit). If the test isn't there, ask the author to add one.

### D6. NFR / performance tests
- **Rule:** Performance-sensitive changes need a benchmark (`performance-testing/` end-to-end or JMH micro), not just a "should be faster" claim.
- **How:** See A8. Cross-link the perf test to the production code path it exercises so a future regression bisects cleanly.

---

## E. Documentation

### E1. JavaDoc style on new public/internal API
- **Rule:** `///` Markdown comments, not `/** */`. Backtick-fenced code blocks, `[ClassName]` reference links, inline backticks, Markdown lists. (See also F2.)
- **How:** `grep -n '/\*\*' <diff>` for new `/** */` blocks. Also grep new JavaDoc for `{@link `, `{@code `, `<pre>` — should not appear.

### E2. User-facing doc updates
- **Rule:** Public API change → `docs/content/*.md` update in the same PR.
- **How:** Diff includes new factory methods, records, configuration options, or CLI flags but no `docs/content/` change → flag.

### E3. User docs describe what, not why
- **Rule:** `docs/content/*.md` and public-API JavaDoc explain how to use the API. Design rationale belongs in `_designs/`.
- **How:** Read added user-facing docs for "we chose X because Y" or "matches the convention in Z" — these belong elsewhere.

### E4. README / API docs / user guide coverage
- **Rule:** New features should appear in the appropriate doc kind: README for top-level features, API docs (JavaDoc) for new types, user guide / reference docs for behaviour. A change isn't done until the relevant doc kind is updated.
- **How:** For each new user-visible feature, walk: is there a README mention? JavaDoc? `docs/content/` entry? At least one should be true.

### E5. Doc grammar and clarity
- **Rule:** No significant typos or grammar mistakes in new docs.
- **How:** Read added docs end-to-end once. This is a pyramid-low item but worth catching.

---

## F. Coding conventions (nits — automate where possible)

These items should ideally be caught by linting / checkstyle, not manual review. Flag them in a **Nits** footer, not as blockers. If a category is large enough that it recurs across PRs, that's a signal to invest in tooling rather than continuing to flag manually.

### F1. No `var`
- **How:** `grep -nE '^\s*var ' <diff>`. Zero hits expected.

### F2. JEP 467 JavaDoc style
- See E1.

### F3. No fully-qualified class names in code
- **Rule:** Always import; don't use `dev.hardwood.foo.Bar` inline.
- **How:** Read the diff for `dev\.hardwood\.\S+\.\S+\(` patterns mid-method.

### F4. DRY within the same class/package
- **Rule:** Repeated logic gets a private helper. Repeated *classes* (e.g. N near-identical matcher classes) need a justification before merging — generally codegen or a shared helper beats hand-duplication, even when "JIT specialization" is the stated reason.
- **Why:** Maintenance multiplier. Today's 28 hand-duplicated matchers become tomorrow's 56 vector-API + scalar matchers.
- **How:** When 3+ new files share >70% of their structure, flag it. Ask the author to A/B against a shared helper before locking in the pattern. **Note:** DRY at this scale is an implementation-semantics concern, not a style nit — promote it.

### F5. Base class restraint
- **Rule:** Do not pull implementation into abstract base classes unless logic is identical across all subclasses. Shared helpers > shared template methods.
- **How:** New `abstract` classes added in this PR? Each `protected` method is a question: is this *truly* identical across all subclasses, or convenient-for-now identical?

### F6. Cyclomatic complexity
- **Rule:** Keep it low.
- **How:** Eyeball: any new method with > 3 nested control structures should at least be questioned.

### F7. Primitive over boxed
- **Rule:** Prefer primitive paths, even if it means several similar methods. Avoid `Integer`/`Long`/`Object` flow where a primitive works.
- **How:** Grep for `Long\.valueOf`, `Integer\.valueOf`, `<Integer>`, `<Long>` in hot paths.

---

## G. Maven / build (project hygiene)

### G1. Plugin versions only in parent
- **Rule:** All plugin versions declared in the parent `pom.xml`'s `<pluginManagement>`. Module POMs reference by `groupId`/`artifactId` only.
- **How:** `grep -n '<version>' */pom.xml` (excluding parent). Any plugin `<version>` in a module POM is a violation.

### G2. GitHub Actions pinned by SHA
- **Rule:** Any new action use must be SHA-pinned, not `@vN`.
- **How:** Diff `.github/workflows/*.yml` for `uses:` lines; check for `@<40-char-sha>` not `@v1`.

---

## H. Dive TUI (only for cli/dive changes)

### H1. Theme usage
- **Rule:** Style spans only via `Theme.primary()` / `Theme.accent()` / `Theme.selection()` / `Theme.dim()`, or `Style.EMPTY`. No raw `Color.*` constants or literal `Style.EMPTY.bold()` outside `Theme.java`.
- **How:** `grep -n 'Color\.\|Style\.EMPTY\.\(bold\|fg\|bg\)' cli/src/main/java/dev/hardwood/cli/dive/` in the diff. Any hit outside `Theme.java` is a smell.

### H2. List viewport virtualization
- **Rule:** List-shaped screens must build `Row` objects only for the visible viewport. Use `RowWindow.bottomPinned`.
- **Why:** O(N) navigation on dictionaries with 100k+ entries or wide schemas.
- **How:** Grep new TUI list code for full-collection iteration where rows are constructed.

---

## I. Commit messages

### I1. Issue prefix
- **Rule:** Every commit message starts with `#<issue-number>` (e.g. `#90 Include file name in all exceptions raised during reading`).
- **How:** `git log --format="%s" <base>..HEAD` for commits in the PR. Every line must start `#\d+ `.

### I2. Why, not what
- **Rule:** Body explains motivation, not what changed.
- **How:** Read each commit body; if it's a bullet list of "added X, removed Y, refactored Z", flag.

### I3. No Claude as Co-Authored-By
- **Rule:** Never add Claude as a Co-Authored-By trailer. Human co-authors (e.g. a reviewer who pushed a fixup themselves) are fine.
- **How:** `git log --format='%B' <base>..HEAD | grep -i 'co-authored.*claude\|co-authored.*anthropic\|co-authored.*noreply@anthropic'` → zero hits expected. Other Co-Authored-By lines are OK.

### I4. No ephemeral commentary
- **Rule:** Commit message should be useful to a future reader of the diff. Drop interim states, branch-local fixups, "addressed review feedback" without saying which.
- **How:** Read commit bodies for time-bound phrasing ("originally", "first attempt", "needed to fix this after").

---

## Output ordering reminder

Group findings in the output file by **pyramid tier**, not by checklist section. A finding from D1 (test coverage) is a `## Tests` entry; a finding from A1 (silent fallback) is `## Implementation semantics`. The checklist organization here is for *generating* the review, not for *reporting* it. Style-tier items (F.*) belong in the trailing **Nits** section, never in **Blockers**.
