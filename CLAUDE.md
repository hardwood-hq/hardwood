# General

Avoid "not my department" thinking; if, for instance, there are build failures you consider unrelated to our current changes, still make an effort to fix them.
Never add Claude (or any Anthropic identity) as a Co-Authored-By trailer on commit messages. Human co-authors are fine.
Never put a Claude session link or session ID in a pull request description. It resolves only for the account that ran the session, so to everyone else reviewing the change it is a dead link.
When a CLI tool is missing from the dev container (e.g. `cmp: command not found`), surface it to the user and propose adding the providing package to the `Dockerfile`'s `microdnf install` list — unless there is an obvious one-line alternative (e.g. `command -v` for `which`) or a Python equivalent that is just as terse. Do not silently work around it.

# Multiple Sessions

Several Claude sessions may be running against this repository at once. Git allows one HEAD per working tree, so two sessions sharing a checkout also share HEAD, the index and the files: a `git checkout` in one silently relocates the other, and work gets committed to the wrong branch.

Before any branch work, move into your own worktree. Treat `/workspace` as a reference checkout that stays on `main` — it is where every session lands by default, which is exactly why it must not be where any session commits. Name the worktree directory after its branch, so `git worktree list` reads as a registry of what is in flight.

Stage explicit paths. Never `git add -A` or `git commit -a`: another session's uncommitted files may be sitting in the tree, and a catch-all stage sweeps them into your commit.

Verify before committing, not after. Check that `git rev-parse --abbrev-ref HEAD` is the branch you expect and that `git status` lists only files you touched. A commit on the wrong branch is recoverable through the reflog; one that has been pushed or amended over is not always.

Worktrees live under `.claude/worktrees/`, which `.gitignore` excludes. They must stay inside the repository, because `docker-compose.yaml` bind-mounts the repo root and anything outside it is invisible to an IDE on the host.

# Maven

To run Maven, always run ./mvnw (Maven wrapper).
Run ./mvnw verify to build the project.
When doing changes in hardwood-core, install that module before running the performance tests or any other module.
When running Maven commands, always apply a timeout of 180 seconds to detect deadlocks early on.
Enable -Pperformance-test to run performance tests.
All plugin versions must be declared in the parent `pom.xml`'s `<pluginManagement>`. Module POMs reference plugins by `groupId`/`artifactId` only, never with a `<version>` of their own.

# Design

Write plans which affect the system design, e.g. large new features or refactorings, as a Markdown file under _designs_ before implementing.
Mark designs as completed once done.
Update the status in the roadmap after implementing a feature.
Design documents describe the intended end state. Do not include references to the development process, alternative approaches that were considered and rejected, or commentary on how the design evolved. Write as if the reader has no context on the conversation that produced the document.

# Public API

Every new or modified public API (factory method, record, enum, configuration option, etc.) must have a corresponding update to the usage documentation under `docs/content/` before the change is considered complete.
Minimize the surface of the public API, only make user-facing what needs to be user-facing. Keep anything else in an `internal` package.
Similarly, only expose configuration options truly needed in the CLI.
The public / internal split is for external library users; modules within this repository (e.g. `hardwood-cli`, `hardwood-avro`) may depend on `dev.hardwood.internal.*` packages directly. Don't promote internal APIs to public or duplicate them in a sibling module just to avoid the `internal` import.

# Coding

Correctness is a top priority. Adhere to the "fail early" principle: validate inputs, check types, and throw on misuse rather than silently producing wrong results. Silent failures are never an option.
When fixing issues which are bug reports, start with a test case which is failing. Then fix the bug and assert the test passes.
Never do unsafe downcasts with potential value loss. E.g. prefer Math::toIntExact() where applicable.
Keep cyclomatic complexity low.
Avoid fully-qualified class names within the code, always add imports.
Avoid object access and boxing as much as possible. Always prefer primitive access also if it means several similar methods.
Before writing new code, search for existing patterns in the same class/package that accomplish the same thing (e.g., the DRY principle). Extract repeated logic into helper methods within the same class rather than duplicating it. When a pattern appears multiple times, consider consolidating it into a single well-named method with overloads if needed.
Be conservative with base class refactoring. Do not pull implementation details up into abstract base classes unless the logic is truly identical across all subclasses with no foreseeable divergence. Shared helpers are better than shared template methods when subclasses may need different control flow.
Never use `var` syntax. Enforced by the `NoVar` Error Prone check in `error-prone-checks/`.
GitHub Actions should always referenced by SHA.

# Documentation

User-facing docs (`docs/content/*.md` and public-API JavaDoc) describe **what the API does and how to use it**. Do not include rationale for design decisions, comparisons to other internal APIs, or "we chose X to match Y" justifications — users don't need to know why two parts of the project share a convention, only that they do. Design rationale belongs in `_designs/*.md` (audience: maintainers and reviewers); user docs describe the end state directly. The one exception is the `concepts/` directory described below, which is the designated home for reader-facing rationale (the *why* behind the API, not the *why* behind internal design choices).

User-facing docs follow the [Diátaxis](https://diataxis.fr/) model, one directory per kind under `docs/content/`: `tutorial/` (learning-oriented lessons), `how-to/` (goal-oriented guides), `reference/` (look-it-up facts), `concepts/` (understanding and rationale — the Diátaxis *explanation* quadrant). Put a new page in the directory matching its kind and keep the kinds unmixed: tutorials show one working path with no option matrices, how-to guides don't explain rationale, reference doesn't narrate, and concepts pages give no step-by-step instructions. The full structure and rationale live in [_designs/DOCS_DIATAXIS_STRUCTURE.md](_designs/DOCS_DIATAXIS_STRUCTURE.md).

All JavaDoc comments must use Markdown `///` syntax (JEP 467), not the legacy `/** */` block comment style. Enforced by the `NoLegacyJavadoc` Error Prone check in `error-prone-checks/`.
Key rules for `///` Markdown JavaDoc:
- Use backtick-fenced code blocks (e.g. ` ```java `) instead of `<pre>{@code ...}</pre>`.
- Use `[ClassName]` reference links instead of `{@link ClassName}`.
- Use inline backticks instead of `{@code text}`.
- Use Markdown formatting (`**bold**`, `- list items`, etc.) instead of HTML tags.
- Block tags (`@param`, `@return`, `@throws`) work unchanged.
- `@see` tags still require HTML `<a href="...">` for external links, not Markdown link syntax.
- Escape literal square brackets in prose (e.g. `` `array[i]` ``) so they are not interpreted as reference links.

# Testing

To generate test Parquet files, extend tools/simple-datagen.py and run: `source .docker-venv/bin/activate && python tools/simple-datagen.py`
When running Python, use _.docker-venv_ as the venv directory.
Use Python 3.10–3.14 (PyArrow 24.0.0, pinned in `requirements.txt`, supports that range). The pinned versions of `pyarrow` and `thriftpy2` are load-bearing: both directly produce bytes that get checked into `core/src/test/resources/`, so any upgrade must be paired with regenerating the affected fixtures.

# Contributions

Before starting new work, check whether a corresponding GitHub issue exists. If not, create one first so that all commits and pull requests can reference it.
Commit messages must begin with the GitHub issue key (e.g. `#90 Include file name in all exceptions raised during reading`). This applies to every commit, including fixups and amendments.
Focus the body on **why**, not **what** — the diff already shows the what. A short paragraph is usually enough; do not restate the change as a bullet list. See [CONTRIBUTING.md](CONTRIBUTING.md#commit-messages).
A sentence belongs in the message only if it helps a future reader understand the diff. Drop ephemeral minutiae — slips caught and fixed within the same branch, transient process issues, references to interim states that won't survive in the final history, or commentary about how the change was developed rather than what it does.

When filing an issue, apply the relevant labels from the repo's label set. Common ones:
- `bug` — defect / something isn't working as documented or intended.
- `enhancement` — new feature or improvement to existing behavior.
- `documentation` — docs-only change.
- `cli` — anything in the `cli` module (commands, flags, output formatting).
- `dive` — anything in the `hardwood dive` interactive TUI.
- `good first issue` + `help wanted` — well-scoped, self-contained tasks suitable for an external contributor (clear acceptance criteria, no deep architectural decisions, limited blast radius). Apply both together.

Labels are not mutually exclusive — a TUI bug should get `bug` + `cli` + `dive`. Run `gh label list` to see the full current set before inventing one.

# Dive TUI

When changing visual styling in the `hardwood dive` TUI (any code under `cli/src/main/java/dev/hardwood/cli/dive/`), follow the visual-hierarchy decision tree in [_designs/DIVE_THEME.md](_designs/DIVE_THEME.md). Style spans through one of `Theme.primary()` / `Theme.accent()` / `Theme.selection()` / `Theme.dim()`, or leave them at default fg via `Style.EMPTY`. Direct use of `Color.*` constants or literal `Style.EMPTY.bold()` / `Style.EMPTY.fg(...)` outside `Theme.java` is a smell — review against the decision tree before introducing any.

Navigation keys and the `▶` marker follow the two rules in [_designs/DIVE_NAVIGATION_MODEL.md](_designs/DIVE_NAVIGATION_MODEL.md): every key moves the cursor, stopping on every row, and `▶` marks what `Enter` can act on while colour marks the cursor. Route a row-shaped pane through `CursorPane` and a document-shaped one through `ScrollPane` rather than handling the keys in the screen — every pane that deviated from the rules had hand-rolled them.

List-shaped screens must build `Row` objects only for the visible viewport, never for the whole collection. Use `RowWindow.bottomPinned(selection, total, viewport)` to derive the slice and pass `window.selectionInWindow()` to `TableState.select(...)`. Building rows for the entire list is invisible on small inputs but turns navigation O(N) on dictionaries with hundreds of thousands of entries, page lists with thousands of pages, or wide-schema files. See [_designs/DIVE_LIST_VIEWPORT_VIRTUALIZATION.md](_designs/DIVE_LIST_VIEWPORT_VIRTUALIZATION.md).

# Code Reviews

When reviewing a pull request, make sure the principles described here are applied.
When asked to review a PR, write the findings to a Markdown file with a `[ ]` checkbox in front of each actionable item. When subsequently asked to address those review remarks, check the items off (`[x]`) as they are resolved.
