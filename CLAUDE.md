# General

Avoid "not my department" thinking; if, for instance, there are build failures you consider unrelated to our current changes, still make an effort to fix them.
Never add a Co-Authored-By trailer to commit messages.

# Maven

To run Maven, always run ./mvnw (Maven wrapper).
Run ./mvnw verify to build the project.
When doing changes in hardwood-core, install that module before running the performance tests or any other module.
When running Maven commands, always apply a timeout of 180 seconds to detect deadlocks early on.
Enable -Pperformance-test to run performance tests.

# Design

Write plans which affect the system design, e.g. large new features or refactorings, as a Markdown file under _designs_ before implementing.
Mark designs as completed once done.
Update the status in the roadmap after implementing a feature.
Design documents describe the intended end state. Do not include references to the development process, alternative approaches that were considered and rejected, or commentary on how the design evolved. Write as if the reader has no context on the conversation that produced the document.

# Public API

Every new or modified public API (factory method, record, enum, configuration option, etc.) must have a corresponding update to the usage documentation under `docs/content/` before the change is considered complete.
Minimize the surface of the public API, only make user-facing what needs to be user-facing. Keep anything else in an `internal` package.
Similarly, only expose configuration options truly needed in the CLI.

# Coding

Correctness is a top priority. Adhere to the "fail early" principle: validate inputs, check types, and throw on misuse rather than silently producing wrong results. Silent failures are never an option.
When fixing issues which are bug reports, start with a test case which is failing. Then fix the bug and assert the test passes.
Never do unsafe downcasts with potential value loss. E.g. prefer Math::toIntExact() where applicable.
Keep cyclomatic complexity low.
Avoid fully-qualified class names within the code, always add imports.
Avoid object access and boxing as much as possible. Always prefer primitive access also if it means several similar methods.
Before writing new code, search for existing patterns in the same class/package that accomplish the same thing (e.g., the DRY principle). Extract repeated logic into helper methods within the same class rather than duplicating it. When a pattern appears multiple times, consider consolidating it into a single well-named method with overloads if needed.
Be conservative with base class refactoring. Do not pull implementation details up into abstract base classes unless the logic is truly identical across all subclasses with no foreseeable divergence. Shared helpers are better than shared template methods when subclasses may need different control flow.
Never use `var` syntax.
GitHub Actions should always referenced by SHA.

# Documentation

All JavaDoc comments must use Markdown `///` syntax (JEP 467), not the legacy `/** */` block comment style.
Key rules for `///` Markdown JavaDoc:
- Use backtick-fenced code blocks (e.g. ` ```java `) instead of `<pre>{@code ...}</pre>`.
- Use `[ClassName]` reference links instead of `{@link ClassName}`.
- Use inline backticks instead of `{@code text}`.
- Use Markdown formatting (`**bold**`, `- list items`, etc.) instead of HTML tags.
- Block tags (`@param`, `@return`, `@throws`) work unchanged.
- `@see` tags still require HTML `<a href="...">` for external links, not Markdown link syntax.
- Escape literal square brackets in prose (e.g. `` `array[i]` ``) so they are not interpreted as reference links.

# Testing

To generate test Parquet files, extend simple-datagen.py and run: `source .docker-venv/bin/activate && python simple-datagen.py`
When running Python, use _.docker-venv_ as the venv directory.

# Contributions

Before starting new work, check whether a corresponding GitHub issue exists. If not, create one first so that all commits and pull requests can reference it.
Commit messages must begin with the GitHub issue key (e.g. `#90 Include file name in all exceptions raised during reading`). This applies to every commit, including fixups and amendments.

# Code Reviews

When reviewing a pull request, make sure the principles described here are applied.
