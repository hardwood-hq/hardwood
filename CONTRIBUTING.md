# Contributing to Hardwood

Thanks for your interest in contributing. This guide covers how to find work, make changes, and get a pull request merged.

For build instructions and the overall project layout, see the [README](README.md).

## Finding something to work on

- **New to the project?** Look for issues labeled [`good first issue`](https://github.com/hardwood-hq/hardwood/labels/good%20first%20issue). These are scoped to be approachable without prior context on the codebase.
- **Already comfortable with the code?** Issues labeled [`help wanted`](https://github.com/hardwood-hq/hardwood/labels/help%20wanted) are bounded and welcome PRs, but may expect some orientation first.
- **Have an idea not yet tracked?** Open an issue to discuss it before starting work. This avoids wasted effort on something the maintainers would decline or redesign.

## Issue-first workflow

Every change should be linked to a GitHub issue. If one doesn't exist for what you're about to do, file it first — even for small fixes. Commit messages and pull requests reference the issue number.

## Making changes

- Run `./mvnw clean verify` locally before pushing. Docker must be running (the integration tests use Testcontainers); without it, `./mvnw clean verify -DskipITs` runs the unit test suite on its own.
- Every build without `-Dquick` sorts the imports as per the project's conventions; `./mvnw process-sources` does so without building.
- Cover behavior changes with tests. Bug fixes should start with a failing test that reproduces the bug.
- Name a test `*Test` if it needs nothing beyond the JVM and the checkout, and `*IT` if it needs a Docker daemon, a compiled native binary, or the packaged JAR. The name is what puts it in the right phase, and what lets `-DskipITs` skip it; see [TESTING.md](TESTING.md#what-counts-as-an-integration-test).
- If your change adds or modifies a user-facing API (factory method, record, enum, configuration option, CLI option), update the documentation under `docs/content/` in the same PR.
- Keep the public API surface small. Put anything that doesn't need to be user-facing in an `internal` package.

## Raising exceptions

A failure answers one question for the caller: try again, or stop. The type is what answers it, so pick by category rather than by whatever is convenient to throw. `_designs-legacy/EXCEPTION_MODEL.md` has the full model and `docs/content/reference/error-handling.md` is the version users read.

- **`IOException`** — reading or writing the file failed, and another attempt may succeed. Declare it on every method that can reach the file, and only on those. A method that parses a buffer or decodes a page cannot fail at I/O, so it must not say it can; declaring it there makes a corrupt file look like a failed read.
- **`ParquetReadException`** — the bytes arrived and are not valid Parquet. Unchecked, raised where the invalidity is detected rather than at some boundary further out.
- **`ParquetWriteException`** — the writer could not produce the file and neither the caller nor the destination is at fault, such as a codec rejecting a page body. Unchecked.
- **`UnsupportedOperationException`** — the file is correct and Hardwood cannot read it: encryption, an unimplemented encoding, an absent codec library. Let it travel untouched; its message usually names the remedy.
- Misuse of the API keeps its own type — `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`. The caller's code is wrong and no file is involved.
- **`UncheckedIOException`** only where the language forbids declaring: inside a `Runnable`, a `Supplier`, or a `computeIfAbsent` mapping function. Unwrap it in the method enclosing that lambda — the nearest one that can declare — so the wrap lasts a single call. It must never reach a user.

## Adding an encoding

An encoding is a read-side decoder, a write-side encoder, or both. Work through the table for the
direction you are adding. Paths are relative to `core/src/main/java/dev/hardwood/`.

### Read path

| File | Change | Why |
|---|---|---|
| `metadata/Encoding.java` | Add the constant | The name a chunk's metadata reads back as |
| `internal/thrift/ThriftEnumLookup.java` | Add it to `ENCODINGS`, at its Thrift index | Maps the value on the wire to that constant; without it the encoding reads back as `UNKNOWN` |
| `internal/encoding/<Name>Decoder.java` | Implement `ValueDecoder` | Reads a page's values into the reader's primitive arrays |
| `internal/reader/PageDecoder.java` | Add the case to `decodeTypedValues`, refusing the physical types the encoding is not defined over | The reader's only dispatch point; the compiler names the case but cannot name the types, and an undefined pair left unguarded decodes into a page of the wrong shape |

### Write path

| File | Change | Why |
|---|---|---|
| `writer/ColumnEncoding.java` | Add the policy | What a caller can name in `WriterConfig` |
| `internal/writer/EncodingSupport.java` | Add it to `supports`, naming its legal physical types | The pairs the writer accepts, rejected at writer creation rather than at flush |
| `internal/encoding/<Name>Encoder.java` | Implement the inverse of the decoder | Produces one page's value section |
| `internal/writer/<Type>ValueEncoder.java` | Add the case to `encode`, in each type that may carry it | Each physical type encodes from its own value store |
| `internal/writer/ColumnChunkBuffer.java` | Map the policy in `valueEncoding` | Names the encoding in the page header and `ColumnMetaData.encodings` |
| `WriterEncodingPolicyTest`, `CoverageDomain`, `WriterInteropTest` | Restate the policy in each switch, and add it to the interop axis | Sweeps it over every legal type, the coverage domain, and the parquet-java interop axis |

Both paths are guarded by exhaustive switches, so the compiler names most of the rows above; the
Thrift table is the one row caught by a test rather than by the compiler. What nothing checks is
whether the decoder is *correct*, so cover that yourself — a round trip through the writer where
Hardwood can write the encoding, and a `parquet-testing` fixture where it can only read it.

Finally, update `docs/content/` for the public enums, and `ROADMAP.md` and `FORMAT_COVERAGE.md` for
the capability itself. Nothing in the build checks those.

## Design docs and plans

`_designs/` describes how each subsystem works today: its structure, contracts and invariants. A change to behaviour a design document describes updates that document in the same PR.

Larger changes (new features, refactorings that affect the system design) start with a plan under `_plans/`: the intended end state, and the stages that get there. Open the plan as a PR so it can be reviewed before implementation starts. The PR that completes the work folds the end state into `_designs/` and deletes the plan.

## Agent skills

This repository is the source of truth for the [agent skills](https://agentskills.io) under `skills/` (e.g. `skills/hardwood-cli/`, which teaches AI coding agents how to drive the `hardwood` CLI). They live here, alongside the code they document, so a change to a CLI flag, output label, or command is made in the same PR as the matching skill edit — the skill can't drift from the tool.

Distribution is automated. The [`hardwood-hq/hardwood-skills`](https://github.com/hardwood-hq/hardwood-skills) repository wraps `skills/` in the Claude Code plugin and marketplace manifests and is what users install. It is a **published mirror** — never edit its `skills/` directly. When a change to `skills/` lands on `main`, the [Trigger skills publish](.github/workflows/skills-trigger.yml) workflow fires a `repository_dispatch` at that repository, which re-syncs `skills/` from here and commits it; you don't need to do anything.

To publish out-of-band — testing against a local checkout, or a manual backfill — run the same sync by hand:

```shell
tools/publish-skills.sh /path/to/hardwood-skills
```

then commit and push in that checkout (the script prints the exact commands).

## Commit messages

Every commit message begins with the issue key:

```
#123 Brief description of the change
```

This applies to every commit, including fixups.

Focus the body on **why** the change is being made, with at most a high-level overview of **what**. The diff already shows the what; don't restate it as a bullet list. Keep it concise — a short paragraph is usually enough.

## Local git configuration

Two repository-local settings are worth applying once per clone. Both live in `.git/config`, which
is not version-controlled, so a fresh clone starts without them.

**`core.commentChar`** — required. `#` is git's comment character, and every Hardwood commit subject
starts with `#<issue>`. Any git operation that runs a message through editor cleanup
(`git rebase --continue` after a conflict, `git commit --amend` without `-m`, `reword` and `squash`
steps in `git rebase -i`) deletes lines beginning with `#` — which is your entire subject line. With
a body, the body's first paragraph is silently promoted to the subject; without one the message
becomes empty, git refuses the commit, and `git rebase --continue` loops without advancing.

```shell
git config core.commentChar ';'
```

**`core.checkStat` / `core.trustctime`** — only if you edit the working tree from two places at once,
typically a dev container or VM with the repo bind-mounted from the host. The same files then report
different `st_uid`/`st_gid`, `st_dev`, `st_ino` and sub-second timestamps on each side, and git's
index caches all of those. The cache can only be valid for one side at a time, so whichever side ran
git last invalidates it for the other. The symptom is a working tree that reports every file as
modified with no content change behind any of them — most visibly as a `Local uncommitted changes,
not checked in to index` node in `gitk` and `git gui`, which read the index without refreshing it.

```shell
git config core.checkStat minimal
git config core.trustctime false
```

This drops the fields that differ across the mount and compares whole-second `mtime` plus file size,
which agree. The trade-off is documented: a modification made within the same second that leaves the
file the same size can go undetected. Skip both settings if you work from the host only.

## Opening a pull request

The [PR template](.github/PULL_REQUEST_TEMPLATE.md) covers the basics: build passes, commit message format, test coverage, and documentation updates. Please run through it before requesting review.

## A note on AI-assisted contributions

This project is aiming for a high-quality, maintainable codebase.

This informs our stance on using coding agents:
LLM assistance is welcome; vibe coding is not.
Use whatever tools you like, but you are responsible for the changes you submit.
You must understand at least the key parts of a change and be able to stand behind it in review.
Built with AI, not by AI.

Specifically, it is not acceptable to point an LLM to an issue from the tracker,
accept its output without consideration and open a pull request.
Instead, satisfy yourself a) that the original issue was sensible to begin with,
b) that the generated code solves that issue, and c) that it does so in line with the
project's standards for correctness, completeness and performance.

In addition, pull requests opened by autonomous agents, with no person submitting them, are closed without review,
whether or not the change itself is correct.
The same applies to unsolicited pull requests from accounts submitting at scale across many unrelated projects,
whether or not a person is behind them: the project has no review capacity for them,
and that holds regardless of how good an individual change may be.

Code reviews are also how we get to know the people we're working with,
and that only works when there is an actual human being on the other side of the conversation,
who has an interest in this project.
Contributing under your real name is encouraged for the same reason, though it is not a requirement.

The PR template asks you to confirm that the change comes from a human
who understands it and can discuss it in review.
Ticking it untruthfully is a conduct issue rather than a code-quality one, and accounts that repeat it are blocked.
If a PR of yours is closed under this policy and that was a misread, say so on the PR and it will be re-evaluated.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
