# Documentation

This document covers how Hardwood's user-facing documentation is organised, checked, built and published: the Diátaxis structure of `docs/content/`, the prose rules and the script that enforces their mechanical part, the MkDocs site, the two-repository publish flow that deploys the site and the JavaDoc to `hardwood.dev`, and the API change reports published beside the JavaDoc. It does not cover the content of individual pages, or the Markdown JavaDoc conventions and the Error Prone checks that enforce them ([BUILD_INFRASTRUCTURE.md](BUILD_INFRASTRUCTURE.md)). The rules contributors follow when writing a page are stated in the "Documentation" section of [CLAUDE.md](../CLAUDE.md); this document gives the structure those rules serve and the reasoning behind it.

## Audiences

Hardwood's documentation has three homes, split by reader:

| Home | Reader | Content |
|---|---|---|
| `docs/content/` | Library and CLI users | What the API does and how to use it |
| Public-API JavaDoc | Library users | Class- and method-level contracts |
| `_designs/` | Maintainers and reviewers | Structure, contracts, invariants and the reasoning behind them |

User docs and JavaDoc describe the end state directly. Rationale for internal design decisions, comparisons between internal APIs and "we chose X to match Y" justifications stay out of them and belong in `_designs/`. The one place user docs carry rationale is `concepts/` (see below), limited to the reasoning a user needs to understand the API.

The JavaDoc is the authoritative class-level reference. Prose reference pages link into it through absolute `/api/latest/…` URLs rather than duplicating member lists.

## Diátaxis structure

The pages under `docs/content/` follow the [Diátaxis](https://diataxis.fr/) model. Diátaxis divides documentation along two axes, acquiring versus applying a skill and practical versus theoretical knowledge, which yields four kinds of page. Each kind answers one reader need:

| Kind | Reader need | Directory | Nav group |
|---|---|---|---|
| Tutorial | "Teach me, I'm new": learning by doing | `tutorial/` | Tutorial |
| How-to guide | "Help me accomplish a goal" | `how-to/` | How-to Guides |
| Reference | "Tell me the precise facts" | `reference/` | Reference |
| Explanation | "Help me understand" | `concepts/` | Concepts |

A reader arrives with one of these intents, and a page written for two of them serves neither: a tutorial that branches into options loses the newcomer, and a reference page that narrates slows down the reader looking up one fact. Keeping one kind per page, one directory per kind and one nav group per directory lets a reader pick the right group from their intent alone, and lets a writer decide where a new page goes from the question it answers.

### Placement rules

| Directory | Contains | Must not contain |
|---|---|---|
| `tutorial/` | A guided lesson with one path that works as written and a concrete result at each step | Alternatives ("you could also…"), option matrices, rationale |
| `how-to/` | Goal-titled guides that assume the reader knows what they want to do | Rationale; exhaustive lists of formats or options (link the reference page) |
| `reference/` | Look-it-up facts: configuration, exceptions, accessors, CLI, package map | Narrative, teaching, persuasion |
| `concepts/` | The mental models the other kinds assume, and the reasoning behind the API | Step-by-step instructions, exhaustive option tables |

Each fact has one home. A how-to guide that needs an exact format or list links the reference page that holds it; a concepts page that needs a mechanic links the how-to guide that shows it. `how-to/index.md` is the section's landing page and holds a practical decision table between the two readers; the reasoning behind that table lives in `concepts/reader-models.md`.

The tutorial (`tutorial/first-read.md`) reads a real, publicly downloadable dataset (NYC TLC yellow-taxi trip data), so every snippet runs against a file the reader has. It is a single lesson; depth and option coverage belong to the how-to and reference pages it points to at the end.

### Concepts as the home for reader-facing rationale

Users sometimes need to know why the API behaves as it does in order to use it well: why there are two reader APIs, why a filter on a nested column behaves the way it does, why the compatibility module is stricter than parquet-java in specific places. That reasoning is explanation in the Diátaxis sense, and `concepts/` is the only directory where it may appear. Confining it there keeps how-to guides and reference pages free of justification, and gives every "why" a single page that the task-oriented pages can link.

The boundary with `_designs/` is the reader. A concepts page explains the API a user calls (the file layout that makes projection and pushdown possible, the layer model behind `ColumnReader`, the semantics of timestamps). A design document explains the implementation a maintainer changes. A concepts page does not cite internal classes, design documents or the history of a decision.

### Pages outside the four kinds

`index.md` (the landing page), `getting-started.md` (installation, coordinates, optional libraries), `contributing.md` and `release-notes.md` sit at the content root as top-level nav entries. Getting Started is the prerequisite the tutorial builds on and a returning user reaches for it independently of the four kinds; the release notes are a chronological changelog. The nav also carries external entries that point outside the MkDocs tree: the JavaDoc (`/api/latest/`) and the API change report (`/api-changes/latest/`) under Reference, and the `dive` web demo (`/experiments/dive-web/`) at top level.

File paths mirror the nav groups, so a page's URL names its kind (`/how-to/…`, `/reference/…`, `/concepts/…`, `/tutorial/…`). The nav itself is the `nav:` block of `docs/mkdocs.yml`; that file is the single list of pages and their titles, and a new page is added there in the group matching its directory.

## Prose rules and the prose check

Every sentence in the user docs states something the reader can check or act on. The rules (no praise, no announcements, no slogans, no questions to the reader, no self-labelling pages, "not X, but Y" only against a likely misreading, asides set off without runs of em-dashes, one home per fact) are stated in the "Documentation" section of [CLAUDE.md](../CLAUDE.md), which is the contributor-facing statement of them.

`tools/docs-prose-check.py` enforces the mechanical part. It scans every Markdown file under `docs/content/` (or a directory given as its argument) and exits non-zero on any finding:

| Check | What it flags |
|---|---|
| Filler intensifiers | "genuinely", "simply", "actually" |
| Announcements | "is worth noting", "note that", "keep in mind", "importantly" and similar lead-ins |
| Signposts and slogans | "in short", "in other words", "let's", "here's" and a short list of known slogans |
| Self-labelling | "this page explains", "look-it-up reference" |
| Questions | A sentence ending in `?`, except in the body of an `!!! example` admonition (the "Try it yourself" callout that links a runnable example) |
| Em-dash density | More em-dashes per 1,000 words than the script's limit, on a page long enough for the density to be meaningful |

Matching runs over whole prose units (paragraphs, list items, headings, table cells) with inline code, link targets, fenced code blocks and HTML comments removed, so a phrase broken across a line wrap is found and code is never flagged. A unit that legitimately needs a flagged phrase carries `<!-- prose-check: ignore -->`. The check cannot judge whether a sentence carries a fact, whether a page sits in the right Diátaxis kind, or whether a fact has a second home; those remain review responsibilities.

The check runs as the "Check docs prose" step of the `build-modules` job in `.github/workflows/pr-build.yml`, on every pull request to `main`. Untested (the script has no tests of its own).

## Site stack

The site is built with MkDocs and the Material theme, configured in `docs/mkdocs.yml` with `docs_dir: content`. `docs/requirements.txt` pins the Python dependencies (MkDocs, Material, mike), and both CI and local preview install from it. The configuration lives under `docs/` rather than the repository root so that the Maven project root holds only the build.

| Piece | Location | Role |
|---|---|---|
| Site configuration and nav | `docs/mkdocs.yml` | Theme, Markdown extensions, nav, `extra` variables |
| Placeholder hook | `docs/hooks/page_variables.py` | Replaces `{{name}}` in page Markdown with `extra.name` from the config; an unknown name is left as written |
| Commit hook | `docs/hooks/git_commit.py` | Sets `extra.commit_sha` to the source commit the site is built from |
| Theme overrides | `docs/overrides/partials/` | Logo, and a footer that links the source commit |
| Local preview | `docs/Dockerfile` | `mkdocs serve` with live reload; usage in `README.md` |
| SEO patch | `docs/patch-seo.sh` | Post-build canonical and `noindex` tags (see below) |

The `extra` block carries the values that change per release: `hardwood_version` (the Maven coordinates in examples), `cli_release_tag` and `cli_docker_tag` (the CLI download and image). Pages refer to them only through placeholders, never as literals, so that `release.sh` can set them for the tagged docs build by editing `docs/mkdocs.yml` alone; on `main` the CLI values point at the rolling `1.0-early-access` release.

The footer's "Built from" link names the Hardwood commit the site was built from. `git_commit.py` takes it from the `SOURCE_REF` environment variable when set, and from `git rev-parse HEAD` otherwise. The order matters: in the publish workflow the working directory is the site repository, whose `HEAD` and `GITHUB_SHA` belong to the site repository, and only `SOURCE_REF` names the Hardwood commit. Any stamp of the source commit on published output follows the same rule.

Documentation pages carry the CC BY-SA 4.0 license header; the site footer states that license.

## Publishing topology

Two repositories take part:

| Repository | Role |
|---|---|
| `hardwood-hq/hardwood` | Source of the docs, the site configuration, the report tooling; fires the publish event |
| `hardwood-hq/hardwood-hq.github.io` | Builds and deploys the site to its `gh-pages` branch, served as `hardwood.dev`; never committed to by hand |

The source repository never writes to the site repository. It sends a `repository_dispatch` event of type `publish-docs` with a payload `{source_ref, version, update_latest}`, authenticated by the `DOCS_DISPATCH_TOKEN` secret: a fine-grained token scoped to the site repository with **Contents: Read and write**, the permission GitHub files the `repository_dispatch` endpoint under. The token could therefore push to the site repository; the source repository's workflows only ever dispatch with it, and the site repository commits to `gh-pages` with its own `GITHUB_TOKEN`. The deploy logic lives in the repository being deployed.

`source_ref` is always a full commit SHA, so the site builds a fixed snapshot even when `main` moves between dispatch and build.

| Version | Fired by | Ref | `update_latest` |
|---|---|---|---|
| `dev` | `docs-trigger.yml`, after every successful Main Build on `main` | The built commit | `false` |
| A release, e.g. `1.0.0.CR1` | `docs-publish.yml`, run by hand | The ref given, resolved to a SHA; by convention the `v<VERSION>-docs` tag | Chosen per run; unchecked for a back-port so it does not displace a newer line |

Versioned docs are published by hand after a release, from a `-docs` tag, so that post-release additions (the announcement link, release-note errata) reach the published version without moving the release tag. The procedure is in `RELEASING.md`.

The site repository's `publish.yml` checks out the site repository, checks out Hardwood at `SOURCE_REF` into `source/` with full history, and then:

1. Builds the aggregated JavaDoc for the four published library modules (`hardwood-core`, `hardwood-avro`, `hardwood-s3`, `hardwood-aws-auth`) and strips non-deterministic metadata from its SVG diagrams with `tools/strip-svg-metadata.sh`.
2. Generates the API change report (next section).
3. Rewrites `/api/latest/` and `/api-changes/latest/` in the config and pages to the version being published, so each versioned site links its own JavaDoc and report.
4. Deploys the prose docs with mike under the version label; for a release with `update_latest`, it also moves the `latest` alias and sets it as the site default. `dev` never claims `latest`.
5. Copies the JavaDoc to `api/<version>/` and the report to `api-changes/<version>/`, and to the `latest/` paths when `update_latest` is set for a release.
6. Refreshes the `dive` web demo at `experiments/dive-web/` from the `1.0-early-access` release asset.
7. Runs `docs/patch-seo.sh`: pages under versioned and `latest` directories get a canonical link to the same page under `/latest/` (and `/api/latest/` for JavaDoc); `dev` and `api/dev` get `noindex`. The script is idempotent.
8. Amends everything into mike's commit and pushes, so one publish is one `gh-pages` commit.

Steps that depend on tooling a ref may predate (the SVG strip, the report generator) are skipped when the tool is absent at `SOURCE_REF`, so an old ref can be republished.

The resulting URL space:

| Path | Content |
|---|---|
| `/` | Redirects to `/latest/` |
| `/latest/`, `/<version>/`, `/dev/` | Prose docs |
| `/api/latest/`, `/api/<version>/`, `/api/dev/` | JavaDoc |
| `/api-changes/latest/`, `/api-changes/<version>/`, `/api-changes/dev/` | API change reports |

## API change reports

Each report compares the public API of two builds of the four published library modules with japicmp. `tools/api-report.sh <old> [<new>]` runs the comparison; `tools/publish-api-report.sh <version> <outputDir>` resolves the two sides for a site version and stages the result.

| Version | New side | Old side |
|---|---|---|
| `dev` | The local snapshot | The highest release |
| Release `V` | Published JARs of `V` | Published JARs of the release immediately before `V` in version order |

Release versions come from `v<maven-version>` git tags; `-docs` re-tags and the rolling `1.0-early-access` tag are excluded. Release branches are tagged as siblings of one another, so the predecessor is the version-order predecessor rather than a git ancestor: a bugfix release compares against its own line's prior release even when a newer line's pre-release was tagged in between. A version with no predecessor produces no report and the generator exits successfully.

The order comes from `sort -V`, which is lexical and matches Maven's version order only because release qualifiers are restricted to `Alpha<N>`, `Beta<N>`, `CR<N>` and `Final<N>`. The generator aborts on any release tag with another qualifier (`RC`, for instance, sorts after `Final` lexically but before it in Maven), since the predecessor would otherwise be silently wrong. Introducing a new qualifier requires extending that check and re-checking the order.

For a release both sides are published artifacts, so the report is correct from any checkout and the publish workflow never builds old code. `api-report.sh` installs the local modules first in either case, because japicmp's shared classpath resolves the current project's own artifact; the installed snapshot backs the classpath only and does not enter the diff.

The japicmp configuration in the parent `pom.xml` reports only modified elements and excludes `dev.hardwood.internal` and `dev.hardwood.*.internal`, matching the public/internal split. `api-report.sh` writes per-module reports, a concatenated text diff and a unified HTML page that splices every module's report inline under a table of contents. Only the unified HTML is published, as `index.html`; it is self-contained and nothing on the site links the other files.

The caption names both sides: `HEAD (<short-sha>) vs <old>` for `dev`, `<new> vs <old>` for a release. In the HTML each side links to its GitHub commit (the release tag's commit, or for `dev` the source commit, resolved from `SOURCE_REF` before `GITHUB_SHA` and `HEAD` per the stamping rule above); a side whose commit cannot be resolved is plain text.

Untested (no automated check exercises the generator or the version resolution).

## CI gates

| Gate | Where | Covers |
|---|---|---|
| Prose check | `pr-build.yml`, `build-modules` job | Mechanical prose rules on `docs/content/` |
| Public-API docs rule | Review | A new or changed public API updates `docs/content/` in the same PR |
| Diátaxis placement, one home per fact | Review | Not automated |

No PR-build step builds the site. A broken nav entry, link or placeholder surfaces at publish time, in the `dev` build after the change reaches `main`. Untested.

## Boundaries

- The site runs on MkDocs 1.x; its future under MkDocs 2.0 and the alternatives are tracked in #181.
- Normative claims in `docs/content/` are not all pinned by tests that enforce them (#573).
- Several facts have more than one home across how-to, reference and concepts pages (#1268).
- The published reports cover the four library modules. The `parquet-java-compat` module's own japicmp report, a shim-completeness check outside this pipeline, fails to run (#932).
- The `dive` screenshot gallery can drift from the TUI without failing any build (#892).
