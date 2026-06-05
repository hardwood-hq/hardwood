# Publishing API Change Reports

**Status: Implemented**

## Overview

The japicmp-based API change report produced by `tools/api-report.sh` is published on the
website alongside the JavaDoc, under `/api-changes/<version>/`. Each report compares the public
API of two builds of the four published modules (`hardwood-core`, `hardwood-avro`,
`hardwood-s3`, `hardwood-aws-auth`):

- A **tagged release** is compared against the immediately preceding tagged release
  (e.g. `1.0.0.CR1` against `1.0.0.Beta2`).
- The **dev** build (`/api-changes/dev/`) is compared against the latest tagged release.

`/api-changes/latest/` aliases the most recent release report, mirroring `/api/latest/` for
JavaDoc. Reports for all releases from the first comparable pair (`1.0.0.Alpha1` → `1.0.0.Beta1`)
onward are published; the very first release has no predecessor and therefore no report.

---

## Repositories

| Repo | Role |
|---|---|
| `hardwood-hq/hardwood` | Owns the report generator (`tools/`) and the mkdocs nav link |
| `hardwood-hq/hardwood-hq.github.io` | Generates and deploys the reports to `gh-pages` — never committed to manually |

The publishing flow reuses the existing `publish-docs` `repository_dispatch`: when the site repo
builds the docs and JavaDoc for a version, it also generates and copies the change report. No new
dispatch event or trigger is introduced. The dev report therefore refreshes automatically after
every successful Main Build (via `docs-trigger.yml`), and versioned reports publish when
`docs-publish.yml` is run for a release — exactly as JavaDoc does.

---

## Version resolution

The comparison endpoints are derived from the source repo's git tags. Release tags follow the
`v<maven-version>` convention (`v1.0.0.Beta2`); `-docs` re-tags and the rolling
`1.0-early-access` tag are not releases and are excluded. Release branches are tagged as
siblings rather than ancestors of one another, so the predecessor cannot come from git
ancestry — it is the version-order predecessor, independent of when each tag was created. A
bugfix release therefore compares against its own line's prior release regardless of newer
pre-releases tagged in between (e.g. `1.0.1.Final` against `1.0.0.Final`, not a `1.1.0.Beta1`
tagged earlier).

The release list is sorted with `sort -V`. That is a lexical sort, not a Maven version
comparator; it produces the correct order only because release qualifiers are restricted to
`Alpha<N>`/`Beta<N>`/`CR<N>`/`Final<N>`, which already sort alphabetically into Maven's
precedence. The generator enforces this invariant: a tag whose qualifier falls outside that set
(e.g. `RC`, which Maven orders before `Final` but which sorts after it lexically) aborts the run
rather than risk a silently wrong predecessor.

- For `dev`: the new side is the local `HEAD` snapshot; the old side is the highest release.
- For release `V`: the new side is `V`, the old side is the release immediately preceding `V` in
  version order. If `V` has no predecessor, no report is produced.

---

## Generator: `tools/publish-api-report.sh`

A thin wrapper over `tools/api-report.sh` that resolves the comparison endpoints and lays the
result out for publishing:

```
tools/publish-api-report.sh <version> <outputDir>
  <version>    Maven release version (e.g. 1.0.0.CR1) or "dev"
  <outputDir>  directory to receive index.html + the per-module reports
```

It resolves old/new as described above, invokes `api-report.sh`, then copies `target/japicmp/`
into `<outputDir>` and exposes the unified `api-report.html` as `index.html` so
`/api-changes/<version>/` serves cleanly. When the version has no predecessor it prints a notice
and exits 0 without writing output.

For a release version both compared sides are the published JARs (the release and its
predecessor), so the report is correct from any checkout — the workflows never need to check out
or build old code. `api-report.sh` still installs the local modules first regardless of the
compared versions: japicmp resolves with `ONE_COMMON_CLASSPATH`, which puts the current project's
own artifact on the shared classpath, and the goal fails to resolve that artifact if it is not
installed. The installed snapshot only backs the classpath; the diff is computed strictly between
the two requested versions.

---

## Site repo: `publish.yml` additions

The source checkout fetches full history (`fetch-depth: 0`) so the generator can enumerate
release tags. After the JavaDoc build, a step runs the generator into a workspace-level staging
directory. The existing `gh-pages` deploy step copies the staged report into `api-changes/$VERSION/`
(and, when `update_latest` is set for a non-dev version, into `api-changes/latest/`), then folds it
into the same amended commit that publishes the docs and JavaDoc.

## Site repo: `backfill-api-reports.yml`

A `workflow_dispatch` one-shot that generates the historical reports (`1.0.0.Beta1`,
`1.0.0.Beta2`, `1.0.0.CR1` by default; overridable via input) and commits them under
`api-changes/<version>/` on `gh-pages`, also seeding `api-changes/latest` from the highest
version. It checks out `hardwood` at `main` (which carries the report tooling that the historical
tags lack) with full history, resolves each version's predecessor from tags, and runs the
generator per version. Because release-to-release comparisons use the published JARs, no
per-version source checkout is needed.

---

## mkdocs nav

`docs/mkdocs.yml` gains a Reference entry linking to `/api-changes/latest/`, beside the existing
`API (JavaDoc): /api/latest/`. The site `publish.yml` rewrites `/api-changes/latest/` to
`/api-changes/$VERSION/` for versioned builds using the same mechanism already applied to
`/api/latest/`.
