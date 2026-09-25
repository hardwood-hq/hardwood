<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Documentation Structure — Diátaxis Alignment

**Status: Implemented**

Tracked as [#517](https://github.com/hardwood-hq/hardwood/issues/517).

## Overview

The Hardwood prose documentation under `docs/content/` is organized according to the
[Diátaxis](https://diataxis.fr/) model. Diátaxis divides documentation along two axes —
*acquisition vs. application* of skill, and *practical vs. theoretical* knowledge — yielding
four document kinds, each serving one user need and not bleeding into the others:

| Kind | User need | Hardwood section |
|---|---|---|
| **Tutorial** | "Teach me, I'm new" — learning by doing | _Tutorial_ |
| **How-to guide** | "Help me accomplish a goal" | _How-to Guides_ |
| **Reference** | "Tell me the precise facts" | _Reference_ |
| **Explanation** | "Help me understand" | _Concepts_ |

The site navigation in `docs/mkdocs.yml` has one top-level group per kind, plus `Home` and
`Contributing`. A reader's intent maps to exactly one group, and each page stays within its
kind: tutorials do not branch into option matrices, how-to guides do not explain rationale,
reference does not narrate, and explanation does not give step-by-step instructions.

## Navigation

```yaml
nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Tutorial:
    - Read Your First Parquet File: tutorial/first-read.md
  - How-to Guides:
    - Overview: how-to/index.md
    - Read Row by Row: how-to/row-reader.md
    - Read Column by Column: how-to/column-reader.md
    - Filter, Project, Limit, and Split: how-to/query-controls.md
    - Read Multiple Files as One Dataset: how-to/multi-file.md
    - Read into Avro GenericRecords: how-to/avro.md
    - Read from S3: how-to/s3.md
    - Read with the parquet-java API: how-to/compat.md
    - Read Variant Columns: how-to/variant.md
    - Read Geospatial Columns: how-to/geospatial.md
    - Inspect File Metadata: how-to/metadata.md
  - Concepts:
    - How a Parquet File Is Laid Out: concepts/parquet-layout.md
    - The Concurrency Model: concepts/concurrency-model.md
    - RowReader vs. ColumnReader: concepts/reader-models.md
    - Compatibility Philosophy: concepts/compatibility-philosophy.md
  - Reference:
    - Configuration: reference/configuration.md
    - Error Handling: reference/error-handling.md
    - CLI: reference/cli.md
    - Package Structure: reference/packages.md
    - API (JavaDoc): /api/latest/
  - Contributing: contributing.md
  - Release Notes: release-notes.md
```

`Getting Started` (installation, dependency coordinates, optional libraries) stays a top-level
on-ramp directly below `Home`: it is the prerequisite the Tutorial builds on, and a returning
user reaches for it independently of the four content kinds. `Home` is the landing page and is
not itself a Diátaxis quadrant.

File paths mirror the nav buckets, so a page's URL reflects its kind: how-to guides live under
`content/how-to/` (`/how-to/…`), reference under `content/reference/` (`/reference/…`), and the
Tutorial and Concepts pages under `content/tutorial/` and `content/concepts/`. The how-to
overview is `how-to/index.md`, giving the section a `/how-to/` landing URL. The four un-bucketed
top-level tabs (`index.md`, `getting-started.md`, `contributing.md`, `release-notes.md`) stay at
the content root. `edit_uri` (`edit/main/docs/content/`) resolves through the subfolders
unchanged. The generated JavaDoc keeps its own top-level `/api/…` URL — it is published outside
the MkDocs tree by the release pipeline (see [DOCS_MIGRATION.md](DOCS_MIGRATION.md)) and is
linked via absolute `/api/latest/…` URLs, so relocating prose pages does not affect it.

## Tutorial

A single guided lesson, `tutorial/first-read.md`, that takes a newcomer end-to-end with one
guaranteed-to-work path and a concrete artifact at each step. It uses a real, publicly
downloadable dataset (NYC TLC yellow-taxi trip data) so the reader runs working code, not
pseudo-code against an imagined schema.

The lesson arc:

1. Confirm prerequisites and download the sample file.
2. Print the file's schema (introduces metadata introspection and de-risks exact column types).
3. Read rows with `RowReader` and print a handful.
4. Narrow the read with projection, a predicate, and `head(...)`.
5. Sum a column with `ColumnReader` to feel the columnar path.
6. Point onward to the relevant How-to guides and Concepts pages.

Tutorial constraints, enforced on this page: one path only (no "you could also…"), every
snippet runs as written, no rationale or alternatives (those live in Concepts), minimal
prose between steps. Depth and option coverage are deliberately absent — that is the How-to
and Reference job.

## How-to Guides

The task-oriented pages, relocated under `how-to/` and presented as goal-titled guides. Content
is unchanged beyond the deep "which reader and why" discussion in the overview
(`how-to/index.md`), whose rationale moves to the `reader-models.md` explanation page; the
overview keeps the practical decision table and links to the explanation for the reasoning. Each
guide answers "how do I _do_ X" and assumes the reader already knows what they want.

## Reference

Information-oriented, look-it-up material: `configuration.md` (system properties, JVM flags, JFR
events), `error-handling.md` (the exceptions the readers throw and when), `cli.md`,
`packages.md` (the public/internal package map), and the generated JavaDoc. Reference describes
the machinery and states facts; it does not teach or persuade.

The drop-in parquet-java compatibility module is documented as a How-to guide (`how-to/compat.md`,
"Read with the parquet-java API") rather than Reference: its content is task-oriented usage of the
shim API, and the rationale for its stricter semantics lives in
`concepts/compatibility-philosophy.md`.

`packages.md` holds the package-by-package table that previously sat on the landing page. It is
a lookup of which packages are public versus internal, paired with the JavaDoc it links into —
reference material, not landing-page orientation.

`release-notes.md` is a top-level nav entry in the rightmost position, not nested under
Reference: it is a chronological changelog rather than look-it-up reference material, and a
returning user reaches for "what changed" independently of the four content kinds.

## Concepts

The Diátaxis *explanation* quadrant, surfaced in the nav as **Concepts**. Understanding-oriented
pages that give the context the other three kinds assume. Four pages:

- **`concepts/parquet-layout.md`** — the file → row group → column chunk → page → dictionary
  hierarchy, and why the structure enables projection, predicate pushdown, and parallel decode.
  This is the mental model every How-to guide silently relies on; it makes the 2 GB
  column-chunk limit and the row-group split convention legible.
- **`concepts/concurrency-model.md`** — the shared thread pool, parallel page decode, the
  assembly pipeline, and cross-file prefetching; how `Hardwood` / `HardwoodContext` own the pool
  and what is and is not thread-safe for callers.
- **`concepts/reader-models.md`** — why two reader APIs exist, the row-materialization vs.
  columnar-batch trade-off, and the layer model that underlies `ColumnReader`. This is the
  "why" behind the decision table in the How-to overview (`how-to/index.md`).
- **`concepts/compatibility-philosophy.md`** — why Hardwood reads what parquet-java reads but
  applies stricter semantics in specific places (uniform SQL three-valued logic for `notEq`,
  validation-on-open for multi-file schema), and how to opt back into the looser behavior.

Concepts pages do not contain step-by-step instructions or exhaustive option tables; they
link to the How-to and Reference pages that do.

## Out of scope

- No change to the build, deploy, or versioning pipeline — see [DOCS_MIGRATION.md](DOCS_MIGRATION.md).
- The JavaDoc (`/api/latest/`) remains the authoritative class-level reference; the prose
  Reference section links to it rather than duplicating it.
