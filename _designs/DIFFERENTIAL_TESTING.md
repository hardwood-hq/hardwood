# Design: differential testing against a query oracle

**Status: Proposed.** Tracking issue: #548 (1.0 public API hardening). Companion to
the enforced interaction matrix `core/src/test/java/dev/hardwood/BuilderCombinationTest.java`
and the model in `_designs/ROW_SELECTION_SEMANTICS.md`.

## Goal

Catch reader bugs — both *composition* defects (a control correct in isolation that
misbehaves once combined with a filter, e.g. #538) and *spec-conformance* defects (decode
edge cases, e.g. #537) — automatically, by reading the same Parquet file through hardwood and
through a trusted reference and diffing the results across many query shapes.

The interaction matrix pins which builder combinations are *legal* and that they build and
iterate. Differential testing pins that the legal combinations return the **right rows** —
the half the matrix deliberately defers.

## Architecture

Three pieces:

- **Oracle: DuckDB via JDBC, inside JUnit.** DuckDB's SQL `WHERE` / `LIMIT` / `OFFSET`
  semantics are exactly the logical model in `ROW_SELECTION_SEMANTICS.md`, so each builder
  query maps one-to-one to a SQL statement over `read_parquet('<file>')`. The `duckdb_jdbc`
  jar runs in-process; no external process or Python at test time.
- **Corpus: pyarrow-generated fixtures.** hardwood is a reader, not a writer, so files are
  produced ahead of time by `tools/simple-datagen.py` and checked in. The corpus is
  deterministic (fixed seed) and byte-stable, consistent with the pinned-pyarrow rule —
  *fuzzing happens over query plans, not over files at test time.*
- **Harness: `DifferentialReadTest`.** For each `(file, query)` pair it executes the query
  through hardwood's builder and through the translated SQL, then diffs.

## File order and the `__row__` column

`head` / `tail` / `skip` are **file-order** operations; SQL `LIMIT` / `OFFSET` without an
`ORDER BY` is unordered, and DuckDB may parallelize and reorder its scan. To make the oracle
deterministic and file-order-aligned, every generated file carries a synthetic monotonic
column **`__row__` = physical row position** (`0..N-1`). File order is then recoverable as
`ORDER BY __row__`, and the control mappings are exact:

| hardwood                       | DuckDB SQL                                                              |
|--------------------------------|-------------------------------------------------------------------------|
| `filter(p).head(n)`            | `... WHERE p ORDER BY __row__ LIMIT n`                                   |
| `filter(p).skip(m).head(n)`    | `... WHERE p ORDER BY __row__ LIMIT n OFFSET m`                          |
| `filter(p).skip(m)`            | `... WHERE p ORDER BY __row__ OFFSET m`                                  |
| `filter(p).tail(n)`            | `SELECT * FROM (... WHERE p ORDER BY __row__ DESC LIMIT n) ORDER BY __row__` |
| `filter(rgp:byteRange).…`      | `... WHERE __row__ BETWEEN <lo> AND <hi> …` (range derived from row-group metadata) |

The mappings describe the intended end state. `filter` + `skip` (and `filter` + `skip` + `head`)
are currently rejected at `build()` (tracked by #541), and `filter` + `tail` is rejected as a
non-goal for 1.0 (#542); the matrix asserts those rejections directly until each lands.

`__row__` is also the comparison key (below). It makes these *instrumented* fixtures, distinct
from the real-world fixtures already checked in — those continue to cover "decode an actual
file"; the differential corpus covers "are the right rows selected and decoded."

## Query translation

A small translation layer maps builder options to SQL:

- `filter(FilterPredicate)` → `WHERE` clause (per-operator: `gt`/`lt`/`eq`/range/…).
- `head(n)` → `LIMIT n`; `skip(m)` → `OFFSET m`; `tail(n)` → `ORDER BY __row__ DESC LIMIT n`
  wrapped to restore ascending order.
- `filter(RowGroupPredicate.byteRange(start, end))` → the `__row__` interval covered by the
  row groups whose midpoint falls in `[start, end)`, computed from the file's row-group
  metadata (the same midpoint rule hardwood applies).
- `projection(...)` → the `SELECT` column list (always including `__row__`).

## Comparison

Two layers, cheapest-and-highest-signal first:

1. **Row identity.** Project `__row__` on both sides and compare the ordered sequence. This
   single check catches the entire #538 class — wrong rows, wrong count, wrong order under
   composition — with no value-type plumbing.
2. **Values.** For each projected column, compare values row-by-row with type-aware
   normalization (decimal scale, `byte[]` equality, timestamp instants, float exactness with
   explicit `NaN` handling — a reader must reproduce stored values bit-for-bit, so float
   comparison is exact, not epsilon).

## Relationship to the interaction matrix

`DifferentialReadTest` draws its query shapes from the same `Combo` templates as
`BuilderCombinationTest`, parameterized with concrete predicate thresholds and limit values.
The matrix decides *legal vs rejected*; the differential harness verifies *the rows* for the
legal cells. A combination is therefore covered end to end: it appears once in the matrix
(builds vs rejects) and again here (returns the correct rows).

## Phasing

- **P1.** Flat schema; required and nullable numeric/string columns; single- and multi-row-group
  files; `filter` / `head` / `tail` / `skip` / `projection` and their legal combinations;
  row-identity comparison via `__row__`. Sufficient to catch the #538 class. (Combinations the
  matrix rejects at `build()` are pinned there, not here.)
- **P2.** Full per-column value comparison across all physical and logical types (decimal,
  timestamp, time, binary, uuid); `byteRange` via metadata→`__row__` translation; filter
  null-semantics pinned (see below).
- **P3.** Seeded random query fuzzing with failure shrinking to a minimal reproducer; broader
  corpus (compression, encodings, value distributions, nested types); pyarrow as an optional
  second oracle cross-checking DuckDB.

## Decisions

- **DuckDB is the reference of truth.** A disagreement is investigated as a hardwood defect
  first; a residue are semantic questions that must be pinned rather than bugs.
- **Filter null-semantics is one such question.** SQL `WHERE x > 5` excludes nulls under
  three-valued logic; hardwood's `FilterPredicate` must define the same, and the harness forces
  that definition to be explicit rather than latent. Pinned in P2.
- **Test weight is isolated.** The `duckdb_jdbc` native library and the corpus add build weight,
  so the harness carries the `differential` JUnit tag. A fast inner-loop build can opt out via
  `-DexcludedGroups=differential`; no Surefire profile is wired by default (consistent with the
  existing `large` tag), keeping the Maven surface small.

## Non-goals

- **Performance/throughput comparison.** This harness asserts *correctness of results only*,
  never timing or bytes-read (those are covered by the performance-test profile).
- **Replacing the real-world fixtures.** The instrumented corpus is additive; the existing
  checked-in files remain the conformance baseline for decoding untouched real files.
- **Generating files at test time.** Files are generated offline by `simple-datagen.py` and
  checked in; the test run does not invoke pyarrow.
