# Predicate rule audit

An ad-hoc tool that measures Hardwood's filter predicates against the rule in
[`_designs/PREDICATE_LITERALS.md`](../../_designs/PREDICATE_LITERALS.md), and compares them with
parquet-java 1.17.1 and DuckDB 1.4.4. Run it when the rule changes: a new annotation, a new literal
type, a change to pruning, or an engine upgrade that may change the design's differences table. It is
not part of the build; CI compiles it but does not run it.

## Running

```
tools/predicate-audit/run.sh [report-dir]
```

It needs `.docker-venv` (PyArrow and thriftpy2, from `requirements.txt`) and takes about three
minutes. The report goes to `tools/predicate-audit/target/report` unless a directory is given.

The script:
1. builds Hardwood and the tool under the `predicate-audit` Maven profile;
2. writes the fixtures with parquet-java (`PredicateAudit fixtures`);
3. derives variants by rewriting footers (`derive_fixtures.py`);
4. runs every step (`PredicateAudit audit`).

## What it measures

- **Matrix** (`matrix.tsv`): about 72,000 predicate cells, each read through five paths.
  - **Paths:** the `RowReader` by default, forced onto its record-level path and without metadata filtering; the `ColumnReader` with and without metadata filtering.
  - **Cases:** every literal kind a column takes and some it does not, around stored values, in gaps, past the carrier's range and at type edge cases (`NaN` payloads, signed zeros, sub-unit instants, padded decimals, non-canonical `INT96`), under every operator, `not` form and set form.
  - **Fixture groups**, each in single row group, multiple row groups, dictionary and Bloom filter layouts:

    | Group | What it holds |
    |---|---|
    | `flat` | one column per row of the design's per-column table |
    | `exotic` | `BSON`, `NULL`, `GEOMETRY` |
    | `ts12` | `TIMESTAMP` over `FIXED_LEN_BYTE_ARRAY(12)` at every unit, past the `INT64` nanosecond range; annotated and given bounds by `derive_fixtures.py` |
    | `legacy` | converted types only |
    | `dropped` | annotations the physical type cannot carry |
    | `lowcard` | 40 distinct values, so every chunk is dictionary-encoded |
    | `nested` | nulls at every level of a struct, and a `LIST` |

- **Resolver** (`resolver.tsv`): one literal of every kind under every operator against every column and group, including `VARIANT` leaves, `MAP`s and repeated leaves, with each outcome and message. Also the build-time checks.
- **Consultation** (`consultation.tsv`): evidence that Bloom filters (a zeroed copy loses rows) and dictionaries (the `dev.hardwood.RowGroupFilter` JFR event) are consulted, so agreement is not an artefact of layouts the reader ignores. Each check states the expected outcome, including a Bloom filter left unread for a comparison that is not byte-exact.
- **Engines** (`engines.tsv`): about 50 predicates in their Hardwood, parquet-java `filter2` and DuckDB SQL forms, with every engine's rows next to the rule's. It includes a PyArrow file for `NaN` bounds and nanosecond timestamps.

`summary.md` tallies the matrix per group and layout, and lists every disagreement and every consultation check whose outcome is not the expected one.

A step whose run fails names the log holding its stderr.

## The oracle

`Oracle.java` restates the rule independently of `FilterPredicateResolver`: which predicates a
column refuses, and which stored values the others match. It compares exactly, with nanosecond
`BigInteger`s and `BigDecimal`s, and takes stored values from `Columns.java`, which also feeds the
fixture writer, never from Hardwood's accessors. A disagreement is therefore Hardwood's, the
oracle's or the design's. Read `Oracle.java` against the design before trusting a clean run.

`intersects` decides row groups rather than rows. Its cells are reported under "row groups" and
not checked row by row.

## Extending

- **A column type:** add it to `Columns.Sem`, `Columns.flat()` (or another group), `FixtureWriter.parquetType`, `Oracle.takes` / `unholdable` / `compare` / `fixedWidth` / `byteOrdered` / `unitNanos` and `Cases.literals`. The switches over `Columns.Sem` are exhaustive, so the compiler names each one a new type is missing from.
- **An engine comparison:** add an entry to `EngineComparison.entries()`.
- **A footer shape parquet-java does not write:** rewrite it in `derive_fixtures.py` with `tools/parquet_annotators.py`.

The `parquet-java-compat` shim is not compared: its own `org.apache.parquet` classes cannot share a
classpath with real parquet-java. `ParquetReaderCompatTest` covers it.
