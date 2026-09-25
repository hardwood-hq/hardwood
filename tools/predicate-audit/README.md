# Predicate rule audit

A tool that measures Hardwood's filter predicates against the rule in
[`_designs/PREDICATE_MODEL.md`](../../_designs/PREDICATE_MODEL.md), and compares them with
parquet-java 1.18.1 and DuckDB 1.4.4. The PR build's `predicate-audit` job runs it for every change to
`core/src/main`, the tool, `tools/parquet_annotators.py`, `requirements.txt`, the root, `core` or
`test-bom` POM (the last pins the parquet-java and DuckDB versions) or `.github/workflows/pr-build.yml`,
and uploads the report as the `predicate-audit-report` artifact.

## Running

```
tools/predicate-audit/run.sh [report-dir]
```

It needs `.docker-venv` (PyArrow and thriftpy2, from `requirements.txt`) and takes under a
minute, most of it the build. The report goes to `tools/predicate-audit/target/report` unless a directory is given.

The script:
1. builds Hardwood and the tool under the `predicate-audit` Maven profile;
2. writes the fixtures with parquet-java (`PredicateAudit fixtures`);
3. derives variants by rewriting footers (`derive_fixtures.py`);
4. runs every step (`PredicateAudit audit`).

## What it measures

- **Matrix** (`matrix.tsv`): about 72,000 predicate cells, each read through five paths.
  - **Paths:** the `RowReader` by default, forced onto its record-level path and without metadata filtering; the `ColumnReader` with and without metadata filtering. The three `RowReader` paths project `__row__` alone and let the reader decode the predicate's own columns beside it, so a cell stands up a worker per predicate column rather than one per column of the fixture. The `ColumnReader` paths read `__row__` the same way, so every path is an augmented-projection read and none decodes a column the predicate does not name.
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
    | `nested` | nulls at every level of a struct, a `LIST`, and a struct required all the way down beside a top-level column named like one of its leaves |

- **Resolver** (`resolver.tsv`): one literal of every kind under every operator against every column and group, including `VARIANT` leaves, `MAP`s and repeated leaves, with each outcome and message. Also the build-time checks, each with the outcome the rule gives it.
- **Consultation** (`consultation.tsv`): evidence that Bloom filters (a zeroed copy loses rows) and dictionaries (the `dev.hardwood.RowGroupFilter` JFR event) are consulted, so agreement is not an artefact of layouts the reader ignores. Each check states the expected outcome, including a Bloom filter left unread for a comparison that is not byte-exact.
- **Accessor round-trip** (`roundtrip.tsv`): every value an accessor returns for the rows around each column's edge cases, passed back as an `eq` literal, is expected to match its own row. The exception the rule states is a `String` read from bytes that are not well-formed UTF-8. This is the one step that reads values through Hardwood's accessors.
- **Engines** (`engines.tsv`): about 50 predicates in their Hardwood, parquet-java `filter2` and DuckDB SQL forms, with every engine's rows next to the rule's. It includes a PyArrow file for `NaN` bounds and nanosecond timestamps.

`summary.md` tallies the matrix per group and layout, and lists every disagreement, every consultation check whose outcome is not the expected one and every round-trip that does not.

A step whose run fails names the log holding its stderr.

## The baseline

`baseline.tsv` lists the findings a run is expected to produce, and `run.sh` exits with status 3
when the run's findings differ from it in either direction: a finding it does not list, or a listed
one the run no longer produces. `summary.md` then opens with the drift, and `findings.tsv` holds
every finding of the run in baseline form.

A finding is a line starting with its step:

| Step | Line | Expected in a clean tree |
|---|---|---|
| `matrix`, `resolver`, `consultation`, `roundtrip` | the step's disagreement line | never |
| `roundtrip-throw` | file, row, column and accessor of a logical read that threw | a stored value its logical type cannot represent |
| `engine` | predicate id and the answers of Hardwood, parquet-java and DuckDB | a row of the design's differences table |

Change the baseline only with the change that moves a finding, and give each entry a `#` comment
naming the design row or the defect behind it. A disagreement with the rule is fixed, not
baselined.

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

## Probing by hand

- The compat module's own `org.apache.parquet` classes shadow `parquet-column`, so real parquet-java needs a classpath without the compat module.
- `FilterApi.in` with `Set.of` throws a `NullPointerException`; use a `HashSet`.
- DuckDB cannot open a file holding a `BSON` column.
