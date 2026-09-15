---
name: predicate-audit
description: >-
  Audit Hardwood's filter predicate literal rule (`_designs/PREDICATE_LITERALS.md`, epic #1198) end
  to end with fresh eyes and fresh measurements: logic, completeness, spec correctness, conciseness,
  consistency across design, docs, code, tests and release notes, and every difference from DuckDB
  and parquet-java. Use when the user types /predicate-audit, asks to audit or re-run the predicate
  analysis, or after a change to predicate literals, operators, pruning or annotations.
---

Audit the predicate literal rule as it stands. Treat it as a proposal to challenge, not to confirm:
the design, the tests and any earlier audit were written by people who believed them correct.

## Guardrails

- **Produce findings only.** Do not implement fixes, file or edit issues, comment on GitHub, change the design doc, commit or push unless asked.
- Probes and scratch fixtures go in a worktree under `.claude/worktrees/`, removed when done; `/workspace` stays on its branch.
- Every Maven command gets a 180 s timeout.
- Label anything you did not measure as untested. Do not repeat an earlier audit's claim as fact; re-measure it.

## Inputs

- `_designs/PREDICATE_LITERALS.md`: the rule, the per-column table, and the parquet-java / DuckDB differences table.
- `docs/content/reference/query-controls.md`, `reference/accessors.md`, `how-to/query-controls.md`, and the current section of `docs/content/release-notes.md`.
- `FilterPredicate` (factories and JavaDoc), `FilterPredicateResolver`, `ColumnLiterals`, `TextColumns`, and the tests `PredicatePathAgreementTest`, `FilterPredicateResolverTest`, `FilterPredicateTest`, `ParquetReaderCompatTest`.
- Epic #1198 and its sub-issues (`gh issue view`) for the decision history, and any open predicate issues (`gh issue list --search predicate`).
- An earlier audit report under `_reviews/` if one exists locally, for comparison **after** you reach your own conclusions.

## 1. Assess the rule

- **Logic:** does every row of the per-column table follow from the rules? Check the rules against each other, and every example in the design and reference against the code.
- **Completeness:** enumerate the space and name what the design does not decide. Cover:
  - every physical type × annotation, including legacy converted types, `TIME` / `TIMESTAMP` by unit and `isAdjustedToUTC`, and `INT(n)` by sign;
  - groups, `VARIANT` leaves, and columns inside structs and below repeated paths;
  - every literal kind and operator, including `not`, the set form and its negation, `isNull`, `intersects` and null literals.
- **Correctness:** check each "compared as" and "defines an order" claim against parquet-format, fetched fresh from apache/parquet-format (`LogicalTypes.md`, `parquet.thrift`, `Geospatial.md`) at the latest release tag and at master. Note which claims depend on which.
- **Conciseness:** which rules are redundant or special-cased? Is there a simpler rule deciding the same table?
- **Consistency:** design, reference docs, JavaDoc, release notes and tests must say the same thing.

## 2. Measure

Run the audit tool (see `tools/predicate-audit/README.md`):

```
tools/predicate-audit/run.sh <report-dir>
```

- **Before trusting it,** read `tools/predicate-audit/src/main/java/dev/hardwood/tools/predicateaudit/Oracle.java` against the design. The oracle is the rule restated; if the rule changed since it was written, update the oracle first, or its agreement means nothing.
- **When the change under audit adds a column type, literal or file shape the tool does not cover,** extend the tool as its README describes, and run it on the base commit as well as the change, so every disagreement can be attributed.
- **Explain every disagreement** in `summary.md`: a Hardwood defect, an oracle defect, or a design gap.
- **For each `engines.tsv` row where an engine differs,** classify the cause as a rule decision (name it), an engine defect, or a Hardwood defect. Compare against the design's differences table: rows that no longer reproduce, and differences it lacks.
- **Measure what the tool does not cover in a probe of your own:**
  - the `parquet-java-compat` shim, which cannot share a classpath with real parquet-java;
  - files from other writers;
  - anything the change introduces.

Classpath traps when probing by hand:
- The compat module's own `org.apache.parquet` classes shadow `parquet-column`, so real parquet-java needs a classpath without the compat module.
- `FilterApi.in` with `Set.of` throws a `NullPointerException`; use a `HashSet`.
- DuckDB cannot open a file holding a `BSON` column.

## 3. Report

Write `_reviews/predicate-rule-audit-<yyyy-mm-dd>.md` in PR-review format, with one `[ ]` checkbox per actionable item, grouped API > implementation > docs > tests. Back each item with measured evidence, or label it untested. Flag real defects and gaps, not taste. Include:

- a Summary block: What, Why, Assessment;
- decisions that need the maintainer, each with options and a recommendation;
- the engine differences, with causes, and what changed against the design's table;
- the report directory and commit the tool ran against, so the run can be repeated;
- if an earlier audit report exists, what it found that still stands, what it missed, and anything it called done that is not.

Then hand back a summary of the top findings and the decisions needed.
