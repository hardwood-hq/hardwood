# Record Filter Fusion (Stage 4) Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Work top-to-bottom; each task ends in a commit so the branch stays bisectable.

**Goal:** Implement arity-2 fusion so compound `And`/`Or` matchers carry no inner virtual call, defeating inner-site megamorphism on the record-filter hot path.

**Architecture:** Add two internal helpers (`RecordFilterFusion`, `RecordFilterFusionIndexed`) that return per-tuple synthetic-class matchers for eligible arity-2 leaves; wire them into `RecordFilterCompiler` ahead of the existing fixed-arity matchers; gate behind a `-Dhardwood.recordfilter.fusion` system property so benchmarks can A/B the contribution. See [_designs/RECORD_FILTER_FUSION.md](RECORD_FILTER_FUSION.md) for the spec and eligibility matrix.

**Tech Stack:** Java 25, Maven wrapper, JUnit 5 + AssertJ + parameterized tests, JMH (perf-test profile), the existing `RecordFilterEvaluator` legacy oracle for equivalence.

---

## File Structure

| Path | Disposition |
|------|-------------|
| `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java` | New — name-keyed AND + OR fusion |
| `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusionIndexed.java` | New — indexed AND + OR fusion |
| `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` | Modify — wire fusion + disable flag |
| `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java` | New — three-way equivalence (fused vs name-keyed-generic vs legacy oracle) |
| `performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMegamorphicBenchmark.java` | New — JMH micro |
| `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterMegamorphicEndToEndTest.java` | New — e2e benchmark on 10M-row Parquet |
| `_designs/RECORD_FILTER_FUSION.md` | Modify — fill `Results` section after benchmarks run |

The two fusion helpers are package-private. Nothing public-facing changes.

---

## Conventions Used Throughout

- **Exact build command:** `./mvnw -pl core -am verify -DskipITs` for fast iteration on the core module. Always wrap with `timeout 180` per CLAUDE.md.
- **No `var`** — required by CLAUDE.md.
- **Markdown JavaDoc (`///`)** for every new public-or-package class and non-trivial method. Backtick-fenced code blocks; `[ClassName]` reference links.
- **Commits begin with `#193`** (the active GitHub issue for this work).
- **Co-Authored-By trailer is never added** (CLAUDE.md).
- **Sealed-type switch coverage** — `compile(...)`'s switch over `ResolvedPredicate` must remain exhaustive. Don't introduce a default arm.

---

## Chunk 1: Foundation — Disable Flag and Empty Fusion Skeletons

This chunk gets the wiring in place and lets the existing test suite continue to pass before any fusion logic exists. It produces a "fusion off" build that is byte-equivalent to the current behavior.

### Task 1.1: Add the `FUSION_ENABLED` flag to `RecordFilterCompiler`

**Files:**
- Modify: `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java`

- [ ] **Step 1: Add the static flag.**

Insert after the `IN_LIST_BINARY_SEARCH_THRESHOLD` constant near the top of the class:

```java
/// Compile-time fusion of arity-2 AND/OR leaves into per-tuple synthetic
/// classes. Default true; set `-Dhardwood.recordfilter.fusion=false` to
/// route through the generic [And2Matcher] / [Or2Matcher] for benchmark
/// A/B comparison. Read once at class init.
private static final boolean FUSION_ENABLED =
        !"false".equalsIgnoreCase(System.getProperty("hardwood.recordfilter.fusion"));
```

- [ ] **Step 2: Verify the build still passes.**

Run: `timeout 180 ./mvnw -pl core -am verify -DskipITs`
Expected: `BUILD SUCCESS` — nothing reads the flag yet.

- [ ] **Step 3: Commit.**

```bash
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java
git commit -m "#193 Add fusion-disable flag to RecordFilterCompiler"
```

### Task 1.2: Scaffold `RecordFilterFusion` (name-keyed) — null-returning stubs

**Files:**
- Create: `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java`

- [ ] **Step 1: Create the file with empty entry points.**

```java
/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FieldPath;
import dev.hardwood.schema.FileSchema;

/// Arity-2 AND/OR fusion: when both children of a compound are simple
/// primitive leaves of compatible types, the compiler emits a single
/// fused matcher whose body inlines both comparisons as primitive
/// bytecode operations — no inner virtual call in the row loop.
///
/// Each `(typeA, opA, typeB, opB, connective)` produces a distinct
/// synthetic lambda class, so the fused matcher's body cannot be
/// polluted by other shapes and stays fast even when the generic
/// [RecordFilterCompiler] `And2Matcher` / `Or2Matcher` call site goes
/// megamorphic.
///
/// See [_designs/RECORD_FILTER_FUSION.md](../../../../../../../_designs/RECORD_FILTER_FUSION.md)
/// for the eligibility matrix and semantics.
final class RecordFilterFusion {

    private static final String[] EMPTY_PATH = new String[0];

    private RecordFilterFusion() {
    }

    /// Returns a fused AND matcher for the two children, or null if the
    /// pair is not eligible for fusion (caller falls back to the generic
    /// [RecordFilterCompiler] `And2Matcher`).
    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        return null;
    }

    /// Returns a fused OR matcher for the two children, or null if the
    /// pair is not eligible.
    static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
        return null;
    }

    // Path resolution helpers, mirroring RecordFilterCompiler's private versions.

    /// Walks the row through the captured intermediate struct path.
    /// Returns null if any intermediate struct is null. For top-level
    /// columns `path` is empty and the row itself is returned.
    static StructAccessor resolve(StructAccessor row, String[] path) {
        StructAccessor current = row;
        for (int i = 0; i < path.length; i++) {
            String segment = path[i];
            if (current.isNull(segment)) {
                return null;
            }
            current = current.getStruct(segment);
        }
        return current;
    }

    static String[] pathSegments(FileSchema schema, int columnIndex) {
        List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();
        if (elements.size() <= 1) {
            return EMPTY_PATH;
        }
        String[] out = new String[elements.size() - 1];
        for (int i = 0; i < out.length; i++) {
            out[i] = elements.get(i);
        }
        return out;
    }

    static String leafName(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().leafName();
    }
}
```

> Note: confirm the import for `FieldPath` is unused and drop it before saving — kept here as a hint that you may need `schema.FieldPath` later if you split helpers out. Actually the helpers above only reach into `fieldPath()` returns; remove the unused import to satisfy the project lint.

- [ ] **Step 2: Compile.**

Run: `timeout 180 ./mvnw -pl core -am compile`
Expected: `BUILD SUCCESS`. The class is unused at this point.

- [ ] **Step 3: Commit.**

```bash
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java
git commit -m "#193 Scaffold RecordFilterFusion with null-returning stubs"
```

### Task 1.3: Scaffold `RecordFilterFusionIndexed` — null-returning stubs

**Files:**
- Create: `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusionIndexed.java`

- [ ] **Step 1: Create the file with the indexed-overload entry points.**

```java
/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.internal.reader.IndexedAccessor;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Indexed-access counterpart to [RecordFilterFusion]. Used when the
/// reader implements [IndexedAccessor] (i.e. `FlatRowReader`) and both
/// leaves operate on top-level columns. The cast is safe by construction:
/// the projection-aware compile entry point in [RecordFilterCompiler] is
/// only invoked from readers that guarantee the row implements
/// [IndexedAccessor].
final class RecordFilterFusionIndexed {

    private RecordFilterFusionIndexed() {
    }

    static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, ProjectedSchema projection) {
        return null;
    }

    static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b,
            FileSchema schema, ProjectedSchema projection) {
        return null;
    }

    static boolean isTopLevel(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().elements().size() <= 1;
    }
}
```

- [ ] **Step 2: Compile and commit.**

```bash
timeout 180 ./mvnw -pl core -am compile
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusionIndexed.java
git commit -m "#193 Scaffold RecordFilterFusionIndexed with null-returning stubs"
```

### Task 1.4: Wire fusion into `RecordFilterCompiler.compileAnd` and `compileOr`

**Files:**
- Modify: `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java`

- [ ] **Step 1: Update `compileAnd` to consult fusion first.**

Replace the existing body with:

```java
private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema,
        ProjectedSchema projection) {
    if (FUSION_ENABLED && children.size() == 2) {
        if (projection != null) {
            RowMatcher fused = RecordFilterFusionIndexed.tryFuseAnd2(
                    children.get(0), children.get(1), schema, projection);
            if (fused != null) {
                return fused;
            }
        }
        RowMatcher fused = RecordFilterFusion.tryFuseAnd2(
                children.get(0), children.get(1), schema);
        if (fused != null) {
            return fused;
        }
    }
    RowMatcher[] compiled = compileAll(children, schema, projection);
    return switch (compiled.length) {
        case 1 -> compiled[0];
        case 2 -> new And2Matcher(compiled[0], compiled[1]);
        case 3 -> new And3Matcher(compiled[0], compiled[1], compiled[2]);
        case 4 -> new And4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
        default -> new AndNMatcher(compiled);
    };
}
```

- [ ] **Step 2: Update `compileOr` symmetrically against `tryFuseOr2`.** Same shape.

- [ ] **Step 3: Run the existing test suite.**

Run: `timeout 180 ./mvnw -pl core -am test -Dtest='RecordFilter*'`
Expected: all existing record-filter tests pass — fusion still returns null for everything, so nothing changes.

- [ ] **Step 4: Commit.**

```bash
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java
git commit -m "#193 Wire RecordFilterFusion lookup into compileAnd/compileOr"
```

---

## Chunk 2: Equivalence Test Harness

Before writing any fusion lambdas, create the test harness so each combo can be implemented red-green-refactor against the legacy oracle.

### Task 2.1: Create `RecordFilterFusionTest` skeleton

**Files:**
- Create: `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java`

- [ ] **Step 1: Create the file with the three-way equivalence helper.**

The helper compiles the predicate three ways: legacy oracle, name-keyed compile (no projection), and indexed compile (with projection). All three must agree. The test then asserts the agreed value matches the parametrized expected value.

```java
/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.hardwood.internal.reader.IndexedAccessor;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Three-way equivalence: fused matcher (via [RecordFilterCompiler] with
/// fusion enabled) must agree with the name-keyed generic matcher (via the
/// existing `And2Matcher` / `Or2Matcher` path) and with the legacy
/// [RecordFilterEvaluator.matchesRow] oracle for every fused
/// (combo × connective × opA × opB × same/diff column × null / non-null)
/// tuple.
class RecordFilterFusionTest {

    /// Compiles `predicate` three ways and asserts all agree with `expected`.
    /// `genericMatcher` is built by walking the predicate manually so that
    /// fusion is bypassed regardless of the FUSION_ENABLED flag — this
    /// keeps the test independent of the system property at test time.
    static void assertEquivalent(ResolvedPredicate predicate, StructAccessor row,
            FileSchema schema, ProjectedSchema projection, boolean expected) {
        boolean legacy = RecordFilterEvaluator.matchesRow(predicate, row, schema);
        boolean fusedName = RecordFilterCompiler.compile(predicate, schema).test(row);
        boolean fusedIndexed = RecordFilterCompiler.compile(predicate, schema, projection).test(row);
        assertThat(fusedName).as("legacy/fused-name disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(fusedIndexed).as("legacy/fused-indexed disagreed for %s", predicate).isEqualTo(legacy);
        assertThat(legacy).as("legacy oracle disagreed with expected for %s", predicate).isEqualTo(expected);
    }

    static ProjectedSchema projectAll(FileSchema schema) {
        return ProjectedSchema.create(schema, ColumnProjection.all());
    }

    // Schema helpers added per type-combo task — see RecordFilterIndexedTest
    // for the existing pattern (e.g. twoLongSchema, twoIntSchema).
    // Stub-row helpers added per type-combo task — see RecordFilterIndexedTest's
    // BaseIndexedRow / TwoLongIndexedRow for the pattern.
}
```

- [ ] **Step 2: Verify the file compiles.**

Run: `timeout 180 ./mvnw -pl core -am test-compile`
Expected: `BUILD SUCCESS`. Test class has no test methods yet — JUnit ignores it.

- [ ] **Step 3: Commit.**

```bash
git add core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java
git commit -m "#193 Add RecordFilterFusionTest equivalence harness"
```

---

## Chunk 3: First Combo — `long + long` AND (Name-Keyed)

This is the canonical reference combo. Get it fully tested and committed before replicating the pattern across other combos. Two stub-row classes (`TwoLongIndexedRow` from `RecordFilterIndexedTest`) inspire the helpers used here.

### Task 3.1: Add `long + long` test cases

**Files:**
- Modify: `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java`

- [ ] **Step 1: Copy `TwoLongIndexedRow` and `BaseIndexedRow` into the new test class as private nested classes.** Same shape as `RecordFilterIndexedTest.java:213-323`. Keeping them local keeps the test self-contained.

- [ ] **Step 2: Add a `twoLongSchema(String, String)` helper.** Same as `RecordFilterIndexedTest.java:173-180`.

- [ ] **Step 3: Add a parameterized test for `long+long` AND, different columns.**

```java
@ParameterizedTest(name = "long+long AND diff: a {0} {1} AND b {2} {3} on a={4},b={5} → {6}")
@MethodSource("longLongAndDiffCases")
void longLongAndDiff(Operator opA, long vA, Operator opB, long vB,
        long aVal, long bVal, boolean expected) {
    FileSchema schema = twoLongSchema("a", "b");
    ProjectedSchema projection = projectAll(schema);
    TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", bVal, false);
    ResolvedPredicate p = new ResolvedPredicate.And(List.of(
            new ResolvedPredicate.LongPredicate(0, opA, vA),
            new ResolvedPredicate.LongPredicate(1, opB, vB)));
    assertEquivalent(p, row, schema, projection, expected);
}

static Stream<Arguments> longLongAndDiffCases() {
    // Cover every (opA, opB) pair × pass/fail. 36 op pairs × 2 outcomes = 72 cases.
    // Build by computing expected from the operators in code so we don't
    // hand-code 72 boolean answers (which would just duplicate the impl
    // logic and make refactors painful).
    return cartesian(Operator.values(), Operator.values()).flatMap(pair -> {
        Operator opA = pair[0];
        Operator opB = pair[1];
        return Stream.of(
                Arguments.of(opA, 50L, opB, 100L, 50L, 100L,
                        evalLong(opA, 50L, 50L) && evalLong(opB, 100L, 100L)),
                Arguments.of(opA, 50L, opB, 100L, 49L, 99L,
                        evalLong(opA, 49L, 50L) && evalLong(opB, 99L, 100L)));
    });
}

private static Stream<Operator[]> cartesian(Operator[] left, Operator[] right) {
    return Arrays.stream(left).flatMap(l ->
            Arrays.stream(right).map(r -> new Operator[] { l, r }));
}

/// Mirrors `RecordFilterEvaluator`'s long-leaf semantics for use as the
/// expected-value generator: applies `op` between `actual` and `target`.
private static boolean evalLong(Operator op, long actual, long target) {
    return switch (op) {
        case EQ -> actual == target;
        case NOT_EQ -> actual != target;
        case LT -> actual < target;
        case LT_EQ -> actual <= target;
        case GT -> actual > target;
        case GT_EQ -> actual >= target;
    };
}
```

- [ ] **Step 4: Add a null-leaf case.**

```java
@Test
void longLongAndDiff_nullLeafShortCircuits() {
    FileSchema schema = twoLongSchema("a", "b");
    ProjectedSchema projection = projectAll(schema);
    ResolvedPredicate p = new ResolvedPredicate.And(List.of(
            new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
            new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
    // a=null → false regardless of b
    assertEquivalent(p, new TwoLongIndexedRow("a", 5L, true, "b", 100L, false), schema, projection, false);
    // b=null → false even when a passes
    assertEquivalent(p, new TwoLongIndexedRow("a", 5L, false, "b", 100L, true), schema, projection, false);
}
```

- [ ] **Step 5: Run the tests. They must pass already** because the fusion stubs return null and the generic `And2Matcher` is correct. The point of running now is to confirm the harness compiles and the cases are sensible.

Run: `timeout 180 ./mvnw -pl core -am test -Dtest='RecordFilterFusionTest'`
Expected: all green.

- [ ] **Step 6: Commit.**

```bash
git add core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java
git commit -m "#193 Add long+long AND diff-column equivalence cases"
```

### Task 3.2: Implement `long + long` AND fusion in `RecordFilterFusion`

**Files:**
- Modify: `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java`

- [ ] **Step 1: Update `tryFuseAnd2` to dispatch by type.** Add the `long+long` arm:

```java
static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema) {
    if (a instanceof ResolvedPredicate.LongPredicate la
            && b instanceof ResolvedPredicate.LongPredicate lb) {
        return fuseLongLongAnd(la, lb, schema);
    }
    return null;
}
```

- [ ] **Step 2: Add `fuseLongLongAnd` that splits same-column from different-column.**

```java
private static RowMatcher fuseLongLongAnd(ResolvedPredicate.LongPredicate la,
        ResolvedPredicate.LongPredicate lb, FileSchema schema) {
    String[] pathA = pathSegments(schema, la.columnIndex());
    String nameA = leafName(schema, la.columnIndex());
    if (la.columnIndex() == lb.columnIndex()) {
        return longRangeAnd(pathA, nameA, la.op(), la.value(), lb.op(), lb.value());
    }
    String[] pathB = pathSegments(schema, lb.columnIndex());
    String nameB = leafName(schema, lb.columnIndex());
    return longLongAndDiff(pathA, nameA, la.op(), la.value(), pathB, nameB, lb.op(), lb.value());
}
```

- [ ] **Step 3: Implement `longLongAndDiff` — the exemplar for every diff-column AND fusion in this codebase.**

This is the canonical shape. All other combos (int+int, double+double, binary+binary, cross-types) follow the same nested-switch structure with the type-appropriate getter and comparator. Show one full block here so the others can be replicated mechanically.

```java
private static RowMatcher longLongAndDiff(String[] pA, String nA, Operator opA, long vA,
        String[] pB, String nB, Operator opB, long vB) {
    return switch (opA) {
        case EQ -> switch (opB) {
            case EQ -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB;
            };
            case NOT_EQ -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) != vB;
            };
            case LT -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) < vB;
            };
            case LT_EQ -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) <= vB;
            };
            case GT -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) > vB;
            };
            case GT_EQ -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA == null || sA.isNull(nA) || sA.getLong(nA) != vA) return false;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) >= vB;
            };
        };
        // ... 5 more cases for opA: NOT_EQ, LT, LT_EQ, GT, GT_EQ.
        // Each has 6 inner cases for opB. The opA condition negates the
        // pass-through check (e.g. NOT_EQ: `sA.getLong(nA) == vA` rejects;
        // LT: `>= vA` rejects; LT_EQ: `> vA` rejects; GT: `<= vA` rejects;
        // GT_EQ: `< vA` rejects).
        // Total: 6 outer × 6 inner = 36 lambdas.
        default -> throw new AssertionError("filled in below — see comment");
    };
}
```

Replace the `default ->` arm with the remaining 5 outer cases (`NOT_EQ`, `LT`, `LT_EQ`, `GT`, `GT_EQ`), each with 6 inner cases. The structure is mechanical — apply this rule for the first-leaf rejection check (which short-circuits to `false` when the leaf fails):

| `opA` | First-leaf rejection condition |
|------|--------------------------------|
| `EQ` | `sA.getLong(nA) != vA` |
| `NOT_EQ` | `sA.getLong(nA) == vA` |
| `LT` | `sA.getLong(nA) >= vA` |
| `LT_EQ` | `sA.getLong(nA) > vA` |
| `GT` | `sA.getLong(nA) <= vA` |
| `GT_EQ` | `sA.getLong(nA) < vA` |

The second-leaf check is the operator's positive form (`==`, `!=`, `<`, `<=`, `>`, `>=`).

- [ ] **Step 4: Implement `longRangeAnd` — same-column variant.**

The same-column variant resolves the path and loads the value once, then applies both ops to the loaded primitive. Significantly more compact:

```java
private static RowMatcher longRangeAnd(String[] p, String n, Operator opA, long vA,
        Operator opB, long vB) {
    return switch (opA) {
        case EQ -> switch (opB) {
            case EQ -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v == vB;
            };
            case NOT_EQ -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v != vB;
            };
            case LT -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v < vB;
            };
            case LT_EQ -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v <= vB;
            };
            case GT -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v > vB;
            };
            case GT_EQ -> row -> {
                StructAccessor s = resolve(row, p);
                if (s == null || s.isNull(n)) return false;
                long v = s.getLong(n);
                return v == vA && v >= vB;
            };
        };
        // ... 5 more outer cases for opA. Each opA's body is `v <relation> vA && v <relation> vB`
        // where the inner relation comes from opB.
        default -> throw new AssertionError("filled in similarly");
    };
}
```

Fill in the remaining 5 outer cases, mechanical replication.

- [ ] **Step 5: Run the tests.** They were already passing — re-running confirms nothing regresses with fusion engaged.

Run: `timeout 180 ./mvnw -pl core -am test -Dtest='RecordFilter*'`
Expected: all green. The fused matcher returns the same boolean as the generic.

- [ ] **Step 6: Add a sanity test that proves fusion is actually being used.**

```java
@Test
void longLongAndProducesFusedClass() {
    FileSchema schema = twoLongSchema("a", "b");
    ResolvedPredicate p = new ResolvedPredicate.And(List.of(
            new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 0L),
            new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
    RowMatcher m = RecordFilterCompiler.compile(p, schema);
    // The generic And2Matcher is a named inner class; the fused matcher is a
    // synthetic lambda. If fusion fired, the matcher class is NOT
    // RecordFilterCompiler$And2Matcher.
    assertThat(m.getClass().getSimpleName())
            .as("fused matcher should not be the generic And2Matcher")
            .doesNotContain("And2Matcher");
}
```

- [ ] **Step 7: Run, then commit.**

```bash
timeout 180 ./mvnw -pl core -am test -Dtest='RecordFilterFusionTest'
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java \
        core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java
git commit -m "#193 Implement long+long AND fusion"
```

### Task 3.3: Repeat the pattern for `long + long` OR

- [ ] **Step 1: Add `tryFuseOr2` dispatch arm and `fuseLongLongOr` / `longLongOrDiff` / `longRangeOr` helpers.**

Same shape as the AND helpers, but the connective is `||`. The first-leaf rejection inverts: instead of "reject and short-circuit on fail," OR is "accept and short-circuit on success." Concretely:

```java
private static RowMatcher longLongOrDiff(String[] pA, String nA, Operator opA, long vA,
        String[] pB, String nB, Operator opB, long vB) {
    return switch (opA) {
        case EQ -> switch (opB) {
            case EQ -> row -> {
                StructAccessor sA = resolve(row, pA);
                if (sA != null && !sA.isNull(nA) && sA.getLong(nA) == vA) return true;
                StructAccessor sB = resolve(row, pB);
                return sB != null && !sB.isNull(nB) && sB.getLong(nB) == vB;
            };
            // ... 5 more inner cases varying the second leaf's positive comparison.
        };
        // ... 5 more outer cases varying the first leaf's positive comparison.
    };
}
```

> Important: OR semantics on null leaves match the legacy oracle — null on leaf A returns false for that leaf, *not* true. So the first-leaf "accept" condition is `sA != null && !sA.isNull(nA) && positiveCmp(...)`. Don't accidentally treat null as "leaf failed → check leaf B" in a way that flips the boolean.

- [ ] **Step 2: Add OR test cases — diff-column AND null-leaf.**

```java
@ParameterizedTest(name = "long+long OR diff: a {0} {1} OR b {2} {3} on a={4},b={5} → {6}")
@MethodSource("longLongOrDiffCases")
void longLongOrDiff(Operator opA, long vA, Operator opB, long vB,
        long aVal, long bVal, boolean expected) {
    FileSchema schema = twoLongSchema("a", "b");
    ProjectedSchema projection = projectAll(schema);
    TwoLongIndexedRow row = new TwoLongIndexedRow("a", aVal, false, "b", bVal, false);
    ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
            new ResolvedPredicate.LongPredicate(0, opA, vA),
            new ResolvedPredicate.LongPredicate(1, opB, vB)));
    assertEquivalent(p, row, schema, projection, expected);
}

static Stream<Arguments> longLongOrDiffCases() {
    return cartesian(Operator.values(), Operator.values()).flatMap(pair -> {
        Operator opA = pair[0];
        Operator opB = pair[1];
        return Stream.of(
                Arguments.of(opA, 50L, opB, 100L, 50L, 100L,
                        evalLong(opA, 50L, 50L) || evalLong(opB, 100L, 100L)),
                Arguments.of(opA, 50L, opB, 100L, 49L, 99L,
                        evalLong(opA, 49L, 50L) || evalLong(opB, 99L, 100L)));
    });
}

@Test
void longLongOrDiff_nullLeafEvaluatesOther() {
    FileSchema schema = twoLongSchema("a", "b");
    ProjectedSchema projection = projectAll(schema);
    ResolvedPredicate p = new ResolvedPredicate.Or(List.of(
            new ResolvedPredicate.LongPredicate(0, Operator.GT_EQ, 100L),
            new ResolvedPredicate.LongPredicate(1, Operator.LT, 1000L)));
    // a=null, b passes → true
    assertEquivalent(p, new TwoLongIndexedRow("a", 5L, true, "b", 100L, false), schema, projection, true);
    // a=null, b fails → false
    assertEquivalent(p, new TwoLongIndexedRow("a", 5L, true, "b", 9999L, false), schema, projection, false);
    // both null → false
    assertEquivalent(p, new TwoLongIndexedRow("a", 5L, true, "b", 100L, true), schema, projection, false);
}
```

- [ ] **Step 3: Run + commit.**

```bash
timeout 180 ./mvnw -pl core -am test -Dtest='RecordFilterFusionTest'
git add core/src/main/java/dev/hardwood/internal/predicate/RecordFilterFusion.java \
        core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java
git commit -m "#193 Implement long+long OR fusion"
```

---

## Chunk 4: Replicate the Pattern for the Remaining Type Combos

Each combo follows the structure laid down in Chunk 3. For each, do: (1) extend `tryFuseAnd2` / `tryFuseOr2` dispatch, (2) add `fuse{Type}{Type}{And|Or}`, (3) add `…Diff` and (where applicable) `…Range` helpers, (4) extend the test class with a parameterized cases method and a null-leaf test, (5) commit.

The commit message convention per combo: `#193 Implement <combo> <connective> fusion`.

### Task 4.1: `int + int` AND + OR

- [ ] **Step 1:** Implement `fuseIntIntAnd`, `intIntAndDiff`, `intRangeAnd`, plus the OR siblings. Use `getInt` and `int` literals; comparison is direct (`==`, `<`, etc.) — same as long.
- [ ] **Step 2:** Add tests `intIntAndDiff`, `intIntOrDiff`, range and null variants. Expected-value generator mirrors `evalLong` but with `int` parameters.
- [ ] **Step 3:** Run + commit (one commit per connective).

### Task 4.2: `double + double` AND + OR

- [ ] **Step 1:** Implement `fuseDoubleDoubleAnd`, `doubleDoubleAndDiff`, `doubleRangeAnd`, plus OR.
- [ ] **Step 2: Use `Double.compare` for every comparison** to match the legacy NaN ordering — see `RecordFilterCompiler.doubleLeaf` (line 296) for the precedent.
- [ ] **Step 3:** Add tests including NaN cases (`Double.NaN` on either side, plus `NaN` operand value). Expected-value generator: `evalDouble(op, actual, target)` using `Double.compare`.
- [ ] **Step 4:** Run + commit.

### Task 4.3: `boolean + boolean` AND + OR (different-column only)

- [ ] **Step 1:** Implement `fuseBooleanBooleanAnd` and the OR sibling. No `…Range` helper — same-column boolean fusion is excluded by spec.
- [ ] **Step 2: Honour the legacy fallback** for operators outside `EQ`/`NOT_EQ`: `RecordFilterCompiler.booleanLeaf` at line 313 returns `true` for any non-null boolean when the operator is anything other than `EQ`/`NOT_EQ`. Mirror that in the fused body's default arm. The compile-time switch over op picks the correct arm; the fallback arm is `(sA != null && !sA.isNull(nA)) <connective> ...`.
- [ ] **Step 3:** Add tests for both operator-honoured and operator-fallback cases.
- [ ] **Step 4:** Run + commit.

### Task 4.4: `binary + binary` AND + OR

- [ ] **Step 1:** Implement `fuseBinaryBinaryAnd`, `binaryBinaryAndDiff`, `binaryRangeAnd`, plus OR. Use `getBinary` / `getBinaryAt`. Comparison goes through `BinaryComparator.compareSigned` or `compareUnsigned` selected per leaf at compile time from `BinaryPredicate.signed`.
- [ ] **Step 2:** For `binaryRangeAnd` (same column), load the byte[] reference once, then run two comparator calls against the two operand byte-arrays.
- [ ] **Step 3:** Add tests over UTF-8 strings and pure binary cases. The existing `BinaryPredicate.signed` flag is column-determined for same-column same-type fusion; for diff-column, both leaves carry their own flags.
- [ ] **Step 4:** Run + commit.

### Task 4.5: Cross-type `long + double` (with canonicalisation)

- [ ] **Step 1: Add the `(long, double)` arm and the `(double, long)` swap arm to `tryFuseAnd2`.** Both flow through the same `fuseLongDoubleAnd` after canonicalisation — see pg2's `RecordFilterFusion.java:51-55` for the canonical-swap pattern.

```java
if (a instanceof ResolvedPredicate.LongPredicate la && b instanceof ResolvedPredicate.DoublePredicate db) {
    return fuseLongDoubleAndDiff(la, db, schema);
}
if (a instanceof ResolvedPredicate.DoublePredicate da && b instanceof ResolvedPredicate.LongPredicate lb) {
    // AND/OR are commutative for pure leaves; canonicalise to (long, double).
    return fuseLongDoubleAndDiff(lb, da, schema);
}
```

- [ ] **Step 2:** Implement `fuseLongDoubleAndDiff` — different-column only (same-column would mean reading the same physical column as two different types, which the resolved predicate layer doesn't produce). 36 op pairs; long side uses direct compare, double side uses `Double.compare`.
- [ ] **Step 3:** Add OR sibling.
- [ ] **Step 4:** Add tests covering both orderings and null-leaf cases.
- [ ] **Step 5:** Run + commit.

### Task 4.6: Cross-type `int + long`

- [ ] **Step 1:** Implement `fuseIntLongAndDiff` and OR. Same canonicalisation pattern (canonical = `(int, long)`).
- [ ] **Step 2:** Tests + commit.

### Task 4.7: Cross-type `int + double`

- [ ] **Step 1:** Implement `fuseIntDoubleAndDiff` and OR. Canonical = `(int, double)`.
- [ ] **Step 2:** Tests + commit.

---

## Chunk 5: Indexed Variants in `RecordFilterFusionIndexed`

`RecordFilterFusionIndexed` is the projected-index counterpart. Same matrix, same null-handling, but the row is cast to `IndexedAccessor` once and indexed reads replace name-keyed reads.

### Task 5.1: Common indexed leaf access

- [ ] **Step 1: Add a top-level eligibility gate to both `tryFuseAnd2` and `tryFuseOr2`.** A pair is indexed-eligible only if both leaves are top-level (`isTopLevel(schema, columnIndex)`); otherwise return null and let the caller try the name-keyed path.

```java
static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b,
        FileSchema schema, ProjectedSchema projection) {
    int colA = leafColumnIndex(a);
    int colB = leafColumnIndex(b);
    if (colA < 0 || colB < 0) return null;
    if (!isTopLevel(schema, colA) || !isTopLevel(schema, colB)) return null;
    int idxA = projection.toProjectedIndex(colA);
    int idxB = projection.toProjectedIndex(colB);
    // ... type dispatch as in RecordFilterFusion ...
}

private static int leafColumnIndex(ResolvedPredicate p) {
    return switch (p) {
        case ResolvedPredicate.IntPredicate ip -> ip.columnIndex();
        case ResolvedPredicate.LongPredicate lp -> lp.columnIndex();
        case ResolvedPredicate.DoublePredicate dp -> dp.columnIndex();
        case ResolvedPredicate.BooleanPredicate bp -> bp.columnIndex();
        case ResolvedPredicate.BinaryPredicate bp -> bp.columnIndex();
        default -> -1;
    };
}
```

- [ ] **Step 2: Cast pattern at the top of every fused lambda.**

```java
case EQ -> row -> {
    IndexedAccessor a = (IndexedAccessor) row;
    if (a.isNullAt(idxA) || a.getLongAt(idxA) != vA) return false;
    return !a.isNullAt(idxB) && a.getLongAt(idxB) == vB;
};
```

The cast is safe by construction (see spec, "Cast safety in indexed fusion").

### Task 5.2: Mirror every Chunk 4 combo in `RecordFilterFusionIndexed`

For each combo implemented in Chunk 4, add the indexed equivalent in `RecordFilterFusionIndexed`. The structure is mechanical: replace `resolve(row, path) → sX` with `(IndexedAccessor) row → a` and replace `getX(name)` / `isNull(name)` with `getXAt(idx)` / `isNullAt(idx)`. No path resolution loop.

- [ ] One commit per combo: `#193 Implement <combo> <connective> indexed fusion`.

The existing `RecordFilterFusionTest`'s `assertEquivalent` already exercises the indexed path through the projection-aware `compile` overload. No new test methods needed — adding indexed implementations causes the existing tests to also exercise the indexed code path.

### Task 5.3: Indexed-specific test for non-top-level fallback

- [ ] **Step 1: Add a test confirming that nested-path predicates fall through to name-keyed fusion (or the generic matcher) and produce equivalent results.** The existing `RecordFilterIndexedTest` covers this for single leaves; we want one for fused arity-2.

Construct a schema with a nested struct (`outer.inner` containing two long fields) and assert the fused-AND case still produces the right result through the name-keyed path — the indexed entry returns null because the leaves are not top-level.

- [ ] **Step 2:** Run + commit.

---

## Chunk 6: Megamorphic JMH Benchmark

Goal: a JMH benchmark that proves the fusion contribution under deliberate inline-cache pollution.

### Task 6.1: Create `RecordFilterMegamorphicBenchmark`

**Files:**
- Create: `performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMegamorphicBenchmark.java`

- [ ] **Step 1: Build the benchmark from the pg2 template.** The pg2 commit `e3f4c60` has a working version covering 8 shapes — see git history for reference. Expand to ~12 shapes per the spec's _JMH micro_ section. Use this exact set:

```java
private static ResolvedPredicate[] buildPredicates() {
    return new ResolvedPredicate[] {
            // long+long same-column (BETWEEN)
            and(new LongPredicate(0, Operator.GT_EQ, 0L),
                new LongPredicate(0, Operator.LT, 9_999L)),
            // long+long same-column OR
            or(new LongPredicate(0, Operator.LT, -1L),
               new LongPredicate(0, Operator.GT, 8_192L)),
            // int+int same-column (BETWEEN)
            and(new IntPredicate(2, Operator.GT_EQ, 0),
                new IntPredicate(2, Operator.LT, 100)),
            // int+int same-column OR
            or(new IntPredicate(2, Operator.EQ, 5),
               new IntPredicate(2, Operator.EQ, 47)),
            // double+double same-column (BETWEEN)
            and(new DoublePredicate(1, Operator.GT_EQ, 0.0),
                new DoublePredicate(1, Operator.LT, 1_000.0)),
            // long+double diff-column AND
            and(new LongPredicate(0, Operator.GT_EQ, 0L),
                new DoublePredicate(1, Operator.LT, 999.0)),
            // long+double diff-column OR
            or(new LongPredicate(0, Operator.NOT_EQ, -1L),
               new DoublePredicate(1, Operator.LT_EQ, 1_000.0)),
            // int+long diff-column AND
            and(new IntPredicate(2, Operator.LT, 50),
                new LongPredicate(0, Operator.GT, 100L)),
            // int+double diff-column AND
            and(new IntPredicate(2, Operator.GT_EQ, 0),
                new DoublePredicate(1, Operator.LT, 500.0)),
            // boolean+boolean diff-column AND (degenerate, but exercises the path)
            and(new BooleanPredicate(3, Operator.EQ, true),
                new BooleanPredicate(3, Operator.NOT_EQ, false)),
            // double+long canonical-swap variant
            and(new DoublePredicate(1, Operator.GT, -1.0),
                new LongPredicate(0, Operator.LT, 9_999L)),
            // binary+binary diff-column AND (string-as-bytes)
            and(new BinaryPredicate(4, Operator.GT_EQ, "abc".getBytes(UTF_8), false),
                new BinaryPredicate(4, Operator.LT, "xyz".getBytes(UTF_8), false)),
    };
}
```

(Use the package-qualified ResolvedPredicate.* constructors and small helper methods for `and`/`or` to keep the table readable.)

- [ ] **Step 2: Two `@Benchmark` arms.**

`fusedMegamorphic` compiles each predicate via `RecordFilterCompiler.compile(predicate, schema)` (default fusion-on JVM). `genericMegamorphic` compiles via the same call but with `-Dhardwood.recordfilter.fusion=false` set on the JMH fork JVM args:

```java
@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "-Dhardwood.recordfilter.fusion=true" })
public class RecordFilterMegamorphicFusedBenchmark { ... }

@Fork(value = 2, jvmArgs = { "-Xms512m", "-Xmx512m", "-Dhardwood.recordfilter.fusion=false" })
public class RecordFilterMegamorphicGenericBenchmark { ... }
```

Use two separate benchmark classes rather than one with two arms — this is the cleanest way to flip a `static final` field via system property, since both arms in the same JVM would otherwise share the flag.

- [ ] **Step 3: Build the schema and 4096-row stub batch.** Five columns: `id long`, `value double`, `tag int`, `flag boolean`, `bin binary`. Stub row implements `StructAccessor` only (the JMH does not need indexed access — name-keyed fusion is sufficient for measuring the fusion contribution; indexed fusion is exercised by the e2e benchmark).

- [ ] **Step 4: Inner loop — every matcher × every row.** Per pg2's pattern.

```java
@Benchmark
public void fusedMegamorphic(Blackhole bh) {
    RowMatcher[] ms = matchers;
    StructAccessor[] batch = rows;
    for (int i = 0; i < batch.length; i++) {
        StructAccessor r = batch[i];
        for (int j = 0; j < ms.length; j++) {
            bh.consume(ms[j].test(r));
        }
    }
}
```

`@OperationsPerInvocation(BATCH_SIZE * SHAPE_COUNT)` so reported `ns/op` is per-row-per-shape.

- [ ] **Step 5: Build, compile, and run a smoke test.**

```bash
timeout 180 ./mvnw -pl core install -DskipTests
timeout 300 ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar -wi 1 -i 1 -f 1 RecordFilterMegamorphic
```

Expected: numbers come back. Don't trust the absolute values from a smoke run — only that the benchmark runs without error.

- [ ] **Step 6: Commit.**

```bash
git add performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMegamorphicBenchmark.java
git commit -m "#193 Add RecordFilterMegamorphicBenchmark JMH"
```

---

## Chunk 7: End-to-End Benchmark

### Task 7.1: Create `RecordFilterMegamorphicEndToEndTest`

**Files:**
- Create: `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterMegamorphicEndToEndTest.java`

- [ ] **Step 1: Pattern after `RecordFilterBenchmarkTest.java`.** Study `RecordFilterBenchmarkTest.java:230-267` for the lazy-Parquet-generation pattern. Extend the schema:

```java
Schema schema = SchemaBuilder.record("benchmark")
        .fields()
        .requiredLong("id")
        .requiredDouble("value")
        .requiredInt("tag")
        .requiredBoolean("flag")
        .requiredBytes("bin")
        .endRecord();

// ... in the loop:
record.put("id", (long) i);
record.put("value", rng.nextDouble() * 1000.0);
record.put("tag", rng.nextInt(100));
record.put("flag", rng.nextBoolean());
record.put("bin", java.nio.ByteBuffer.wrap(("k" + (i % 1000)).getBytes(UTF_8)));
```

Save to `target/record_filter_megamorphic.parquet`. Lazy-create on first run.

- [ ] **Step 2: Define the same ~12 query shapes from the JMH benchmark using the public `FilterPredicate` API.**

Each query maps to a `FilterPredicate.and(...)` or `.or(...)` call. Pick numerical thresholds that produce non-trivial selectivity (so the loop doesn't degenerate to either matching nothing or matching everything). Document the expected match-count range in a comment for each shape.

- [ ] **Step 3: Run all shapes sequentially in a single JVM invocation.**

```java
private static long runQuery(FilterPredicate pred) throws Exception {
    long count = 0;
    try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(BENCHMARK_FILE));
         RowReader rows = reader.buildRowReader().filter(pred).build()) {
        while (rows.hasNext()) {
            rows.next();
            count++;
        }
    }
    return count;
}
```

Loop: warmup pass → measurement passes (`-Dperf.runs`, default 5) over all 12 shapes. Print per-shape mean + total wall time.

- [ ] **Step 4: Detection of fusion mode.** The test reads `System.getProperty("hardwood.recordfilter.fusion")` once and includes the value in the printed header so users can tell which mode the numbers came from. The test does not toggle the property within the JVM (it is read once at `RecordFilterCompiler` class load).

- [ ] **Step 5: Run with fusion on.**

```bash
timeout 600 ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
    -Dtest='RecordFilterMegamorphicEndToEndTest' -Dperf.runs=5 \
    -Dhardwood.recordfilter.fusion=true
```

Expected: the test runs to completion. Capture the printed numbers.

- [ ] **Step 6: Run with fusion off.**

```bash
timeout 600 ./mvnw test -Pperformance-test -pl performance-testing/end-to-end \
    -Dtest='RecordFilterMegamorphicEndToEndTest' -Dperf.runs=5 \
    -Dhardwood.recordfilter.fusion=false
```

Capture those numbers.

- [ ] **Step 7: Commit.**

```bash
git add performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterMegamorphicEndToEndTest.java
git commit -m "#193 Add RecordFilterMegamorphicEndToEndTest"
```

---

## Chunk 8: Run Benchmarks and Fill in Results

### Task 8.1: JMH measurement run

- [ ] **Step 1: Install core, build benchmarks.**

```bash
timeout 180 ./mvnw -pl core install -DskipTests
timeout 300 ./mvnw -pl performance-testing/micro-benchmarks package -Pperformance-test
```

- [ ] **Step 2: Run both fused and generic JMH suites.**

```bash
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
    RecordFilterMegamorphicFusedBenchmark | tee /tmp/jmh-fused.txt
java -jar performance-testing/micro-benchmarks/target/benchmarks.jar \
    RecordFilterMegamorphicGenericBenchmark | tee /tmp/jmh-generic.txt
```

Default JMH config (2 forks × 5 warmup × 5 measurement) — no flags needed.

- [ ] **Step 3: Tabulate** mean ns/op and 99.9% CI for each shape × mode.

### Task 8.2: End-to-end measurement run

- [ ] **Step 1: Run both modes** as in Chunk 7 Steps 5–6. Capture wall time per shape and totals.

### Task 8.3: Fill in `_designs/RECORD_FILTER_FUSION.md` Results section

- [ ] **Step 1: Replace the placeholder Results tables.** Match the format of `_designs/RECORD_FILTER_COMPILATION.md` Results section (lines 209–249). Include:

  - Hardware / JDK / Maven versions.
  - JMH per-shape table: shape name, generic ns/op ± CI, fused ns/op ± CI, speedup.
  - End-to-end per-shape table: shape, generic ms, fused ms, speedup.
  - One paragraph commenting on the largest wins (likely the diff-column AND/OR shapes, where pre-fusion paid for two megamorphic inner sites per row) and the smallest (likely binary, where the comparator dominates the body cost).

- [ ] **Step 2: Flip the design-doc status from `Pending` to `Implemented`.** Edit the second line.

- [ ] **Step 3: Update the roadmap as required by CLAUDE.md.** Look for a roadmap file referenced by the README or under `docs/`; add a Stage 4 entry next to the existing Stages 1–3.

- [ ] **Step 4: Run the full `./mvnw verify` once.** Required by CLAUDE.md before considering the task complete:

```bash
timeout 600 ./mvnw verify
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Commit.**

```bash
git add _designs/RECORD_FILTER_FUSION.md
# plus any roadmap changes
git commit -m "#193 Stage 4 fusion: results and design status update"
```

---

## Acceptance

The work is complete when all of the following hold:

1. `timeout 600 ./mvnw verify` passes on the branch.
2. `RecordFilterFusionTest` exercises every fused (combo × connective × opA × opB × same/diff column × null/non-null) tuple and all assertions pass.
3. `RecordFilterMegamorphicBenchmark` produces a measurable fused-vs-generic delta on the local hardware. The delta direction must be: fused faster than generic for every shape that hits a fusion arm. Magnitude is reported, not gated.
4. `RecordFilterMegamorphicEndToEndTest` runs under both `-Dhardwood.recordfilter.fusion=true` and `=false` and produces stable results across `-Dperf.runs=5`.
5. `_designs/RECORD_FILTER_FUSION.md` has its Results section filled in and its status set to `Implemented`.
6. The branch's commit history mirrors the chunk structure — at minimum one commit per Chunk 4 combo and per Chunk 5 indexed combo, so the work bisects cleanly.
