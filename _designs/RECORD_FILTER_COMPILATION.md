# Plan: Record Filter Compilation

**Status: Implemented**

## Context

Record-level filtering evaluates a `ResolvedPredicate` tree against every decoded row that survived page-level pruning. The original implementation walked the predicate tree in [`RecordFilterEvaluator.matchesRow`] for *each* row, paying a sealed-type switch over 12 predicate kinds, an operator switch (6 operators), name-keyed accessor lookups (`StructAccessor.isNull(name)` and `getX(name)` both go through a `name → index` hash), and iterator allocation for every `And`/`Or` recursion. None of this work depends on the row — only on the predicate tree, which is fixed at reader-construction time.

This design replaces the per-row interpreter with a compiler that produces one `RowMatcher` per query. The compiler resolves field paths, picks the right operator, and bakes both into a small lambda whose call site is monomorphic per query, so HotSpot inlines the comparison directly. For top-level columns, leaves use the indexed `getXxx(int)` accessors already exposed on [RowReader] to bypass the name-keyed hash on the hot path. Compound `And`/`Or` nodes use fixed-arity classes (arities 2/3/4) so the JIT sees stable receiver-typed call sites and inlines the leaf bodies through the compound matcher.

The design has three architectural pieces:

1. Compile the predicate tree into a `RowMatcher` graph. Hoist field-name lookups, struct-path resolution, and operator switches out of the per-row loop.
2. For top-level columns, bypass name-keyed accessors via the indexed `getXxx(int)` accessors on [RowReader].
3. Replace the generic per-child loop in `And`/`Or` with fixed-arity classes (arities 2/3/4) so the JIT can inline through stable receiver-typed call sites.

---

## Step 1: Compile the Predicate Tree

### New file: `internal/predicate/RowMatcher.java`

Functional interface returned by the compiler. Single method `boolean test(StructAccessor row)`. Internal — not part of the public API.

### New file: `internal/predicate/RecordFilterCompiler.java`

Walks a `ResolvedPredicate` tree once and returns one `RowMatcher` per node.

```java
public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema);
```

| Node | Compiled form |
|---|---|
| `IntPredicate(col, op, v)` | Lambda per operator with the leaf field name and operand baked in |
| `LongPredicate(col, op, v)` | Same, 6 lambdas (one per operator) |
| `Float`/`Double`/`Boolean`/`Binary` | Same shape; float/double use `Float.compare`/`Double.compare` to match the legacy NaN ordering |
| `Int`/`Long`/`BinaryIn` | One-time sort; binary search above 16 entries, linear scan below |
| `IsNull` / `IsNotNull` | Resolves intermediate struct path and returns `acc == null \|\| acc.isNull(name)` (and inverse) |
| `And(children)` | Recursively compile each child, then a fixed-arity matcher (see Step 3) |
| `Or(children)` | Same as `And` but disjunctive |

Two concrete decisions that follow from the JIT-inlining argument:

- **One lambda per `(type, operator)` pair**, not one class per type with a switch inside. Splitting the switch out at compile time turns the per-row branch into a literal comparison.
- **`And`/`Or` children are stored as `RowMatcher[]`**, not `List<RowMatcher>`. Iterating an `Object[]` avoids `Iterator` allocation in the hot loop.

Path resolution: `pathSegments` extracts the intermediate struct names as a `String[]`; for top-level columns (`elements().size() ≤ 1`) the path is the shared `EMPTY_PATH` and the resolve loop is a no-op.

### Modify `internal/reader/FilteredRowReader.java`

Replace the `(ResolvedPredicate, FileSchema)` pair with a single `RowMatcher`. Inside `hasNext()`:

```java
while (delegate.hasNext()) {
    delegate.next();
    if (matcher.test(delegate)) {
        hasMatch = true;
        return true;
    }
}
```

### Modify `internal/reader/FlatRowReader.java` and `internal/reader/NestedRowReader.java`

Compile the predicate at reader-construction time and pass the resulting matcher to `FilteredRowReader`.

### Equivalence

`RecordFilterEvaluator.matchesRow` is retained as the oracle. A test asserts the compiled matcher returns the same boolean as the legacy evaluator for every predicate variant, every operator, both null branches, nested intermediate-null cases, deeply nested compounds, and the small/large `IN`-list paths.

**Files:**
- `core/src/main/java/dev/hardwood/internal/predicate/RowMatcher.java` (new)
- `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` (new)
- `core/src/main/java/dev/hardwood/internal/reader/FilteredRowReader.java` (modify — accept `RowMatcher`)
- `core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java` (modify — compile at construction)
- `core/src/main/java/dev/hardwood/internal/reader/NestedRowReader.java` (modify — compile at construction)
- `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterCompilerTest.java` (new — equivalence with legacy oracle)

---

## Step 2: Index-Based Fast Path

Single-leaf dispatch through `StructAccessor.getLong(name)` and `isNull(name)` runs a custom `name → index` hash on the row for every leaf evaluation. For top-level columns, the index is known at compile time, so the compiler can emit leaves that read at a fixed field index without going through the name-keyed API.

### Use the indexed accessors on `RowReader`

[RowReader] already exposes a parallel set of indexed accessors — `getInt(int)`, `getLong(int)`, `isNull(int)`, etc. — alongside the name-keyed [StructAccessor] methods it inherits. The `int` argument is the field index the *reader* uses, not a file column index:

- For [dev.hardwood.internal.reader.FlatRowReader], the field index is the projected leaf-column index, since every leaf is a top-level field.
- For [dev.hardwood.internal.reader.NestedRowReader], the field index is the projected top-level field index in the row reader's projected fields.

No new interface is introduced; the indexed leaves piggyback on the indexed accessors already on [RowReader].

### Modify `internal/predicate/RecordFilterCompiler.java`

A 3-arg overload takes a callback mapping a *file leaf-column index* to the *reader field index* the indexed accessors expect:

```java
public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema,
        IntUnaryOperator topLevelFieldIndex);
```

For each leaf, if `topLevelFieldIndex != null` and the column's `FieldPath` has length 1 (top-level), the compiler translates the file column index via the callback and emits an indexed-leaf lambda whose body casts the row to [RowReader] and reads through the resolved field index. Callers signal "not directly addressable" by returning `-1` from the callback — the compiler then falls back to the name-keyed leaf for that column. Nested paths (path length > 1) always fall back to the name-keyed factories regardless.

Indexed leaves are inline factories on `RecordFilterCompiler` paralleling the name-keyed ones — one per `(type, operator)` pair, plus `IsNull` / `IsNotNull`. Compound matchers are unchanged: `compileAnd` and `compileOr` thread the callback through to their children.

The cast `(RowReader) row` is safe by construction. Only `FlatRowReader` and `NestedRowReader` invoke the 3-arg overload, and both implement [RowReader]. A misuse fails fast with `ClassCastException`.

### How each reader maps file columns to field indices

- `FlatRowReader.create` passes `projectedSchema::toProjectedIndex` — every leaf is top-level, so the projected leaf-column index *is* the reader's field index.
- `NestedRowReader.create` precomputes a `fileLeafColumnIndex → projectedTopLevelFieldIndex` lookup that returns `-1` for any column whose path is not a single top-level element or whose top-level field is not in the projection. The compiler then emits indexed leaves for projected top-level columns and falls back to name-keyed leaves for columns inside nested structs.

The indexed path is gated on `isTopLevel(schema, columnIndex)` — even within a reader that supports indexed access, the compiler must fall back to name-based access for any leaf whose path traverses a struct.

### Modify `internal/reader/FlatRowReader.java` and `internal/reader/NestedRowReader.java` (compile call)

`FlatRowReader.create` calls the 3-arg overload with `projectedSchema::toProjectedIndex`. `NestedRowReader.create` builds the top-level lookup array at construction time and passes `col -> topLevelLookup[col]`.

### Equivalence

`RecordFilterIndexedTest` exercises the indexed path. For every primitive single-leaf predicate and all 36 `(opA, opB)` pairs of compound shapes, it builds a stub row implementing [RowReader], then asserts that the indexed compile path, the name-keyed compile path, and the legacy `RecordFilterEvaluator.matchesRow` oracle all return the same boolean.

**Files:**
- `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` (modify — add 3-arg overload, indexed-leaf factories)
- `core/src/main/java/dev/hardwood/internal/reader/FlatRowReader.java` (modify — call the 3-arg overload)
- `core/src/main/java/dev/hardwood/internal/reader/NestedRowReader.java` (modify — call the 3-arg overload with the top-level lookup)
- `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterIndexedTest.java` (new — three-way equivalence)

---

## Step 3: Fixed-Arity AND/OR Matchers

A generic `RowMatcher[]` walker for `And` / `Or` is an inlining barrier: the bound check + array load make HotSpot unable to prove the receiver type at the per-iteration `compiled[i].test(row)` call site, so the leaf body stays behind a virtual call.

Compound matchers use fixed-arity classes for arities 2/3/4. Each is a small final-field class whose body is a hand-written boolean expression:

```java
private static final class And3Matcher implements RowMatcher {
    private final RowMatcher a, b, c;
    And3Matcher(RowMatcher a, RowMatcher b, RowMatcher c) { ... }
    @Override
    public boolean test(StructAccessor row) {
        return a.test(row) && b.test(row) && c.test(row);
    }
}
```

`compileAnd` / `compileOr` switch on `children.size()`:

```java
return switch (compiled.length) {
    case 1 -> compiled[0];
    case 2 -> new And2Matcher(compiled[0], compiled[1]);
    case 3 -> new And3Matcher(compiled[0], compiled[1], compiled[2]);
    case 4 -> new And4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
    default -> new AndNMatcher(compiled);
};
```

Symmetric `Or2Matcher`/`Or3Matcher`/`Or4Matcher`. Arities ≥ 5 fall back to a generic `AndNMatcher` / `OrNMatcher` array walker — those shapes are rare and the fallback still benefits from a monomorphic outer call site.

The leaf factories from Steps 1 and 2 produce a *distinct* synthetic class per `(type, operator)`. So when a query compiles to `And2Matcher(longGtLeaf, doubleLtLeaf)`, the `a.test(row)` and `b.test(row)` call sites inside `And2Matcher.test` see one specific receiver class each. HotSpot inlines through both, and the leaf bodies fold into the `And2Matcher.test` body — JIT-driven fusion without combinatorial source code.

**Files:**
- `core/src/main/java/dev/hardwood/internal/predicate/RecordFilterCompiler.java` (modify — add fixed-arity matchers)

---

## Validation

End-to-end: `performance-testing/end-to-end/.../RecordFilterBenchmarkTest` covers five scenarios over a synthetic 10M-row Parquet file:

| Scenario | Predicate | What it stresses |
|---|---|---|
| No filter | — | Read throughput floor |
| Match-all (1 leaf) | `id >= 0` | Single-leaf dispatch on every row |
| Selective (1%) | `id < 100_000` | Predicate cost amortised over rejected rows |
| Compound match-all | `id >= 0 AND value < +inf` | Two-leaf AND on every row |
| Page+record combined | `id BETWEEN ... AND value < 500` | Page filter prunes 99 %; record filter runs on the survivors |

JMH: `performance-testing/micro-benchmarks/.../RecordFilterMicroBenchmark` measures the predicate-only cost in isolation by pre-loading a 4096-row in-memory batch and running both the legacy `matchesRow` and the compiled `RowMatcher` against the same predicate in the same JVM.

`./mvnw verify` continues to pass — the optimisation is internal-only, no public API changes, and `RecordFilterEvaluator` remains as the equivalence oracle for tests.

---

## Results

Hardware: macOS aarch64 (Apple Silicon), Oracle JDK 25.0.3, Maven wrapper 3.9.12.

### JMH micro-benchmark (`RecordFilterMicroBenchmark`)

2 forks × 5 warmup × 5 measurement iterations. `ns/op` is per-row cost over a 4096-row in-memory batch (`@OperationsPerInvocation(BATCH_SIZE)`). Match-all predicates only — every row evaluated, so dispatch overhead is the entire cost. The `Main branch` column compiles `And`/`Or` through the generic `RowMatcher[]` walker (Steps 1–2 only); the `Fixed-arity` column adds the Step 3 specialised compounds.

| Shape | Legacy ns/op | Main branch ns/op | Fixed-arity ns/op | Speedup vs legacy | Speedup vs main |
|---|---:|---:|---:|---:|---:|
| `single` (`id >= 0`) | 2.631 ± 0.019 | 0.513 ± 0.007 | **0.502 ± 0.018** | **5.24×** | **1.02×** |
| `and2` | 10.183 ± 0.259 | 2.039 ± 0.019 | **0.581 ± 0.007** | **17.53×** | **3.51×** |
| `and3` | 21.304 ± 0.130 | 8.223 ± 0.112 | **0.583 ± 0.045** | **36.54×** | **14.10×** |
| `and4` | 30.989 ± 0.654 | 13.424 ± 0.184 | **0.643 ± 0.015** | **48.19×** | **20.88×** |
| `or2` | 4.493 ± 0.044 | 0.955 ± 0.005 | **0.510 ± 0.005** | **8.81×** | **1.87×** |
| `nested` (and-of-or) | 33.574 ± 1.927 | 10.747 ± 0.286 | **0.948 ± 0.013** | **35.42×** | **11.34×** |
| `intIn5` | 3.714 ± 0.040 | 1.401 ± 0.025 | **1.354 ± 0.047** | **2.74×** | **1.03×** |
| `intIn32` | 6.827 ± 0.042 | 3.116 ± 0.036 | **3.071 ± 0.023** | **2.22×** | **1.01×** |

(Errors are 99.9 % confidence intervals from JMH. `Speedup vs legacy` is `Fixed-arity` vs `Legacy`; `Speedup vs main` is `Fixed-arity` vs `Main branch` and isolates the Step 3 contribution.)

The compound shapes (`and2`/`and3`/`and4`/`nested`) are where Step 3 contributes most — without fixed-arity matchers, the inner `compiled[i].test(row)` call site is a virtual call through an array element, so HotSpot cannot inline the leaf body and the per-leaf cost is paid as a real call. Fixed-arity classes give each inner call site one stable receiver type per query, the leaf body inlines, and the entire compound collapses to inline primitive comparisons (~0.6 ns/row regardless of arity 2/3/4).

`single` and the `IN`-list shapes are unaffected by Step 3 (no compound) — both columns are within run-to-run noise. The `IN`-list relative wins are smaller because the inner search dominates once the dispatch overhead is removed.

### End-to-end (`RecordFilterBenchmarkTest -Dperf.runs=10`)

10 M rows × `id: long`, `value: double`. Average wall-clock per run:

| Scenario | Avg time | Records/sec |
|---|---:|---:|
| No filter (baseline) | 19.5 ms | 512,420,431 |
| Match-all (1 leaf, `id >= 0`) | 27.8 ms | 360,084,022 |
| Selective (`id < 1%`) | 2.0 ms | 51,020,403 |
| Compound match-all (`id >= 0 AND value < +inf`) | 50.3 ms | 198,695,317 |
| Page+record combined (`id BETWEEN ... AND value < 500`) | 3.1 ms | 15,961,579 |

Headline ratios from this run:

- **Match-all overhead: 42.3 %** (19.5 ms → 27.8 ms). Predicate-only cost ≈ `(27.8 − 19.5) / 10 M = 0.83 ns/row` — close to the JMH single-leaf floor.
- **Selective speedup: 10.0×** (19.5 ms → 2.0 ms).
- **Compound overhead: 157.9 %** (19.5 ms → 50.3 ms). Predicate-only cost ≈ `(50.3 − 19.5) / 10 M = 3.08 ns/row`.
- **Page+record speedup: 6.2×** (19.5 ms → 3.1 ms).

End-to-end the predicate-only saving is bounded by I/O, decode, and projection plumbing, so the headline numbers move less than the JMH micro suggests: at single-leaf cost ≈ 0.83 ns/row the compiled path is already close to the no-filter floor, and the residual overhead for compound match-all is dominated by the second leaf's value access rather than dispatch.

---

## Risks and edge cases

- **`IsNull` semantics for missing nested paths.** In the legacy evaluator, `acc == null` (intermediate struct missing) yields `true` for `IsNull` and `false` for `IsNotNull`. The compiled matchers preserve this — `resolve()` returns `null` if any intermediate struct is null, and the leaf factories check `a == null` before any value access.
- **`BinaryPredicate.signed`.** Captured at compile time and baked into the chosen comparator; never re-checked per row.
- **`BooleanPredicate` with non-`EQ`/`NOT_EQ` operators.** The legacy evaluator returns `true` for any non-null boolean in this case; the compiled boolean leaves preserve this fallback in their `default` switch arm.
- **Empty `And`/`Or`.** Already rejected at `ResolvedPredicate` construction; the test suite covers it.
- **Indexed cast safety.** Only `FlatRowReader.create` and `NestedRowReader.create` invoke the 3-arg compile overload, and both implement [RowReader]. The cast `(RowReader) row` is a contract between the compiler and the readers that opt into it.
