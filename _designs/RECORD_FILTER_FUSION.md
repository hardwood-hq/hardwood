# Plan: Record Filter Arity-2 Fusion

**Status: Pending**

## Context

Stages 1–3 of [RECORD_FILTER_COMPILATION](RECORD_FILTER_COMPILATION.md) eliminated per-row dispatch in two places: the predicate-tree interpreter (replaced by a compiled `RowMatcher` graph) and the name-keyed accessor (replaced by indexed access for flat top-level columns). The remaining cost in compound predicates is the inner virtual call inside the fixed-arity AND/OR matchers:

```java
private static final class And2Matcher implements RowMatcher {
    private final RowMatcher a, b;
    public boolean test(StructAccessor row) {
        return a.test(row) && b.test(row);
    }
}
```

When a single JVM runs many distinct query shapes through `FilteredRowReader.hasNext`, the inline cache at the inner call sites `a.test(row)` and `b.test(row)` accumulates more than two receiver classes and goes megamorphic. HotSpot can no longer fold the leaf comparison into the `And2Matcher.test` body; the leaf becomes an indirect call and per-row cost regresses by a factor of 2–3×.

This stage fuses arity-2 compound matchers into single synthetic classes per `(typeA, opA, typeB, opB, connective, accessMode)` tuple. Each fused class has the comparison hard-coded in its body — no inner virtual call. The outer site `matcher.test(row)` is still subject to megamorphism across queries, but with the inner sites gone the per-row body is direct primitive arithmetic regardless of how many shapes flow through the JVM.

---

## Fusion Eligibility Matrix

Both children of an `And` or `Or` node are simple primitive leaves. Fusion applies to the following type combinations.

### Same-type (different and same column)

| Type combination    | Same-column (e.g. `col >= a AND col < b`) | Different-column |
|---------------------|:-----------------------------------------:|:----------------:|
| `int + int`         | ✓                                         | ✓                |
| `long + long`       | ✓                                         | ✓                |
| `double + double`   | ✓                                         | ✓                |
| `boolean + boolean` |                                           | ✓                |
| `binary + binary`   | ✓                                         | ✓                |

Same-column fusion resolves the path and loads the value once per row, evaluating both operators against the loaded primitive. `binary + binary` same-column loads the byte-array reference once and runs two comparator calls against the two operand byte-arrays.

`boolean + boolean` same-column is omitted — with only two distinct operand values and only `EQ`/`NOT_EQ` honoured, the surface is too narrow to justify a code path.

### Cross-type numeric (different column only)

| Canonical form (after AND/OR commutativity) | Aliases handled by canonicalisation |
|---------------------------------------------|-------------------------------------|
| `int + long`                                | `long + int`                        |
| `int + double`                              | `double + int`                      |
| `long + double`                             | `double + long`                     |

Both `And` and `Or` are commutative for pure-leaf children, so both orderings dispatch to the canonical fused matcher.

### Connectives and access modes

Every combination above is implemented for:
- both `And` and `Or`,
- both name-keyed access (`StructAccessor.getX(name)`) and indexed access (`IndexedAccessor.getXAt(int)`).

For each fusion target the code emits a distinct concrete class per `(opA, opB)` pair (6 × 6 = 36 per target). Each leaf operator is `EQ`, `NOT_EQ`, `LT`, `LT_EQ`, `GT`, `GT_EQ`. Boolean leaves with operators outside `EQ`/`NOT_EQ` reduce to the legacy non-null check, matching `RecordFilterEvaluator.matchesRow`.

---

## Implementation

### New file: `internal/predicate/RecordFilterFusion.java`

Name-keyed AND and OR fusion. Public API:

```java
static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema);
static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b, FileSchema schema);
```

Returns `null` when the pair is not eligible (caller falls back to the generic `And2Matcher` / `Or2Matcher`). Internal — package-private.

The body is a switch on `(typeA, typeB)` that dispatches to a per-combination helper. Each helper:

1. Resolves both leaves' intermediate struct paths and leaf field names at compile time.
2. If both columns have the same `columnIndex` and the type combination has a same-column variant, dispatches to a `…Range` helper that loads the value once.
3. Otherwise dispatches to a `…Diff` helper.
4. Each helper switches on `(opA, opB)` and returns one of 36 lambdas with both operators baked in as primitive comparisons.

Cross-type combinations (`int + long`, `int + double`, `long + double`) canonicalise the order before dispatching, so `(double, long)` and `(long, double)` flow through one set of 36 lambdas.

### New file: `internal/predicate/RecordFilterFusionIndexed.java`

Mirrors `RecordFilterFusion` but uses `IndexedAccessor.getXAt(int)` rather than `StructAccessor.getX(name)`. Public API:

```java
static RowMatcher tryFuseAnd2(ResolvedPredicate a, ResolvedPredicate b,
        FileSchema schema, ProjectedSchema projection);
static RowMatcher tryFuseOr2(ResolvedPredicate a, ResolvedPredicate b,
        FileSchema schema, ProjectedSchema projection);
```

Returns `null` if either leaf is non-top-level (path length > 1) or the pair is not eligible. Top-level column indices are translated to projected indices at compile time.

### Modify `internal/predicate/RecordFilterCompiler.java`

`compileAnd` and `compileOr` consult fusion before falling back to the generic fixed-arity matchers when `children.size() == 2`:

```java
private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema,
        ProjectedSchema projection) {
    if (FUSION_ENABLED && children.size() == 2) {
        if (projection != null) {
            RowMatcher fused = RecordFilterFusionIndexed.tryFuseAnd2(
                    children.get(0), children.get(1), schema, projection);
            if (fused != null) return fused;
        }
        RowMatcher fused = RecordFilterFusion.tryFuseAnd2(
                children.get(0), children.get(1), schema);
        if (fused != null) return fused;
    }
    RowMatcher[] compiled = compileAll(children, schema, projection);
    return switch (compiled.length) { /* unchanged */ };
}
```

`compileOr` follows the same shape against `tryFuseOr2`. Arity ≥ 3 falls back to the existing fixed-arity / generic matchers regardless of fusion.

### Disable flag

A static `boolean FUSION_ENABLED` is read once at class init from the system property `hardwood.recordfilter.fusion`. Default `true`; set `-Dhardwood.recordfilter.fusion=false` to disable. Internal-only knob, used by benchmarks to A/B the fusion contribution. Not part of the public API.

### Why each tuple is a distinct concrete class

Fusion only works because each `(typeA, opA, typeB, opB, connective, accessMode)` produces a *distinct concrete class*. The outer call site `matcher.test(row)` is permitted to be megamorphic — what matters is that the body of each fused matcher contains no virtual call that itself goes megamorphic. Any shared dispatch point inside the body (a `BiPredicate` field, a `Comparator`, a generic helper that takes the operator as a parameter) would defeat fusion: HotSpot cannot fold a comparison through a megamorphic call.

For this reason, every operator pair within every type combination is hand-written as its own lambda. Mechanical repetition is the cost of avoiding the inner dispatch.

---

## Null and NaN Semantics

Fused matchers preserve `RecordFilterEvaluator.matchesRow` semantics exactly:

- A leaf whose intermediate struct path is null returns false. The fused body checks `resolve(row, path) == null` before any value access (name-keyed) or relies on `IndexedAccessor`'s flat top-level guarantee that the row reference itself is non-null (indexed).
- A leaf whose direct field is null returns false. Fused bodies check `accessor.isNull(name)` / `isNullAt(index)` before reading the primitive.
- AND fusion short-circuits on the first false — the body uses `&&` between the two leaves.
- OR fusion short-circuits on the first true — the body uses `||`. When leaf A is null/false, leaf B is still evaluated.
- Float and double comparisons use `Float.compare` / `Double.compare`, matching the legacy NaN ordering used by the per-leaf factories in `RecordFilterCompiler`.
- Binary comparisons honour the per-leaf `signed` flag captured at compile time. Same-column binary fusion may have only one effective `signed` value (it is a column property), but the two flags are baked independently for diff-column fusion.

---

## Testing

### Equivalence — `core/src/test/java/dev/hardwood/internal/predicate/RecordFilterFusionTest.java` (new)

Three-way equivalence check. For every fused (combo × connective × opA × opB × same-column / different-column × null / non-null leaf value) tuple:

1. Build a stub row implementing `StructAccessor` (and `IndexedAccessor` where applicable).
2. Compile the predicate three ways:
    - Through `RecordFilterCompiler.compile` with fusion enabled (the fused matcher).
    - Through `RecordFilterCompiler.compile` with `-Dhardwood.recordfilter.fusion=false` (the generic `And2Matcher` / `Or2Matcher` over name-keyed leaves).
    - Through `RecordFilterEvaluator.matchesRow` (the legacy oracle).
3. Assert all three return the same boolean.

### Existing tests

- `RecordFilterCompilerTest` (Stage 1) and `RecordFilterIndexedTest` (Stage 3) continue to pass unchanged — fusion is transparent when the legacy oracle is used as the equivalence reference.
- `./mvnw verify` continues to pass; no public API changes.

---

## Benchmarks

Two artifacts target the megamorphic regime that motivates this stage.

### JMH micro — `RecordFilterMegamorphicBenchmark`

Path: `performance-testing/micro-benchmarks/src/main/java/dev/hardwood/benchmarks/RecordFilterMegamorphicBenchmark.java` (new).

Setup constructs ~12 distinct arity-2 predicate shapes covering every fusion combo × connective:

- `id BETWEEN a AND b` (long+long same-column AND)
- `tag BETWEEN x AND y` (int+int same-column AND)
- `value BETWEEN v1 AND v2` (double+double same-column AND)
- `id < a OR id > b` (long+long same-column OR)
- `tag == x OR tag == y` (int+int same-column OR)
- `id >= a AND value < v` (long+double diff-column AND)
- `tag < x AND value < v` (int+double diff-column AND)
- `id < a AND tag > x` (int+long diff-column AND)
- `id >= a OR value < v` (long+double diff-column OR)
- `tag <= x OR id > a` (int+long diff-column OR)
- `flag == true AND flag == false` (boolean+boolean diff-column AND, degenerate)
- `bin >= "abc" AND bin < "xyz"` (binary+binary same-column AND)

Two arms, both iterating BATCH_SIZE rows × all shapes per `@Benchmark` invocation:

- `fusedMegamorphic` — matchers compiled with fusion enabled. Each matcher is a unique synthetic class; the outer site sees N receiver types but each body has no inner virtual call.
- `genericMegamorphic` — matchers compiled with `-Dhardwood.recordfilter.fusion=false`. Each matcher is a `Generic AndN/OrN`, all sharing the same inner `a.test(row)` / `b.test(row)` bytecode locations, which accumulate the leaf classes and go megamorphic.

`@OperationsPerInvocation(BATCH_SIZE * SHAPE_COUNT)` so reported `ns/op` is per-row-per-shape cost.

### End-to-end — `RecordFilterMegamorphicEndToEndTest`

Path: `performance-testing/end-to-end/src/test/java/dev/hardwood/perf/RecordFilterMegamorphicEndToEndTest.java` (new).

Lazy-generates `target/record_filter_megamorphic.parquet` (10M rows, schema `id long, value double, tag int, flag boolean, bin binary`) on first run. Same generation pattern as the existing `RecordFilterBenchmarkTest`, extended with three additional columns.

The test runs the same ~12 query shapes as the JMH benchmark through `ParquetFileReader.buildRowReader().filter(...).build()`, sequentially in the same JVM, multiple iterations (`-Dperf.runs`, default 5). The point is to drive `FilteredRowReader.hasNext`'s outer call site to ≥12 receiver classes — every shape contributes one fused class with fusion on, or shares the small set of generic `AndN`/`OrN` classes with fusion off (where the inner sites carry the megamorphism instead).

Two modes per query, controlled by the system property:

- `-Dhardwood.recordfilter.fusion=true` (default): fused matchers.
- `-Dhardwood.recordfilter.fusion=false`: generic matchers.

The harness runs all shapes under each mode (separate JVM invocations recommended in CI to keep code-cache state isolated), captures wall time per shape, and prints a side-by-side comparison plus aggregate throughput.

---

## Risks and Edge Cases

- **Cast safety in indexed fusion.** Indexed leaves cast the row to `IndexedAccessor`. The cast is safe by construction — `RecordFilterFusionIndexed.tryFuseAnd2` / `tryFuseOr2` is reached only via the projection-aware `RecordFilterCompiler.compile` overload, which is only invoked from `FlatRowReader` (which `implements IndexedAccessor`). Same contract as the indexed leaves introduced in Stage 3.
- **Canonicalisation correctness.** Cross-type AND/OR canonicalisation relies on commutativity. Both `And` and `Or` are commutative regardless of leaf order or null-yielding leaves; the legacy evaluator uses the same commutative semantics via short-circuit on first match/miss. Three-way equivalence tests cover both orderings.
- **Same-column fusion of intermediate struct paths.** Fusion is allowed for same-column same-type when both leaves resolve identical intermediate struct paths. Different paths (e.g. `outer.x` and `inner.x`) compare by `columnIndex`, which is unique per leaf in the file schema, so the same-column branch only fires when the actual physical column matches.
- **Outer-site megamorphism.** Stage 4 does not eliminate megamorphism at `FilteredRowReader.hasNext`'s `matcher.test(row)` site. The cost is bounded — one inline-cache miss per row instead of three — and is largely hidden for long single-query scans by C2's speculative inlining. Workloads that rapidly interleave many short queries pay more. See _Future work_ below.

---

## Future Work

### Option B — Wider type and predicate matrix

Add fusion for: `float + float` (same and diff column), `float + numeric` cross-type, binary cross-type with numeric, `IsNull` / `IsNotNull` paired with any leaf type. Each addition follows the same 36-lambda-per-target pattern.

### Option C — Reduced scope

If maintenance cost ever outweighs the megamorphism benefit for the cross-type combos, the matrix can be trimmed back to same-type only (`int+int`, `long+long`, `double+double`, `boolean+boolean`, `binary+binary`) with no behavioural impact other than slower per-row cost on cross-type predicates.

### Stage 5 — Eliminate outer-site megamorphism via reader specialisation

The residual outer-site megamorphism at `FilteredRowReader.hasNext` is the next bottleneck for workloads with many interleaved short queries. Three mitigations exist:

1. **`-XX:TypeProfileWidth=8`**. Raises HotSpot's bimorphic-to-megamorphic threshold. Free runtime tuning, helps up to ~8 shapes. Documented as a deployment knob.
2. **Per-query reader specialisation**. Generate a bespoke `FilteredRowReader` subclass per query whose `hasNext` body has the fused matcher comparison inlined directly. Each query then has its own bytecode location for the matcher call, monomorphic by construction. Requires runtime bytecode generation (`ClassFile API` or ASM).
3. **Iteration inversion**. Move the `while (delegate.hasNext()) delegate.next(); test(...)` loop into the matcher itself, with `runScan(delegate, consumer)` overridden per fused class. Self-calls within each generated class devirtualise. Combinatorial explosion unless paired with code generation.

Option 1 is a deployment change. Options 2 and 3 are larger, separate stages.

### Code-generation alternative for the fusion source

Each fused matcher's source is mechanical, and the file is large (~4000–5000 lines for the matrix above). Hand-writing is the chosen mechanism for now: stack traces point at real source, the build is unaffected, and the file is rarely edited once the matrix is settled. If the matrix grows substantially (e.g. into Option B), a build-time generator that emits the same bytecode shape from a template + table is a natural refactor. The template language would need to preserve the per-tuple distinct-concrete-class property described in _Implementation_; any approach that introduces a shared dispatch point inside the body would defeat fusion.

---

## Results

_To be filled after running the benchmarks._

### JMH micro — `RecordFilterMegamorphicBenchmark`

| Shape                             | Generic ns/op | Fused ns/op | Speedup |
|-----------------------------------|--------------:|------------:|--------:|
| _populated after benchmark run_   |               |             |         |

### End-to-end — `RecordFilterMegamorphicEndToEndTest`

| Scenario                          | Generic time | Fused time | Speedup |
|-----------------------------------|-------------:|-----------:|--------:|
| _populated after benchmark run_   |              |            |         |

Hardware, JDK, and Maven wrapper version recorded alongside the numbers.
