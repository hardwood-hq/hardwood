## Drain-Side Record Filtering (#250)

**Status: Implemented and on by default** for any query that decomposes into column-local leaves on distinct top-level columns. The path trades single-threaded predicate throughput for cross-core parallelism: in end-to-end multi-column AND scenarios it beats the compiled per-row path (1.5–5× on the measured workloads, scaling with leaf count); in single-threaded JMH microbenchmarks it is 3.8–6.9× slower per row than the compiled path. The end-to-end wins come from running per-column matchers on the existing drain threads in parallel rather than serially on the consumer thread — see [Performance](#performance--end-to-end-full-coverage-recordfilterscenariosbenchmarktest--dperfruns5) for the full picture. It is taken even for single-leaf queries (the early leaf-count gate from v1 has been removed — see the rationale under [Eligibility](#eligibility)). Ineligible shapes fall back automatically to `FilteredRowReader` via a `null` return from `BatchFilterCompiler.tryCompile`. There is no opt-in flag.

## Context

Record-level filter predicates on the flat reader run on the consumer thread. `FlatRowReader` polls one fully-decoded batch per column from each `BatchExchange`, then `FilteredRowReader` advances `rowIndex` and calls the compiled `RowMatcher` per row. The compiled per-row path produced by `RecordFilterCompiler` (#193) is very tight — fixed-arity matchers, indexed-leaf accessors, JIT-inlined short-circuit AND — but its work is serial and proportional to the number of leaves.

Two structural facts about the pipeline make a different placement plausible:

- **Per-column drain threads already touch every value.** The drain copies decoded page values into the typed batch array, so the array is hot in the drain core's L1 immediately after assembly. Evaluating a column-local predicate against that array on the same thread sees zero cross-core cache traffic.
- **The drain threads are independent.** Per-column filter evaluation parallelises across cores at one column per thread, with no synchronisation between drains.

This design moves the eligible part of record-level filtering from the consumer thread into the drain threads.

The architecture has four pieces:

1. A `BatchMatcher` interface and per-`(type, op)` concrete classes that operate on a column's typed array + null `BitSet` and write a per-batch matches `long[]`.
2. A `BatchFilterCompiler` that decomposes an eligible `ResolvedPredicate` into per-column `BatchMatcher`s and returns `null` for any non-eligible shape (full fallback to `FilteredRowReader`).
3. A `matches` field on `BatchExchange.Batch` and an extra step at the end of `FlatColumnWorker.publishCurrentBatch` to evaluate the column's matcher.
4. Eligibility-gated changes in `FlatRowReader` to intersect per-column matches into a `combinedWords` array and iterate via `Long.numberOfTrailingZeros` instead of wrapping in `FilteredRowReader`.

Eligible queries take the drain-side path; ineligible queries fall back to `FilteredRowReader` with no behavioural change. The eligibility decision is made once at `FlatRowReader.create` time via `BatchFilterCompiler.tryCompile`.

---

## Eligibility

A `ResolvedPredicate` is **eligible** iff:

- It is either a single column-local leaf, or `ResolvedPredicate.And(children)` whose children are all column-local leaves, and
- All leaves target **distinct** top-level projected columns (no two leaves on the same column), and
- Every leaf has `(type, op)` supported by `BatchFilterCompiler`.

A leaf is **column-local** iff:

- Its `FieldPath` is top-level (length 1), and
- Its column maps to a non-negative projected index, and
- Its `(type, op)` is one of: `long` / `double` / `int` / `float` × `{EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ}`, `boolean` × `{EQ, NOT_EQ}`, `IntIn` / `LongIn`, `IsNull` / `IsNotNull`.

`Or`, `Not`, intermediate-struct paths, geospatial predicates, `Binary*` leaves, and any leaf on a fragment-less column make the entire query non-eligible. Multiple leaves on the same column (e.g. `id >= x AND id <= y`) are folded into a single `AndBatchMatcher` per column that runs both children and word-AND merges their bitmaps. A dedicated fused range matcher would be tighter, but the general composite already moves the `range` shape onto the drain-side path.

Per-column composition is enforced positionally: `BatchFilterCompiler.tryCompile` allocates a `ColumnBatchMatcher[]` slot per projected column index and folds any further leaf for that column into an `AndBatchMatcher` over the existing slot occupant:

```java
result[projected] = result[projected] == null
        ? matcher
        : new AndBatchMatcher(result[projected], matcher);
```

`AndBatchMatcher` runs both children against the same batch, writing the first child's bits into the caller's `outWords` and the second into a per-instance scratch buffer, then word-ANDs the active range. The composite is type-agnostic — both children handle their own `batch.values` cast.

There is no separate leaf-count gate. The v1 prototype carried an `if (leaves.size() < 2) return null;` short-circuit on the theory that single-fragment queries pay drain-side overhead with no parallelism payoff; in practice the single-leaf drain-side path is within noise of the compiled path on the end-to-end benchmark (see the `single` row in the table below) and strictly wins once a matcher gains a vectorised body, so the gate was removed.

---

## `BatchMatcher` and per-`(type, op)` classes

Sealed interface representing a per-column fragment over a single batch:

```java
public sealed interface BatchMatcher
        permits LongBatchMatcher, DoubleBatchMatcher, IntBatchMatcher,
                FloatBatchMatcher, BooleanBatchMatcher, NullBatchMatcher {
    void test(BatchExchange.Batch batch, long[] outWords);
    int columnIndex();
}
```

`outWords` is a `long[]` sized `(batchCapacity + 63) >>> 6`, owned by the `Batch`. A bit at index `i` is set iff row `i` definitely matches (NULL → unset). The matcher overwrites every word it touches; the caller does not pre-clear.

Per-`(type, op)` final classes implement the typed body. The optimised inner loop uses a per-word accumulator pattern:

```java
final class LongGtBatchMatcher implements LongBatchMatcher {
    private final int columnIndex;
    private final long literal;
    // ...

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        long[] vals = (long[]) batch.values;
        BitSet nulls = batch.nulls;
        int n = batch.recordCount;
        long lit = literal;
        int fullWords = n >>> 6;
        int tail = n & 63;

        if (nulls == null) {
            for (int w = 0; w < fullWords; w++) {
                int base = w << 6;
                long word = 0L;
                for (int b = 0; b < 64; b++) {
                    word |= ((vals[base + b] > lit) ? 1L : 0L) << b;
                }
                outWords[w] = word;
            }
            // tail-word handling, then zero trailing slots past recordCount
        }
        // BitSet-null branch is structurally identical
    }
}
```

Key shape choices:

- **No `Arrays.fill`.** Each output word is written exactly once, in full, at the end of a 64-element block. Trailing slots past `recordCount` are zeroed in a small final loop.
- **Branchless inner loop.** `(cond ? 1L : 0L) << b` lowers to `csel` on AArch64 / `setcc` on x86-64; the per-element cost stops depending on selectivity.
- **Word-level accumulator.** `long word` lives in a register for 64 iterations — one store per 64 elements, not 64 read-modify-writes.
- **Hoisted `literal`, `recordCount`, `fullWords`, `tail`.** The inner `for (b = 0; b < 64; b++)` is a constant-bound loop that the JIT unrolls.
- **`nulls == null` specialised outside the per-word loops.** One branch per call, not per element.

NULL semantics: each fragment writes "definitely matches" — false on NULL. Word-wise AND of per-column matches gives SQL three-valued AND because any `unknown` conjunct unsets the bit. This contract does not generalise to OR-spanning-columns; that case is excluded by the eligibility rule.

`Null*BatchMatcher` short-circuits the per-bit loop entirely — `IsNullBatchMatcher.test` bulk-copies `nulls.toLongArray()` and masks off bits past `recordCount`; `IsNotNullBatchMatcher.test` bulk-inverts the same.

---

## `Batch.matches` and drain-side evaluation

`BatchExchange.Batch` gains a `long[] matches` field. Allocated once at construction (sized to `(batchCapacity + 63) >>> 6`) **only** for columns that actually have a fragment installed — other columns leave `matches == null`, which `FlatRowReader` interprets as "all-ones" during intersection.

`FlatColumnWorker` gains a `BatchMatcher fragment` field and a `setFragment(BatchMatcher)` setter. At the end of `publishCurrentBatch`, immediately before pushing the batch onto the `readyQueue`:

```java
if (fragment != null) {
    fragment.test(currentBatch, currentBatch.matches);
}
```

Cost is paid on the drain thread, on hot data, in parallel with peer drains. When `fragment == null`, `publishCurrentBatch` is unchanged.

---

## Intersection and iteration in `FlatRowReader`

`FlatRowReader.create` calls `BatchFilterCompiler.tryCompile`. A non-null result drives the drain-side path; otherwise the existing branch (`FilteredRowReader` if a filter is set, plain reader if not) is used.

The reader gains:

- `long[] combinedWords` — the per-batch combined match mask.
- `int[] fragmentColumns` — the projected indices of columns with a fragment, computed once at construction. Iterating this array in the hot path avoids the per-batch null check on `Batch.matches`.

`loadNextBatch`, after polling all column batches and when `drainSide` is true, calls `intersectMatches`:

```java
private void intersectMatches() {
    int n = batchSize;
    int activeWords = (n + 63) >>> 6;
    int[] cols = fragmentColumns;
    long[] combined = combinedWords;

    long[] first = previousBatches[cols[0]].matches;
    System.arraycopy(first, 0, combined, 0, activeWords);
    for (int c = 1; c < cols.length; c++) {
        long[] m = previousBatches[cols[c]].matches;
        for (int w = 0; w < activeWords; w++) {
            combined[w] &= m[w];
        }
    }
}
```

Only the words covering `[0, batchSize)` are touched — bits past `batchSize` are not read by the consumer (`nextSetBit` is bounded), so leaving them stale is safe and avoids the trailing zero-fill the original shape paid every batch.

`hasNext()` / `next()` advance via a `nextSetBit` helper using `Long.numberOfTrailingZeros` over `combinedWords`. Accessors (`getLong(int)`, `isNull(int)`, etc.) are unchanged — they still index `flatValueArrays[idx][rowIndex]`; the only difference is which rows `rowIndex` visits.

---

## Validation

### Correctness

`DrainSideOracleTest` (`core/src/test/java/dev/hardwood/internal/predicate/DrainSideOracleTest.java`) is a three-way oracle: for every supported `(type, op)` and 2-column AND combination over `long`/`double` columns, build a synthetic batch (random + boundary + nulls) and assert that `RecordFilterEvaluator.matchesRow` (legacy), the compiled `RowMatcher` (today's path), and the drain-side `BatchMatcher` + intersect + iterate return the same surviving row indices.

`BatchMatcherTest` covers per-matcher behaviour including word-boundary cases, NaN ordering for double, and null exclusion.

The full `./mvnw verify` passes — the drain-side path runs by default for every eligible query in the existing test suite.

### Performance — end-to-end, full coverage (`RecordFilterScenariosBenchmarkTest -Dperf.runs=5`)

The numbers below were captured when the single-leaf gate still existed; rows annotated `fallback (gate)` / `gated → fallback` ran the compiled path on both runs at the time. With the gate now removed, those scenarios take the drain-side path too — at parity with compiled per the row-level data already present.

10 M rows × `id: long, value: double, tag: int, flag: boolean`. Each scenario is its own `@Test` so JIT warmup ordering doesn't bias later cells. The numbers below were captured on the branch that introduced drain-side filtering against the previous compiled-only path on `main` — the comparison is a pre/post snapshot, not a runtime toggle.

| Scenario                                              | Compiled (ms) | Drain (ms) | Ratio       | Path             |
|-------------------------------------------------------|--------------:|-----------:|------------:|------------------|
| `and2`  long+double                       — match-all |        53.19  |    36.40   |  **1.46×**  | drain (2 cols)   |
| `and3`  long+double+int                   — match-all |        76.45  |    39.37   |  **1.94×**  | drain (3 cols)   |
| `and4`  long+double+int+bool              — ~50% pass |       114.21  |    22.56   |  **5.06×**  | drain (4 cols)   |
| `and2`  selective ~0.1%                   — sparse    |         3.20  |     2.77   |    1.16×    | drain (2 cols)   |
| `and2`  ~50% selectivity                  — half      |       107.12  |    45.45   |  **2.36×**  | drain (2 cols)   |
| `and2`  empty result                      — no match  |         0.48  |     0.34   |    1.41×    | drain (2 cols)   |
| `single`  one leaf                        — match-all |        48.99  |    47.95   |    1.02×    | fallback (gate)  |
| `or`  long ‖ double                       — half      |        99.93  |    98.75   |    1.01×    | fallback (or)    |
| `range`  id BETWEEN x AND y AND value < c — ~10% pass |         8.96  |     8.68   |    1.03×    | fallback (dup)   |
| `intIn5`  selective ~5%                   — sparse    |        66.92  |    66.00   |    1.01×    | fallback (gate)  |

What this maps:

- **Drain wins everywhere it's eligible.** Every two-or-more-column AND beats compiled, with the ratio scaling roughly with leaf count: `and2` 1.46×, `and3` 1.94×, `and4` 5.06× — compiled's per-row work is linear in leaves, drain's is parallel across drain threads.
- **`and4` is the cleanest signal.** A four-leaf AND that compiled has to evaluate serially on the consumer thread (114 ms) costs only 22 ms when the leaves run on four drain threads in parallel. That's the parallelism story in pure form.
- **Selectivity matters.** Selective `and2` (~0.1%) is only 1.16× because the consumer-side iteration cost (`nextSetBit`) shrinks alongside compiled's per-row cost — both paths get cheap together. Mid-selectivity `and2` (~50%) widens to 2.36× because compiled still pays per-row work on every survivor while drain's bitmap-iteration cost per survivor is roughly the same.
- **The four fallback rows (`single`, `or`, `range`, `intIn5`) all sit at ~1.0×** — the gate fires, drain returns null, the query takes the same `FilteredRowReader` path on both runs. Confirms the gate works and that drain-side adds no overhead on ineligible queries.
- **`range` (`id BETWEEN x AND y AND value < c`)** would benefit from drain if the compiler fused same-column comparisons into a `LongRangeBatchMatcher`. Currently it falls back. That's the most realistic eligibility expansion.

Predicate-only overhead per row, compound match-all (`and2`):

- Compiled: `(53.19 − no-filter baseline ~17 ms) / 10 M ≈ 3.6 ns/row`
- Drain: `(36.40 − 17) / 10 M ≈ 1.9 ns/row`
- Reduction: **47%** predicate-only time.

### JMH micro (`RecordFilterMicroBenchmark`)

JMH, fork=1, warmup=3×1s, measurement=5×1s, single-threaded. `ns/op` is per-row over a 4096-row in-memory batch. Match-all predicates only.

| Shape    | Compiled ns/op | DrainSide ns/op | Drain / Compiled | Notes                              |
|----------|---------------:|----------------:|-----------------:|------------------------------------|
| single   | 0.412          | gated → fallback | n/a              | Single-leaf gate trips.            |
| and2     | 0.479          | 3.311           | 6.9× slower      | 2 columns, lowest leaf count.      |
| and3     | 0.561          | 3.551           | 6.3× slower      | 3 columns.                         |
| and4     | 0.651          | 2.481           | 3.8× slower      | 4 columns — best per-leaf cost.    |
| or2      | 0.413          | gated → fallback | n/a              | OR not eligible.                   |
| nested   | 0.986          | gated → fallback | n/a              | OR-inside-AND not eligible.        |
| intIn5   | 1.364          | gated → fallback | n/a              | Single-fragment IN-list.           |
| intIn32  | 3.126          | gated → fallback | n/a              | Same.                              |

The drain-side path is still slower single-threaded across every eligible shape — the codegen claim from v1 (that a hand-written batch loop would beat the inlined indexed-accessor leaf) does not hold even with the rewritten matchers. **But the gap shrinks as leaf count grows**: `and2` is 6.9× slower, `and4` is 3.8×. Larger leaf counts amortise the per-batch overhead (`Arrays.fill`-free output, per-word accumulator, `intersectMatches`) over more comparison work, while the compiled path's cost grows linearly in leaves. The per-leaf cost of drain-side has dropped to roughly half what v1 measured.

Why drain-side stays slower single-threaded:

1. **`BitSet.get(int)` is opaque to autovectorisation.** Even though the value comparison is now branchless, the null-check still goes through a virtual call into a non-final method that does its own shift, mask, bounds check, and array load. HotSpot will not autovectorise around it. The compiled per-row path doesn't have this problem because it uses indexed accessors that the JIT inlines through.
2. **Match-all pays full `O(n)` matcher cost.** The compiled path's iterator under match-all is effectively `rowIndex++`; drain-side runs the matcher loop in full whether 100% or 0% of rows pass.
3. **Per-survivor `nextSetBit`.** The consumer iterates surviving rows via `Long.numberOfTrailingZeros` + clear-low-bit. At match-all density, that's per-row work the compiled path doesn't do.

These gaps close at higher leaf counts and disappear once the drains run in parallel — see the e2e table above.

---

## Risks and edge cases

- **NULL semantics under intersection.** Each fragment writes "matches" as false on NULL. Word-wise AND of per-column matches gives SQL three-valued AND because any `unknown` conjunct unsets the bit. Reinforced by the three-way oracle.
- **`maxRows` semantics.** `FlatColumnWorker.copyPageRange` enforces `maxRows` as "rows scanned" — the drain stops assembling once `maxRows` rows have been copied. The drain-side filter runs after assembly, so `maxRows` continues to bound *scanned* rows, not *returned* rows. Behaviour matches the compiled path.
- **Wasted decode.** A highly selective predicate on column A still requires column B to be fully decoded — drains are independent. The drain-side filter does not reduce decode cost; it only reduces filter cost.
- **Word-array sizing.** `outWords.length = (batchCapacity + 63) >>> 6`, not `(recordCount + 63) >>> 6`. `Batch` recycles across pages of varying sizes; sizing to capacity avoids reallocation. The matcher zeroes trailing slots past `recordCount` so stale bits from the previous batch don't leak through `intersectMatches`.
- **Cast safety.** `LongGtBatchMatcher.test` casts `batch.values` to `long[]`. The cast is safe by construction: a `BatchFilterCompiler`-produced `LongBatchMatcher` is only installed on a `FlatColumnWorker` whose column was already typed as INT64 at projection time. A misuse fails fast with `ClassCastException`.

---

## Follow-ups

The fallback rows in the e2e table — `or`, `range`, `intIn5` — each fall back for a different reason. Listed roughly in increasing order of work-to-lift.

### Same-column leaf fusion (`range`)

**Why it falls back today.** The compiler keys its output `BatchMatcher[]` by projected column index — one slot per column. Two leaves on the same column (`id >= a AND id < b`) both want the same slot, and `tryCompile` returns `null`:

```java
if (result[projected] != null) {
    // Two leaves on the same column: not supported in v1.
    return null;
}
```

**What to build.** A fused `LongRangeBatchMatcher` (and `DoubleRangeBatchMatcher`) that does both compares in one pass:

```java
word |= ((vals[i] >= lo & vals[i] < hi) ? 1L : 0L) << b;
```

One read of `vals[]`, one bitmap, no intersect. Strictly cheaper than two single-leaf matchers AND-merged. Compiler change: group leaves by `(projected column, type)` before dispatching; size ≥ 2 routes to a fusion factory. Falls through to the existing one-matcher-per-column path otherwise.

**Payoff.** Unlocks the `Page+record`-shaped predicate (the most common compound after match-all in real workloads — `id BETWEEN a AND b AND value op c`). The benchmark's `range` row should move from the "fallback" pile into the "drain wins" pile.

**Difficulty.** Low. Two new matcher classes plus a grouping pass in `BatchFilterCompiler.tryCompile`. No changes to drain plumbing or intersect logic.

### Cross-column `Or` (`or`, `nested`)

**Why it falls back today.** Two stacked reasons:

1. **Intersect helper is AND-only.** `FlatRowReader.intersectMatches` does `combined[w] &= m[w]`. A cross-column OR would need word-wise `|`. The structural change is small but the helper currently has no notion of "this query is an OR."
2. **Bitmap can't represent NULL distinctly.** Each `BatchMatcher` writes "definitely matches" — bit unset means either "doesn't match" or "is null." That's correct for AND (SQL `null ∧ x` is false; an unset bit gives the same result). For OR it's wrong: `null ∨ true` is `true`, but the bitmap encoding can't distinguish a null 0-bit from a non-match 0-bit, so cross-column OR would silently drop rows that should pass via the non-null disjunct.

**What to build.** Either:

- **Two-bit-per-row encoding**: alongside `matches`, carry `notNull` per column. Word-wise: `combined = (notNull0 & matches0) | (notNull1 & matches1)`. Doubles the bitmap memory and adds one AND per column per word, but expressively complete.
- **Per-column tri-state**: `matches` semantics flip to "true / false / unknown," with separate `definitelyTrue` / `definitelyFalse` long[]s. Cleaner three-valued semantics, but invasive — every matcher writes two outputs.

The simpler path is `notNullWords` per column (which the codebase wants anyway for autovectorisation reasons — see below) plus a `BatchScan` that knows the predicate tree shape and chooses `&` vs `|` per node.

**Payoff.** Unlocks `or` and any `nested` shape with column-spanning OR. The compiled per-row path's main remaining moat.

**Difficulty.** Medium. The intersect change is mechanical; the NULL fix is plumbing across every matcher and the `Batch` type. Has to land on top of the `notNullWords` migration to be efficient.

### `BitSet nulls` → `long[] notNullWords`

**Why it matters.** The biggest remaining drag on single-threaded drain-side throughput. `BitSet.get(int)` is a virtual call into a non-final method that does its own shift, mask, bounds check, and array load — HotSpot will not autovectorise around it. Even though the value comparison in every matcher is now branchless (`(cond ? 1L : 0L) << b`), the null check inside that comparison blocks SIMD lowering. Compiled per-row doesn't have this problem because it uses indexed accessors the JIT inlines through.

**What to build.** Replace `BatchExchange.Batch.nulls: BitSet` with `notNullWords: long[]` (bit `i` set iff row `i` is **not** null). Sized to capacity once at construction. The drain populates it directly during the def-level decode pass, and matchers consult `(notNullWords[i >>> 6] >>> i) & 1L) == 1L` — a primitive load + AND that the JIT folds into the SIMD pass.

**Risk.** This is a broad migration. Every reader path that touches `Batch.nulls` has to switch:

- `FlatRowReader.flatNulls` (private, easy)
- `FlatColumnWorker.markNulls` (the writer, easy)
- `ColumnReader.getElementNulls()` — **public API**, currently returns `BitSet`. Either keep returning `BitSet` (constructed lazily from `notNullWords` on the public boundary) or break callers. The lazy construction is the safer landing.

**Payoff.** Unblocks Vector API matchers (technique below) and meaningfully closes the single-threaded gap on the JMH micro. Probably halves `and2`'s 6.9× single-threaded deficit on its own.

**Difficulty.** Medium-broad. Surgical in each file, but touches many. Land in isolation with no v2-only code so any regression bisects cleanly.

### Vector API matchers

**Why.** A `LongVector.compare(GT, lit).toLong()` lowers to a single SIMD instruction (`pcmpgtq` on x86-64, `cmgt` on AArch64) over `SPECIES.length()` lanes per cycle. Per-element cost drops from O(1) instructions to O(1/lane-count). The hardware is sitting there; the matcher loop is exactly the shape SIMD eats.

**Prerequisite.** `notNullWords` migration. Without it, the SIMD comparison can't be paired with a SIMD-friendly null mask — you'd lower the comparison and immediately stall on `BitSet.get` per element.

**What to build.** Multi-release `core/src/main/java22/dev/hardwood/internal/predicate/matcher/.../Vector*BatchMatcher.java`. Strategy pattern via the existing `VectorSupport.isAvailable()` (already used by the SIMD encoders). Per-`(type, op)` pair, mirroring the scalar layout. The output bitmap accumulates word-aligned (`SPECIES.length()` divides 64), so several SIMD iterations contribute to one word; null masking happens in a separate word-wise AND pass after.

**Payoff.** With the `notNullWords` migration in front of it, this is what could put drain-side ahead of compiled even single-threaded — the lever the v1 design over-promised.

**Difficulty.** High. Multi-release plumbing per matcher (28 files × 2 = 56 classes). Verify each on both scalar fallback (JDK 21 path) and SIMD path. Worth doing only after `notNullWords` lands.

### Per-batch min/max + sentinel matches

**Why.** A match-all predicate (`id >= 0`) currently runs the matcher loop in full to produce a bitmap of all 1s, intersects it, and iterates 4096 surviving rows via `nextSetBit`. Compiled does `rowIndex++` per row. The drain-side `O(n)` matcher cost is structural — it can't be hidden by parallelism for single-fragment match-all queries.

**What to build.** During the drain's copy pass, capture per-batch `minLong` / `maxLong` (analogous for double / int / float). Each matcher reads them first; if the literal lies outside the range, it stamps a sentinel singleton on `Batch.matches`:

```java
public static final long[] ALL_ONES_WORDS = new long[0];
public static final long[] ALL_ZEROS_WORDS = new long[0];
```

Identity-checked by the consumer. Match-all batches: no allocation, no fill, no intersect, no iteration. All-miss batches: dropped wholesale before the consumer sees them. Only mixed batches pay the bitmap cost — which is exactly the regime where the per-row compiled path also pays its full cost.

**Subtle.** NaN handling for double / float: a running `Math.min` over NaN produces NaN, and the matcher's `Double.compare` semantics don't agree with `<`. The drain has to set `statsValid = false` if any NaN is seen and the matcher takes the per-row path for that batch.

**Payoff.** Closes the single-fragment match-all gap. With the v1 leaf-count gate already removed, this would also make the drain-side path strictly preferable to compiled on single-leaf match-all instead of merely at parity.

**Difficulty.** Low-medium. Two compare-and-update per row in the drain (cheap, branchless via `Math.min`/`Math.max`). New fields on `Batch`. Sentinel identity-comparison in the intersect helper.

### Word-level adaptive iteration on the consumer

**Why.** The other half of the match-all problem. Even with `ALL_ONES_WORDS` short-circuiting batch-level, mixed batches still iterate via `nextSetBit` (one `numberOfTrailingZeros` + clear-low-bit per survivor). Compiled at 100% selectivity is `rowIndex++`. Branch-on-popcount per word: dense words emit 64 contiguous indices in a tight `for`, sparse words bit-iterate.

```java
long combined = m0[w] & m1[w];
int pop = Long.bitCount(combined);
if (pop == 0) continue;
if (pop == 64) { for (int b = 0; b < 64; b++) emit(base + b); continue; }
while (combined != 0) { emit(base + Long.numberOfTrailingZeros(combined)); combined &= combined - 1; }
```

**Payoff.** Match-all consumer iteration becomes essentially `rowIndex++`-equivalent (the dense branch is a fixed 64-iter `for` with no bit work). Closes the per-survivor cost gap in the JMH micro and on dense e2e scenarios.

**Difficulty.** Low. One change in `FlatRowReader.hasNext` / `next`. Can fuse the intersect into the per-word loop while at it (compute `combined` per word from the per-column matches, never materialise a full `long[]`).

### `NestedRowReader` support

**Why it's hard.** rep/def-driven row assembly does not partition cleanly per leaf. A nested column produces N values per record (offsets + repetition levels); the per-row "matches" bit is at the **record** level, but the matcher would have to walk the offset structure to map values back to records. Not impossible, but a structurally different design from the flat case.

**Difficulty.** High. Out of scope for v1; tracked separately if the use case shows up.

### Bloom-filter / dictionary-aware matchers

**Why.** Dictionary-encoded pages let you evaluate the predicate **in dictionary space** — one compare per dictionary entry (typically 100s), then a gather over the per-row dictionary indices. For `IN`-list and equality predicates on high-cardinality columns this is asymptotically faster than per-row. Bloom filters give a cheap pre-check for `EQ`/`IN` shapes before any decode work.

**Difficulty.** Medium-high, but **independent of the drain-side architecture**. Lives at the page-decoder layer, not the matcher layer. Mentioned for completeness; tracked separately.

### Public-API surface for `ColumnReader` filtering

**Why.** `ColumnReader` accepts a filter for row-group / page pruning today, but yields raw decoded batches — record-level filtering is the caller's job. A drain-side `BatchMatcher` could be installed on the `FlatColumnWorker` behind a `ColumnReader` too, with the matches mask exposed alongside the values:

```java
boolean[] flag = col.getBooleans();
long[] matches = col.getMatches();   // ← new accessor
for (int w = 0; w < (count + 63) >>> 6; w++) {
    long word = matches[w];
    while (word != 0) { … }
}
```

That hands the bitmap iteration off to the caller (who's in the best position to decide what to do with surviving rows) but moves the per-row evaluation onto the drain.

**Difficulty.** Medium. Public API addition (`ColumnReader.getMatches()` or similar), internal wiring through `BatchExchange.detaching()` mode, and a contract decision: does cross-column AND make sense at the `ColumnReaders` (projection) level, or stay one filter per `ColumnReader`?

---

## Roadmap

`ROADMAP.md` §9.4 "Predicate Pushdown" gets an entry for drain-side per-batch evaluation now that the path is on by default. The implementation itself is tracked under issue #250.
