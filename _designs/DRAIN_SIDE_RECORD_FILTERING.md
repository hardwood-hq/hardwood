## Drain-Side Record Filtering (#250)

**Status: Implemented and on by default** for any query that decomposes into column-local leaves on distinct top-level columns. The path trades single-threaded predicate throughput for cross-core parallelism: in end-to-end multi-column AND scenarios it beats the compiled per-row path (1.5–5× on the measured workloads, scaling with leaf count); in single-threaded JMH microbenchmarks it is 3.8–6.9× slower per row than the compiled path. The end-to-end wins come from running per-column matchers on the existing drain threads in parallel rather than serially on the consumer thread — see [Performance](#performance--end-to-end-full-coverage-recordfilterscenariosbenchmarktest--dperfruns5) for the full picture. It is taken even for single-leaf queries (the early leaf-count gate from v1 has been removed — see the rationale under [Eligibility](#eligibility)). Ineligible shapes fall back automatically to the row reader's own record matcher, via a `null` return from `BatchFilterCompiler.tryCompile`. There is no opt-in flag.

## Context

Record-level filter predicates on the flat reader run on the consumer thread. `FlatRowReader` polls one fully-decoded batch per column from each `BatchExchange`, then advances `rowIndex` and calls the compiled `RowMatcher` per row. The compiled per-row path produced by `RecordFilterCompiler` (#193) is very tight — fixed-arity matchers, indexed-leaf accessors, JIT-inlined short-circuit AND — but its work is serial and proportional to the number of leaves.

Two structural facts about the pipeline make a different placement plausible:

- **Per-column drain threads already touch every value.** The drain copies decoded page values into the typed batch array, so the array is hot in the drain core's L1 immediately after assembly. Evaluating a column-local predicate against that array on the same thread sees zero cross-core cache traffic.
- **The drain threads are independent.** Per-column filter evaluation parallelises across cores at one column per thread, with no synchronisation between drains.

This design moves the eligible part of record-level filtering from the consumer thread into the drain threads.

The architecture has four pieces:

1. A `ColumnBatchMatcher` interface and per-`(type, op)` concrete classes that operate on a column's typed array + validity bitmap and write a per-batch matches `long[]`.
2. A `BatchFilterCompiler` that decomposes an eligible `ResolvedPredicate` into per-column `ColumnBatchMatcher`s and returns `null` for any non-eligible shape (full fallback to the reader's record matcher).
3. A `matches` field on `BatchExchange.Batch` and an extra step at the end of `FlatColumnWorker.publishCurrentBatch` to evaluate the column's matcher.
4. A `BatchMatchMerger` that combines the per-column matches into one per-batch survivor bitmap, which the consumer iterates via `Long.numberOfTrailingZeros` instead of evaluating a `RowMatcher` per row.

Eligible queries take the drain-side path; ineligible queries fall back to the record matcher with no behavioural change. The eligibility decision is made once at `FlatRowReader.create` time via `BatchFilterCompiler.tryCompile`.

---

## Eligibility

A `ResolvedPredicate` is **eligible** iff every leaf in the tree is column-local and supported, and no column appears in more than one independent subtree. `And` and `Or` may nest freely.

A leaf is **column-local** iff:

- Its `FieldPath` is top-level (length 1), and
- Its column maps to a non-negative projected index, and
- Its `(type, op)` is one of: `long` / `double` / `int` / `float` × `{EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ}`, `boolean` × `{EQ, NOT_EQ}`, byte-exact binary × `{EQ}`, `IntIn` / `LongIn` / `DoubleIn` (the last on a `DOUBLE` or a `FLOAT` column, whose stored values are widened before comparison), `IsNull` / `IsNotNull`.

`Not` is lowered to leaf-level operator inversion at resolution time (`ResolvedPredicate.negate`, De Morgan for compounds), so the batch compiler only sees `And` / `Or`. Intermediate-struct paths, geospatial predicates, binary operators other than `EQ`, non-byte-exact binary equality, `BinaryInPredicate`, and any leaf on a fragment-less column make the entire query non-eligible.

The compiler walks the predicate tree bottom-up:

- A subtree that lives entirely on one projected column collapses into a single `ColumnBatchMatcher` slot. Same-column leaves under an `And` chain through `AndBatchMatcher`; under an `Or` they chain through `OrBatchMatcher`. `id >= x AND id <= y` and `id < -5 OR id > 5` both end up as one composite in one column slot.
- A subtree that spans multiple columns produces a `MergePlan` (`And` / `Or`) whose children are either `MergePlan.Column(projectedIndex)` references — for child subtrees that resolved to a single column — or nested merge plans.

`tryCompile` returns a `CompiledBatchFilter(columnMatchers[], mergePlan)`: per-projected-column matcher slots (workers populate `Batch.matches` from these) plus a `MergePlan` the consumer walks to merge the per-column bitmaps. A subtree where the **same** column appears across two independent siblings (e.g. `(a > 5 AND b > 5) OR (a < 0 AND b < 0)`, where column `a` would need two different per-row predicates encoded in one `matches` array) is rejected — the per-column matcher model cannot represent it.

### Why `Or` is safe under "definitely matches" semantics

Each matcher emits bit `i` set iff row `i` **definitely** satisfies the leaf (NULL → unset). For `WHERE A OR B` under SQL three-valued logic, the row should be included iff `A OR B` is definitely true:

| A     | B     | `A OR B` (SQL) | `def(A) \| def(B)` |
|-------|-------|----------------|-------------------:|
| true  | true  | true           | 1 ✓                |
| true  | null  | true           | 1 ✓                |
| false | null  | null → exclude | 0 ✓                |
| null  | null  | null → exclude | 0 ✓                |

Word-wise OR of the existing per-leaf bitmaps is the correct combine for both `OrBatchMatcher` (within one column) and the `MergePlan.Or` consumer walk (across columns) — no auxiliary null tracking, no extra passes.

There is no separate leaf-count gate. The v1 prototype carried an `if (leaves.size() < 2) return null;` short-circuit on the theory that single-fragment queries pay drain-side overhead with no parallelism payoff; in practice the single-leaf drain-side path is within noise of the compiled path on the end-to-end benchmark (see the `single` row in the table below) and strictly wins once a matcher gains a vectorised body, so the gate was removed.

---

## `ColumnBatchMatcher` and per-`(type, op)` classes

A sealed interface representing a per-column fragment over a single batch: `test(Batch, long[] outWords)`, with one final implementation per `(type, op)` pair.

`outWords` is a `long[]` sized `(batchCapacity + 63) >>> 6`, owned by the caller. A bit at index `i` is set iff row `i` definitely matches (NULL → unset). The matcher overwrites every word it touches; the caller does not pre-clear.

Key shape choices in the typed inner loop:

- **No `Arrays.fill`.** Each output word is written exactly once, in full, at the end of a 64-element block. Bits past `recordCount` in the tail word, and stale slots in words past `(recordCount + 63) >>> 6`, are intentionally left untouched — `nextSetBit` / `scanRunEnd` in `FlatRowReader` are bounded by `batchSize`, so any stale set bit at index `>= batchSize` is filtered by the `bit < limit` check.
- **Branchless inner loop.** `(cond ? 1L : 0L) << b` lowers to `csel` on AArch64 / `setcc` on x86-64; the per-element cost stops depending on selectivity.
- **Word-level accumulator.** `long word` lives in a register for 64 iterations — one store per 64 elements, not 64 read-modify-writes.
- **Hoisted `literal`, `recordCount`, `fullWords`, `tail`.** The inner `for (b = 0; b < 64; b++)` is a constant-bound loop that the JIT unrolls.
- **Presence specialised outside the per-word loops.** A batch whose leaves are all present takes a branch chosen once per call, not once per element. Presence is a packed `long[] validity` on the batch (set bit = present), so the null check is a load and a mask rather than a virtual call.

NULL semantics: each fragment writes "definitely matches" — false on NULL. Word-wise AND of per-column matches gives SQL three-valued AND because any `unknown` conjunct unsets the bit; the same contract handles OR cleanly via word-wise OR (see the truth table under [Why `Or` is safe](#why-or-is-safe-under-definitely-matches-semantics)).

`Null*BatchMatcher` short-circuits the per-bit loop entirely — `IsNotNullBatchMatcher.test` bulk-copies the validity words; `IsNullBatchMatcher.test` bulk-inverts them. Like the typed matchers, bits past `recordCount` are left as-is and filtered by the consumer's `bit < limit` check.

---

## `Batch.matches` and drain-side evaluation

`BatchExchange.Batch` gains a `long[] matches` field. Allocated once at construction (sized to `(batchCapacity + 63) >>> 6`) **only** for columns that actually have a fragment installed — other columns leave `matches == null`, and the merge never reads them, because it visits exactly the columns its plan references.

`FlatColumnWorker` gains a `final ColumnBatchMatcher columnFilter` field, constructor-injected so the contract is enforced by the type system rather than a "set before start" call order. It runs the fragment into the batch's own `matches` array at the end of `publishCurrentBatch`, immediately before the batch goes onto the `readyQueue`. Cost is paid on the drain thread, on hot data, in parallel with peer drains. When statistics have already proved the batch's row group matches in full, the worker fills all-ones instead of running the fragment. With no fragment installed, `publishCurrentBatch` is unchanged.

---

## Combine and iteration

`FlatRowReader.create` calls `BatchFilterCompiler.tryCompile`. A non-null `CompiledBatchFilter` drives the drain-side path; otherwise the reader's record-matcher mode is used when a filter is set, and the plain cursor when not.

### `BatchMatchMerger`

One collaborator owns the whole per-batch merge: the plan, the `MergePlanEvaluator` and its scratch pool, the per-column bitmap slots and the combined buffer. Two axes are decided once at construction.

**Where the per-column bitmaps come from.** In *aliasing* mode the consumer's column workers have already run the fragments into each batch's own `matches` array, so the merge reseats pointers and copies nothing. In *owning* mode there are no workers, so the merger runs the fragments itself into buffers it allocates. `FlatRowReader` takes the first, `SelectionEngine` (the column-reader filter path, see [EXACT_COLUMN_READER_FILTERING.md](EXACT_COLUMN_READER_FILTERING.md)) the second; nothing else differs between them.

**Plan shape.** A top-level `MergePlan.Column` has nothing to combine, so the merge yields that one column's bitmap directly — no combined buffer, no evaluator, no scratch pool. Any other plan goes to the evaluator, which populates the combined buffer in place.

The merger visits only the projected columns its plan references, collected once at construction by walking the plan. That set is the merge's contract with its caller: those are the only entries of the batch array it dereferences, so a consumer that stages that array itself need keep only those current. It is derived in one place, so the two consumers cannot disagree about which columns the plan reads.

`merge` runs once per batch, never per row.

### `MergePlanEvaluator`

A recursive walk of the plan: a `Column` node arraycopies the column's bitmap into the output; an `And` node merges its children word-wise with an all-zero short-circuit (skipping later children once the intersection collapses); an `Or` node word-ORs. Column children of an And/Or merge directly from their bitmap; only compound children get a depth-indexed scratch buffer. Sibling frames at the same depth reuse one buffer (their lives don't overlap), while a frame at depth+1 gets a distinct buffer (depth's is still in use). Pool entries persist across calls, so the merge allocates nothing after warm-up.

`DrainSideOracleTest` merges the bitmaps itself rather than through the merger; its independence comes from the **per-row** compile path (`RecordFilterCompiler` + `RowMatcher.test`) it compares against, not from a duplicate evaluator.

Only the words covering `[0, batchSize)` are written — bits past `batchSize` are not read by the consumer, so leaving them stale is safe and avoids a trailing zero-fill every batch.

### Iteration

The reader caches the bitmap the merge returned and advances through it with a `nextSetBit` helper over `Long.numberOfTrailingZeros`, plus a run walk that skips the bit scan while inside a run of consecutive 1s. Accessors (`getLong(int)`, `isNull(int)`, etc.) are unchanged — they still index the batch's typed array; the only difference is which rows the cursor visits.


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
- **The four fallback rows (`single`, `or`, `range`, `intIn5`) all sit at ~1.0×** — the gate fires, drain returns null, the query takes the same record-matcher path on both runs. Confirms the gate works and that drain-side adds no overhead on ineligible queries.
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

The drain-side path is still slower single-threaded across every eligible shape — the codegen claim from v1 (that a hand-written batch loop would beat the inlined indexed-accessor leaf) does not hold even with the rewritten matchers. **But the gap shrinks as leaf count grows**: `and2` is 6.9× slower, `and4` is 3.8×. Larger leaf counts amortise the per-batch overhead (`Arrays.fill`-free output, per-word accumulator, the merge itself) over more comparison work, while the compiled path's cost grows linearly in leaves. The per-leaf cost of drain-side has dropped to roughly half what v1 measured.

Why drain-side stays slower single-threaded:

1. **The bitmap pack does not vectorise.** The value comparison is branchless, but packing a vector compare into bitmap bits has no autovectorisation idiom, so C2 emits a scalar `mov`/`cmp`/`setcc`/`shlx`/`or` per value, unrolled 4× with a rolled remainder. The compiled per-row path pays no packing cost at all.
2. **Match-all pays full `O(n)` matcher cost.** The compiled path's iterator under match-all is effectively `rowIndex++`; drain-side runs the matcher loop in full whether 100% or 0% of rows pass.
3. **Per-survivor `nextSetBit`.** The consumer iterates surviving rows via `Long.numberOfTrailingZeros` + clear-low-bit. At match-all density, that's per-row work the compiled path doesn't do.

These gaps close at higher leaf counts and disappear once the drains run in parallel — see the e2e table above.

---

## Risks and edge cases

- **NULL semantics under intersection.** Each fragment writes "matches" as false on NULL. Word-wise AND of per-column matches gives SQL three-valued AND because any `unknown` conjunct unsets the bit. Reinforced by the three-way oracle.
- **`maxRows` semantics.** `FlatColumnWorker.copyPageRange` enforces `maxRows` as "rows scanned" — the drain stops assembling once `maxRows` rows have been copied. The drain-side filter runs after assembly, so `maxRows` continues to bound *scanned* rows, not *returned* rows. Behaviour matches the compiled path.
- **Wasted decode.** A highly selective predicate on column A still requires column B to be fully decoded — drains are independent. The drain-side filter does not reduce decode cost; it only reduces filter cost.
- **Word-array sizing.** `outWords.length = (batchCapacity + 63) >>> 6`, not `(recordCount + 63) >>> 6`. `Batch` recycles across pages of varying sizes; sizing to capacity avoids reallocation. Matchers leave bits past `recordCount` stale; the consumer's `bit < limit` check in `nextSetBit` and the `Math.min(bit, limit)` clamp in `scanRunEnd` are what filter stale set bits, not a per-batch zero-fill.
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

### `BitSet nulls` → `long[] notNullWords`

**Status: done.** Landed as `BatchExchange.Batch.validity: long[]` (set bit = present), which is the `notNullWords` polarity described here. The rest of this entry is kept for the reasoning it records; the migration itself is no longer outstanding.

**Why it matters.** The biggest remaining drag on single-threaded drain-side throughput. `BitSet.get(int)` is a virtual call into a non-final method that does its own shift, mask, bounds check, and array load — HotSpot will not autovectorise around it. Even though the value comparison in every matcher is now branchless (`(cond ? 1L : 0L) << b`), the null check inside that comparison blocks SIMD lowering. Compiled per-row doesn't have this problem because it uses indexed accessors the JIT inlines through.

**What to build.** Replace `BatchExchange.Batch.nulls: BitSet` with `notNullWords: long[]` (bit `i` set iff row `i` is **not** null). Sized to capacity once at construction. The drain populates it directly during the def-level decode pass, and matchers consult `(notNullWords[i >>> 6] >>> i) & 1L) == 1L` — a primitive load + AND that the JIT folds into the SIMD pass.

**Risk.** This is a broad migration. Every reader path that touches `Batch.nulls` has to switch:

- `FlatRowReader.flatNulls` (private, easy)
- `FlatColumnWorker.markNulls` (the writer, easy)
- `ColumnReader.getElementNulls()` — **public API**, currently returns `BitSet`. Either keep returning `BitSet` (constructed lazily from `notNullWords` on the public boundary) or break callers. The lazy construction is the safer landing.

**Payoff.** Unblocks Vector API matchers (technique below) and meaningfully closes the single-threaded gap on the JMH micro. Probably halves `and2`'s 6.9× single-threaded deficit on its own.

**Difficulty.** Medium-broad. Surgical in each file, but touches many. Land in isolation with no v2-only code so any regression bisects cleanly.

### Vector API matchers

**Status: measured, not pursued.** A working implementation for `int` and `float` reached a green build; it is not merged, because the end-to-end ceiling turned out to be about half a percent. The measurements are recorded here so the next reader starts from them rather than re-deriving them.

**The kernel gain is large and machine-dependent.** One SIMD compare plus a mask-to-bits extraction replaces the scalar loop's six instructions per value. Measured against a 4096-row batch, each vector matcher verified bit-identical to its scalar counterpart before timing:

| kernel | AVX2, 256-bit | NEON, 128-bit |
| --- | ---: | ---: |
| `int` LT | 0.613 → 0.068 ns/value (9.1×) | 0.352 → 0.131 ns/value (2.7×) |
| `float` LT | 0.889 → 0.422 ns/value (2.1×) | 0.624 → 0.199 ns/value (3.1×) |
| `long` GT | 0.546 → 0.316 ns/value (1.7×) | — |

Which type gains more is a property of the machine, not of the types: `int` carries eight lanes per 256-bit vector against four per 128-bit one, and the total-order key transform that `float` needs costs more on x86 than on AArch64. `long` and `double` carry half the lanes of `int` throughout.

**The end-to-end gain is bounded by the matcher's share of a scan, which is about 5 %.** A filtered scan over 2 M rows pinned to one core, filtering on an `int` column, ran 26.1 ms against 27.4 ms scalar — a 1.3 ms saving, inside error bars of ±9.0 and ±4.6 ms, with the long-predicate arm as an unmoved control at 58.7 against 58.3 ms. Two routes agree on what that decomposes to:

- The 1.3 ms saving is 89 % of the matcher's cost, putting the whole scalar matcher at ≈1.5 ms, or 5.3 % of the scan.
- 0.613 ns/value over 2 M rows is 1.23 ms directly, or 4.5 % of the scan.

Either way the residual after a 9× kernel is ≈0.15 ms, **0.5–0.6 % of the scan**, and that is the entire headroom a wider vector would compete for. The parts of the matcher that do not scale with lane count — bitmap word assembly, bounds checks, the word-wise validity AND — are a growing share of what remains, so the realised gain from a wider vector would be smaller still.

`RecordFilterMicroBenchmark` agrees on the magnitude. Raw drain-side numbers for `and3` and `and4` move 11–13 %, but the compiled control moves 6–10 % between the same two JVM launches; normalising each arm against its own control leaves 4–5 %, which is what one `int` leaf out of three or four is worth.

**What it would cost.** Thirteen classes in a multi-release source root that compile only on JDK 22+ and run only when the incubating Vector API module is added, a second implementation of matcher semantics to keep correct — including a hand-rolled `Float.compare` total order whose failure mode is silently wrong NaN and `-0` results rather than a crash — tests that can run only as integration tests, and a two-implementation decision on every matcher added afterwards.

**Two constraints any future attempt inherits.**

- *The species and the operator must both fold to compile-time constants.* Routing either through an instance field or a method parameter drops the kernel onto a generic path that measured roughly 30× slower than the scalar matcher. This forces one class per operator rather than one parameterised by it, and is most of where the class count comes from.
- *`float` cannot compare on float lanes.* The scalar matcher orders by `Float.compare` — every NaN above `+Infinity`, `-0` below `+0` — and the Vector API's float comparison is IEEE. Comparing lanes directly drops every NaN out of the result and ranks `-0` equal to `+0`. Canonicalising NaN and flipping the magnitude bits of negatives reproduces the ordering as one integer comparison.

**What would change the answer.** Not a wider vector, but a larger matcher share of the scan — which is what the late-materialization fork in #500 would do. Revisit then.

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
