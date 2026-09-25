# Early abandonment of a losing dictionary (#9, stage 26a)

**Status: Completed.** Tracking issue: #992. Delivery stage 26a (Optimization) of
[WRITER_SUPPORT.md](../_plans/WRITER_SUPPORT.md), delivered ahead of its place in the sequence because
stage 21a's measurement is what justifies it. What it delivered is in
[Result](#result).

## Context

[WRITER_DICTIONARY_SELECTION.md](WRITER_DICTIONARY_SELECTION.md) settled how a column chunk's
encoding is chosen: values are interned as they arrive, which makes the chunk's cardinality
exact, and at flush the dictionary body plus its index stream is weighed against the values
`PLAIN`. The smaller wins. A chunk that loses resolves its interned indices back into stored
values through `ColumnChunkBuffer.giveUpDictionary()` and writes no dictionary page.

That decision is optimal and it is taken too late. A column whose values are all distinct
interns every one of them — a hash probe and, on a miss, an insert — into a table that grows to
the chunk's cardinality, and then throws the table away. Stage 21a measured what that costs on
the taxi fixture, at 1 M rows on the N300, uncompressed and single-core:

| | `AUTO` | `PLAIN` on the all-distinct columns | Δ |
|---|---|---|---|
| time | 926 ± 51 ms | 530 ± 66 ms | −43% |
| allocation | 203 MB/op | 128 MB/op | −37% |
| produced file | 25,447,147 B | 25,447,111 B | 36 B |

The 36 bytes are nine chunks' `distinct_count`. Everything else about the two files is
identical, so the 43% and the 37% buy nothing at all.

The profile says what kind of cost it is. `LongDictionaryEncoder.indexOf` is 40% of CPU samples
on its own and about 46% with `insert`, `add`, `resizeTable` and `giveUpDictionary` behind it,
while the counters show 14% more instructions against 82% more cycles, IPC 1.99 against 3.11,
and LLC loads rising from 0.69 M to 5.16 M per operation. It is a cache-missing probe into a
table several megabytes wide. A faster hash function would not address it; not building the
table would.

## The decision

**Run the comparison the writer already makes at flush on the chunk's prefix, and give the
dictionary up as soon as it is losing.**

Nothing about the comparison changes, and nothing about the mechanism changes.
`giveUpDictionary()` already resolves interned indices into stored values mid-chunk — the
flush comparison has called it that way since stage 18 — and the arithmetic the comparison needs is
already maintained per value: the interned count, the dictionary's size and plain bytes, and
the running `PLAIN` width of everything seen. The probe is the existing predicate, evaluated
earlier against the same fields.

That framing is the point rather than an implementation detail. A new heuristic would need its
own justification, its own calibration and its own failure modes. This has one question to
answer — *when* to evaluate — and one property to establish: where the prefix and the whole
chunk agree, which is the overwhelmingly common case, the produced file is byte-identical to
today's and only the work differs.

### When the probe runs

At present-value counts of 8192, then 16384, then 32768, doubling to the end of the chunk.

The schedule is in **values, not pages**. A page cut would be the obvious hook and it is the
wrong one: `pageValues` is derived once per file from the widest column's `PLAIN` width, so it
is 52 000 entries for the taxi fixture but over eight million for a schema of one `BOOLEAN`
column — a chunk that would never reach its first page boundary inside a row group, and so
would never be probed. A value count is a property of the chunk rather than of the schema's
widest member.

8192 is far enough in that the dictionary's fixed cost is fairly sampled and the index bit
width has settled, and early enough that abandoning there saves essentially all of the work:
on this fixture a chunk holds about 334 000 values, so the decision is taken over 2.5% of them
and the remaining 97.5% never touch the table. Doubling bounds the number of probes at a
chunk's cardinality logarithm — at most a few dozen O(1) evaluations against hundreds of
thousands of values — so the schedule costs nothing worth measuring even when it never fires.

### The rule, and what bounds its error

**Abandon when the comparison says the dictionary is losing at two consecutive probes.**

A single probe would be enough for a column that is genuinely all-distinct, and wrong for one
whose distinct values are front-loaded: all-distinct across the first 8192 values and then
nothing but repeats. Such a column looks like the worst case at the first probe and is the best
case over the chunk, and deciding once would write it `PLAIN` and lose a dictionary that would
have paid — a size regression, on a file that is still correct, which no round-trip or interop
test would catch.

Requiring two consecutive losses removes exactly that shape at negligible cost. A front-loaded
column has stopped minting distinct values by the second probe, so its distinct ratio has
halved and the comparison has flipped back in the dictionary's favour; it keeps its dictionary
and is decided at flush as it is today. A genuinely all-distinct column loses at 8192 and again
at 16384 and abandons there, over 5% of the chunk instead of 2.5% — which is not a difference
worth trading the guard for.

This is the concern [WRITER_DICTIONARY_SELECTION.md](WRITER_DICTIONARY_SELECTION.md) raises
where it declines to abandon on a single probe: *"What a small cap risks is the column whose
cardinality saturates late: it looks all-distinct over its first values and is written `PLAIN`
on that evidence."* The two-probe rule is what bounds that risk rather than accepting it. It
does not eliminate it — a column that saturates only after 16384 values still loses its
dictionary — which is why the residual is stated under
[What this does not do](#what-this-does-not-do) rather than left implicit.

### Why the comparison is safe to run on a prefix

For a fixed-width column of width `W` holding `V` values of which `D` are distinct, the
dictionary's bytes are `D·W + V·b/8` for an index bit width `b`, against `V·W` for `PLAIN`. The
dictionary loses when `D/V ≥ 1 − b/(8W)`: about 0.77 for an `INT64` column at the bit widths a
few thousand entries reach. The predicate is therefore not a knife edge around a coin flip — it
fires only where the distinct ratio is already high, which is where the ratio is also stable,
and the borderline column it declines to abandon simply keeps interning and is decided at flush
exactly as today. The cost of being cautious is the work this stage exists to remove, and the
cost of being wrong is a larger file; the rule is tuned so that caution is the default.

## What changes, and what does not

| | |
|---|---|
| The comparison | Unchanged, and now evaluated at probe points as well as at flush |
| `giveUpDictionary()` | Unchanged; it is the mechanism, and the probe is a new caller |
| What bounds a dictionary's memory | `rowGroupBufferTargetBytes`, which a dictionary counts against as part of what its chunk retains; a chunk the probe abandons stops adding to it |
| The flush-time decision | Unchanged for every chunk that survives probing |
| `Statistics.distinct_count` | Absent for an abandoned chunk, as it already is for one the cap abandons — the position stage 29 (#982) addresses |
| Named encoding policies | Unaffected; a column under one builds no dictionary to abandon |
| `WriterConfig` | Unchanged. The schedule is internal: it is a property of how the writer decides, not a decision a caller takes |
| Produced files | Byte-identical wherever prefix and chunk agree; a chunk abandoned early is one the flush comparison would have rejected anyway |

## Validation

- **Unit, on the rule.** A chunk of all-distinct values abandons at the second probe and its
  metadata says `PLAIN`; a low-cardinality chunk is never abandoned and keeps `RLE_DICTIONARY`;
  a front-loaded chunk — every value distinct through the first probe, then repeats — keeps its
  dictionary, which is the guard's whole purpose and the one case a single-probe rule would get
  wrong.
- **Byte-identical output.** The existing writer suites pin produced bytes for fixtures whose
  chunks the flush comparison already rejects; those files must not move. A fixture that
  changes is either a genuine early abandonment, which is the point, or a defect.
- **The round trip and the gate.** Every abandoned chunk is a `PLAIN` chunk, a shape both cover
  already; the interop gate and the differential suite run unchanged.
- **The benchmark.** `WriteEncodingBenchmark`'s `AUTO` case should converge on its
  `PLAIN_ON_DISTINCT` case in both time and allocation, and the two files should agree in size:
  identical once `AUTO` stops stating the `distinct_count` that separated them. That convergence
  is the acceptance criterion, and the class asserts the size relationship within a slack that
  covers the statistic either way, so a change that buys speed by writing different pages fails
  rather than reports.

## Result

`WriteEncodingBenchmark` on the N300, 1.50 GHz pinned, single core, 1 M rows, five-second
iterations, guard CLEAN:

| | `AUTO` before | `AUTO` after | `PLAIN_ON_DISTINCT` |
|---|---|---|---|
| uncompressed | 926 ± 51 ms | **537 ± 63 ms** | 517 ± 39 ms |
| `ZSTD` | 1086 ± 7 ms | **708 ± 34 ms** | 699 ± 56 ms |
| allocation, uncompressed | 203 MB/op | **131 MB/op** | 128 MB/op |
| allocation, `ZSTD` | 234 MB/op | **162 MB/op** | 159 MB/op |

A 42% cut uncompressed and 35% under the default codec, and `AUTO` now sits inside the error
bars of the case that skips the interning by configuration — which was the acceptance
criterion, because that case is the ceiling. What remains between them is the probe's own
8192-value prefix, which is the price of deciding from evidence rather than from a caller's
declaration.

The two cases now produce **byte-identical files** — 25,447,105 uncompressed and 13,111,190
under `ZSTD` — where they were 36 bytes apart before. Those 36 bytes were the nine
`distinct_count` fields `AUTO` used to state and no longer can, which is the cost recorded
below. Sizes are compared within one machine: `created_by` carries the build revision, so the
same writer produces files of different length from different checkouts.

## What this does not do

- **It does not keep deciding after a dictionary is abandoned.** That was stage 26b (#979), and it
  is the other half of making the cap a memory bound rather than an encoding verdict. The two
  are complementary: this one declines dictionaries that are losing, and that one rescues
  dictionaries that are winning when the cap cuts the analysis short.
- **It does not remove the residual risk of a late-saturating column.** A column whose values
  are distinct through 16384 and repeating thereafter is written `PLAIN` where a dictionary
  would have paid. Bounding that further means estimating cardinality rather than measuring a
  prefix, which is 26b's machinery, not this stage's.
- **It does not touch the per-value path itself.** The interning that remains — on the columns
  that keep their dictionaries, which is where it earns its keep — costs what it costs.
- **It adds no configuration.** Neither the first probe count nor the doubling is exposed.
