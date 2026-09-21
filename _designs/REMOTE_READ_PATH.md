# Remote Read Path: Needed Requests Only, Coalesced

**Status: In progress.** Tracking issue: #1262.

## Goal

Every request a read issues is needed for its result, and requests close enough to each other are
merged into one under a single gap policy. The results a read returns and the public API are
unchanged.

On a memory-mapped local file a `readRange` is an address slice, so none of this changes local
wall-clock time measurably. On S3 every request is a round trip of 40–240 ms depending on region,
and every byte fetched without being decoded costs transfer time and heap.

## Fetch sequence

### Per file, before any row is produced

| # | Step | Requests |
|---|---|---|
| 1 | Footer | One: the suffix-range read `S3InputFile` issues on open. None when the parsed footer is already known (#837). The leading `PAR1` magic is not read: the trailing magic identifies the file, and every page is located through the footer. |
| 2 | Prune row groups on footer statistics | None |
| 3 | Fetch the page-index and bloom-filter slices the read needs, for the row groups it will reach | Usually one, merged under the gap policy; none when the footer read already covered them |
| 4 | Prune row groups on bloom filters | None |

The slices of step 3 are:

| Structure | Needed for |
|---|---|
| ColumnIndex | Filter columns, when page filtering is enabled |
| OffsetIndex | Projected columns and filter columns |
| Bloom filter | Columns with an `eq` or `in` leaf that statistics left undecided |

Writers lay these out by structure, then row group, then column: all ColumnIndexes, all
OffsetIndexes, all bloom filters, each block ordered `rg0: c0 … cN, rg1: c0 … cN, …`. One column's
entries are therefore strided through a block. The read plans the list of needed slices, sorts it
by offset and merges it under the gap policy: a narrow schema, or a small index region, collapses
to one request, and a wide schema with a large region splits instead of fetching the indexes of
columns the read does not use. Indexes the read does not need are fetched only inside a gap of at
most `G` between two needed slices, and no byte is fetched twice. Like every merge a request spans
at most `MAX`, so an index region larger than that is read in several requests, split between
slices (#1113).

A read that may stop early (`maxRows` under a filter, a consumer that abandons iteration) fetches
the slices for a window of row groups that grows as the read advances, rather than for the whole
file.

### Per row group, on entry

| # | Step | Requests |
|---|---|---|
| 5 | Read the dictionaries a filter can prune with: fully dictionary-encoded filter columns with an `eq` or `in` leaf that statistics and bloom filters left undecided. Drop the row group if they prove the value absent. | One per such column |
| 6 | Page filtering from the ColumnIndex | None |
| 7 | Data: the surviving pages of each projected column; the dictionary page taken from step 5 or merged into the first page group under the gap policy; adjacent columns merged | As few as the gap policy allows |

Planning prunes on statistics and bloom filters and records each filter leaf's decision per row
group (`RowGroupFilterEvaluator.planRowGroup`). On entry, the dictionaries sharpen those recorded
decisions (`refineWithDictionaries`): statistics are not evaluated again and bloom filters are not
read again. The dictionaries stay in the row group's `SharedRowGroupMetadata` until every column
has released the row group, and the fetch plans decode with them. At most the current row group's
and the prefetched next row group's dictionaries are held at once.

A row group its dictionaries drop gets empty fetch plans and no page-index or data read. It is
reported by the `dev.hardwood.RowGroupDictionaryFilter` JFR event; `dev.hardwood.RowGroupFilter`,
emitted once per file at planning, counts it as kept.

## Gap policy

Two byte ranges are fetched as one request when the gap between them is at most `G` and the merged
span is at most `MAX`. `G` is 1 MiB and `MAX` 128 MiB. `CoalescingPolicy` holds the rule, and
every merge on the read path calls it:

| Merge | Where |
|---|---|
| Pages within a column | `RowGroupIterator.coalescePages` |
| Dictionary page into the first page group | `RowGroupIterator.foldsDictionary`; a dictionary page further away gets its own `ChunkHandle`, fetched first and prefetching the first page group |
| Adjacent column chunks | `RowGroupIterator.coalesceAcrossColumns` |
| Page-index and bloom-filter slices | step 3 |

A merge bridges gaps between needed ranges. It never extends a request before the first needed
byte or past the last one.

`G` is the serial break-even between a round trip and the bytes fetched to avoid it, latency ×
bandwidth: 30 ms × 50 MiB/s is about 1.5 MiB. The best value depends on the storage and on how
many requests run in parallel; #763 and #827 measure it. It is kept in one place so that a
storage-dependent value replaces one constant.

## I/O budget

`IoBudget` (core tests) computes from the footer and OffsetIndex the ranges a read has to fetch
for a given projection and set of matching rows: each decoded column's dictionary page, plus the
data pages whose rows intersect the matching rows, or the whole chunk where there is no
OffsetIndex. A read is within budget when every data byte it fetches lies in a needed range or in
a gap of at most `G` between two, and no data byte is fetched twice. Reads issued for row-group
pruning run under the `rg=N pruning` fetch reason and are left out; the pruning tests pin their
cost.

`CountingInputFile` records every read with its fetch reason. Each change to the read path
asserts its effect against the budget.

## Delivery

| # | Theme | Issues | State |
|---|---|---|---|
| 1 | I/O budget tests | #1261 | Done |
| 2 | One gap policy | #1260 | Done |
| 3 | Opening a file in one round trip | #852 (done), #837 | In progress |
| 4 | Dictionaries read once, on entering the row group | #1259 | Done |
| 5 | Only surviving data pages fetched | #1037 (done), #1025, #381 | In progress |
| 6 | Only needed index and bloom-filter slices, merged | #708, #735, #1113 | Open |
