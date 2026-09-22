# Remote Read Path: Needed Requests Only, Coalesced

**Status: In progress.** Tracking issue: #1262.

## Goal

Every request a read issues is needed for its result, and requests close enough to each other are
merged into one under a single gap policy. The results a read returns and the public API are
unchanged.

On a memory-mapped local file a `readRange` is an address slice, so none of this changes local
wall-clock time measurably. On S3 every request is a round trip of 40–240 ms depending on region,
and every byte fetched without being decoded costs transfer time and heap.

The fetch sequence, the gap policy and the I/O budget as built are in [FETCH_PLANNING.md](../_designs/FETCH_PLANNING.md). This plan records the open changes to them. `G` and `MAX` below are the gap policy's `CoalescingPolicy.GAP_BYTES` and `MAX_SPAN_BYTES`.

## Open changes

### Footer

A file whose parsed footer is already known is opened without a request (#837).

### Page-index and bloom-filter slices

The read fetches only the page-index and bloom-filter slices it needs, for the row groups it will reach, merged under the gap policy (#708, #735):

| Structure | Needed for |
|---|---|
| ColumnIndex | Filter columns, when page filtering is enabled |
| OffsetIndex | Projected columns and filter columns |
| Bloom filter | Columns with an `eq` or `in` leaf that statistics left undecided |

Writers lay these out by structure, then row group, then column, so one column's entries are strided through a block. The read plans the list of needed slices, sorts it by offset and merges it under the gap policy, each structure's slices on their own: a narrow schema, or a small index region, collapses to one request, and a wide schema with a large region splits instead of fetching the indexes of columns the read does not use. Indexes the read does not need are fetched only inside a gap of at most `G` between two needed slices, and no byte is fetched twice. None is fetched when the footer read already covered them. Like every merge a request spans at most `MAX`, so an index region larger than that is read in several requests, split between slices (#1113).

The slices are fetched for a window of row groups at a time: a large file's page index reaches hundreds of megabytes. A window takes as many row groups as fit a byte budget, so a file whose page index fits it costs one request per structure for the whole read. [WINDOWED_INDEX_READS.md](WINDOWED_INDEX_READS.md) has the details.

### Gap value

`G` is the serial break-even between a round trip and the bytes fetched to avoid it; the best value depends on the storage and on how many requests run in parallel. #763 and #827 measure it.

## Delivery

| # | Theme | Issues | State |
|---|---|---|---|
| 1 | I/O budget tests | #1261 | Done |
| 2 | One gap policy | #1260 | Done |
| 3 | Opening a file in one round trip | #852 (done), #837 | In progress |
| 4 | Dictionaries read once, on entering the row group | #1259 | Done |
| 5 | Only surviving data pages fetched | #1037 (done), #1025, #381, #1323 (done) | In progress |
| 6 | Only needed index and bloom-filter slices, merged | #708, #735, #1113 | Open |
