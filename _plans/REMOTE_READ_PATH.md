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

The page index is fetched as the slices the read needs, in windows of row groups merged per structure ([FETCH_PLANNING.md](../_designs/FETCH_PLANNING.md#page-index-windows)). Bloom filters follow: the bloom filters of columns with an `eq` or `in` leaf that statistics left undecided, merged under the gap policy on `CoalescedRanges` (#735).

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
| 6 | Only needed index and bloom-filter slices, merged | #708 (done), #1113 (done), #735 | In progress |
