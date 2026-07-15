# Design: bulk-copy whole all-present pages in regular nested assembly

**Status: Completed.** Tracking issue: #786. Potential follow-up: #750.

## Goal

Give `NestedColumnWorker.assembleRegularPage` a whole-page fast path: when an
entire page is proven all-present and is read unmasked, gather its leaf values
into the batch with one `System.arraycopy` and produce whatever the batch's index
mode requires directly, instead of walking the page one element at a time. Any
page that is not wholly present, or is read under a record mask, falls back
unchanged to the existing per-element path.

The optimisation is keyed on **presence, not shape**, so it is not specific to
lists: it applies to any column the regular nested assembly path serves — lists
(fixed or variable length), maps, structs — and, until #732 moves non-repeated
columns to direct addressing, the flat columns of a mixed dataset that are
currently routed through the nested worker. It produces byte-for-byte the same
batch output and requires no format or writer change.

## Background & motivation

Parquet stores only present values, densely. When an entire page is all-present
its leaf values are one contiguous block, 1:1 with page positions; the repetition
levels group them into records but do not move them. Value contiguity is a
property of presence, not of the column being a fixed-width list.

The regular assembly path does not exploit this. For every leaf it runs a
per-element sequence — two capacity checks (`ensureLevelCapacity` +
`ensureValueCapacity`), a bounds-checked `iastore` for each of the definition and
repetition level, a `checkcast` of the destination array, and `copyOneValue` —
around a value copy that is itself two instructions. On an all-present scan this
path executes on the order of 20× the instructions of the equivalent flat read,
and the value copy is the cheap part; the cost is the per-element bookkeeping. The
flat reader already avoids it (`FlatColumnWorker` copies a whole page range in one
`arraycopy`), as does the fixed-size-list fast path per fixed-width page.

## Whole page, not sub-page runs

The unit of this optimisation is the **whole page**, decided by a single O(1)
check, not maximal present runs discovered inside a page.

A page is taken on the fast path when both hold:

- **All-present**: the definition-level stream is a single RLE run of
  `maxDefinitionLevel` covering all `numValues` — the same O(1) header inspection
  (`isSingleRleRunOf`) the flat reader and the fixed-size-list detector already
  use. It reads one varint and one value byte; it does **not** scan the levels.
- **Unmasked**: the read mask selects the whole page (`mask.isAll()`), so the
  page's values are one contiguous, record-aligned span with no gaps.

If either fails — any null or empty entry anywhere on the page, or a record mask
that fragments it — the page is handled entirely by the existing per-element path.

Sub-page present-run copying (scanning the definition levels for runs broken by
nulls or mask gaps, and bulk-copying each run) is deliberately **not** part of
this design: the per-run detection is O(numValues) and the run-boundary
bookkeeping offsets the bulk-copy saving, so it does not pay off. The whole-page
gate is O(1) and adds nothing to the fallback path, which is what makes the win
free to attempt: a page either qualifies wholesale or is untouched. Generalising
to sub-page present runs is a potential follow-up, tracked in #750.

## Design

When a page qualifies, `assembleRegularPage` resolves the page value type once and
processes the page in record-aligned chunks rather than per element.

**Values (all index modes).** Bulk-copy the chunk with one `System.arraycopy` from
the page value array into the batch accumulator, into a destination array whose
type was resolved once for the page (no per-element `checkcast` or `switch
(page)`), growing the accumulator once per chunk.

**Structure — as cheaply as the index mode allows.** The all-present shape makes
every mode's per-batch output trivial to produce, so none of it is done per
element:

- `REAL_VIEW` (the unfiltered `ColumnReader` path, which since #751 derives
  offsets + validity on the drain and drops raw levels): build that view directly
  — record offsets from a repetition-level boundary scan, validity all-true — and
  do **not** materialise raw def/rep level arrays. This is the same output the
  fixed-size-list fast path already produces via its all-present view.
- `REAL_VIEW_KEEP_LEVELS` (exact-filter) and `ALL_ITEMS`: these consumers read the
  raw level arrays, so fill them for the chunk in one pass — definition levels are
  the constant `maxDefinitionLevel` (bulk fill), repetition levels are
  `arraycopy`-ed from the page — instead of the per-element bounds-checked
  `iastore`s.

Record boundaries are identical to the per-element path in every mode, so
cross-column batch alignment is preserved.

### Record-aligned cuts

A nested batch must cut at a **record** boundary, and records have variable
length, so a qualifying page may span more records than fit in the open batch. The
page is copied in batch-capacity-aligned chunks: copy up to the record boundary
that fills the batch, publish, then continue into the next batch. The
repetition-level boundary scan supplies the cut points, so they are known without
touching values.

## Scope

**In scope:** the unfiltered regular `assembleRegularPage` path for any wholly
present page the nested worker handles — lists (fixed or variable length), maps,
structs, and (until #732) non-repeated columns routed through the nested worker.
Transparent: identical batch output and public behaviour.

**Out of scope:**

- **Partially-null or masked pages.** Any page with an interior null/empty, or read
  under a record mask, uses the existing per-element path unchanged. Sub-page run
  copying is explicitly excluded (see above; follow-up #750).

**Builds on (already implemented):**

- **#751 — level materialisation dropped on the drain** ([NESTED_REALVIEW_ON_DRAIN.md]).
  The `REAL_VIEW` mode this exploits — deriving offsets + validity without raw
  levels — exists because of #751; this design makes producing that view for an
  all-present page cheaper (direct, not per-element).
- **#741 — fixed-size-list fast path** ([FIXED_SIZE_LIST_FASTPATH.md]). The
  fixed-`k` present-page case, which additionally uses arithmetic `(rows × k)`
  offsets and skips the repetition scan. It is the shape-specific specialisation of
  this general all-present path.

## Relationship to the fixed-size-list fast path

The fixed-size-list fast path (#741) already ships. It is the fixed-`k`
specialisation of this optimisation: a wholly present page whose records all have
the same length, so its offsets are arithmetic and its repetition scan can be
skipped. This design generalises the same idea — bulk value copy for an
all-present page — to any record shape, and the two share one bulk-copy primitive
(the fixed-list path's per-record `copyValueRun` is refactored onto it).

The fast path currently reports its speedup against the regular per-element path as
baseline. Because that baseline copies element by element, the reported
fast-vs-baseline ratio overstates the win; bulk-copying whole all-present pages in
the regular path makes the baseline honest, after which the fast path's remaining
advantage is only the arithmetic offsets and the skipped repetition scan.

## Correctness

Validated against the differential oracle (DuckDB) and the nested reader tests,
which must stay green across: all-present columns, columns with interior nulls and
empty lists, null and empty top-level records, filtered scans with interval masks
(and the exact-filter `REAL_VIEW_KEEP_LEVELS` path), batch boundaries that fall
inside a qualifying page, and dictionary / byte-array / FLBA leaves. The whole-page
path and the per-element fallback must produce identical batches; the same
all-present page forced through the per-element path folds to the same values,
offsets, validity, and levels.

## Performance

Bulk-copying whole all-present pages removes most of the regular nested path's
per-element instruction overhead, moving a fast-path-off all-present scan
materially toward the flat-column floor. The fixed-`k` fast path, rebuilt on the
shared span-copy primitive, is unchanged or faster — and copying record runs in
one span rather than per record is the dominant win at small `k`, where the
per-record `arraycopy` calls previously dominated. The per-element fallback for
null-bearing or masked pages is not regressed.
