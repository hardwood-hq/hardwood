<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Bulk run copy for the regular nested assembly path

Status: **Planned**

Tracking issue: #750

## Problem

`NestedColumnWorker.assembleRegularPage` materializes a page one value at a time.
For every non-masked position it calls `copyOneValue`, writes a definition level
and a repetition level, and checks accumulator capacity. Profiling a
`LIST<float32>` scan (fast path disabled) shows this per-element sequence — not
the value copy itself — dominates: ~1.26 B instructions/op versus ~60 M for a
flat read of the same values (21×), at IPC 1.58 (compute-bound, not memory). The
per-value machinery is a capacity check, two bounds-checked level `iastore`s, and
a `checkcast` of the value array, around a two-instruction copy.

## Enabling invariant

A decoded page is **level-aligned**: `values()`, `definitionLevels()`, and
`repetitionLevels()` all have `size()` entries, one per level-stream position.
Present values are scattered into `values()` at their level position; null
positions carry a placeholder (`0` / `null` ref). `assembleRegularPage` copies
every non-masked position 1:1 into the batch (nulls included, as placeholder
slots — their null-ness is carried by the definition levels), advancing
`nestedValueCount` once per position.

Therefore, over a contiguous span of kept positions, the batch's values and its
definition and repetition levels are each a **contiguous slice** of the page's
corresponding array. The span can be filled with three `System.arraycopy`s
instead of a per-element loop.

## End state

`assembleRegularPage` processes each page as a sequence of **runs** — maximal
spans of consecutive kept (non-masked) positions that also fit within the current
batch and `maxRows` budget. For each run:

- **Values** — bulk-copied via `copyValueRun` (the same primitive the fixed-size
  fast path uses; primitive types only). Byte-array-backed types
  (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) keep the per-element
  `copyOneValue`, since their batch representation is not a flat array.
- **Levels** — the run's definition and repetition levels are a slice of the
  page's level arrays: `System.arraycopy` when the page carries them, or
  `Arrays.fill` with `maxDefinitionLevel` / `0` for the all-present case where a
  level array is `null`.
- **Record offsets** — a run spans one or more whole top-level records. Record
  starts (`repLevel == 0`) are visited to record `nestedRecordOffsets`; this is a
  per-record step, not per-value.
- **Capacity** — one `ensureNestedCapacity` for the whole run.

Run boundaries are cut at: a masked-out record (the run resumes at the next kept
record), the batch-capacity limit (publish, then continue), the `maxRows` limit,
and page end. Record-boundary detection, masking, batch publishing, and the
first-record-of-stream handling are unchanged in behavior — only the inner
value/level writes move from per-element to per-run.

The change is transparent: identical `NestedBatch` output (values, levels, record
offsets, validity) for all inputs — nulls, masking, multi-level nesting, and
mixed pages. Arbitrary nesting depth is preserved because the deeper structure is
reconstructed downstream from the faithfully copied levels; `assembleRegularPage`
only interprets `repLevel == 0` for top-level record offsets.

## Scope

- Primitive element columns get the run copy; byte-array columns are unchanged.
- This is the general-path analogue of the fixed-size-list fast path
  ([FIXED_SIZE_LIST_FASTPATH.md](FIXED_SIZE_LIST_FASTPATH.md)) and shares its
  `copyValueRun` primitive. It applies whenever a run is all-present and unmasked,
  which is the common case even for variable-length lists.
- Measurement note: this lands as the baseline the fixed-size-list fast path is
  measured against, so the fast path's reported speedup is not inflated by an
  unoptimized per-element reconstruction base.
