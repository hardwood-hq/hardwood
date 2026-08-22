# Column Reader Small-Page Window

Status: Implemented

## Overview

The column reader uses an expanded in-flight window for stored-small compressed pages. Pages below 128 KiB may occupy up to sixteen reorder slots with the default configuration. Uncompressed pages, null placeholders, and compressed pages at or above the threshold retain the original eight-page limit.

Every Parquet page remains an independent executor task. The policy changes how far the retriever may run ahead, not decode work-unit granularity.

## Pipeline

The retriever remains the sole consumer of `PageSource`. For each page it selects an in-flight limit from the page's codec and stored size, waits for capacity, reserves a sequence number and reorder-buffer slot, and submits the page to the shared decode executor.

Decode tasks publish results independently. The drain consumes them in sequence order and retains responsibility for file boundaries, filter boundaries, batch assembly, and row limits.

## Bounds and transitions

The reorder buffer holds twice `MAX_INFLIGHT_PAGES`. A stored-small compressed page may use the full buffer. Other pages throttle at `MAX_INFLIGHT_PAGES`.

When the input transitions from stored-small compressed pages to another page class, the retriever waits until outstanding work falls below the original limit before submitting the next page. Slot reuse remains safe because every limit is at most the physical reorder-buffer capacity.

The 128 KiB threshold targets columns split into many small pages while excluding size-limited pages that already carry enough decode work per executor task. The two-window cap avoids the retention and latency instability observed with a four-window expansion.

## Codec behavior

Uncompressed pages retain the original window and page-sized work units, preserving their cache-local behavior. Compressed pages retain page-sized work units as well; only stored-small pages receive additional submission headroom so the retriever can keep the decode pool supplied.

## Lifecycle and errors

Decode task tracking, shutdown, error propagation, and input-buffer lifetime guarantees are unchanged. The expanded reorder buffer uses the same per-slot filename and filter metadata happens-before chain as the original buffer.

The internal `hardwood.internal.smallPageWindow` diagnostic property disables the expanded policy when set to `false`. The benchmark uses this switch to measure baseline and adaptive behavior from one binary; normal readers default it to `true`.
