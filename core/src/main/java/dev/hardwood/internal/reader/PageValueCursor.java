/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.metadata.Encoding;

/// Reusable cursor that tracks the decode state of a single data page across
/// batch boundaries.
///
/// A page with N values may straddle two (or more) batches when the remaining
/// capacity of the current batch is less than N.  The cursor survives the
/// publish of the full batch and is resumed in the next one:
///
/// <pre>
///   Pass 1: decodeInto(batchA, offset, count1)   → cursor.valuesLeft -= count1
///           publish batchA, take batchB
///   Pass 2: decodeInto(batchB, 0,      count2)   → cursor.valuesLeft -= count2
/// </pre>
///
/// Lifecycle: one instance per reorder-buffer slot, parallel to
/// [PageDecoder.LevelScratch].  The retriever throttle ensures that a slot is
/// not reused until the drain has consumed the page stored in it, so no
/// concurrent access is possible.
///
/// When [#valuesLeft] reaches 0 the cursor is implicitly "parked" until
/// [PageDecoder#fillCursor] is called for the next page.
final class PageValueCursor {

    /// Decompressed page bytes.  May be larger than needed (reused buffer grown to
    /// the largest page seen on this slot); only bytes [0, dataLen) are valid.
    byte[] data = new byte[0];

    /// Number of valid bytes in [#data].
    int dataLen;

    /// Byte offset of the next unread value within [#data].
    int srcPos;

    /// Exclusive byte bound of the value region in [#data].  Values live in
    /// [#srcPos, #srcLimit); bytes beyond that are stale from an earlier page.
    int srcLimit;

    /// Remaining values not yet written to any batch.  Zero means the cursor is
    /// idle (no page in flight).
    int valuesLeft;

    /// The encoding this cursor was filled for, so [FlatColumnWorker] can decide
    /// whether the direct path applies.  {@code null} when idle.
    Encoding encoding;

    // === BYTE_STREAM_SPLIT straddle state ===
    /// Base byte offset of the first BSS stream within [#data].
    /// Only valid when [#encoding] == BYTE_STREAM_SPLIT.
    int bssBaseOffset;
    /// Total number of values on the page for BSS stream sizing.
    /// Only valid when [#encoding] == BYTE_STREAM_SPLIT.
    int bssTotalValues;
    /// Current decode index within the BSS stream (values already consumed).
    /// Starts at 0; incremented by each decodeDirectly call.
    /// Only valid when [#encoding] == BYTE_STREAM_SPLIT.
    int bssCurrentIndex;

    /// Grows [#data] in-place if needed and copies the page bytes into it.
    ///
    /// @param pageData  the decompressed page bytes
    /// @param dataLen   number of valid bytes in pageData
    void copyData(byte[] pageData, int dataLen) {
        if (this.data.length < dataLen) {
            this.data = new byte[dataLen];
        }
        System.arraycopy(pageData, 0, this.data, 0, dataLen);
        this.dataLen = dataLen;
    }

    /// Resets the cursor to idle (called when the page is finished or the worker
    /// falls back to the existing path).
    void reset() {
        valuesLeft = 0;
        encoding = null;
    }

    /// Whether the cursor is currently tracking an in-flight page.
    boolean isActive() {
        return valuesLeft > 0;
    }
}
