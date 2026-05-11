/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;

/// Per-batch values slot for a varlength leaf (`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`
/// / `INT96`).
///
/// `bytes` is the concatenated value buffer; `offsets` is sentinel-suffixed
/// (length `valueCount + 1`). Only bytes in the half-open range
/// `[0, offsets[valueCount])` are valid; bytes beyond that position are
/// unspecified scratch.
///
/// For `FIXED_LEN_BYTE_ARRAY` the `bytes` array is sized exactly to
/// `width * capacity` and `offsets[i] = i * width` is filled trivially by the
/// worker. For variable-length `BYTE_ARRAY` the `bytes` array is pre-allocated
/// to `BatchExchange.BINARY_BYTES_PER_VALUE_HINT * capacity` and grows on
/// overflow via the worker's append path.
public final class BinaryBatchValues {
    public byte[] bytes;
    public int[] offsets;

    public BinaryBatchValues(byte[] bytes, int[] offsets) {
        this.bytes = bytes;
        this.offsets = offsets;
    }

    /// Materialise value `idx` as a fresh `byte[]` copy. Allocates one array
    /// per call — used by convenience accessors and per-row materialisation
    /// paths; hot loops should read [#bytes] / [#offsets] directly.
    public byte[] byteArrayAt(int idx) {
        int start = offsets[idx];
        int len = offsets[idx + 1] - start;
        byte[] result = new byte[len];
        System.arraycopy(bytes, start, result, 0, len);
        return result;
    }

    /// Materialise value `idx` as a UTF-8 decoded `String`. Allocates one
    /// `String` per call.
    public String stringAt(int idx) {
        int start = offsets[idx];
        int len = offsets[idx + 1] - start;
        return new String(bytes, start, len, StandardCharsets.UTF_8);
    }

    /// Length in bytes of value `idx`.
    public int lengthAt(int idx) {
        return offsets[idx + 1] - offsets[idx];
    }

    /// Append `len` bytes from `src[srcOffset..)` as a new value, growing
    /// [#bytes] if it would otherwise overflow. Returns the new value index.
    /// Advances [#offsets] at `valueIdx + 1`. Throws if the cumulative byte
    /// length would overflow `Integer.MAX_VALUE`.
    public void appendAt(int valueIdx, byte[] src, int srcOffset, int len) {
        int start = offsets[valueIdx];
        long needed = (long) start + len;
        if (needed > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Binary batch buffer would exceed int32 (~2 GB) at value " + valueIdx
                    + "; reduce the batch size for this column.");
        }
        if (needed > bytes.length) {
            growBytes((int) needed);
        }
        if (len > 0) {
            System.arraycopy(src, srcOffset, bytes, start, len);
        }
        offsets[valueIdx + 1] = start + len;
    }

    private void growBytes(int minCapacity) {
        long newSize = Math.max((long) bytes.length * 2L, (long) minCapacity);
        if (newSize > Integer.MAX_VALUE) {
            newSize = Integer.MAX_VALUE;
        }
        byte[] grown = new byte[(int) newSize];
        System.arraycopy(bytes, 0, grown, 0, bytes.length);
        bytes = grown;
    }
}
