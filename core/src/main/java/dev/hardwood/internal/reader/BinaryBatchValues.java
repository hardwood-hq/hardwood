/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

    /// Whether this column's values are interned `UTF8` / `JSON` `String`s. Set
    /// once at batch allocation ([BatchExchange#allocateArray]); only these
    /// columns record dictionary indices and reuse cached `String`s. When
    /// `false`, [#stringAt] always materialises from [#bytes].
    public boolean internStrings;

    /// The chunk dictionary backing [#dictIndices], or `null` when no dictionary
    /// page has contributed to this batch (every value then materialises from
    /// [#bytes]). Holds the per-entry `String` cache that lets [#stringAt] reuse
    /// one instance per dictionary entry per chunk.
    public Dictionary.ByteArrayDictionary dictionary;

    /// Per-value dictionary entry index, meaningful only when [#dictionary] is
    /// non-null. `-1` marks a value that must materialise from [#bytes]: a
    /// plain-encoded value, a null position, or a value from a second chunk's
    /// dictionary in a batch that straddles a chunk boundary. Allocated lazily
    /// by [#ensureDictionary] when the first dictionary page lands.
    public int[] dictIndices;

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

    /// Materialise value `idx` as a UTF-8 decoded `String`. A dictionary-encoded
    /// value (when [#dictionary] is set and `dictIndices[idx]` is non-negative)
    /// reuses the chunk dictionary's per-entry interned cache, so repeated values
    /// are decoded once per chunk; any other value allocates one `String` per
    /// call from [#bytes].
    public String stringAt(int idx) {
        Dictionary.ByteArrayDictionary dict = dictionary;
        if (dict != null) {
            int dictIndex = dictIndices[idx];
            if (dictIndex >= 0) {
                return dict.internedString(dictIndex);
            }
        }
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

    /// Records dictionary entry indices for a contiguous page range
    /// `[srcPos, srcPos + length)` landing at `[destPos, destPos + length)`, so
    /// [#stringAt] can reuse one materialised `String` per entry. A no-op for a
    /// non-interned column ([#internStrings] `false`).
    ///
    /// `pageDictIndices` is `null` for a plain (non-dictionary) page; such
    /// values are recorded as `-1` only once the batch is already on the
    /// dictionary path, since otherwise [#stringAt] reads [#bytes] regardless.
    /// The first dictionary page switches the batch on (see [#ensureDictionary]).
    public void recordDictIndices(int[] pageDictIndices, Dictionary.ByteArrayDictionary pageDict,
                                  int srcPos, int destPos, int length) {
        if (!internStrings) {
            return;
        }
        if (pageDictIndices == null) {
            if (dictionary != null) {
                Arrays.fill(dictIndices, destPos, destPos + length, -1);
            }
            return;
        }
        if (ensureDictionary(pageDict, destPos)) {
            System.arraycopy(pageDictIndices, srcPos, dictIndices, destPos, length);
        }
        else {
            Arrays.fill(dictIndices, destPos, destPos + length, -1);
        }
    }

    /// Records the dictionary entry index for a single gathered value at
    /// `destPos`. Used by nested assembly, where kept values are scattered by
    /// the rep/def-level walk rather than copied as a contiguous range. See
    /// [#recordDictIndices] for the range form; the dictionary-switch rules are
    /// identical. A no-op for a non-interned column ([#internStrings] `false`).
    public void recordDictIndex(int[] pageDictIndices, Dictionary.ByteArrayDictionary pageDict,
                                int srcPos, int destPos) {
        if (!internStrings) {
            return;
        }
        if (pageDictIndices == null) {
            if (dictionary != null) {
                dictIndices[destPos] = -1;
            }
            return;
        }
        // ensureDictionary lazily allocates dictIndices, so it must run before
        // the `dictIndices[destPos]` store target is evaluated — otherwise the
        // store binds the pre-allocation (null) array reference.
        int dictIndex = ensureDictionary(pageDict, destPos) ? pageDictIndices[srcPos] : -1;
        dictIndices[destPos] = dictIndex;
    }

    public void recordRepeatedDictIndex(Dictionary.ByteArrayDictionary pageDict, int destPos, int count, int index) {
        if (!internStrings) {
            return;
        }
        if (ensureDictionary(pageDict, destPos)) {
            Arrays.fill(dictIndices, destPos, destPos + count, index);
        } else {
            Arrays.fill(dictIndices, destPos, destPos + count, -1);
        }
    }

    public void recordMappedDictIndices(Dictionary.ByteArrayDictionary pageDict, int[] mappedIndices, int destPos, int count) {
        if (!internStrings) {
            return;
        }
        if (ensureDictionary(pageDict, destPos)) {
            System.arraycopy(mappedIndices, 0, dictIndices, destPos, count);
        } else {
            Arrays.fill(dictIndices, destPos, destPos + count, -1);
        }
    }

    public void fillNullDictIndices(int destPos, int count) {
        if (!internStrings) {
            return;
        }
        if (dictionary != null) {
            Arrays.fill(dictIndices, destPos, destPos + count, -1);
        }
    }

    /// Switches the batch onto the dictionary representation on the first
    /// dictionary page that contributes: adopts `pageDict`, allocates
    /// [#dictIndices] (sized to the value capacity), and backfills the plain
    /// prefix `[0, destPos)` with `-1`. Returns `true` when `pageDict` is this
    /// batch's dictionary — so the caller records the page's indices — and
    /// `false` when the value belongs to a second dictionary in a batch that
    /// straddles a chunk boundary, in which case it falls back to byte
    /// materialisation.
    private boolean ensureDictionary(Dictionary.ByteArrayDictionary pageDict, int destPos) {
        if (dictionary == null) {
            dictionary = pageDict;
            int capacity = offsets.length - 1;
            if (dictIndices == null || dictIndices.length < capacity) {
                dictIndices = new int[capacity];
            }
            Arrays.fill(dictIndices, 0, destPos, -1);
        }
        return dictionary == pageDict;
    }
}
