/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.variant;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// Parses the Variant metadata byte buffer and provides dictionary lookup by
/// field id or by name.
///
/// Layout:
/// - 1-byte header: version (bit 0-3) | sorted_strings (bit 4) | offset_size_minus_one (bit 5-6)
/// - `offset_size` bytes: dictionary_size (unsigned LE)
/// - `(dictionary_size + 1) * offset_size` bytes: offsets into the strings section (unsigned LE)
/// - `offset_size` bytes of the last offset define the total byte length
/// - UTF-8 strings concatenated, indexed by the offset table
public final class VariantMetadata {

    private final byte[] buf;
    private final int offsetSize;
    private final boolean sorted;
    private final int dictionarySize;
    private final int offsetsStart;   // offset of the offset-table start in `buf`
    private final int stringsStart;   // offset of the strings section in `buf`

    public VariantMetadata(byte[] buf) {
        if (buf == null || buf.length < 1) {
            throw new IllegalArgumentException("Variant metadata buffer is empty");
        }
        this.buf = buf;
        int header = buf[0] & 0xFF;
        int version = header & VariantBinary.METADATA_VERSION_MASK;
        if (version != VariantBinary.METADATA_VERSION) {
            throw new IllegalArgumentException("Unsupported Variant metadata version: " + version);
        }
        this.sorted = (header & VariantBinary.METADATA_SORTED_MASK) != 0;
        int offsetSizeMinusOne = (header >>> VariantBinary.METADATA_OFFSET_SIZE_SHIFT) & VariantBinary.METADATA_OFFSET_SIZE_MASK;
        this.offsetSize = offsetSizeMinusOne + 1;

        int headerEnd = 1;
        int requiredForSize = headerEnd + offsetSize;
        if (buf.length < requiredForSize) {
            throw new IllegalArgumentException("Variant metadata buffer truncated before dictionary_size");
        }
        this.dictionarySize = VariantBinary.readUnsignedLE(buf, headerEnd, offsetSize);
        this.offsetsStart = headerEnd + offsetSize;
        int offsetsLen = (dictionarySize + 1) * offsetSize;
        if (buf.length < offsetsStart + offsetsLen) {
            throw new IllegalArgumentException("Variant metadata buffer truncated before offset table");
        }
        this.stringsStart = offsetsStart + offsetsLen;
        int totalStringBytes = readOffset(dictionarySize);
        if (buf.length < stringsStart + totalStringBytes) {
            throw new IllegalArgumentException("Variant metadata buffer truncated before end of strings section");
        }
    }

    /// Returns the raw metadata buffer (unmodified, same reference as passed in).
    public byte[] buffer() {
        return buf;
    }

    /// Number of entries in the dictionary.
    public int size() {
        return dictionarySize;
    }

    /// Whether the writer declared the dictionary as sorted (enables binary search).
    public boolean isSorted() {
        return sorted;
    }

    /// Returns the dictionary string at the given id, decoded as UTF-8.
    ///
    /// @throws IndexOutOfBoundsException if `id` is not in `[0, size())`
    public String getField(int id) {
        if (id < 0 || id >= dictionarySize) {
            throw new IndexOutOfBoundsException("Field id out of range: " + id + " (size=" + dictionarySize + ")");
        }
        int start = readOffset(id);
        int end = readOffset(id + 1);
        return new String(buf, stringsStart + start, end - start, StandardCharsets.UTF_8);
    }

    /// Looks up a field id by its string name. Returns `-1` if not found. Uses
    /// binary search when the dictionary is marked sorted, linear scan otherwise.
    public int findField(String name) {
        if (sorted) {
            return binarySearch(name);
        }
        for (int i = 0; i < dictionarySize; i++) {
            if (name.equals(getField(i))) {
                return i;
            }
        }
        return -1;
    }

    private int binarySearch(String name) {
        byte[] target = name.getBytes(StandardCharsets.UTF_8);
        int lo = 0;
        int hi = dictionarySize - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = compareDictEntry(mid, target);
            if (cmp < 0) {
                lo = mid + 1;
            }
            else if (cmp > 0) {
                hi = mid - 1;
            }
            else {
                return mid;
            }
        }
        return -1;
    }

    /// Compare the dictionary entry at `id` against `target` bytes using unsigned
    /// lexicographic ordering (matches the Variant spec's sort order).
    private int compareDictEntry(int id, byte[] target) {
        int start = readOffset(id);
        int end = readOffset(id + 1);
        int len = end - start;
        int cmp = Arrays.compareUnsigned(buf, stringsStart + start, stringsStart + end,
                target, 0, target.length);
        // Arrays.compareUnsigned returns a non-standard value when one range is a
        // prefix of the other; fall back to length comparison for that case.
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(len, target.length);
    }

    private int readOffset(int index) {
        return VariantBinary.readUnsignedLE(buf, offsetsStart + index * offsetSize, offsetSize);
    }
}
