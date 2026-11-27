/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.schema.ByteColumnOrder;
import dev.hardwood.metadata.Statistics;

/// Accumulates a binary column page's or chunk's `min` / `max` / `null_count` in the column's
/// [ByteColumnOrder]. [BinaryComparator] defines the ordering and carries the cached-prefix fast
/// path: unsigned lexicographic values compare their first eight bytes first, while the other
/// orders delegate each comparison to it. The writer uses three orders:
/// unsigned lexicographic — the type-defined order for an
/// unannotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` and for the string-like annotations —,
/// signed big-endian two's complement, the order of a `DECIMAL`'s represented value, or signed
/// little-endian two's complement, the order of a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`.
///
/// Bounds are **truncated** to at most `truncationLength` bytes so a chunk of long values does
/// not bloat the footer. A truncated `min` keeps the value's first *N* bytes — a prefix is `<=`
/// the original, so it stays a valid lower bound. A truncated `max` keeps the first *N* bytes and
/// increments the last byte that is not `0xFF`, dropping the trailing bytes, yielding the
/// smallest length-`<= N` value that is `>=` the original; if every kept byte is `0xFF` no valid
/// truncated upper bound exists and the `max` bound is omitted. A column annotated as text cuts
/// at the code-point boundary at or before *N* bytes and increments the last code point that has
/// a successor, so its bounds stay valid UTF-8. A truncated bound is flagged inexact; an
/// untruncated bound stays exact. A `ColumnIndex` page bound is truncated the same way, except
/// that a `max` with no shorter upper bound is written whole. The `min` / `max` are copied out of
/// the slices they are taken from: those are the chunk's value store, which the next row group
/// reuses, while the page index holds the bounds until the file is closed.
final class BinaryStatisticsCollector extends BinaryStatistics {

    private final ByteColumnOrder order;
    private final int truncationLength;
    /// Whether the column is annotated as text, whose truncated bounds must stay valid UTF-8.
    private final boolean text;
    private byte[] min;
    private byte[] max;
    /// The first eight bytes of [#min] and [#max], zero-padded where shorter.
    private long minPrefix;
    private long maxPrefix;
    private boolean hasValues;

    BinaryStatisticsCollector(ByteColumnOrder order, int truncationLength, boolean text) {
        this.order = order;
        this.truncationLength = truncationLength;
        this.text = text;
    }

    @Override
    void accept(byte[] array, int offset, int length) {
        if (!hasValues) {
            min = Arrays.copyOfRange(array, offset, offset + length);
            max = min.clone();
            if (order == ByteColumnOrder.BYTES) {
                minPrefix = BinaryComparator.prefix(array, offset, length);
                maxPrefix = minPrefix;
            }
            hasValues = true;
            return;
        }
        if (order == ByteColumnOrder.BYTES) {
            acceptUnsigned(array, offset, length);
            return;
        }
        if (BinaryComparator.compare(array, offset, offset + length, min, order) < 0) {
            min = Arrays.copyOfRange(array, offset, offset + length);
        }
        else if (BinaryComparator.compare(array, offset, offset + length, max, order) > 0) {
            max = Arrays.copyOfRange(array, offset, offset + length);
        }
    }

    /// Collects a page's unsigned lexicographic bounds in one pass over the value store, keeping
    /// the offsets of the current bounds rather than copying a value whenever one extends them. A
    /// page's collector is fresh, so a run normally copies just the final `min` and `max`; a
    /// collector that already holds bounds, and every other order, falls back to [#accept].
    @Override
    void acceptAll(byte[] array, int[] offsets, int from, int to) {
        if (order != ByteColumnOrder.BYTES || hasValues) {
            super.acceptAll(array, offsets, from, to);
            return;
        }
        if (from >= to) {
            return;
        }
        int minOffset = offsets[from];
        int minEnd = offsets[from + 1];
        long minPrefix = BinaryComparator.prefix(array, minOffset, minEnd - minOffset);
        int maxOffset = minOffset;
        int maxEnd = minEnd;
        long maxPrefix = minPrefix;
        for (int i = from + 1; i < to; i++) {
            int offset = offsets[i];
            int end = offsets[i + 1];
            int length = end - offset;
            long prefix = BinaryComparator.prefix(array, offset, length);
            int toMin = Long.compareUnsigned(prefix, minPrefix);
            if (toMin < 0 || toMin == 0 && BinaryComparator.compareUnsignedAfterPrefix(
                    array, offset, length, array, minOffset, minEnd - minOffset) < 0) {
                minOffset = offset;
                minEnd = end;
                minPrefix = prefix;
            }
            else if (Long.compareUnsigned(prefix, maxPrefix) > 0
                    || prefix == maxPrefix && BinaryComparator.compareUnsignedAfterPrefix(
                            array, offset, length, array, maxOffset, maxEnd - maxOffset) > 0) {
                maxOffset = offset;
                maxEnd = end;
                maxPrefix = prefix;
            }
        }
        min = Arrays.copyOfRange(array, minOffset, minEnd);
        max = Arrays.copyOfRange(array, maxOffset, maxEnd);
        this.minPrefix = minPrefix;
        this.maxPrefix = maxPrefix;
        hasValues = true;
    }

    /// Compares an unsigned lexicographic value with the current bounds. The first eight bytes
    /// decide unless their prefixes tie, in which case the remainder or the lengths decide. A new
    /// minimum cannot also exceed the maximum, so this path skips the second comparison.
    private void acceptUnsigned(byte[] array, int offset, int length) {
        long prefix = BinaryComparator.prefix(array, offset, length);
        int toMin = Long.compareUnsigned(prefix, minPrefix);
        if (toMin < 0 || toMin == 0 && BinaryComparator.compareUnsignedAfterPrefix(
                array, offset, length, min, 0, min.length) < 0) {
            min = Arrays.copyOfRange(array, offset, offset + length);
            minPrefix = prefix;
        }
        else if (Long.compareUnsigned(prefix, maxPrefix) > 0
                || prefix == maxPrefix && BinaryComparator.compareUnsignedAfterPrefix(
                        array, offset, length, max, 0, max.length) > 0) {
            max = Arrays.copyOfRange(array, offset, offset + length);
            maxPrefix = prefix;
        }
    }

    /// Takes over `page`'s bound arrays rather than copying them: a page's collector is
    /// discarded once merged.
    @Override
    void mergeValues(BinaryStatistics page) {
        BinaryStatisticsCollector other = (BinaryStatisticsCollector) page;
        if (!other.hasValues) {
            return;
        }
        if (!hasValues) {
            min = other.min;
            max = other.max;
            minPrefix = other.minPrefix;
            maxPrefix = other.maxPrefix;
            hasValues = true;
            return;
        }
        if (compareBounds(other.min, min) < 0) {
            min = other.min;
            minPrefix = other.minPrefix;
        }
        if (compareBounds(other.max, max) > 0) {
            max = other.max;
            maxPrefix = other.maxPrefix;
        }
    }

    @Override
    boolean hasValues() {
        return hasValues;
    }

    @Override
    Statistics toStatistics() {
        if (!hasValues) {
            return new Statistics(null, null, nullCount, null, false);
        }
        byte[] minValue = indexMin();
        byte[] maxValue = max.length > truncationLength ? truncateMax(max) : max;
        return new Statistics(minValue, maxValue, nullCount, null, false, minValue.length == min.length,
                max.length <= truncationLength, null);
    }

    @Override
    byte[] indexMin() {
        return min.length > truncationLength ? Arrays.copyOf(min, cutPoint(min)) : min;
    }

    /// A `ColumnIndex` page's `max` is required, so where no shorter upper bound exists the
    /// value is written whole.
    @Override
    byte[] indexMax() {
        if (max.length <= truncationLength) {
            return max;
        }
        byte[] truncated = truncateMax(max);
        return truncated != null ? truncated : max;
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return BinaryComparator.compare(left, 0, left.length, right, order);
    }

    /// Where a bound longer than [#truncationLength] is cut: at the length itself, or for text at
    /// the code-point boundary at or before it, so the kept prefix stays valid UTF-8. A prefix is
    /// `<=` the value either way. Bytes that are not well-formed UTF-8 are cut at the length.
    private int cutPoint(byte[] value) {
        int cut = truncationLength;
        if (!text) {
            return cut;
        }
        for (int back = 0; back < UTF8_MAX_BYTES && cut > 0; back++) {
            if (!isContinuation(value[cut])) {
                return cut;
            }
            cut--;
        }
        return isContinuation(value[cut]) ? truncationLength : cut;
    }

    /// The smallest bound of at most [#truncationLength] bytes that is `>=` `value`, or `null` where
    /// none exists. For bytes, the kept prefix with its last byte that is not `0xFF` incremented
    /// and the bytes after it dropped. For text, the kept prefix with its last code point that has
    /// a successor replaced by that successor, so the bound stays valid UTF-8.
    private byte[] truncateMax(byte[] value) {
        int cut = cutPoint(value);
        if (text) {
            byte[] bound = incrementLastCodePoint(value, cut);
            // A bound built from well-formed UTF-8 sorts above the value by construction; the
            // comparison keeps one built from anything else from becoming a max below its value.
            if (bound == null || bound != MALFORMED && BinaryComparator.compareUnsigned(bound, value) > 0) {
                return bound;
            }
        }
        byte[] prefix = Arrays.copyOf(value, truncationLength);
        for (int i = truncationLength - 1; i >= 0; i--) {
            if (prefix[i] != (byte) 0xFF) {
                byte[] result = Arrays.copyOf(prefix, i + 1);
                result[i]++;
                return result;
            }
        }
        return null;
    }

    /// The prefix `value[0, cut)` with its last code point that has a successor replaced by it,
    /// the code points after it dropped; `null` where none has a successor that fits, and
    /// [#MALFORMED] where the prefix is not well-formed UTF-8. UTF-8 byte order is code point
    /// order, so the successor sorts above every value starting with the prefix.
    private byte[] incrementLastCodePoint(byte[] value, int cut) {
        int end = cut;
        while (end > 0) {
            int start = end - 1;
            while (start > 0 && end - start < UTF8_MAX_BYTES && isContinuation(value[start])) {
                start--;
            }
            int codePoint = decode(value, start, end);
            if (codePoint < 0) {
                return MALFORMED;
            }
            int next = codePoint == Character.MIN_SURROGATE - 1 ? Character.MAX_SURROGATE + 1 : codePoint + 1;
            if (next <= Character.MAX_CODE_POINT) {
                byte[] encoded = new String(Character.toChars(next)).getBytes(StandardCharsets.UTF_8);
                if (start + encoded.length <= truncationLength) {
                    byte[] result = Arrays.copyOf(value, start + encoded.length);
                    System.arraycopy(encoded, 0, result, start, encoded.length);
                    return result;
                }
            }
            end = start;
        }
        return null;
    }

    /// The code point `value[start, end)` encodes, or `-1` where it is not the one well-formed
    /// UTF-8 sequence of a code point: an overlong form, a surrogate, a code point past
    /// `U+10FFFF`, a lead byte no sequence starts with, or a length the lead does not announce.
    /// Only the well-formed encoding of a code point is guaranteed to sort below the encoding of
    /// its successor.
    private static int decode(byte[] value, int start, int end) {
        int lead = value[start] & 0xFF;
        int length = lead < 0x80 ? 1 : lead >= 0xF8 ? -1 : lead >= 0xF0 ? 4 : lead >= 0xE0 ? 3 : lead >= 0xC0 ? 2 : -1;
        if (length != end - start) {
            return -1;
        }
        if (length == 1) {
            return lead;
        }
        int codePoint = lead & (0xFF >>> (length + 1));
        for (int i = start + 1; i < end; i++) {
            if (!isContinuation(value[i])) {
                return -1;
            }
            codePoint = (codePoint << 6) | (value[i] & 0x3F);
        }
        int shortest = codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
        if (shortest != length || codePoint > Character.MAX_CODE_POINT
                || codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE) {
            return -1;
        }
        return codePoint;
    }

    private static boolean isContinuation(byte b) {
        return (b & 0xC0) == 0x80;
    }

    private static final int UTF8_MAX_BYTES = 4;

    /// Marks a text bound whose bytes are not well-formed UTF-8, truncated as bytes instead.
    private static final byte[] MALFORMED = new byte[0];
}
