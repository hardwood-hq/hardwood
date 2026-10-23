/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;

/// Typed dictionary for dictionary-encoded Parquet columns.
/// Each variant holds a primitive array of dictionary values.
public sealed interface Dictionary {

    int size();

    /// Parse dictionary values from decompressed data.
    ///
    /// @param data decompressed dictionary page data
    /// @param length the number of bytes of `data` the page holds, which a buffer reused across
    ///        pages exceeds
    /// @param numValues number of dictionary entries
    /// @param type physical type of the column
    /// @param typeLength type length for fixed-length types (may be null for variable-length types)
    /// @return typed dictionary
    static Dictionary parse(byte[] data, int length, int numValues, PhysicalType type, Integer typeLength) {
        return switch (type) {
            case INT32, INT64, FLOAT, DOUBLE -> parseNumbers(data, length, numValues, type);
            case BYTE_ARRAY -> ByteArrayDictionary.parseLengthPrefixed(data, length, numValues);
            case FIXED_LEN_BYTE_ARRAY -> ByteArrayDictionary.parseFixedWidth(data, length, numValues, type,
                    requireTypeLength(typeLength), "Unexpected EOF while reading fixed-length byte array");
            case INT96 -> ByteArrayDictionary.parseFixedWidth(data, length, numValues, type,
                    12, "Unexpected EOF while reading INT96");
            case BOOLEAN -> throw new UnsupportedOperationException(
                    "Dictionary encoding not supported for BOOLEAN type");
        };
    }

    private static Dictionary parseNumbers(byte[] data, int length, int numValues, PhysicalType type) {
        int width = type == PhysicalType.INT64 || type == PhysicalType.DOUBLE ? Long.BYTES : Integer.BYTES;
        long bytes = (long) numValues * width;
        if (bytes > length) {
            throw new ParquetReadException("Unexpected EOF while reading " + type + " values");
        }
        requireConsumed(Math.toIntExact(length - bytes), numValues, type);
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, length).order(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case INT32 -> {
                int[] values = new int[numValues];
                buffer.asIntBuffer().get(values);
                yield new IntDictionary(values);
            }
            case INT64 -> {
                long[] values = new long[numValues];
                buffer.asLongBuffer().get(values);
                yield new LongDictionary(values);
            }
            case FLOAT -> {
                float[] values = new float[numValues];
                buffer.asFloatBuffer().get(values);
                yield new FloatDictionary(values);
            }
            case DOUBLE -> {
                double[] values = new double[numValues];
                buffer.asDoubleBuffer().get(values);
                yield new DoubleDictionary(values);
            }
            default -> throw new IllegalArgumentException("Not a numeric physical type: " + type);
        };
    }

    /// The width of a `FIXED_LEN_BYTE_ARRAY` column's values. A file's declared width is
    /// validated before its dictionary is parsed ([FixedWidthValidator]), so a width that is
    /// absent or not positive here is a caller's error.
    private static int requireTypeLength(Integer typeLength) {
        if (typeLength == null || typeLength <= 0) {
            throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY requires a positive type length, got "
                    + typeLength);
        }
        return typeLength;
    }

    /// Entries read at the wrong boundaries would decode into wrong values, not an EOF, so a page
    /// with bytes left over after its last entry is rejected.
    private static void requireConsumed(int leftover, int numValues, PhysicalType type) {
        if (leftover != 0) {
            throw new ParquetReadException("Dictionary page does not end with its " + numValues + " " + type
                    + " values; leftover bytes: " + leftover);
        }
    }

    record IntDictionary(int[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

    }

    record LongDictionary(long[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

    }

    record FloatDictionary(float[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

    }

    record DoubleDictionary(double[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

    }

    /// A class (not a record) so it can hold the lazily-materialised per-chunk
    /// interned `String` cache ([#interned]) alongside the entry bytes.
    final class ByteArrayDictionary implements Dictionary {

        /// The entries' bytes back to back, entry `i` at
        /// `[entryOffsets[i], entryOffsets[i + 1])`. A batch copies this once and points
        /// its values at it (see [BinaryBatchValues#ensureViews()]); nothing holds
        /// the entries in any other form.
        private final byte[] entryBytes;
        private final int[] entryOffsets;

        /// Interned `String` per entry, decoded once per chunk and reused. Lazily
        /// allocated; populated only for columns read as text, via [#internedString(int)].
        private String[] interned;

        private ByteArrayDictionary(byte[] entryBytes, int[] entryOffsets) {
            this.entryBytes = entryBytes;
            this.entryOffsets = entryOffsets;
        }

        /// Parses a PLAIN `BYTE_ARRAY` dictionary page, `[4-byte little-endian length][bytes]` per
        /// entry, in one pass: each entry's bytes are copied into one array and its end recorded.
        static ByteArrayDictionary parseLengthPrefixed(byte[] data, int length, int numValues) {
            if ((long) numValues * Integer.BYTES > length) {
                // The page cannot hold the entries' length prefixes, let alone their bytes
                throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY length");
            }
            byte[] bytes = new byte[length - numValues * Integer.BYTES];
            int[] offsets = new int[numValues + 1];
            int pos = 0;
            int end = 0;
            for (int i = 0; i < numValues; i++) {
                // The check above and the one below keep every remaining length prefix within the page
                int entryLength = (data[pos] & 0xFF) | (data[pos + 1] & 0xFF) << 8
                        | (data[pos + 2] & 0xFF) << 16 | (data[pos + 3] & 0xFF) << 24;
                pos += Integer.BYTES;
                if (entryLength < 0) {
                    throw new ParquetReadException("Invalid BYTE_ARRAY length: " + entryLength);
                }
                if (entryLength > length - pos) {
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY data");
                }
                if (entryLength > bytes.length - end) {
                    // The entry's bytes reach into where the later entries' length prefixes would be
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY length");
                }
                System.arraycopy(data, pos, bytes, end, entryLength);
                pos += entryLength;
                end += entryLength;
                offsets[i + 1] = end;
            }
            requireConsumed(length - pos, numValues, PhysicalType.BYTE_ARRAY);
            return new ByteArrayDictionary(bytes, offsets);
        }

        /// Parses a dictionary page of `width`-byte entries with no length prefixes, whose bytes
        /// already are the flat layout.
        static ByteArrayDictionary parseFixedWidth(byte[] data, int length, int numValues, PhysicalType type,
                                                   int width, String eofMessage) {
            long size = (long) numValues * width;
            if (size > length) {
                throw new ParquetReadException(eofMessage);
            }
            requireConsumed(Math.toIntExact(length - size), numValues, type);
            int[] offsets = new int[numValues + 1];
            for (int i = 1; i <= numValues; i++) {
                offsets[i] = i * width;
            }
            return new ByteArrayDictionary(Arrays.copyOf(data, Math.toIntExact(size)), offsets);
        }

        public byte[] entryBytes() {
            return entryBytes;
        }

        public int[] entryOffsets() {
            return entryOffsets;
        }

        /// A copy of entry `index`. Allocates; reads over many entries should use
        /// [#entryBytes()] and [#entryOffsets()].
        public byte[] entry(int index) {
            return Arrays.copyOfRange(entryBytes, entryOffsets[index], entryOffsets[index + 1]);
        }

        @Override
        public int size() {
            return entryOffsets.length - 1;
        }

        /// Returns dictionary entry `index` as a `String`, decoding it once per chunk
        /// and caching it. Repeated values across the chunk share this one instance.
        /// `index` must be a valid entry index: the row readers and
        /// `ColumnReader.getStrings()` reach this only via
        /// [BinaryBatchValues#stringAt] for a non-null dictionary value, so a wiring
        /// bug surfaces immediately as an out-of-bounds access here.
        public String internedString(int index) {
            String[] cache = interned;
            if (cache == null) {
                cache = new String[size()];
                interned = cache;
            }
            String s = cache[index];
            if (s == null) {
                int from = entryOffsets[index];
                s = new String(entryBytes, from, entryOffsets[index + 1] - from, StandardCharsets.UTF_8);
                cache[index] = s;
            }
            return s;
        }

    }
}
