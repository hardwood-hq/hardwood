/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

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

    /// The entries of a `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` dictionary, in chunks.
    ///
    /// Chunk `c` holds entries `[firstEntry(c), firstEntry(c + 1))`, their bytes back to back in
    /// [#chunkBytes(int)] and their ends in [#chunkOffsets(int)]: entry `firstEntry(c) + k` is
    /// `[chunkOffsets(c)[k], chunkOffsets(c)[k + 1])` of the chunk's bytes. A chunk's bytes and
    /// offsets each stay within [ArrayBound#MAX_BYTES], except the bytes of a chunk holding a
    /// single entry larger than that, so a dictionary held for its column chunk is never a
    /// humongous object however large its page. A batch takes the chunks as buffers and points its
    /// values at them (see [BinaryBatchValues#ensureViews()]); nothing holds the entries in any
    /// other form.
    ///
    /// A class (not a record) so it can hold the lazily-materialised interned `String` cache
    /// ([#internedString(int)]) alongside the entry bytes.
    final class ByteArrayDictionary implements Dictionary {

        /// The most entries a chunk holds, so that its offsets stay within the bound.
        static final int MAX_CHUNK_ENTRIES = ArrayBound.MAX_BYTES / Integer.BYTES - 1;

        private final byte[][] chunkBytes;
        private final int[][] chunkOffsets;
        /// `firstEntries[c]` is the first entry of chunk `c`; the last element is the entry count.
        private final int[] firstEntries;
        private final long byteCount;

        /// Interned `String` per entry and chunk, decoded once per column chunk and reused. Lazily
        /// allocated; populated only for columns read as text, via [#internedString(int)].
        private String[][] interned;

        private ByteArrayDictionary(byte[][] chunkBytes, int[][] chunkOffsets, int[] firstEntries, long byteCount) {
            this.chunkBytes = chunkBytes;
            this.chunkOffsets = chunkOffsets;
            this.firstEntries = firstEntries;
            this.byteCount = byteCount;
        }

        /// Parses a PLAIN `BYTE_ARRAY` dictionary page, `[4-byte little-endian length][bytes]` per
        /// entry: a first pass validates the entries and places them in chunks, a second copies
        /// each entry's bytes into its chunk and records its end.
        static ByteArrayDictionary parseLengthPrefixed(byte[] data, int length, int numValues) {
            if ((long) numValues * Integer.BYTES > length) {
                // The page cannot hold the entries' length prefixes, let alone their bytes
                throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY length");
            }
            ChunkPlan plan = new ChunkPlan();
            int bytesLeft = length - numValues * Integer.BYTES;
            int pos = 0;
            for (int i = 0; i < numValues; i++) {
                // The check above and the one below keep every remaining length prefix within the page
                int entryLength = lengthPrefix(data, pos);
                pos += Integer.BYTES;
                if (entryLength < 0) {
                    throw new ParquetReadException("Invalid BYTE_ARRAY length: " + entryLength);
                }
                if (entryLength > length - pos) {
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY data");
                }
                if (entryLength > bytesLeft) {
                    // The entry's bytes reach into where the later entries' length prefixes would be
                    throw new ParquetReadException("Unexpected EOF while reading BYTE_ARRAY length");
                }
                plan.add(i, entryLength);
                pos += entryLength;
                bytesLeft -= entryLength;
            }
            requireConsumed(length - pos, numValues, PhysicalType.BYTE_ARRAY);
            ByteArrayDictionary dictionary = plan.allocate(numValues, length - (long) numValues * Integer.BYTES);
            pos = 0;
            for (int c = 0; c < dictionary.chunkCount(); c++) {
                byte[] bytes = dictionary.chunkBytes[c];
                int[] offsets = dictionary.chunkOffsets[c];
                int end = 0;
                for (int k = 1; k < offsets.length; k++) {
                    int entryLength = lengthPrefix(data, pos);
                    System.arraycopy(data, pos + Integer.BYTES, bytes, end, entryLength);
                    pos += Integer.BYTES + entryLength;
                    end += entryLength;
                    offsets[k] = end;
                }
            }
            return dictionary;
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
            ChunkPlan plan = new ChunkPlan();
            for (int i = 0; i < numValues; i++) {
                plan.add(i, width);
            }
            ByteArrayDictionary dictionary = plan.allocate(numValues, size);
            for (int c = 0; c < dictionary.chunkCount(); c++) {
                byte[] bytes = dictionary.chunkBytes[c];
                int[] offsets = dictionary.chunkOffsets[c];
                System.arraycopy(data, dictionary.firstEntries[c] * width, bytes, 0, bytes.length);
                for (int k = 1; k < offsets.length; k++) {
                    offsets[k] = k * width;
                }
            }
            return dictionary;
        }

        private static int lengthPrefix(byte[] data, int pos) {
            return (data[pos] & 0xFF) | (data[pos + 1] & 0xFF) << 8
                    | (data[pos + 2] & 0xFF) << 16 | (data[pos + 3] & 0xFF) << 24;
        }

        /// The chunks a parse places its entries in, in a first pass over them: a chunk ends before
        /// an entry that would take its bytes past the bound or its entries past
        /// [#MAX_CHUNK_ENTRIES]. The parse then copies the entries into the chunks it allocates.
        private static final class ChunkPlan {
            private int[] firstEntries = new int[4];
            private int[] sizes = new int[4];
            private int count;

            void add(int entry, int entryLength) {
                int last = count - 1;
                if (count == 0 || entry - firstEntries[last] == MAX_CHUNK_ENTRIES
                        || entryLength > ArrayBound.MAX_BYTES - sizes[last]) {
                    if (count == firstEntries.length) {
                        firstEntries = Arrays.copyOf(firstEntries, count * 2);
                        sizes = Arrays.copyOf(sizes, count * 2);
                    }
                    firstEntries[count] = entry;
                    last = count++;
                }
                sizes[last] += entryLength;
            }

            /// A dictionary of the planned chunks, their bytes and offsets allocated but not
            /// written, for the parse to fill before handing it out.
            ByteArrayDictionary allocate(int numValues, long byteCount) {
                int chunks = Math.max(count, 1);
                byte[][] bytes = new byte[chunks][];
                int[][] offsets = new int[chunks][];
                int[] first = Arrays.copyOf(firstEntries, chunks + 1);
                first[chunks] = numValues;
                for (int c = 0; c < chunks; c++) {
                    bytes[c] = new byte[sizes[c]];
                    offsets[c] = new int[first[c + 1] - first[c] + 1];
                }
                return new ByteArrayDictionary(bytes, offsets, first, byteCount);
            }
        }

        /// The chunks the entries are held in.
        public int chunkCount() {
            return chunkBytes.length;
        }

        /// The bytes of chunk `chunk`'s entries, back to back.
        public byte[] chunkBytes(int chunk) {
            return chunkBytes[chunk];
        }

        /// The ends of chunk `chunk`'s entries in [#chunkBytes(int)], after a leading `0`: entry
        /// `firstEntry(chunk) + k` ends at element `k + 1`.
        public int[] chunkOffsets(int chunk) {
            return chunkOffsets[chunk];
        }

        /// The first entry of chunk `chunk`.
        public int firstEntry(int chunk) {
            return firstEntries[chunk];
        }

        /// The chunk holding entry `entry`. An entry outside the dictionary maps to the first or
        /// last chunk, where reading its offsets fails with an out-of-bounds access.
        public int chunkOf(int entry) {
            int high = firstEntries.length - 2;
            int low = 0;
            while (low < high) {
                int mid = (low + high + 1) >>> 1;
                if (firstEntries[mid] <= entry) {
                    low = mid;
                }
                else {
                    high = mid - 1;
                }
            }
            return low;
        }

        /// The bytes the entries take, all chunks together.
        public long byteCount() {
            return byteCount;
        }

        /// Tests entry `entry` with `test`, handing it the bytes where they sit.
        public boolean testEntry(int entry, BinaryValueTest test) {
            int chunk = chunkOf(entry);
            int[] offsets = chunkOffsets[chunk];
            int k = entry - firstEntries[chunk];
            return test.testValue(chunkBytes[chunk], offsets[k], offsets[k + 1]);
        }

        /// Whether any entry passes `test`.
        public boolean anyEntry(BinaryValueTest test) {
            for (int c = 0; c < chunkBytes.length; c++) {
                byte[] bytes = chunkBytes[c];
                int[] offsets = chunkOffsets[c];
                for (int k = 0; k < offsets.length - 1; k++) {
                    if (test.testValue(bytes, offsets[k], offsets[k + 1])) {
                        return true;
                    }
                }
            }
            return false;
        }

        /// A copy of entry `index`. Allocates; reads over many entries should use
        /// [#chunkBytes(int)] and [#chunkOffsets(int)].
        public byte[] entry(int index) {
            int chunk = chunkOf(index);
            int[] offsets = chunkOffsets[chunk];
            int k = index - firstEntries[chunk];
            return Arrays.copyOfRange(chunkBytes[chunk], offsets[k], offsets[k + 1]);
        }

        @Override
        public int size() {
            return firstEntries[firstEntries.length - 1];
        }

        /// Returns dictionary entry `index` as a `String`, decoding it once per chunk
        /// and caching it. Repeated values across the chunk share this one instance.
        /// `index` must be a valid entry index: the row readers and
        /// `ColumnReader.getStrings()` reach this only via
        /// [BinaryBatchValues#stringAt] for a non-null dictionary value, so a wiring
        /// bug surfaces immediately as an out-of-bounds access here.
        public String internedString(int index) {
            String[][] cache = interned;
            if (cache == null) {
                cache = new String[chunkBytes.length][];
                interned = cache;
            }
            int chunk = chunkOf(index);
            int[] offsets = chunkOffsets[chunk];
            String[] strings = cache[chunk];
            if (strings == null) {
                strings = new String[offsets.length - 1];
                cache[chunk] = strings;
            }
            int k = index - firstEntries[chunk];
            String s = strings[k];
            if (s == null) {
                int from = offsets[k];
                s = new String(chunkBytes[chunk], from, offsets[k + 1] - from, StandardCharsets.UTF_8);
                strings[k] = s;
            }
            return s;
        }

    }
}
