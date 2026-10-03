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

import dev.hardwood.internal.encoding.PlainDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.metadata.PhysicalType;

/// Typed dictionary for dictionary-encoded Parquet columns.
/// Each variant holds a primitive array of dictionary values.
public sealed interface Dictionary {

    int size();

    /// Decode dictionary values into a Page using the given index decoder.
    /// This avoids megamorphic dispatch in the caller by moving type-specific
    /// logic into the Dictionary implementation.
    Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                    int[] definitionLevels, int[] repetitionLevels, int maxDefLevel);

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
        PlainDecoder decoder = new PlainDecoder(data, 0, length, type, typeLength);

        return switch (type) {
            case INT32 -> {
                int[] values = new int[numValues];
                decoder.readInts(values, null, 0);
                yield new IntDictionary(values);
            }
            case INT64 -> {
                long[] values = new long[numValues];
                decoder.readLongs(values, null, 0);
                yield new LongDictionary(values);
            }
            case FLOAT -> {
                float[] values = new float[numValues];
                decoder.readFloats(values, null, 0);
                yield new FloatDictionary(values);
            }
            case DOUBLE -> {
                double[] values = new double[numValues];
                decoder.readDoubles(values, null, 0);
                yield new DoubleDictionary(values);
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, null, 0);
                yield new ByteArrayDictionary(values);
            }
            case BOOLEAN -> throw new UnsupportedOperationException(
                    "Dictionary encoding not supported for BOOLEAN type");
        };
    }

    record IntDictionary(int[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            int[] output = new int[numValues];
            indexDecoder.readDictionaryInts(output, values, definitionLevels, maxDefLevel);
            return new Page.IntPage(output, definitionLevels, repetitionLevels, maxDefLevel, numValues);
        }
    }

    record LongDictionary(long[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            long[] output = new long[numValues];
            indexDecoder.readDictionaryLongs(output, values, definitionLevels, maxDefLevel);
            return new Page.LongPage(output, definitionLevels, repetitionLevels, maxDefLevel, numValues);
        }
    }

    record FloatDictionary(float[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            float[] output = new float[numValues];
            indexDecoder.readDictionaryFloats(output, values, definitionLevels, maxDefLevel);
            return new Page.FloatPage(output, definitionLevels, repetitionLevels, maxDefLevel, numValues);
        }
    }

    record DoubleDictionary(double[] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            double[] output = new double[numValues];
            indexDecoder.readDictionaryDoubles(output, values, definitionLevels, maxDefLevel);
            return new Page.DoublePage(output, definitionLevels, repetitionLevels, maxDefLevel, numValues);
        }
    }

    /// A class (not a record) so it can hold the lazily-materialised per-chunk
    /// interned `String` cache ([#interned]) alongside the entry bytes.
    final class ByteArrayDictionary implements Dictionary {

        /// The entries' bytes back to back, entry `i` at
        /// `[entryOffsets[i], entryOffsets[i + 1])`. A batch copies this once and points
        /// its values at it (see [BinaryBatchValues#viewDictionaryRange]); nothing holds
        /// the entries in any other form.
        private final byte[] entryBytes;
        private final int[] entryOffsets;

        /// Interned `String` per entry, decoded once per chunk and reused. Lazily
        /// allocated; populated only for columns read as text, via [#internedString(int)].
        private String[] interned;

        /// Flattens `values`, which the dictionary does not keep.
        ByteArrayDictionary(byte[][] values) {
            int[] offsets = new int[values.length + 1];
            long total = 0;
            for (int i = 0; i < values.length; i++) {
                total += values[i].length;
                offsets[i + 1] = Math.toIntExact(total);
            }
            byte[] flat = new byte[Math.toIntExact(total)];
            for (int i = 0; i < values.length; i++) {
                System.arraycopy(values[i], 0, flat, offsets[i], values[i].length);
            }
            this.entryBytes = flat;
            this.entryOffsets = offsets;
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

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            int[] dictIndices = new int[numValues];
            indexDecoder.readDictionaryIndices(dictIndices, size(), definitionLevels, maxDefLevel);
            return new Page.DictionaryByteArrayPage(this, dictIndices, definitionLevels, repetitionLevels,
                    maxDefLevel, numValues);
        }
    }
}
