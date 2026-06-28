/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
    /// @param numValues number of dictionary entries
    /// @param type physical type of the column
    /// @param typeLength type length for fixed-length types (may be null for variable-length types)
    /// @return typed dictionary
    static Dictionary parse(byte[] data, int numValues, PhysicalType type, Integer typeLength) throws IOException {
        PlainDecoder decoder = new PlainDecoder(data, 0, type, typeLength);

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
        private final byte[][] values;

        /// Interned `String` per entry, decoded once per chunk and reused. Lazily
        /// allocated; populated only for UTF8 / JSON columns via [#internedString(int)].
        private String[] interned;

        ByteArrayDictionary(byte[][] values) {
            this.values = values;
        }

        public byte[][] values() {
            return values;
        }

        @Override
        public int size() {
            return values.length;
        }

        /// Returns dictionary entry `index` as a `String`, decoding it once per chunk
        /// and caching it. Repeated values across the chunk share this one instance.
        /// `index` must be a valid entry index: the row reader reaches this only via
        /// [BinaryBatchValues#stringAt] for a non-null dictionary value, so a wiring
        /// bug surfaces immediately as an out-of-bounds access here.
        String internedString(int index) {
            String[] cache = interned;
            if (cache == null) {
                cache = new String[values.length];
                interned = cache;
            }
            String s = cache[index];
            if (s == null) {
                s = new String(values[index], StandardCharsets.UTF_8);
                cache[index] = s;
            }
            return s;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            byte[][] output = new byte[numValues][];
            int[] dictIndices = new int[numValues];
            indexDecoder.readDictionaryByteArrays(output, dictIndices, values, definitionLevels, maxDefLevel);
            // Carry the dictionary and per-value entry indices so the row reader
            // can intern repeated values to one String per chunk.
            return new Page.ByteArrayPage(output, definitionLevels, repetitionLevels, maxDefLevel,
                    numValues, this, dictIndices);
        }
    }
}
