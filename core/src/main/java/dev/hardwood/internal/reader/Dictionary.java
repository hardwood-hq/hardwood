/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import dev.hardwood.internal.encoding.PlainDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.metadata.PhysicalType;

/**
 * Typed dictionary for dictionary-encoded Parquet columns.
 * Each variant holds a primitive array of dictionary values.
 */
public sealed interface Dictionary {

    int size();

    /**
     * Decode dictionary values into a Page using the given index decoder.
     * This avoids megamorphic dispatch in the caller by moving type-specific
     * logic into the Dictionary implementation.
     */
    Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                    int[] definitionLevels, int[] repetitionLevels, int maxDefLevel);

    /**
     * Parse dictionary values from decompressed data.
     *
     * @param data decompressed dictionary page data
     * @param numValues number of dictionary entries
     * @param type physical type of the column
     * @param typeLength type length for fixed-length types (may be null for variable-length types)
     * @return typed dictionary
     */
    static Dictionary parse(byte[] data, int numValues, PhysicalType type, Integer typeLength) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);
        PlainDecoder decoder = new PlainDecoder(dataStream, type, typeLength);

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

    record ByteArrayDictionary(byte[][] values) implements Dictionary {
        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Page decodePage(RleBitPackingHybridDecoder indexDecoder, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int maxDefLevel) {
            byte[][] output = new byte[numValues][];
            indexDecoder.readDictionaryByteArrays(output, values, definitionLevels, maxDefLevel);
            return new Page.ByteArrayPage(output, definitionLevels, repetitionLevels, maxDefLevel, numValues);
        }
    }
}
