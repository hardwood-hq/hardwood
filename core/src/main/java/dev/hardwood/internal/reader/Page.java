/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.internal.encoding.HybridStreamCursor;

/// Sealed interface for typed column page data with primitive arrays.
///
/// This eliminates boxing overhead by storing values directly in typed arrays.
/// All access is via typed accessors - there is no generic getObject() method.
///
/// Implementations correspond to Parquet physical types:
///
/// - [BooleanPage] - BOOLEAN
/// - [IntPage] - INT32
/// - [LongPage] - INT64
/// - [FloatPage] - FLOAT
/// - [DoublePage] - DOUBLE
/// - [ByteArrayPage] - BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
public sealed interface Page {

    int size();

    int maxDefinitionLevel();

    int[] definitionLevels();

    int[] repetitionLevels();

    /// Number of elements per row when this page is a fixed-width fixed-size-list
    /// page (every row a present list of exactly this many elements), or `0` when
    /// it is a regular page. A fixed-width page carries `null` level arrays: its
    /// boundaries are implicit at multiples of `fixedListK`. See
    /// [FixedSizeListDetector].
    int fixedListK();

    /// Returns the def-level run cursor for fused consumption, or `null` when
    /// the page was decoded via the materializing path. When non-null,
    /// [#definitionLevels()] returns `null` and the consumer drives the cursor
    /// run by run instead.
    default HybridStreamCursor defLevelCursor() { return null; }

    /// Returns the dictionary index run cursor for fused consumption, or `null`
    /// for non-dictionary pages or pages decoded via the materializing path.
    default HybridStreamCursor indexCursor() { return null; }
    
    /// Returns the dictionary for this page, or `null` if not dictionary-encoded.
    default Dictionary dictionary() { return null; }

    default boolean isNull(int index) {
        int[] defLevels = definitionLevels();
        if (defLevels == null) {
            return false;
        }
        return defLevels[index] < maxDefinitionLevel();
    }

    /// Returns a copy of `page` marked as a fixed-width fixed-size-list page with
    /// the given element count. The values are shared; the level arrays (already
    /// `null` on a fixed-width decode) are carried through. This lets the fast path
    /// reuse the regular value decoders and stamp the shape afterwards.
    static Page withFixedListK(Page page, int fixedListK) {
        return switch (page) {
            case BooleanPage p -> new BooleanPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), (Dictionary) p.dictionary(), p.defLevelCursor(), p.indexCursor(), fixedListK);
            case IntPage p -> new IntPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), (Dictionary.IntDictionary) p.dictionary(), p.defLevelCursor(), p.indexCursor(), fixedListK);
            case LongPage p -> new LongPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), (Dictionary.LongDictionary) p.dictionary(), p.defLevelCursor(), p.indexCursor(), fixedListK);
            case FloatPage p -> new FloatPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), (Dictionary.FloatDictionary) p.dictionary(), p.defLevelCursor(), p.indexCursor(), fixedListK);
            case DoublePage p -> new DoublePage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), (Dictionary.DoubleDictionary) p.dictionary(), p.defLevelCursor(), p.indexCursor(), fixedListK);
            case ByteArrayPage p -> new ByteArrayPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), p.size(), p.dictionary(), p.dictIndices(), p.defLevelCursor(), p.indexCursor(), fixedListK);
        };
    }

    record BooleanPage(boolean[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary dictionary, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK) implements Page {
        public BooleanPage(boolean[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, 0);
        }

        public boolean get(int index) {
            return values[index];
        }
    }

    record IntPage(int[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.IntDictionary dictionary, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK) implements Page {
        public IntPage(int[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, 0);
        }

        public int get(int index) {
            return values[index];
        }
    }

    record LongPage(long[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.LongDictionary dictionary, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK) implements Page {
        public LongPage(long[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, 0);
        }

        public long get(int index) {
            return values[index];
        }
    }

    record FloatPage(float[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.FloatDictionary dictionary, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK) implements Page {
        public FloatPage(float[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, 0);
        }

        public float get(int index) {
            return values[index];
        }
    }

    record DoublePage(double[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.DoubleDictionary dictionary, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK) implements Page {
        public DoublePage(double[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, 0);
        }

        public double get(int index) {
            return values[index];
        }
    }

    record ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.ByteArrayDictionary dictionary, int[] dictIndices, HybridStreamCursor defLevelCursor, HybridStreamCursor indexCursor, int fixedListK)
            implements Page {
        /// Page from a non-dictionary (`PLAIN`) decode: no shared dictionary, so
        /// values cannot be interned per entry (`dictionary` / `dictIndices` null).
        ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, null, null, 0);
        }

        ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                int size, Dictionary.ByteArrayDictionary dictionary, int[] dictIndices) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, dictionary, dictIndices, null, null, 0);
        }

        public byte[] get(int index) {
            return values[index];
        }
    }
}
