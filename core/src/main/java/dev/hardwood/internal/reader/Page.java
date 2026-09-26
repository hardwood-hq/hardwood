/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

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

    /// The number of values (leaves) on this page — the logical length of the
    /// level and value arrays. The level arrays may be physically longer (see
    /// [#definitionLevels]), so this is the count callers must iterate, never
    /// `definitionLevels().length`.
    int size();

    int maxDefinitionLevel();

    /// Definition levels for this page, or `null` when every leaf is present
    /// (the all-present convention; see [#allPresent]).
    ///
    /// When non-null the array holds one level per value but **may be longer than
    /// [#size]**: it is a pooled decode buffer reused across pages and grown to the
    /// largest page seen, so its tail beyond `size()` is stale. Read only the first
    /// `size()` entries; never use `definitionLevels().length` as the value count.
    int[] definitionLevels();

    /// Repetition levels for this page, or `null` for a non-repeated (flat) column.
    ///
    /// As with [#definitionLevels], when non-null the array is a pooled buffer that
    /// **may be longer than [#size]**; only the first `size()` entries are valid.
    int[] repetitionLevels();

    /// Number of elements per row when this page is a fixed-width fixed-size-list
    /// page (every row a present list of exactly this many elements), or `0` when
    /// it is a regular page. A fixed-width page carries `null` level arrays: its
    /// boundaries are implicit at multiples of `fixedListK`. See
    /// [FixedSizeListDetector].
    int fixedListK();

    /// True when every leaf on this page is present — no null or empty parents.
    /// Represented as a `null` definition-level array (the reader's all-present
    /// convention, produced by [PageDecoder]'s O(1) def-gate for both flat and
    /// nested columns), so callers test presence without inspecting per-value
    /// levels. A repetition-level array is still present for nested columns.
    default boolean allPresent() {
        return definitionLevels() == null;
    }

    default boolean isNull(int index) {
        if (allPresent()) {
            return false;
        }
        return definitionLevels()[index] < maxDefinitionLevel();
    }

    /// Returns a copy of `page` marked as a fixed-width fixed-size-list page with
    /// the given element count. The values are shared; the level arrays (already
    /// `null` on a fixed-width decode) are carried through. This lets the fast path
    /// reuse the regular value decoders and stamp the shape afterwards.
    static Page withFixedListK(Page page, int fixedListK) {
        return withShape(page, page.size(), fixedListK);
    }

    /// Returns a copy of `page` whose logical length is `size`. The value, level and
    /// dictionary-index arrays are shared; positions from `size` on are ignored.
    static Page withSize(Page page, int size) {
        return withShape(page, size, page.fixedListK());
    }

    private static Page withShape(Page page, int size, int fixedListK) {
        return switch (page) {
            case BooleanPage p -> new BooleanPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, fixedListK);
            case IntPage p -> new IntPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, fixedListK);
            case LongPage p -> new LongPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, fixedListK);
            case FloatPage p -> new FloatPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, fixedListK);
            case DoublePage p -> new DoublePage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, fixedListK);
            case ByteArrayPage p -> new ByteArrayPage(p.values(), p.definitionLevels(), p.repetitionLevels(),
                    p.maxDefinitionLevel(), size, p.dictionary(), p.dictIndices(), fixedListK);
        };
    }

    record BooleanPage(boolean[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, int fixedListK) implements Page {
        BooleanPage(boolean[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, 0);
        }

        public boolean get(int index) {
            return values[index];
        }
    }

    record IntPage(int[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, int fixedListK) implements Page {
        IntPage(int[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, 0);
        }

        public int get(int index) {
            return values[index];
        }
    }

    record LongPage(long[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, int fixedListK) implements Page {
        LongPage(long[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, 0);
        }

        public long get(int index) {
            return values[index];
        }
    }

    record FloatPage(float[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, int fixedListK) implements Page {
        FloatPage(float[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, 0);
        }

        public float get(int index) {
            return values[index];
        }
    }

    record DoublePage(double[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, int fixedListK) implements Page {
        DoublePage(double[] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel, int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, 0);
        }

        public double get(int index) {
            return values[index];
        }
    }

    record ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
            int size, Dictionary.ByteArrayDictionary dictionary, int[] dictIndices, int fixedListK)
            implements Page {
        /// Page from a non-dictionary (`PLAIN`) decode: no shared dictionary, so
        /// values cannot be interned per entry (`dictionary` / `dictIndices` null).
        ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                int size) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, null, null, 0);
        }

        ByteArrayPage(byte[][] values, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                int size, Dictionary.ByteArrayDictionary dictionary, int[] dictIndices) {
            this(values, definitionLevels, repetitionLevels, maxDefinitionLevel, size, dictionary, dictIndices, 0);
        }

        public byte[] get(int index) {
            return values[index];
        }
    }
}
