/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.ParquetReadException;

/// What a decoded page of a column decoded through levels reports for its placement and for the
/// sizes of its cells, computed on its task.
///
/// The page is trimmed to its kept records before it is measured, so its kept values are its
/// values. The measure says where the page's records start and how many bytes its values hold, so
/// that a [NestedSegment] of any of its record ranges knows its value and byte counts in O(1).
///
/// - [#values()]: the page's values, a position for each null or empty parent included.
/// - [#records()]: the records starting on the page. A column without repetition has one record
///   per value, a fixed-size-list page one per `k` values, any other page one per repetition level
///   of `0`.
/// - [#leadingValues()]: the values before the first record start, which belong to the record the
///   previous page ends with. Only a `V1` page with repetition can have any.
/// - [#recordStart(int)]: where record `i` starts, in values.
/// - [#bytes(int, int)]: the bytes of a range of plain binary values; `0` for any other page.
///
/// The record starts and the byte prefix live in the page's own [PageDecoder.LevelScratch], which
/// the page keeps until its segments are filled.
final class PageMeasure {

    private final int values;
    private final int records;
    private final int leadingValues;
    private final int firstRepetitionLevel;
    /// Where each record starts, for a page whose repetition levels give its records; `null` when
    /// the starts are arithmetic.
    private final int[] recordStarts;
    private final int fixedListK;
    private final boolean allPresent;
    /// The page's binary values, `null` for a column of another type.
    private final BinaryPageValues binary;
    /// Where a masked page read in place keeps its values, `null` for any other page.
    private KeptRuns runs;

    private PageMeasure(int values, int records, int leadingValues, int firstRepetitionLevel, int[] recordStarts,
                        int fixedListK, boolean allPresent, BinaryPageValues binary) {
        this.values = values;
        this.records = records;
        this.leadingValues = leadingValues;
        this.firstRepetitionLevel = firstRepetitionLevel;
        this.recordStarts = recordStarts;
        this.fixedListK = fixedListK;
        this.allPresent = allPresent;
        this.binary = binary;
    }

    /// Measures a page read in two passes from its levels, before its values are decoded.
    ///
    /// @throws ParquetReadException if the page's records disagree with its metadata
    static PageMeasure of(PageDecoder.InPlaceNestedPage page, int metadataRecords, PageDecoder.LevelScratch scratch) {
        PageMeasure measure = of(page.size(), page.fixedListK(), page.repetitionLevels(),
                page.definitionLevels() == null, page.binary(), metadataRecords, scratch);
        measure.runs = page.runs();
        return measure;
    }

    private static PageMeasure of(int size, int k, int[] repetitionLevels, boolean allPresent,
                                  BinaryPageValues binary, int metadataRecords, PageDecoder.LevelScratch scratch) {
        if (k > 0 || repetitionLevels == null) {
            int records = k > 0 ? size / k : size;
            checkMetadataRecords(records, metadataRecords);
            return new PageMeasure(size, records, 0, 0, null, k, allPresent, binary);
        }
        int[] starts = scratch.recordStarts(size);
        int records = 0;
        for (int i = 0; i < size; i++) {
            if (repetitionLevels[i] == 0) {
                starts[records++] = i;
            }
        }
        int leading = records > 0 ? starts[0] : size;
        checkMetadataRecords(records, metadataRecords);
        return new PageMeasure(size, records, leading, size > 0 ? repetitionLevels[0] : 0, starts, 0,
                allPresent, binary);
    }

    /// Fails unless the records a page decodes to are the records its metadata gives, where the
    /// metadata gives them.
    private static void checkMetadataRecords(int records, int metadataRecords) {
        if (metadataRecords != PageInfo.UNKNOWN_RECORDS && records != metadataRecords) {
            throw new ParquetReadException("Invalid column chunk: page holds " + records
                    + " records but its header or offset index gives " + metadataRecords);
        }
    }

    int values() {
        return values;
    }

    int records() {
        return records;
    }

    int leadingValues() {
        return leadingValues;
    }

    /// The page's first repetition level, which is not `0` when it has [#leadingValues()].
    int firstRepetitionLevel() {
        return firstRepetitionLevel;
    }

    /// Where record `i` starts, in values; [#values()] for `i == records()`.
    int recordStart(int i) {
        if (i == records) {
            return values;
        }
        if (recordStarts != null) {
            return recordStarts[i];
        }
        return fixedListK > 0 ? i * fixedListK : i;
    }

    /// The bytes of the plain binary values `[from, to)`; `0` for a page of any other values.
    int bytes(int from, int to) {
        if (binary == null) {
            return 0;
        }
        return runs == null ? binary.bytes(from, to) : runs.bytes(binary, from, to);
    }

    /// Elements per record of a fixed-size-list page, `0` for any other page.
    int fixedListK() {
        return fixedListK;
    }

    /// Whether every leaf of the page is present: it has no definition levels.
    boolean allPresent() {
        return allPresent;
    }

    /// The page's binary values, `null` for a column of another type.
    BinaryPageValues binary() {
        return binary;
    }

    /// The dictionary of a page of dictionary-encoded binary values, `null` for any other page.
    Dictionary.ByteArrayDictionary dictionary() {
        return binary == null ? null : binary.dictionary();
    }
}
