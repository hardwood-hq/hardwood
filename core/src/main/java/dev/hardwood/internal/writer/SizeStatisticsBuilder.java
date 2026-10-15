/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.schema.ColumnSchema;

/// Assembles a chunk's size statistics from its pages, in page order: per page the repetition
/// and definition level histograms the `ColumnIndex` carries and the unencoded `BYTE_ARRAY` bytes
/// the `OffsetIndex` carries, and their sums as the chunk's `SizeStatistics`.
///
/// The repetition level histogram is kept where the column repeats, the definition level
/// histogram where the column's maximum definition level is above 1. At level 1 the only levels
/// are null and present, which the null count gives. The unencoded bytes are kept for a
/// `BYTE_ARRAY` column only, as the format defines them.
final class SizeStatisticsBuilder {

    private final int repetitionBuckets; // 0 where the histogram is omitted
    private final int definitionBuckets;
    private final boolean countsBytes;
    private long[] repetitionHistograms;
    private long[] definitionHistograms;
    private long[] pageBytes;
    private int pageCount;

    SizeStatisticsBuilder(ColumnSchema column) {
        int maxRepetitionLevel = column.maxRepetitionLevel();
        int maxDefinitionLevel = column.maxDefinitionLevel();
        this.repetitionBuckets = maxRepetitionLevel > 0 ? maxRepetitionLevel + 1 : 0;
        this.definitionBuckets = maxDefinitionLevel > 1 ? maxDefinitionLevel + 1 : 0;
        this.countsBytes = column.type() == PhysicalType.BYTE_ARRAY;
        this.repetitionHistograms = new long[repetitionBuckets * 16];
        this.definitionHistograms = new long[definitionBuckets * 16];
        this.pageBytes = new long[countsBytes ? 16 : 0];
    }

    /// Whether the chunk's pages are counted in unencoded `BYTE_ARRAY` bytes, which the caller
    /// supplies to [#add].
    boolean countsBytes() {
        return countsBytes;
    }

    /// Adds the page holding the `entries` level entries from `entryFrom`.
    ///
    /// @param repetitionLevels the chunk's repetition levels a byte per entry, `null` when the
    ///        column does not repeat
    /// @param definitionLevels the chunk's definition levels a byte per entry, `null` when the
    ///        column is unlevelled
    /// @param unencodedBytes the page's present values' bytes without their length prefixes;
    ///        ignored unless [#countsBytes]
    void add(byte[] repetitionLevels, byte[] definitionLevels, int entryFrom, int entries, long unencodedBytes) {
        int page = pageCount++;
        repetitionHistograms = count(repetitionHistograms, repetitionBuckets, page, repetitionLevels, entryFrom,
                entries);
        definitionHistograms = count(definitionHistograms, definitionBuckets, page, definitionLevels, entryFrom,
                entries);
        if (countsBytes) {
            if (page == pageBytes.length) {
                pageBytes = Arrays.copyOf(pageBytes, page * 2);
            }
            pageBytes[page] = unencodedBytes;
        }
    }

    /// Tallies one page's levels into its slice of `histograms`, growing the array where the page
    /// does not fit, and returns the array.
    private static long[] count(long[] histograms, int buckets, int page, byte[] levels, int entryFrom,
                                int entries) {
        if (buckets == 0) {
            return histograms;
        }
        int base = page * buckets;
        if (base == histograms.length) {
            histograms = Arrays.copyOf(histograms, base * 2);
        }
        for (int entry = entryFrom; entry < entryFrom + entries; entry++) {
            histograms[base + (levels[entry] & 0xFF)]++;
        }
        return histograms;
    }

    /// The pages' repetition level histograms concatenated page by page, or `null` where they
    /// are omitted.
    long[] repetitionLevelHistograms() {
        return repetitionBuckets == 0 ? null : Arrays.copyOf(repetitionHistograms, pageCount * repetitionBuckets);
    }

    /// The pages' definition level histograms concatenated page by page, or `null` where they
    /// are omitted.
    long[] definitionLevelHistograms() {
        return definitionBuckets == 0 ? null : Arrays.copyOf(definitionHistograms, pageCount * definitionBuckets);
    }

    /// The pages' unencoded `BYTE_ARRAY` bytes, or `null` for a column of another type.
    long[] unencodedByteArrayDataBytes() {
        return countsBytes ? Arrays.copyOf(pageBytes, pageCount) : null;
    }

    /// The chunk's `SizeStatistics`, or `null` where it would hold no field.
    SizeStatistics build() {
        if (repetitionBuckets == 0 && definitionBuckets == 0 && !countsBytes) {
            return null;
        }
        Long bytes = null;
        if (countsBytes) {
            long total = 0;
            for (int page = 0; page < pageCount; page++) {
                total += pageBytes[page];
            }
            bytes = total;
        }
        return new SizeStatistics(bytes, sum(repetitionHistograms, repetitionBuckets),
                sum(definitionHistograms, definitionBuckets));
    }

    /// The pages' histograms added bucket by bucket, or `null` where they are omitted.
    private long[] sum(long[] histograms, int buckets) {
        if (buckets == 0) {
            return null;
        }
        long[] chunk = new long[buckets];
        for (int i = 0; i < pageCount * buckets; i++) {
            chunk[i % buckets] += histograms[i];
        }
        return chunk;
    }
}
