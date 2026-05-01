/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for `SequentialFetchPlan.computeChunkSize`, focused on the
/// "fetch the whole column when small enough" threshold introduced for #382.
class SequentialFetchPlanChunkSizeTest {

    private static final int ONE_MIB = 1024 * 1024;

    @Test
    void smallColumnUnderHeadFetchesEntireChunk() {
        // 2 MB column under a tight `head(N)` — without #382 the chunk
        // size would clamp to the 1 MB floor; with #382 it returns the
        // full column length so the column is coalesce-safe.
        int columnLength = 2 * ONE_MIB;
        int chunk = SequentialFetchPlan.computeChunkSize(
                columnLength, fakeMetaData(columnLength, 100_000), 30);

        assertThat(chunk).isEqualTo(columnLength);
    }

    @Test
    void columnAtThresholdBoundaryFetchesEntireChunk() {
        // Exactly 4 MB sits at the threshold — included.
        int columnLength = 4 * ONE_MIB;
        int chunk = SequentialFetchPlan.computeChunkSize(
                columnLength, fakeMetaData(columnLength, 100_000), 30);

        assertThat(chunk).isEqualTo(columnLength);
    }

    @Test
    void columnAboveThresholdStaysTruncated() {
        // 5 MB column — the head(N)-driven floor of 1 MB still applies
        // so the per-column truncation keeps a 50+ MB column from
        // pulling its full bytes for a 30-row preview.
        int columnLength = 5 * ONE_MIB;
        int chunk = SequentialFetchPlan.computeChunkSize(
                columnLength, fakeMetaData(columnLength, 100_000), 30);

        assertThat(chunk).isEqualTo(ONE_MIB);
    }

    @Test
    void dynamicEstimateLandsBetweenFloorAndCeiling() {
        // Above the 4 MB short-circuit, `computeChunkSize` derives the
        // size from `bytesPerValue × maxRows × safetyFactor`. For a
        // 50 MB column with 100 values (500 KB / value) and head(10),
        // the estimate is 10 × 500 KB × 2 = 10 MB — comfortably above
        // the 1 MB floor and below the 128 MB ceiling. Pins the
        // dynamic path so a regression that breaks the interpolation
        // is observable.
        int columnLength = 50 * ONE_MIB;
        int chunk = SequentialFetchPlan.computeChunkSize(
                columnLength, fakeMetaData(columnLength, 100), 10);

        assertThat(chunk)
                .as("dynamic estimate should land between floor and ceiling")
                .isGreaterThan(ONE_MIB)
                .isLessThan(columnLength);
    }

    @Test
    void noMaxRowsReturnsDefaultCeiling() {
        // Without head(N), behavior is unchanged: full chunk size up to
        // the configured ceiling.
        int columnLength = 50 * ONE_MIB;
        int chunk = SequentialFetchPlan.computeChunkSize(
                columnLength, fakeMetaData(columnLength, 100_000), 0);

        // computeChunkSize returns the ceiling; the build() caller
        // applies `Math.min(columnChunkLength, ceiling)`.
        assertThat(chunk).isGreaterThanOrEqualTo(columnLength);
    }

    private static ColumnMetaData fakeMetaData(long totalCompressedSize, long numValues) {
        return new ColumnMetaData(
                PhysicalType.INT64,
                List.of(Encoding.PLAIN),
                FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED,
                numValues,
                totalCompressedSize,
                totalCompressedSize,
                Map.of(),
                0L,
                null,
                null,
                null);
    }
}
