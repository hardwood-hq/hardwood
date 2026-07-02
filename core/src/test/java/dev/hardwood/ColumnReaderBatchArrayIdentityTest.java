/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Pins the public-API non-aliasing contract documented on [ColumnReader]:
/// every array and [dev.hardwood.Validity] handed back by an accessor
/// is freshly allocated by the [ColumnReader#nextBatch()] that produced it,
/// and a later `nextBatch()` never reuses or overwrites an array returned for
/// an earlier batch. That guarantee is what lets a caller retain a returned
/// array and process it after advancing — including handing it to another
/// thread.
///
/// The contract is currently load-bearing but unpinned: the natural post-1.0
/// perf optimization is to switch the per-batch buffers to reusable scratch,
/// which would silently make consecutive batches alias the same arrays and
/// corrupt every caller using the documented hand-off pattern. These tests
/// lock the invariant in before that pressure shows up.
///
/// `batch_array_identity_test.parquet` carries one flat column per accessor
/// family plus a `list<int32>`, four rows with the null pattern
/// `present / null / null / present`. Read with `batchSize(2)` it yields two
/// batches that each carry a real (backed) validity bitmap — never the shared
/// [dev.hardwood.Validity#NO_NULLS] singleton — so the validity check
/// compares freshly allocated backing `long[]`s rather than the always-fresh
/// wrapper.
class ColumnReaderBatchArrayIdentityTest {

    private static final Path FILE =
            Paths.get("src/test/resources/batch_array_identity_test.parquet");

    @Test
    void intArrayFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("ints", ColumnReader::getInts);
    }

    @Test
    void longArrayFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("longs", ColumnReader::getLongs);
    }

    @Test
    void floatArrayFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("floats", ColumnReader::getFloats);
    }

    @Test
    void doubleArrayFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("doubles", ColumnReader::getDoubles);
    }

    @Test
    void booleanArrayFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("flags", ColumnReader::getBooleans);
    }

    /// The leaf validity is reported through an always-fresh wrapper, so the
    /// load-bearing pin is its backing bitmap: `getLeafValidity().words()` must
    /// be a distinct `long[]` per batch. Both batches carry nulls, so neither
    /// collapses to the [dev.hardwood.Validity#NO_NULLS] singleton.
    @Test
    void leafValidityBitmapFreshPerBatch() throws Exception {
        assertBackingFreshPerBatch("ints", col -> col.getLeafValidity().words());
    }

    /// Both varlength leaf buffers — the byte buffer and the sentinel-suffixed
    /// offsets — are fresh per batch.
    @Test
    void binaryBuffersFreshPerBatch() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("labels").batchSize(2).build()) {

            assertThat(col.nextBatch()).isTrue();
            byte[] bytes0 = col.getBinaryValues();
            int[] offsets0 = col.getBinaryOffsets();

            assertThat(col.nextBatch()).isTrue();
            byte[] bytes1 = col.getBinaryValues();
            int[] offsets1 = col.getBinaryOffsets();

            assertThat(bytes0).as("getBinaryValues() buffer").isNotSameAs(bytes1);
            assertThat(offsets0).as("getBinaryOffsets() buffer").isNotSameAs(offsets1);
        }
    }

    /// The layered case: for a `REPEATED` layer, `getLayerOffsets(k)`,
    /// `getLayerValidity(k).words()` and the leaf value array must all be fresh
    /// per batch. Both batches contain a null list, so the layer validity is
    /// backed in each.
    @Test
    void layerBuffersFreshPerBatch() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("nums.list.element").batchSize(2).build()) {

            assertThat(col.nextBatch()).isTrue();
            assertThat(col.getLayerKind(0)).isEqualTo(LayerKind.REPEATED);
            int[] offsets0 = col.getLayerOffsets(0);
            long[] layerValidity0 = col.getLayerValidity(0).words();
            int[] leaf0 = col.getInts();
            assertThat(layerValidity0).as("batch 0 list-layer validity must be backed").isNotNull();

            assertThat(col.nextBatch()).isTrue();
            int[] offsets1 = col.getLayerOffsets(0);
            long[] layerValidity1 = col.getLayerValidity(0).words();
            int[] leaf1 = col.getInts();
            assertThat(layerValidity1).as("batch 1 list-layer validity must be backed").isNotNull();

            assertThat(offsets0).as("getLayerOffsets(0) buffer").isNotSameAs(offsets1);
            assertThat(layerValidity0).as("getLayerValidity(0) bitmap").isNotSameAs(layerValidity1);
            assertThat(leaf0).as("nested leaf value array").isNotSameAs(leaf1);
        }
    }

    /// The non-aliasing guarantee is only useful if the retained array also
    /// keeps its contents — "never reuses **or overwrites**". Capture batch 0's
    /// values, advance, and assert the held array still reads batch 0's data
    /// (rows `[10, null]` → `ints[0] == 10`) and is a distinct object from
    /// batch 1's.
    @Test
    void retainedArrayNotOverwrittenAfterAdvance() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader("ints").batchSize(2).build()) {

            assertThat(col.nextBatch()).isTrue();
            int[] batch0 = col.getInts();
            assertThat(batch0[0]).isEqualTo(10);

            assertThat(col.nextBatch()).isTrue();
            int[] batch1 = col.getInts();

            assertThat(batch1).isNotSameAs(batch0);
            // Batch 1 is rows [null, 40]; the real-items-only leaf holds 40.
            assertThat(batch1[batch1.length - 1]).isEqualTo(40);
            // The retained batch-0 array is untouched by the advance.
            assertThat(batch0[0]).isEqualTo(10);
        }
    }

    /// Reads two batches of `column` and asserts that the backing storage
    /// returned by `accessor` is a distinct, non-null object across them. The
    /// fixture is sized so both `nextBatch()` calls succeed and (for validity)
    /// both return a backed bitmap.
    private static void assertBackingFreshPerBatch(String column, Function<ColumnReader, Object> accessor)
            throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             ColumnReader col = reader.buildColumnReader(column).batchSize(2).build()) {

            assertThat(col.nextBatch()).isTrue();
            Object first = accessor.apply(col);
            assertThat(first).as("batch 0 backing array").isNotNull();

            assertThat(col.nextBatch()).isTrue();
            Object second = accessor.apply(col);
            assertThat(second).as("batch 1 backing array").isNotNull();

            assertThat(first).as("a later nextBatch() must not reuse an earlier batch's array")
                    .isNotSameAs(second);
        }
    }
}
