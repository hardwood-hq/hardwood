/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Oracle test to verify parity between the fused decode path and the materialising path.
class FlatColumnWorkerParityTest {

    private static final Path YELLOW_TRIPDATA = Path.of("src/test/resources/yellow_tripdata_sample.parquet");
    private static final Path NULLABLE_PRIMITIVES = Path.of("src/test/resources/nullable_primitives_test.parquet");
    private static final Path PAGE_INDEX = Path.of("src/test/resources/page_index_test.parquet");
    private static final Path FUSED_TINY_PAGES = Path.of("src/test/resources/fused_tiny_pages_dict.parquet");

    private static class MaterialisingFlatColumnWorker extends FlatColumnWorker {
        MaterialisingFlatColumnWorker(PageSource source,
                                      BatchExchange<BatchExchange.Batch> exchange,
                                      ColumnSchema columnSchema,
                                      int maxBatchCapacity,
                                      dev.hardwood.internal.compression.DecompressorFactory decompressors,
                                      Executor executor) {
            super(source, exchange, columnSchema, maxBatchCapacity, decompressors, executor, 0, null);
        }

        @Override
        protected boolean supportsFusedPath() {
            return false;
        }
    }

    private static class FusedFlatColumnWorker extends FlatColumnWorker {
        FusedFlatColumnWorker(PageSource source,
                              BatchExchange<BatchExchange.Batch> exchange,
                              ColumnSchema columnSchema,
                              int maxBatchCapacity,
                              dev.hardwood.internal.compression.DecompressorFactory decompressors,
                              Executor executor) {
            super(source, exchange, columnSchema, maxBatchCapacity, decompressors, executor, 0, null);
        }

        @Override
        protected boolean supportsFusedPath() {
            return true;
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parityTestYellowTripData() throws Exception {
        runParityTestOnAllColumns(YELLOW_TRIPDATA);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parityTestNullablePrimitives() throws Exception {
        runParityTestOnAllColumns(NULLABLE_PRIMITIVES);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parityTestPageIndex() throws Exception {
        runParityTestOnAllColumns(PAGE_INDEX);
    }

    /// Regression guard for the run-fused decode path over many tiny dictionary
    /// pages. Two hazards that only surface with small pages are covered here:
    /// a page's cursor outliving the reused decompression buffer, and a page
    /// whose values all map to a single dictionary entry (index bit width 0).
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void parityTestFusedTinyPages() throws Exception {
        runParityTestOnAllColumns(FUSED_TINY_PAGES);
    }

    private void runParityTestOnAllColumns(Path file) throws Exception {
        runParityTestOnAllColumns(file, 1024);
    }

    private void runParityTestOnAllColumns(Path file, int batchCapacity) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {

            FileSchema schema = reader.getFileSchema();

            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                ColumnSchema column = schema.getColumn(colIdx);
                
                // Nested columns are out of scope for FlatColumnWorker
                if (column.maxRepetitionLevel() > 0) {
                    continue;
                }

                List<BatchExchange.Batch> matBatches = runWorker(
                        file, schema, colIdx, column, batchCapacity, context, false);
                List<BatchExchange.Batch> fusedBatches = runWorker(
                        file, schema, colIdx, column, batchCapacity, context, true);

                assertThat(matBatches.size())
                        .as("Column %s should produce same number of batches", column.name())
                        .isEqualTo(fusedBatches.size());

                for (int i = 0; i < matBatches.size(); i++) {
                    BatchExchange.Batch mat = matBatches.get(i);
                    BatchExchange.Batch fused = fusedBatches.get(i);

                    assertThat(fused.recordCount)
                            .as("Batch %d record count should match for %s", i, column.name())
                            .isEqualTo(mat.recordCount);

                    if (mat.recordCount > 0) {
                        assertBitmapsEqual(mat.validity, fused.validity, mat.recordCount, column.name(), i);
                        assertValuesEqual(mat.values, fused.values, mat.recordCount, column, i);
                    }
                }
            }
        }
    }

    private List<BatchExchange.Batch> runWorker(Path file, FileSchema schema, int colIdx,
                                                ColumnSchema column, int batchCapacity,
                                                HardwoodContextImpl context, boolean useFusedPath) throws Exception {
        RowGroupIterator iterator = createIterator(file, schema, context);

        BatchExchange<BatchExchange.Batch> exchange = BatchExchange.recycling(
                column.name(), () -> {
                    BatchExchange.Batch b = new BatchExchange.Batch();
                    b.values = BatchExchange.allocateArray(column, batchCapacity);
                    return b;
                });

        FlatColumnWorker worker = useFusedPath
                ? new FusedFlatColumnWorker(new PageSource(iterator, colIdx), exchange, column, batchCapacity, context.decompressorFactory(), context.executor())
                : new MaterialisingFlatColumnWorker(new PageSource(iterator, colIdx), exchange, column, batchCapacity, context.decompressorFactory(), context.executor());

        worker.start();

        List<BatchExchange.Batch> result = new ArrayList<>();
        BatchExchange.Batch batch;
        while ((batch = exchange.poll()) != null) {
            BatchExchange.Batch copy = new BatchExchange.Batch();
            copy.recordCount = batch.recordCount;
            if (batch.validity != null) {
                copy.validity = batch.validity.clone();
            }
            if (batch.values != null) {
                copy.values = cloneValues(batch.values, column);
            }
            result.add(copy);
            exchange.recycle(batch);
        }
        exchange.checkError();
        worker.close();

        return result;
    }

    private RowGroupIterator createIterator(Path file, FileSchema schema,
                                            HardwoodContextImpl context) throws Exception {
        InputFile inputFile = InputFile.of(file);
        inputFile.open();

        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        RowGroupIterator iterator = new RowGroupIterator(
                java.util.List.of(inputFile), context, 0);
        iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
        iterator.initialize(
                ProjectedSchema.create(schema, dev.hardwood.schema.ColumnProjection.all()), null);
        reader.close();
        return iterator;
    }

    private void assertBitmapsEqual(long[] mat, long[] fused, int recordCount, String colName, int batchIdx) {
        if (mat == null && fused == null) return;
        
        // If materialised path yields null, it implies all are valid.
        // If fused yields a bitmap, all bits for the valid count must be 1.
        if (mat == null) {
            int numWords = (recordCount + 63) >>> 6;
            for (int i = 0; i < numWords; i++) {
                int limit = (i == numWords - 1 && (recordCount & 63) != 0) ? (recordCount & 63) : 64;
                long mask = (1L << limit) - 1;
                if (limit == 64) mask = -1L;
                
                assertThat(fused[i] & mask).as("Fused validity has unexpected nulls in batch %d for col %s", batchIdx, colName).isEqualTo(mask);
            }
            return;
        }

        if (fused == null) {
            int numWords = (recordCount + 63) >>> 6;
            for (int i = 0; i < numWords; i++) {
                int limit = (i == numWords - 1 && (recordCount & 63) != 0) ? (recordCount & 63) : 64;
                long mask = (1L << limit) - 1;
                if (limit == 64) mask = -1L;
                
                assertThat(mat[i] & mask).as("Mat validity has unexpected nulls in batch %d for col %s", batchIdx, colName).isEqualTo(mask);
            }
            return;
        }

        int numWords = (recordCount + 63) >>> 6;
        for (int i = 0; i < numWords; i++) {
            int limit = (i == numWords - 1 && (recordCount & 63) != 0) ? (recordCount & 63) : 64;
            long mask = (1L << limit) - 1;
            if (limit == 64) mask = -1L;

            assertThat(fused[i] & mask)
                    .as("Validity bitmask mismatch in word %d of batch %d for col %s", i, batchIdx, colName)
                    .isEqualTo(mat[i] & mask);
        }
    }

    private void assertValuesEqual(Object matVals, Object fusedVals, int recordCount, ColumnSchema column, int batchIdx) {
        if (matVals instanceof BinaryBatchValues matBbv) {
            BinaryBatchValues fusedBbv = (BinaryBatchValues) fusedVals;
            
            // Check mapping
            if (matBbv.internStrings) {
                for (int i = 0; i < recordCount; i++) {
                    assertThat(fusedBbv.dictIndices[i])
                            .as("String intern index mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                            .isEqualTo(matBbv.dictIndices[i]);
                }
            } else {
                for (int i = 0; i <= recordCount; i++) {
                    assertThat(fusedBbv.offsets[i])
                            .as("String offset mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                            .isEqualTo(matBbv.offsets[i]);
                }
            }
        } else {
            switch (column.type()) {
                case INT32 -> {
                    int[] matArray = (int[]) matVals;
                    int[] fusedArray = (int[]) fusedVals;
                    for (int i = 0; i < recordCount; i++) {
                        assertThat(fusedArray[i])
                                .as("INT32 mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                                .isEqualTo(matArray[i]);
                    }
                }
                case INT64 -> {
                    long[] matArray = (long[]) matVals;
                    long[] fusedArray = (long[]) fusedVals;
                    for (int i = 0; i < recordCount; i++) {
                        assertThat(fusedArray[i])
                                .as("INT64 mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                                .isEqualTo(matArray[i]);
                    }
                }
                case FLOAT -> {
                    float[] matArray = (float[]) matVals;
                    float[] fusedArray = (float[]) fusedVals;
                    for (int i = 0; i < recordCount; i++) {
                        assertThat(fusedArray[i])
                                .as("FLOAT mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                                .isEqualTo(matArray[i]);
                    }
                }
                case DOUBLE -> {
                    double[] matArray = (double[]) matVals;
                    double[] fusedArray = (double[]) fusedVals;
                    for (int i = 0; i < recordCount; i++) {
                        assertThat(fusedArray[i])
                                .as("DOUBLE mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                                .isEqualTo(matArray[i]);
                    }
                }
                // BOOLEAN columns use RLE encoding (not RLE_DICTIONARY), so the fused
                // path gate in PageDecoder never fires for them. This arm is unreachable
                // under the current gate; it stays as a safety net if the gate widens.
                case BOOLEAN -> {
                    boolean[] matArray = (boolean[]) matVals;
                    boolean[] fusedArray = (boolean[]) fusedVals;
                    for (int i = 0; i < recordCount; i++) {
                        assertThat(fusedArray[i])
                                .as("BOOLEAN mismatch at pos %d in batch %d for %s", i, batchIdx, column.name())
                                .isEqualTo(matArray[i]);
                    }
                }
                default -> throw new UnsupportedOperationException("Unsupported type: " + column.type());
            }
        }
    }

    private Object cloneValues(Object values, ColumnSchema column) {
        if (values instanceof BinaryBatchValues bbv) {
            BinaryBatchValues clone = (BinaryBatchValues) BatchExchange.allocateArray(column, bbv.offsets.length - 1);
            System.arraycopy(bbv.offsets, 0, clone.offsets, 0, bbv.offsets.length);
            if (bbv.internStrings) {
                if (bbv.dictIndices != null) {
                    clone.dictIndices = new int[bbv.dictIndices.length];
                    System.arraycopy(bbv.dictIndices, 0, clone.dictIndices, 0, bbv.dictIndices.length);
                }
                clone.dictionary = bbv.dictionary;
            } else {
                clone.bytes = bbv.bytes.clone();
            }
            return clone;
        } else if (values instanceof int[] a) {
            return a.clone();
        } else if (values instanceof long[] a) {
            return a.clone();
        } else if (values instanceof float[] a) {
            return a.clone();
        } else if (values instanceof double[] a) {
            return a.clone();
        } else if (values instanceof boolean[] a) {
            return a.clone();
        } else {
            throw new UnsupportedOperationException("Unsupported values type: " + values.getClass());
        }
    }
}
