/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.LinkedHashMap;
import java.util.Map;

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.FileManager;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Holds multiple {@link ColumnReader} instances backed by a shared {@link FileManager}
 * for cross-file prefetching across multiple Parquet files.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create();
 *      MultiFileParquetReader parquet = hardwood.openAll(files);
 *      MultiFileColumnReaders columns = parquet.createColumnReaders(
 *          ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))) {
 *
 *     ColumnReader col0 = columns.getColumnReader("passenger_count");
 *     ColumnReader col1 = columns.getColumnReader("trip_distance");
 *     ColumnReader col2 = columns.getColumnReader("fare_amount");
 *
 *     while (col0.nextBatch() & col1.nextBatch() & col2.nextBatch()) {
 *         int count = col0.getRecordCount();
 *         double[] v0 = col0.getDoubles();
 *         // ...
 *     }
 * }
 * }</pre>
 */
public class MultiFileColumnReaders implements AutoCloseable {

    private final Map<String, ColumnReader> readersByName;
    private final ColumnReader[] readersByIndex;

    MultiFileColumnReaders(HardwoodContext context, FileManager fileManager,
                           FileManager.InitResult initResult) {
        FileSchema schema = initResult.schema();
        ProjectedSchema projectedSchema = initResult.projectedSchema();
        String firstFileName = initResult.firstFileState().path().getFileName().toString();

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        this.readersByName = new LinkedHashMap<>(projectedColumnCount);
        this.readersByIndex = new ColumnReader[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            int[] thresholds = null;
            if (columnSchema.maxRepetitionLevel() > 0) {
                thresholds = NestedLevelComputer.computeLevelNullThresholds(
                        schema.getRootNode(), columnSchema.columnIndex());
            }
            ColumnReader reader = new ColumnReader(
                    columnSchema,
                    initResult.firstFileState().pageInfosByColumn().get(i),
                    context,
                    ColumnReader.DEFAULT_BATCH_SIZE,
                    thresholds,
                    fileManager,
                    i,
                    firstFileName);

            readersByName.put(columnSchema.name(), reader);
            readersByIndex[i] = reader;
        }
    }

    /**
     * Get the number of projected columns.
     */
    public int getColumnCount() {
        return readersByIndex.length;
    }

    /**
     * Get the ColumnReader for a named column.
     *
     * @param columnName the column name (must have been requested in the projection)
     * @return the ColumnReader for the column
     * @throws IllegalArgumentException if the column was not requested
     */
    public ColumnReader getColumnReader(String columnName) {
        ColumnReader reader = readersByName.get(columnName);
        if (reader == null) {
            throw new IllegalArgumentException("Column '" + columnName + "' was not requested");
        }
        return reader;
    }

    /**
     * Get the ColumnReader by index within the requested columns.
     *
     * @param index index within the requested column names (0-based)
     * @return the ColumnReader at the given index
     */
    public ColumnReader getColumnReader(int index) {
        return readersByIndex[index];
    }

    @Override
    public void close() {
        for (ColumnReader reader : readersByIndex) {
            reader.close();
        }
    }
}
