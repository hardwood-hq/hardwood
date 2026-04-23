/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// High-performance row reader for flat schemas.
///
/// This is a `final` class with all hot-path methods defined directly (not inherited),
/// giving the JIT a single concrete class with monomorphic call sites. Supports all
/// primitive types, logical type conversions (date, time, timestamp, decimal, UUID,
/// string), and both name-based and index-based access.
public final class FlatRowReader implements RowReader {

    private static final BitSet NO_NULLS = new BitSet(0);

    private final BatchExchange<BatchExchange.Batch>[] exchanges;
    private final FlatColumnWorker[] columnWorkers;
    private final int columnCount;

    // Schema info for name lookup and logical type conversion
    private final FileSchema fileSchema;
    private final ProjectedSchema projectedSchema;
    private final StringToIntMap nameToIndex;
    private final PhysicalType[] physicalTypes;
    private final ColumnSchema[] columnSchemas;

    // Hot fields — directly owned, no inheritance
    private Object[] flatValueArrays;
    private BitSet[] flatNulls;
    private BatchExchange.Batch[] previousBatches;
    private int rowIndex = -1;
    private int batchSize = 0;
    private boolean exhausted;

    public FlatRowReader(BatchExchange<BatchExchange.Batch>[] exchanges, FlatColumnWorker[] columnWorkers,
                         FileSchema fileSchema, ProjectedSchema projectedSchema) {
        this.exchanges = exchanges;
        this.columnWorkers = columnWorkers;
        this.columnCount = exchanges.length;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.flatValueArrays = new Object[columnCount];
        this.flatNulls = new BitSet[columnCount];
        this.previousBatches = new BatchExchange.Batch[columnCount];

        // Build name-to-index map and cache column metadata
        this.nameToIndex = new StringToIntMap(columnCount);
        this.physicalTypes = new PhysicalType[columnCount];
        this.columnSchemas = new ColumnSchema[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema col = fileSchema.getColumn(originalIndex);
            nameToIndex.put(col.name(), i);
            physicalTypes[i] = col.type();
            columnSchemas[i] = col;
        }
    }

    /// Eagerly loads the first batch. Must be called after construction.
    public void initialize() {
        if (!loadNextBatch()) {
            exhausted = true;
        }
    }

    // ==================== Factory ====================

    /// Creates a flat v3 pipeline and returns a [RowReader].
    ///
    /// Wires up `RowGroupIterator → PageSource → ColumnWorker → BatchExchange → FlatRowReader`,
    /// starts all column workers, initializes the reader, and wraps with
    /// [FilteredRowReader] if a filter is present.
    ///
    /// @param rowGroupIterator pre-configured iterator (file opened, first file set, initialized)
    /// @param schema the file schema
    /// @param projectedSchema the projected column schema
    /// @param context the hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited), enforced by [ColumnWorker] drain
    /// @return a [FlatRowReader] or [FilteredRowReader]
    public static RowReader create(RowGroupIterator rowGroupIterator,
                                   FileSchema schema,
                                   ProjectedSchema projectedSchema,
                                   HardwoodContextImpl context,
                                   ResolvedPredicate filter,
                                   long maxRows) {
        int batchSize = BatchSizing.computeOptimalBatchSize(projectedSchema);
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        FlatColumnWorker[] workers = new FlatColumnWorker[projectedColumnCount];
        @SuppressWarnings("unchecked")
        BatchExchange<BatchExchange.Batch>[] buffers = new BatchExchange[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageSource pageSource = new PageSource(rowGroupIterator, i);

            PhysicalType physType = columnSchema.type();
            BatchExchange<BatchExchange.Batch> buffer = BatchExchange.recycling(
                    columnSchema.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(physType, batchSize);
                        return b;
                    });
            FlatColumnWorker worker = new FlatColumnWorker(
                    pageSource, buffer, columnSchema, batchSize,
                    context.decompressorFactory(), context.executor(), maxRows);

            buffers[i] = buffer;
            workers[i] = worker;
            worker.start();
        }

        FlatRowReader reader = new FlatRowReader(buffers, workers, schema, projectedSchema);
        reader.initialize();
        if (filter != null) {
            return new FilteredRowReader(reader, filter, schema);
        }
        return reader;
    }


    // ==================== Iteration ====================

    @Override
    public boolean hasNext() {
        if (exhausted) {
            return false;
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadNextBatch();
    }

    @Override
    public void next() {
        rowIndex++;
    }

    // ==================== Null Check ====================

    @Override
    public boolean isNull(int columnIndex) {
        return flatNulls[columnIndex].get(rowIndex);
    }

    @Override
    public boolean isNull(String name) {
        return isNull(resolveIndex(name));
    }

    // ==================== Primitive Accessors by Index ====================

    @Override
    public int getInt(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((int[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public long getLong(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((long[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public float getFloat(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((float[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public double getDouble(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((double[]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        if (flatNulls[columnIndex].get(rowIndex)) {
            throwNull(columnIndex);
        }
        return ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
    }

    // ==================== Primitive Accessors by Name ====================

    @Override
    public int getInt(String name) {
        return getInt(resolveAndValidate(name, PhysicalType.INT32));
    }

    @Override
    public long getLong(String name) {
        return getLong(resolveAndValidate(name, PhysicalType.INT64));
    }

    @Override
    public float getFloat(String name) {
        return getFloat(resolveAndValidate(name, PhysicalType.FLOAT));
    }

    @Override
    public double getDouble(String name) {
        return getDouble(resolveAndValidate(name, PhysicalType.DOUBLE));
    }

    @Override
    public boolean getBoolean(String name) {
        return getBoolean(resolveAndValidate(name, PhysicalType.BOOLEAN));
    }

    // ==================== String / Binary ====================

    @Override
    public String getString(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return new String(((byte[][]) flatValueArrays[columnIndex])[rowIndex], StandardCharsets.UTF_8);
    }

    @Override
    public String getString(String name) {
        return getString(resolveIndex(name));
    }

    @Override
    public byte[] getBinary(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
    }

    @Override
    public byte[] getBinary(String name) {
        return getBinary(resolveIndex(name));
    }

    // ==================== Logical Type Accessors ====================

    @Override
    public LocalDate getDate(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        int rawValue = ((int[]) flatValueArrays[columnIndex])[rowIndex];
        return LogicalTypeConverter.convertToDate(rawValue, physicalTypes[columnIndex]);
    }

    @Override
    public LocalDate getDate(String name) {
        return getDate(resolveIndex(name));
    }

    @Override
    public LocalTime getTime(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((int[]) flatValueArrays[columnIndex])[rowIndex];
        }
        else {
            rawValue = ((long[]) flatValueArrays[columnIndex])[rowIndex];
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(),
                (LogicalType.TimeType) col.logicalType());
    }

    @Override
    public LocalTime getTime(String name) {
        return getTime(resolveIndex(name));
    }

    @Override
    public Instant getTimestamp(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        if (col.type() == PhysicalType.INT96) {
            byte[] rawValue = ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
            return LogicalTypeConverter.int96ToInstant(rawValue);
        }
        long rawValue = ((long[]) flatValueArrays[columnIndex])[rowIndex];
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(),
                (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        return getTimestamp(resolveIndex(name));
    }

    @Override
    public BigDecimal getDecimal(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = columnSchemas[columnIndex];
        Object rawValue = switch (col.type()) {
            case INT32 -> ((int[]) flatValueArrays[columnIndex])[rowIndex];
            case INT64 -> ((long[]) flatValueArrays[columnIndex])[rowIndex];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
            default -> throw new IllegalArgumentException(
                    "Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(),
                (LogicalType.DecimalType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(resolveIndex(name));
    }

    @Override
    public UUID getUuid(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return LogicalTypeConverter.convertToUuid(
                ((byte[][]) flatValueArrays[columnIndex])[rowIndex],
                physicalTypes[columnIndex]);
    }

    @Override
    public UUID getUuid(String name) {
        return getUuid(resolveIndex(name));
    }

    // ==================== Generic Value ====================

    @Override
    public Object getValue(int columnIndex) {
        if (isNull(columnIndex)) {
            return null;
        }
        return switch (physicalTypes[columnIndex]) {
            case INT32 -> ((int[]) flatValueArrays[columnIndex])[rowIndex];
            case INT64 -> ((long[]) flatValueArrays[columnIndex])[rowIndex];
            case FLOAT -> ((float[]) flatValueArrays[columnIndex])[rowIndex];
            case DOUBLE -> ((double[]) flatValueArrays[columnIndex])[rowIndex];
            case BOOLEAN -> ((boolean[]) flatValueArrays[columnIndex])[rowIndex];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    ((byte[][]) flatValueArrays[columnIndex])[rowIndex];
        };
    }

    @Override
    public Object getValue(String name) {
        return getValue(resolveIndex(name));
    }

    // ==================== Nested (not supported for flat) ====================

    @Override public PqStruct getStruct(String name) { throw nestedUnsupported(); }
    @Override public PqStruct getStruct(int i) { throw nestedUnsupported(); }
    @Override public PqIntList getListOfInts(String name) { throw nestedUnsupported(); }
    @Override public PqIntList getListOfInts(int i) { throw nestedUnsupported(); }
    @Override public PqLongList getListOfLongs(String name) { throw nestedUnsupported(); }
    @Override public PqLongList getListOfLongs(int i) { throw nestedUnsupported(); }
    @Override public PqDoubleList getListOfDoubles(String name) { throw nestedUnsupported(); }
    @Override public PqDoubleList getListOfDoubles(int i) { throw nestedUnsupported(); }
    @Override public PqList getList(String name) { throw nestedUnsupported(); }
    @Override public PqList getList(int i) { throw nestedUnsupported(); }
    @Override public PqMap getMap(String name) { throw nestedUnsupported(); }
    @Override public PqMap getMap(int i) { throw nestedUnsupported(); }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return columnCount;
    }

    @Override
    public String getFieldName(int index) {
        int originalIndex = projectedSchema.toOriginalIndex(index);
        return fileSchema.getColumn(originalIndex).name();
    }

    // ==================== Batch Loading ====================

    private boolean loadNextBatch() {
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].recycle(previousBatches[i]);
                previousBatches[i] = null;
            }
            BatchExchange.Batch batch;
            try {
                batch = exchanges[i].poll();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (batch == null || batch.recordCount == 0) {
                // Check for pipeline errors before returning exhausted —
                // the pipeline may have errored after publishing partial results.
                for (int j = 0; j < columnCount; j++) {
                    exchanges[j].checkError();
                }
                // If earlier columns returned data but this one is empty,
                // the file is corrupt (column count mismatch).
                if (i > 0) {
                    throw new IllegalStateException(
                            "Column count mismatch: column " + i + " produced no data"
                            + " while earlier columns had " + batchSize + " records");
                }
                exhausted = true;
                return false;
            }
            flatValueArrays[i] = batch.values;
            flatNulls[i] = batch.nulls != null ? batch.nulls : NO_NULLS;
            previousBatches[i] = batch;
            if (i == 0) {
                batchSize = batch.recordCount;
            }
        }
        rowIndex = -1;
        return true;
    }

    // ==================== Close ====================

    @Override
    public void close() {
        if (columnWorkers != null) {
            for (FlatColumnWorker worker : columnWorkers) {
                worker.close();
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].recycle(previousBatches[i]);
                previousBatches[i] = null;
            }
            exchanges[i].drainReady();
        }
    }

    // ==================== Internal ====================

    private int resolveIndex(String name) {
        int index = nameToIndex.get(name);
        if (index < 0) {
            throw new IllegalArgumentException("Column not in projection: " + name);
        }
        return index;
    }

    private int resolveAndValidate(String name, PhysicalType expectedType) {
        int index = resolveIndex(name);
        if (physicalTypes[index] != expectedType) {
            throw new IllegalArgumentException(
                    "Field '" + name + "' has physical type " + physicalTypes[index]
                            + ", expected " + expectedType);
        }
        return index;
    }

    private void throwNull(int columnIndex) {
        String name = getFieldName(columnIndex);
        throw new NullPointerException("Column '" + name + "' is null at row " + rowIndex);
    }

    private static UnsupportedOperationException nestedUnsupported() {
        return new UnsupportedOperationException("Nested type access not supported for flat schemas");
    }
}
