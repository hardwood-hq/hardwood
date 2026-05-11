/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.Experimental;
import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.FlatColumnWorker;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedColumnWorker;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageSource;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Batch-oriented column reader for reading a single column across all row groups.
///
/// Exposes a column's batch as typed leaf values plus a layer-model view of
/// the schema chain between root and leaf. Each non-leaf node along the chain
/// contributes zero or one [LayerKind] layer:
///
/// - `OPTIONAL` group → [LayerKind#STRUCT]
/// - `LIST` / `MAP`-annotated group → [LayerKind#REPEATED]
/// - `REQUIRED` group / synthetic LIST scaffolding → no layer
///
/// Layers are numbered `0..getLayerCount() - 1` outermost-to-innermost. A flat
/// column (no enclosing nullable groups, no repetition) reports
/// `getLayerCount() == 0` and is queried solely through [#getLeafValidity()]
/// plus the typed value accessors.
///
/// **Polarity:** validity bitmaps carry **set bit = present** semantics. A
/// `null` return is the sparse representation of "every item at that scope
/// is present in the current batch."
///
/// **Real items only.** Layer offsets and the leaf array are sized to
/// real-items-only counts. Phantom positions for null/empty parents are
/// excluded; `getLayerOffsets(k)[i+1] - getLayerOffsets(k)[i] == 0`
/// distinguishes empty from null at REPEATED layers (validity carries the
/// null bit).
///
/// **This API is [Experimental]:** the shape of the batch accessors and
/// layer representation may change in future releases without prior
/// deprecation.
@Experimental
public class ColumnReader implements AutoCloseable {

    static final int DEFAULT_BATCH_SIZE = 262_144;

    private final ColumnSchema column;
    private final boolean nested;
    private final NestedLevelComputer.Layers layers;
    private final BatchExchange<BatchExchange.Batch> flatBuffer;
    private final BatchExchange<NestedBatch> nestedBuffer;
    private final AutoCloseable columnWorker;
    private final RowGroupIterator rowGroupIterator;

    // Current batch state (flat uses BatchExchange.Batch, nested uses NestedBatch)
    private BatchExchange.Batch currentFlatBatch;
    private NestedBatch currentNestedBatch;
    private int recordCount;
    private boolean exhausted;

    // Real-items-only view for nested batches, computed lazily and cached per
    // batch. Invalidated in `nextBatch()`.
    private NestedLevelComputer.RealView currentRealView;
    private boolean realViewComputed;

    // Cached real-items-only typed leaf arrays for nested batches. Allocated
    // on first access and invalidated in `nextBatch()`.
    private Object cachedRealValues;
    private byte[] cachedRealBinaryBytes;
    private int[] cachedRealBinaryOffsets;
    private byte[][] cachedBinaries;
    private String[] cachedStrings;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    @SuppressWarnings("unchecked")
    private ColumnReader(ColumnSchema column, boolean nested,
                         NestedLevelComputer.Layers layers,
                         BatchExchange<?> buffer, AutoCloseable columnWorker,
                         RowGroupIterator rowGroupIterator) {
        this.column = column;
        this.nested = nested;
        this.layers = layers;
        this.flatBuffer = nested ? null : (BatchExchange<BatchExchange.Batch>) buffer;
        this.nestedBuffer = nested ? (BatchExchange<NestedBatch>) buffer : null;
        this.columnWorker = columnWorker;
        this.rowGroupIterator = rowGroupIterator;
    }

    static ColumnReader forFlat(ColumnSchema column, BatchExchange<BatchExchange.Batch> flatBuffer,
                                AutoCloseable columnWorker, RowGroupIterator rowGroupIterator) {
        return new ColumnReader(column, false,
                NestedLevelComputer.Layers.of(new LayerKind[0], new int[0]),
                flatBuffer, columnWorker, rowGroupIterator);
    }

    static ColumnReader forNested(ColumnSchema column, NestedLevelComputer.Layers layers,
                                  BatchExchange<NestedBatch> nestedBuffer,
                                  AutoCloseable columnWorker, RowGroupIterator rowGroupIterator) {
        return new ColumnReader(column, true, layers, nestedBuffer, columnWorker, rowGroupIterator);
    }

    // ==================== Batch Iteration ====================

    /// Advance to the next batch.
    ///
    /// **Multi-column alignment.** Every [ColumnReader] over the same file produces
    /// batches at the same row boundaries — call `nextBatch()` on each in turn and they
    /// will report identical [#getRecordCount()]s for the matching batch. This holds
    /// because the per-column drain workers all use the same internal batch capacity and
    /// every reader observes the same total row count per row group.
    ///
    /// Consumers reading multiple columns in lockstep should generally prefer
    /// [ColumnReaders] (obtained from
    /// [ParquetFileReader#buildColumnReaders(dev.hardwood.schema.ColumnProjection)]),
    /// which shares a single [dev.hardwood.internal.reader.RowGroupIterator] across all
    /// columns and exposes a single coordinated [ColumnReaders#nextBatch()] that drives
    /// every reader and validates alignment in one call.
    ///
    /// @return true if a batch is available, false if exhausted
    public boolean nextBatch() {
        if (exhausted) {
            return false;
        }

        if (nested) {
            NestedBatch batch = pollNestedBatch();
            if (batch == null || batch.recordCount == 0) {
                nestedBuffer.checkError();
                exhausted = true;
                currentFlatBatch = null;
                currentNestedBatch = null;
                return false;
            }
            currentNestedBatch = batch;
            currentFlatBatch = null;
            recordCount = batch.recordCount;
            currentFileName = batch.fileName;
        }
        else {
            BatchExchange.Batch batch = pollFlatBatch();
            if (batch == null || batch.recordCount == 0) {
                flatBuffer.checkError();
                exhausted = true;
                currentFlatBatch = null;
                currentNestedBatch = null;
                return false;
            }
            currentFlatBatch = batch;
            currentNestedBatch = null;
            recordCount = batch.recordCount;
            currentFileName = batch.fileName;
        }

        // Invalidate per-batch caches.
        realViewComputed = false;
        currentRealView = null;
        cachedRealValues = null;
        cachedRealBinaryBytes = null;
        cachedRealBinaryOffsets = null;
        cachedBinaries = null;
        cachedStrings = null;

        return true;
    }

    private BatchExchange.Batch pollFlatBatch() {
        try {
            return flatBuffer.poll();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private NestedBatch pollNestedBatch() {
        try {
            return nestedBuffer.poll();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /// Number of top-level records in the current batch.
    public int getRecordCount() {
        checkBatchAvailable();
        return recordCount;
    }

    /// Total number of leaf values in the current batch — sized to real items
    /// only (phantom slots from null/empty parents are excluded). For flat
    /// columns this equals [#getRecordCount()].
    public int getValueCount() {
        checkBatchAvailable();
        if (!nested) {
            return recordCount;
        }
        return ensureRealView().valueCount();
    }

    // ==================== Layer Metadata ====================

    /// Number of layers in this column's schema chain. `0` for a flat column.
    /// Stable for the lifetime of this reader and safe to call before the
    /// first [#nextBatch()] — useful for sizing consumer-side buffers.
    public int getLayerCount() {
        return layers.count();
    }

    /// Layer kind at `layer`. Stable for the lifetime of this reader and
    /// safe to call before the first [#nextBatch()].
    public LayerKind getLayerKind(int layer) {
        checkLayer(layer);
        return layers.kinds()[layer];
    }

    // ==================== Per-Layer Buffers ====================

    /// Validity at `layer`. Returns [Validity#NO_NULLS] when no item at
    /// that layer is null in the current batch (the sparse fast path);
    /// otherwise returns a wrapper over the per-item null bitmap.
    public Validity getLayerValidity(int layer) {
        checkBatchAvailable();
        checkLayer(layer);
        return Validity.of(ensureRealView().layerValidity()[layer]);
    }

    /// Offsets at `layer`. Length == count(layer) + 1, with `offsets[count(layer)]`
    /// equal to count(layer + 1) (or to [#getValueCount()] for the innermost layer).
    /// `offsets[i+1] - offsets[i] == 0` denotes an empty list/map.
    ///
    /// @throws IllegalStateException if `layer` is outside `[0, getLayerCount())`
    ///         or if the layer is not [LayerKind#REPEATED]
    public int[] getLayerOffsets(int layer) {
        checkBatchAvailable();
        checkLayer(layer);
        if (layers.kinds()[layer] != LayerKind.REPEATED) {
            throw new IllegalStateException(prefix() + "Layer " + layer
                    + " is " + layers.kinds()[layer] + ", not REPEATED");
        }
        return ensureRealView().layerOffsets()[layer];
    }

    // ==================== Leaf Validity ====================

    /// Validity over the leaf-value array, indexed `0..getValueCount()`.
    /// Returns [Validity#NO_NULLS] when no leaf in the current batch is
    /// null.
    public Validity getLeafValidity() {
        checkBatchAvailable();
        BitSet raw = nested
                ? ensureRealView().leafValidity()
                : currentFlatBatch.validity;
        return Validity.of(raw);
    }

    // ==================== Typed Value Arrays ====================

    public int[] getInts() {
        checkBatchAvailable();
        Object values = realLeafValues();
        if (!(values instanceof int[] a)) {
            throw typeMismatch("int");
        }
        return a;
    }

    public long[] getLongs() {
        checkBatchAvailable();
        Object values = realLeafValues();
        if (!(values instanceof long[] a)) {
            throw typeMismatch("long");
        }
        return a;
    }

    public float[] getFloats() {
        checkBatchAvailable();
        Object values = realLeafValues();
        if (!(values instanceof float[] a)) {
            throw typeMismatch("float");
        }
        return a;
    }

    public double[] getDoubles() {
        checkBatchAvailable();
        Object values = realLeafValues();
        if (!(values instanceof double[] a)) {
            throw typeMismatch("double");
        }
        return a;
    }

    public boolean[] getBooleans() {
        checkBatchAvailable();
        Object values = realLeafValues();
        if (!(values instanceof boolean[] a)) {
            throw typeMismatch("boolean");
        }
        return a;
    }

    // ==================== Varlength Leaf Buffers ====================

    /// Backing byte buffer for a varlength leaf. Capacity-sized: only bytes
    /// in the half-open range `[0, getBinaryOffsets()[getValueCount()])` are
    /// valid; bytes beyond that position are unspecified.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public byte[] getBinaryValues() {
        checkBatchAvailable();
        ensureRealBinary();
        return cachedRealBinaryBytes;
    }

    /// Sentinel-suffixed offsets into [#getBinaryValues()]. Length ==
    /// `getValueCount() + 1`; the byte length of value `i` is
    /// `offsets[i+1] - offsets[i]`. For `FIXED_LEN_BYTE_ARRAY` columns the
    /// offsets are trivially `i * width`.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public int[] getBinaryOffsets() {
        checkBatchAvailable();
        ensureRealBinary();
        return cachedRealBinaryOffsets;
    }

    // ==================== Convenience Accessors ====================

    /// Materialises one `byte[]` per leaf value, copying out of the binary
    /// buffer. Returns `null` at indexes where [#getLeafValidity()] is unset.
    /// Allocates one byte array per leaf — hot loops should consult
    /// [#getBinaryValues()] + [#getBinaryOffsets()] directly.
    ///
    /// The returned array has length [#getValueCount()] — i.e. the **real
    /// leaf count**, not [#getRecordCount()]. For a flat column the two
    /// coincide; for `list<binary>` and similar nested chains they
    /// differ, and lookups must go through the appropriate layer offsets
    /// rather than indexing by record.
    public byte[][] getBinaries() {
        checkBatchAvailable();
        if (cachedBinaries != null) {
            return cachedBinaries;
        }
        ensureRealBinary();
        int n = getValueCount();
        Validity validity = getLeafValidity();
        byte[][] result = new byte[n][];
        for (int i = 0; i < n; i++) {
            if (validity.isNull(i)) {
                result[i] = null;
                continue;
            }
            int start = cachedRealBinaryOffsets[i];
            int len = cachedRealBinaryOffsets[i + 1] - start;
            byte[] copy = new byte[len];
            System.arraycopy(cachedRealBinaryBytes, start, copy, 0, len);
            result[i] = copy;
        }
        cachedBinaries = result;
        return result;
    }

    /// Convenience: materialises one `String` per leaf value by UTF-8 decoding
    /// the slice of [#getBinaryValues()] for each entry. Returns `null` at
    /// indexes where [#getLeafValidity()] is unset. BSON columns are not
    /// string-decoded; use [#getBinaries()] / [#getBinaryValues()] for those.
    ///
    /// The returned array has length [#getValueCount()] — i.e. the **real
    /// leaf count**, not [#getRecordCount()]. For a flat column the two
    /// coincide; for `list<string>` and similar nested chains they
    /// differ, and lookups must go through the appropriate layer offsets
    /// rather than indexing by record.
    public String[] getStrings() {
        checkBatchAvailable();
        if (cachedStrings != null) {
            return cachedStrings;
        }
        ensureRealBinary();
        int n = getValueCount();
        Validity validity = getLeafValidity();
        String[] result = new String[n];
        for (int i = 0; i < n; i++) {
            if (validity.isNull(i)) {
                result[i] = null;
                continue;
            }
            int start = cachedRealBinaryOffsets[i];
            int len = cachedRealBinaryOffsets[i + 1] - start;
            result[i] = new String(cachedRealBinaryBytes, start, len, StandardCharsets.UTF_8);
        }
        cachedStrings = result;
        return result;
    }

    // ==================== Raw Levels (escape hatch) ====================

    /// Raw definition levels for the current batch. Returns `null` for flat
    /// columns; their validity is fully captured by [#getLeafValidity()].
    public int[] getDefinitionLevels() {
        checkBatchAvailable();
        if (!nested) {
            return null;
        }
        return currentNestedBatch.definitionLevels;
    }

    /// Raw repetition levels for the current batch. Returns `null` for columns
    /// whose `maxRepetitionLevel == 0`.
    public int[] getRepetitionLevels() {
        checkBatchAvailable();
        if (!nested) {
            return null;
        }
        if (column.maxRepetitionLevel() == 0) {
            return null;
        }
        return currentNestedBatch.repetitionLevels;
    }

    // ==================== Metadata ====================

    public ColumnSchema getColumnSchema() {
        return column;
    }

    @Override
    public void close() {
        if (columnWorker != null) {
            try {
                columnWorker.close();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to close column worker", e);
            }
        }
        if (rowGroupIterator != null) {
            rowGroupIterator.close();
        }
    }

    // ==================== Internal ====================

    private String prefix() {
        return ExceptionContext.filePrefix(currentFileName);
    }

    private NestedLevelComputer.RealView ensureRealView() {
        if (realViewComputed) {
            return currentRealView;
        }
        if (!nested) {
            throw new IllegalStateException(prefix() + "Real view not available for flat columns");
        }
        currentRealView = NestedLevelComputer.computeRealView(
                currentNestedBatch.definitionLevels,
                currentNestedBatch.repetitionLevels,
                currentNestedBatch.valueCount,
                currentNestedBatch.recordCount,
                column.maxDefinitionLevel(),
                layers);
        realViewComputed = true;
        return currentRealView;
    }

    /// Returns the leaf-values backing array sized to [#getValueCount()].
    /// For flat columns this is the underlying batch array; for nested
    /// columns it's either pass-through (no compaction needed when the chain
    /// has no `REPEATED` layers) or a freshly compacted typed array, cached
    /// per batch.
    private Object realLeafValues() {
        if (!nested) {
            return currentFlatBatch.values;
        }
        if (cachedRealValues != null) {
            return cachedRealValues;
        }
        NestedLevelComputer.RealView rv = ensureRealView();
        Object raw = currentNestedBatch.values;
        int[] map = rv.realToRawLeaf();
        if (map == null) {
            cachedRealValues = raw;   // pass-through: STRUCT-only chain
        }
        else {
            cachedRealValues = compactPrimitive(raw, map);
        }
        return cachedRealValues;
    }

    private static Object compactPrimitive(Object raw, int[] map) {
        int n = map.length;
        return switch (raw) {
            case int[] a -> {
                int[] out = new int[n];
                for (int i = 0; i < n; i++) out[i] = a[map[i]];
                yield out;
            }
            case long[] a -> {
                long[] out = new long[n];
                for (int i = 0; i < n; i++) out[i] = a[map[i]];
                yield out;
            }
            case float[] a -> {
                float[] out = new float[n];
                for (int i = 0; i < n; i++) out[i] = a[map[i]];
                yield out;
            }
            case double[] a -> {
                double[] out = new double[n];
                for (int i = 0; i < n; i++) out[i] = a[map[i]];
                yield out;
            }
            case boolean[] a -> {
                boolean[] out = new boolean[n];
                for (int i = 0; i < n; i++) out[i] = a[map[i]];
                yield out;
            }
            case BinaryBatchValues bbv -> compactBinary(bbv, map);
            default -> throw new IllegalStateException("Unexpected leaf array type: " + raw.getClass());
        };
    }

    private static BinaryBatchValues compactBinary(BinaryBatchValues raw, int[] map) {
        int n = map.length;
        int totalBytes = 0;
        int[] outOffsets = new int[n + 1];
        for (int i = 0; i < n; i++) {
            int rawIdx = map[i];
            int len = raw.offsets[rawIdx + 1] - raw.offsets[rawIdx];
            outOffsets[i] = totalBytes;
            totalBytes += len;
        }
        outOffsets[n] = totalBytes;
        byte[] outBytes = new byte[totalBytes];
        for (int i = 0; i < n; i++) {
            int rawIdx = map[i];
            int rawStart = raw.offsets[rawIdx];
            int len = raw.offsets[rawIdx + 1] - rawStart;
            System.arraycopy(raw.bytes, rawStart, outBytes, outOffsets[i], len);
        }
        return new BinaryBatchValues(outBytes, outOffsets);
    }

    private void ensureRealBinary() {
        if (cachedRealBinaryBytes != null) {
            return;
        }
        Object raw;
        BinaryBatchValues bbv;
        if (!nested) {
            raw = currentFlatBatch.values;
            if (!(raw instanceof BinaryBatchValues)) {
                throw typeMismatch("byte[]");
            }
            bbv = (BinaryBatchValues) raw;
            cachedRealBinaryBytes = bbv.bytes;
            cachedRealBinaryOffsets = trimOffsetsToLeafCount(bbv.offsets, recordCount);
            return;
        }
        // Nested: realLeafValues() may compact, may pass through.
        Object leaf = realLeafValues();
        if (!(leaf instanceof BinaryBatchValues compacted)) {
            throw typeMismatch("byte[]");
        }
        cachedRealBinaryBytes = compacted.bytes;
        cachedRealBinaryOffsets = trimOffsetsToLeafCount(compacted.offsets, getValueCount());
    }

    /// The per-batch [BinaryBatchValues] is sized to the worker's batch
    /// capacity; only the prefix `[0, valueCount + 1]` of the offsets is
    /// meaningful at the public-API surface. Trim if needed (the bytes
    /// buffer itself stays capacity-sized — the public contract documents
    /// it that way).
    private static int[] trimOffsetsToLeafCount(int[] offsets, int leafCount) {
        if (offsets.length == leafCount + 1) {
            return offsets;
        }
        return Arrays.copyOf(offsets, leafCount + 1);
    }

    private void checkBatchAvailable() {
        if (currentFlatBatch == null && currentNestedBatch == null) {
            throw new IllegalStateException(prefix() + "No batch available. Call nextBatch() first.");
        }
    }

    private void checkLayer(int layer) {
        if (layer < 0 || layer >= layers.count()) {
            throw new IllegalStateException(prefix()
                    + "Layer " + layer + " out of range [0, " + layers.count() + ")");
        }
    }

    private IllegalStateException typeMismatch(String expected) {
        return new IllegalStateException(prefix()
                + "Column '" + column.name() + "' is " + column.type() + ", not " + expected);
    }

    // ==================== Factory ====================

    /// Create a ColumnReader for a named column with optional page-level
    /// filtering. `filter` may be `null`.
    static ColumnReader create(String columnName, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context, ResolvedPredicate filter) {
        return create(schema.getColumn(columnName), schema, inputFile, rowGroups, context, filter);
    }

    /// Create a ColumnReader for a column by index with optional page-level
    /// filtering. `filter` may be `null`.
    static ColumnReader create(int columnIndex, FileSchema schema,
                               InputFile inputFile, List<RowGroup> rowGroups,
                               HardwoodContextImpl context, ResolvedPredicate filter) {
        return create(schema.getColumn(columnIndex), schema, inputFile, rowGroups, context, filter);
    }

    private static ColumnReader create(ColumnSchema columnSchema, FileSchema schema,
                                       InputFile inputFile, List<RowGroup> rowGroups,
                                       HardwoodContextImpl context, ResolvedPredicate filter) {
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema,
                ColumnProjection.columns(columnSchema.fieldPath().toString()));

        RowGroupIterator rowGroupIterator = new RowGroupIterator(
                List.of(inputFile), context, 0);
        rowGroupIterator.setFirstFile(schema, rowGroups);
        rowGroupIterator.initialize(projectedSchema, filter);

        return createFromIterator(columnSchema, schema, rowGroupIterator, context, 0, rowGroupIterator);
    }

    /// Creates a ColumnReader from a pre-configured RowGroupIterator.
    /// Used by both single-file and multi-file paths.
    ///
    /// Routing rule: any column whose schema chain contributes at least one
    /// `STRUCT` or `REPEATED` layer is driven by [NestedColumnWorker] (def
    /// levels are needed to compute per-layer validity); only the
    /// no-layers-at-all case takes the [FlatColumnWorker] fast path.
    static ColumnReader createFromIterator(ColumnSchema columnSchema, FileSchema schema,
                                           RowGroupIterator rowGroupIterator,
                                           HardwoodContextImpl context,
                                           int projectedColumnIndex,
                                           RowGroupIterator ownedIterator) {
        NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(
                schema.getRootNode(), columnSchema.columnIndex());
        boolean nested = layers.count() > 0 || columnSchema.maxRepetitionLevel() > 0;

        PageSource pageSource = new PageSource(rowGroupIterator, projectedColumnIndex);

        if (nested) {
            BatchExchange<NestedBatch> nestedBuf = BatchExchange.detaching(
                    columnSchema.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(columnSchema, DEFAULT_BATCH_SIZE);
                        return b;
                    });
            NestedColumnWorker nestedWorker = new NestedColumnWorker(
                    pageSource, nestedBuf, columnSchema, DEFAULT_BATCH_SIZE,
                    context.decompressorFactory(), context.executor(), 0,
                    layers);
            nestedWorker.start();
            return ColumnReader.forNested(columnSchema, layers, nestedBuf, nestedWorker, ownedIterator);
        }
        else {
            BatchExchange<BatchExchange.Batch> flatBuf = BatchExchange.detaching(
                    columnSchema.name(), () -> {
                        BatchExchange.Batch b = new BatchExchange.Batch();
                        b.values = BatchExchange.allocateArray(columnSchema, DEFAULT_BATCH_SIZE);
                        return b;
                    });
            FlatColumnWorker flatWorker = new FlatColumnWorker(
                    pageSource, flatBuf, columnSchema, DEFAULT_BATCH_SIZE,
                    context.decompressorFactory(), context.executor(), 0, null);
            flatWorker.start();
            return ColumnReader.forFlat(columnSchema, flatBuf, flatWorker, ownedIterator);
        }
    }
}
