/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import dev.hardwood.Experimental;
import dev.hardwood.Validity;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.BatchExchange;
import dev.hardwood.internal.reader.BinaryBatchValues;
import dev.hardwood.internal.reader.LeafCompaction;
import dev.hardwood.internal.reader.LogicalAccessorKind;
import dev.hardwood.internal.reader.NestedBatch;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

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
/// **Array ownership.** Every array and [Validity] handed back by an
/// accessor ([#getInts()], [#getLongs()], [#getLayerOffsets(int)],
/// [#getLeafValidity()], and the rest) belongs to the current batch and is
/// freshly allocated by the [#nextBatch()] call that produced it. A later
/// [#nextBatch()] never reuses or overwrites an array returned for an
/// earlier batch — so a returned array may be kept and read after the reader
/// has advanced, including handed off to another thread for processing. The
/// reader itself is still a single-threaded cursor: only one consumer thread
/// may call [#nextBatch()]. (The capacity-sizing note on [#getBinaryValues()]
/// is about array *length*, not reuse; that buffer is fresh per batch too.)
///
/// **This API is [Experimental]:** the shape of the batch accessors and
/// layer representation may change in future releases without prior
/// deprecation.
@Experimental
public class ColumnReader implements Closeable {

    private final ColumnScan scan;
    private final int payloadIndex;
    private final ColumnSchema column;
    private final boolean nested;
    private final NestedLevelComputer.Layers layers;

    /// The [ColumnScan#generation()] this view last adopted. When it equals the
    /// scan's, [#nextBatch()] advances the scan; when it is one behind, a sibling
    /// already did, and this view adopts that step; further behind, adopting would
    /// skip a step and is refused.
    private long consumedGeneration;

    // Current batch state, adopted from the payload cursor (flat uses
    // BatchExchange.Batch, nested uses NestedBatch)
    private BatchExchange.Batch currentFlatBatch;
    private NestedBatch currentNestedBatch;
    private int recordCount;

    // Real-items-only view for nested batches, computed lazily and cached per
    // batch. Invalidated when a step is adopted.
    private NestedLevelComputer.RealView currentRealView;
    private boolean realViewComputed;

    // Cached real-items-only typed leaf arrays for nested batches. Allocated
    // on first access and invalidated when a step is adopted.
    private Object cachedRealValues;
    private byte[] cachedRealBinaryBytes;
    private int[] cachedRealBinaryOffsets;
    private byte[][] cachedBinaries;
    private String[] cachedStrings;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    /// A view of payload column `payloadIndex` of `scan`.
    ColumnReader(ColumnScan scan, int payloadIndex, FileSchema schema, ColumnSchema column) {
        this.scan = scan;
        this.payloadIndex = payloadIndex;
        this.column = column;
        this.layers = NestedLevelComputer.computeLayers(schema.getRootNode(), column.columnIndex());
        this.nested = ColumnCursor.isNested(layers, column);
    }

    // ==================== Batch Iteration ====================

    /// Advance to the next batch.
    ///
    /// **One reader advances only itself.** Readers built separately — each from its own
    /// [ParquetFileReader#buildColumnReader(String)] — are independent cursors, and the
    /// batch a reader hands out is sized for the columns *it* reads. A batch of a
    /// `BYTE_ARRAY` column therefore covers fewer rows than a batch of an `INT32` one over
    /// the same file, and a filtered reader is sized for its predicate's columns as well as
    /// its own. Two such readers reach different rows on their nth batch.
    ///
    /// So do not pair them: `while (a.nextBatch() & b.nextBatch())` ends when whichever
    /// reader has the larger batches runs out first, and the values it took from each on any
    /// given turn are from different rows. Neither reader reports anything wrong, because
    /// neither is wrong — each is a complete, correct read of its own column.
    ///
    /// To read several columns of the same rows, use [ColumnReaders], from
    /// [ParquetFileReader#buildColumnReaders(dev.hardwood.schema.ColumnProjection)]. Its
    /// readers share one decode pipeline and one batch size. [ColumnReaders#nextBatch()]
    /// advances the whole group in one call. Calling this method on each member in turn also
    /// moves the group once per turn: the first member called advances the group, and each
    /// other member takes up that same batch. A member that the group has moved on by more
    /// than one batch since it last took one up would skip a batch, so this method throws
    /// [IllegalStateException] for it instead.
    ///
    /// @return true if a batch is available, false if exhausted
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say: a footer or a page index that will not parse, a
    ///         dictionary page the metadata places outside its column chunk, a page whose
    ///         checksum fails, values that do not decode under the encoding declared for
    ///         them. In a multi-file read this covers a later file that is not Parquet at
    ///         all, or whose schema cannot be reconciled with the first file's
    /// @throws IllegalStateException if this reader, or any reader of its group, was closed, or
    ///         if the group moved on by more than one batch since this reader last took one up
    public boolean nextBatch() throws IOException {
        scan.requireOpen();
        long behind = scan.generation() - consumedGeneration;
        if (behind == 0) {
            scan.advance();
        }
        else if (behind > 1) {
            throw new IllegalStateException(prefix() + "ColumnReader '" + column.name()
                    + "' would skip " + (behind - 1) + " batch(es): other readers of its group"
                    + " advanced the group past them. Call nextBatch() on every reader of the"
                    + " group in turn, or use ColumnReaders.nextBatch()");
        }
        return adoptCurrentStep();
    }

    /// Takes up the scan's current step: points this view at the payload cursor's
    /// current batch, or at none once the scan is exhausted, and drops the
    /// per-batch caches of the previous step.
    ///
    /// @return whether the step holds a batch
    boolean adoptCurrentStep() {
        consumedGeneration = scan.generation();
        invalidatePerBatchCaches();
        if (!scan.hasBatch()) {
            currentFlatBatch = null;
            currentNestedBatch = null;
            return false;
        }
        ColumnCursor cursor = scan.cursor(payloadIndex);
        currentFlatBatch = cursor.flatBatch();
        currentNestedBatch = cursor.nestedBatch();
        recordCount = cursor.recordCount();
        currentFileName = cursor.fileName();
        return true;
    }

    /// Clears the lazily-computed, per-batch derived state (nested real view and
    /// the materialised binary/string views) so the accessors recompute against
    /// the batch now current.
    private void invalidatePerBatchCaches() {
        realViewComputed = false;
        currentRealView = null;
        cachedRealValues = null;
        cachedRealBinaryBytes = null;
        cachedRealBinaryOffsets = null;
        cachedBinaries = null;
        cachedStrings = null;
    }

    /// Number of top-level records in the current batch.
    public int getRecordCount() {
        checkBatchAvailable();
        return recordCount;
    }

    /// Total number of leaf values in the current batch — sized to real items
    /// only (phantom slots from null/empty parents are excluded). For flat
    /// columns this equals [#getRecordCount()]. For a repeated column it is not
    /// bounded by the configured batch size — the batch size caps records, not
    /// leaf values — so a batch may carry more leaf values than records.
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
        long[] raw = nested
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
    /// indexes where [#getLeafValidity()] is unset.
    ///
    /// The column has to hold text: a `BYTE_ARRAY` annotated `STRING`, `ENUM` or
    /// `JSON`, or one carrying no annotation. Every other binary column — a
    /// `DECIMAL`, a `UUID`, a `BSON`, an `INT96` — stores bytes that stand for
    /// something other than characters, and reading them as text is refused with
    /// an `IllegalArgumentException`; use [#getBinaries()] / [#getBinaryValues()]
    /// for those.
    ///
    /// The returned array has length [#getValueCount()] — i.e. the **real
    /// leaf count**, not [#getRecordCount()]. For a flat column the two
    /// coincide; for `list<string>` and similar nested chains they
    /// differ, and lookups must go through the appropriate layer offsets
    /// rather than indexing by record.
    ///
    /// @throws IllegalArgumentException if the column does not hold text
    public String[] getStrings() {
        checkBatchAvailable();
        if (cachedStrings != null) {
            return cachedStrings;
        }
        LogicalAccessorKind.requireText(currentFileName, column.name(), column.type(), column.logicalType());
        BinaryBatchValues bbv = realLeafBinary();
        int n = getValueCount();
        Validity validity = getLeafValidity();
        String[] result = new String[n];
        for (int i = 0; i < n; i++) {
            // stringAt interns via the chunk dictionary when the batch kept it,
            // else decodes from bytes; nulls must be guarded (never interned).
            result[i] = validity.isNull(i) ? null : bbv.stringAt(i);
        }
        cachedStrings = result;
        return result;
    }

    // ==================== Metadata ====================

    public ColumnSchema getColumnSchema() {
        return column;
    }

    /// Releases the resources held by this reader. Closing any reader of a
    /// [ColumnReaders] group closes the whole group, after which [#nextBatch()] on any
    /// of its readers throws [IllegalStateException]. Idempotent: calling it more
    /// than once has no further effect.
    @Override
    public void close() throws IOException {
        scan.close();
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
        if (currentNestedBatch.realView != null) {
            // Computed on the drain (real-items ColumnReader path).
            currentRealView = currentNestedBatch.realView;
        }
        else {
            // A batch derived by consumer-side record selection carries no drain
            // view; build it from the sliced levels.
            currentRealView = currentNestedBatch.fixedListK > 0
                    ? fixedListRealView(currentNestedBatch)
                    : NestedLevelComputer.computeRealView(
                            currentNestedBatch.definitionLevels,
                            currentNestedBatch.repetitionLevels,
                            currentNestedBatch.valueCount,
                            currentNestedBatch.recordCount,
                            column.maxDefinitionLevel(),
                            layers);
        }
        realViewComputed = true;
        return currentRealView;
    }

    /// Real-items view for a fixed-width fixed-size-list batch: all leaves present, so
    /// the single `REPEATED` layer carries arithmetic offsets and every validity
    /// is `null`; `realToRawLeaf` is `null` because the dense value stream is
    /// already the real-items stream (identity), so leaf values pass through
    /// without compaction.
    private NestedLevelComputer.RealView fixedListRealView(NestedBatch batch) {
        int k = batch.fixedListK;
        int recordCount = batch.recordCount;
        int layerCount = layers.count();
        return new NestedLevelComputer.RealView(
                NestedLevelComputer.fixedListLayerOffsets(k, recordCount, layers),
                new long[layerCount][], null,
                Math.multiplyExact(recordCount, k), null);
    }

    /// Returns the leaf-values backing array sized to [#getValueCount()].
    /// For flat columns this is the underlying batch array. For nested columns
    /// it is, in order: the drain's pre-compacted `realValues` when present;
    /// otherwise pass-through of the batch values when no compaction is needed
    /// (no `REPEATED` layer, or an all-present batch with no phantom positions);
    /// otherwise a freshly compacted typed array (batches derived by record
    /// selection). Cached per batch.
    private Object realLeafValues() {
        if (cachedRealValues != null) {
            return cachedRealValues;
        }
        cachedRealValues = trimToValueCount(rawLeafValues());
        return cachedRealValues;
    }

    /// The untrimmed leaf-values backing store. For flat columns this is the
    /// underlying batch array. For nested columns it is, in order: the drain's
    /// pre-compacted `realValues` when present; otherwise pass-through of the
    /// batch values when no compaction is needed (no `REPEATED` layer, or an
    /// all-present batch with no phantom positions); otherwise a freshly
    /// compacted typed array (batches derived by record selection).

    private Object rawLeafValues() {
        if (!nested) {
            return currentFlatBatch.values;
        }
        NestedBatch batch = currentNestedBatch;
        if (batch.realValues != null) {
            return batch.realValues;               // pre-compacted on the drain
        }
        int[] map = ensureRealView().realToRawLeaf();
        return map == null
                ? batch.values                     // pass-through, no compaction
                : LeafCompaction.compact(batch.values, map);
    }

    /// Trims a primitive leaf array to exactly [#getValueCount()] entries so the
    /// capacity tail — stale or zero-filled values past the batch's real leaf
    /// count — is never exposed through the typed accessors. Already-exact arrays
    /// and non-array payloads ([BinaryBatchValues], whose offsets are trimmed on
    /// their own path) pass through untouched, so only the final short batch of a
    /// column ever pays a copy.
    private Object trimToValueCount(Object values) {
        int n = getValueCount();
        return switch (values) {
            case int[] a -> a.length == n ? a : Arrays.copyOf(a, n);
            case long[] a -> a.length == n ? a : Arrays.copyOf(a, n);
            case float[] a -> a.length == n ? a : Arrays.copyOf(a, n);
            case double[] a -> a.length == n ? a : Arrays.copyOf(a, n);
            case boolean[] a -> a.length == n ? a : Arrays.copyOf(a, n);
            default -> values;
        };
    }

    private void ensureRealBinary() {
        if (cachedRealBinaryBytes != null) {
            return;
        }
        BinaryBatchValues bbv = realLeafBinary();
        int leafCount = nested ? getValueCount() : recordCount;
        cachedRealBinaryBytes = bbv.bytes;
        cachedRealBinaryOffsets = trimOffsetsToLeafCount(bbv.offsets, leafCount);
    }

    /// The current batch's varlength leaf values, after any nested compaction.
    private BinaryBatchValues realLeafBinary() {
        Object leaf = nested ? realLeafValues() : currentFlatBatch.values;
        if (!(leaf instanceof BinaryBatchValues bbv)) {
            throw typeMismatch("byte[]");
        }
        return bbv;
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
}
