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
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Batch-oriented column reader for reading a single column across all row groups.
///
/// Exposes a column's batch as typed leaf values plus a layer-model view of
/// the schema chain between root and leaf. Each node along the chain
/// contributes zero or one [LayerKind] layer:
///
/// - `OPTIONAL` group → [LayerKind#STRUCT]
/// - `LIST` / `MAP`-annotated group → [LayerKind#REPEATED]
/// - unannotated `repeated` field (group or primitive leaf) outside a
///   `LIST` / `MAP` scaffold → [LayerKind#REPEATED]
/// - `REQUIRED` group / synthetic LIST scaffolding → no layer
///
/// Layers are numbered `0..getLayerCount() - 1` outermost-to-innermost. A flat
/// column (no enclosing nullable groups, no repetition) reports
/// `getLayerCount() == 0` and is queried solely through [#getLeafValidity()]
/// plus the typed value accessors.
///
/// **Polarity:** validity bitmaps carry **set bit = present** semantics.
/// [Validity#NO_NULLS] is the sparse representation of "every item at that
/// scope is present in the current batch."
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
/// freshly allocated by the call that produced it — [#nextBatch()], or
/// [ColumnReaders#nextBatch()] for a reader of a group — except
/// [Validity#NO_NULLS], a shared immutable singleton. [#getBinaryDictionary()]
/// returns an object whose entries never change, the same for every batch drawn
/// from one dictionary. A later advance never reuses or overwrites an array
/// returned for an earlier batch — so a
/// returned array may be kept and read after the reader has advanced,
/// including handed off to another thread for processing. The reader itself
/// is still a single-threaded cursor: only one consumer thread may advance it.
/// (The note on [#getBinaryValues()] that the array is not sized to the values is
/// about its *length*, not reuse; that buffer is fresh per batch too.) Treat returned
/// arrays as read-only: the accessors of one batch share arrays (the dictionary ids,
/// for instance, back [#getStrings()]'s reuse of `String`s), so changing one can
/// change what another accessor of the batch returns.
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

    /// Whether this view belongs to a [ColumnReaders] group, which advances and closes the
    /// scan; otherwise this view is the scan's only one and does both itself.
    private final boolean groupMember;

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
    private int[] cachedRealBinaryStarts;
    private int[] cachedRealBinaryEnds;
    private boolean dictionaryResolved;
    private int[] cachedDictionaryIds;
    private BinaryDictionary cachedBinaryDictionary;
    /// The last dictionary handed out, kept across batches so every batch drawn from the same
    /// chunk dictionary returns the same instance.
    private BinaryDictionary lastBinaryDictionary;
    private byte[][] cachedBinaries;
    private String[] cachedStrings;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;

    /// A view of payload column `payloadIndex` of `scan`, advanced by a [ColumnReaders] group
    /// when `groupMember` is set and by its own [#nextBatch()] otherwise.
    ColumnReader(ColumnScan scan, int payloadIndex, FileSchema schema, ColumnSchema column, boolean groupMember) {
        this.scan = scan;
        this.payloadIndex = payloadIndex;
        this.groupMember = groupMember;
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
    /// readers share one decode pipeline and one batch size, and [ColumnReaders#nextBatch()]
    /// advances all of them. A reader obtained from a [ColumnReaders] shows the group's
    /// current batch and does not advance on its own: this method throws
    /// [IllegalStateException] for it.
    ///
    /// @return true if a batch is available, false if exhausted
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say: a footer or a page index that will not parse, a
    ///         dictionary page the metadata places outside its column chunk, a page whose
    ///         checksum fails, values that do not decode under the encoding declared for
    ///         them. In a multi-file read this covers a later file that is not Parquet at
    ///         all, or whose schema cannot be reconciled with the first file's
    /// @throws IllegalStateException if this reader was closed, or was obtained from a
    ///         [ColumnReaders]
    public boolean nextBatch() throws IOException {
        if (groupMember) {
            throw new IllegalStateException("ColumnReader '" + column.fieldPath()
                    + "' belongs to a ColumnReaders group: advance the group with ColumnReaders.nextBatch()");
        }
        scan.advance();
        return adoptCurrentStep();
    }

    /// Takes up the scan's current step: points this view at the payload cursor's
    /// current batch, or at none once the scan is exhausted, and drops the
    /// per-batch caches of the previous step.
    ///
    /// @return whether the step holds a batch
    boolean adoptCurrentStep() {
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
        cachedRealBinaryStarts = null;
        cachedRealBinaryEnds = null;
        dictionaryResolved = false;
        cachedDictionaryIds = null;
        cachedBinaryDictionary = null;
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

    /// The bytes a varlength leaf's values are read from. Value `i` occupies
    /// `[getBinaryStarts()[i], getBinaryEnds()[i])`.
    ///
    /// Values need not be contiguous or in value order, and several may cover the
    /// same bytes, as values of a dictionary-encoded column naming the same
    /// dictionary entry may. The array is not sized to the values; bytes outside
    /// every value's range are unspecified.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public byte[] getBinaryValues() {
        checkBatchAvailable();
        ensureRealBinary();
        return cachedRealBinaryBytes;
    }

    /// Start of each value in [#getBinaryValues()], inclusive. Length ==
    /// `getValueCount()`. A null value's range is empty.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public int[] getBinaryStarts() {
        checkBatchAvailable();
        ensureRealBinary();
        return cachedRealBinaryStarts;
    }

    /// End of each value in [#getBinaryValues()], exclusive. Length ==
    /// `getValueCount()`; the byte length of value `i` is `ends[i] - starts[i]`.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public int[] getBinaryEnds() {
        checkBatchAvailable();
        ensureRealBinary();
        return cachedRealBinaryEnds;
    }

    // ==================== Dictionary ====================

    /// Each value's entry in the batch's dictionary: value `i` is entry
    /// `getDictionaryIds()[i]` of [#getBinaryDictionary()], and `-1` at a null value.
    /// Length == `getValueCount()`.
    ///
    /// Returns `null` unless every non-null value of the batch was decoded from the
    /// column chunk's dictionary: for every batch of a column without a dictionary, and,
    /// where a writer switched to plain encoding after its dictionary filled up, for the
    /// batch holding the switch and the rest of that row group. Read such a batch through
    /// the value accessors. A batch never draws on two dictionaries: a read that includes
    /// a binary column ends its batches at row-group boundaries. Dictionary ids are exposed for `BYTE_ARRAY`,
    /// `FIXED_LEN_BYTE_ARRAY` and `INT96` columns; for any other column this returns `null`.
    public int[] getDictionaryIds() {
        checkBatchAvailable();
        if (!hasBinaryLeaf()) {
            return null;
        }
        ensureDictionary();
        return cachedDictionaryIds;
    }

    /// The dictionary [#getDictionaryIds()] refers to, or `null` when the ids are
    /// `null`. Every batch drawn from the same dictionary returns the same instance, so
    /// comparing it with the previous batch's (`!=`) tells when the dictionary changed.
    ///
    /// @throws IllegalStateException for non-byte-array leaves
    public BinaryDictionary getBinaryDictionary() {
        checkBatchAvailable();
        ensureDictionary();
        return cachedBinaryDictionary;
    }

    // ==================== Convenience Accessors ====================

    /// Materialises one `byte[]` per leaf value, copying out of the binary
    /// buffer. Returns `null` at indexes where [#getLeafValidity()] is unset.
    /// Allocates one byte array per leaf — hot loops should consult
    /// [#getBinaryValues()] with [#getBinaryStarts()] and [#getBinaryEnds()] directly.
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
            result[i] = Arrays.copyOfRange(cachedRealBinaryBytes, cachedRealBinaryStarts[i], cachedRealBinaryEnds[i]);
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

    /// Releases the resources held by this reader. Idempotent: calling it more than once has
    /// no further effect. A reader obtained from a [ColumnReaders] holds no resources of its
    /// own, and closing it has no effect; [ColumnReaders#close()] releases the group.
    @Override
    public void close() throws IOException {
        if (!groupMember) {
            scan.close();
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
    /// and non-array payloads ([BinaryBatchValues], whose views are trimmed on
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
        cachedRealBinaryStarts = trimToLeafCount(bbv.starts, leafCount);
        cachedRealBinaryEnds = trimToLeafCount(bbv.ends, leafCount);
    }

    /// Resolves the dictionary view of the current batch: ids only when every non-null
    /// value has one.
    private boolean hasBinaryLeaf() {
        PhysicalType type = column.type();
        return type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY
                || type == PhysicalType.INT96;
    }

    private void ensureDictionary() {
        if (dictionaryResolved) {
            return;
        }
        BinaryBatchValues bbv = realLeafBinary();
        dictionaryResolved = true;
        if (bbv.dictionary == null) {
            return;
        }
        int leafCount = nested ? getValueCount() : recordCount;
        int[] ids = trimToLeafCount(bbv.dictIndices, leafCount);
        Validity validity = getLeafValidity();
        boolean anyNull = validity.hasNulls();
        for (int i = 0; i < leafCount; i++) {
            if (ids[i] < 0 && (!anyNull || validity.isNotNull(i))) {
                return;
            }
        }
        cachedDictionaryIds = ids;
        if (lastBinaryDictionary == null || !lastBinaryDictionary.wraps(bbv.dictionary)) {
            lastBinaryDictionary = new BinaryDictionary(bbv.dictionary, column, currentFileName);
        }
        cachedBinaryDictionary = lastBinaryDictionary;
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
    /// capacity; only the first `leafCount` views are meaningful at the
    /// public-API surface. Trim if needed (the bytes buffer itself is not
    /// trimmed — the public contract documents it that way).
    private static int[] trimToLeafCount(int[] views, int leafCount) {
        if (views.length == leafCount) {
            return views;
        }
        return Arrays.copyOf(views, leafCount);
    }

    private void checkBatchAvailable() {
        if (currentFlatBatch == null && currentNestedBatch == null) {
            throw new IllegalStateException(prefix() + "No batch available. Call "
                    + (groupMember ? "ColumnReaders.nextBatch()" : "nextBatch()") + " first.");
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
