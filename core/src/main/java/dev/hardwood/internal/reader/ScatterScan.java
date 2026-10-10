/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import dev.hardwood.InputFile;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// SPIKE: scatter decode. Decode tasks write straight into row-aligned batch arrays at offsets
/// the scheduler computes from page headers, so there is no per-column drain, no page-to-batch
/// copy, and one hand-off per batch for all columns.
///
/// Scope: one file, read one column chunk at a time through [InputFile#readRange]; flat `INT32`/`INT64`/`FLOAT`/`DOUBLE` columns (`PLAIN` or
/// dictionary) and dictionary-encoded binary columns (exposed as dictionary ids), V1 data pages. No filter, no row cap. Batches never span row groups.
public final class ScatterScan implements AutoCloseable {

    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final int RING = 1 << 14;
    private static final long PARK_NANOS = 1_000_000L;
    /// SPIKE experiment: `HW_RECYCLE=1` reuses the arrays of released batches for later batches
    /// instead of allocating fresh ones. A released batch's arrays are overwritten, so this is only
    /// valid for consumers that keep no array beyond the sink call / the next `next()`.
    private static final boolean RECYCLE = "1".equals(System.getenv("HW_RECYCLE"));
    /// SPIKE experiment: dictionary ids of a binary column without nulls are unpacked straight
    /// into the batch instead of into scratch followed by a copy; `HW_DIRECT_IDS=0` turns it off.
    private static final boolean DIRECT_IDS = !"0".equals(System.getenv("HW_DIRECT_IDS"));

    /// One row-aligned batch over every projected column.
    public static final class Batch {
        /// Rows in this batch; the arrays may be longer (a recycled batch keeps its capacity).
        public int rowCount;
        /// `long[]`, `double[]`, `int[]` or `float[]` per column.
        public final Object[] values;
        /// Presence bitmap per column (set bit = present); `null` for a required column.
        public final long[][] validity;
        /// The [Dictionary] a binary column's ids refer to; batches never span row groups.
        public final Object[] dictionaries;
        final AtomicInteger pendingRows;
        volatile boolean ready;
        private final int columnCount;

        /// Prepares a released batch for `rows` new rows: value arrays are fully overwritten by
        /// the decode tasks, so only the OR-merged validity bits and the dictionaries are cleared.
        void reset(int rows) {
            rowCount = rows;
            Arrays.fill(dictionaries, null);
            for (long[] bits : validity) {
                if (bits != null) {
                    Arrays.fill(bits, 0L);
                }
            }
            ready = false;
            pendingRows.set(rows * columnCount);
        }

        Batch(int rowCount, ColumnSchema[] columns) {
            this.rowCount = rowCount;
            this.columnCount = columns.length;
            this.values = new Object[columns.length];
            this.validity = new long[columns.length][];
            this.dictionaries = new Object[columns.length];
            for (int c = 0; c < columns.length; c++) {
                values[c] = switch (columns[c].type()) {
                    case INT64 -> new long[rowCount];
                    case DOUBLE -> new double[rowCount];
                    case INT32 -> new int[rowCount];
                    case FLOAT -> new float[rowCount];
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new int[rowCount];
                    default -> throw new UnsupportedOperationException("spike: " + columns[c].type());
                };
                if (columns[c].maxDefinitionLevel() > 0) {
                    validity[c] = new long[(rowCount + 63) >>> 6];
                }
            }
            this.pendingRows = new AtomicInteger(rowCount * columns.length);
        }
    }

    private static final class Scratch {
        int[] defs = new int[0];
        int[] indices = new int[0];

        int[] defs(int n) {
            if (defs.length < n) {
                defs = new int[n];
            }
            return defs;
        }

        int[] indices(int n) {
            if (indices.length < n) {
                indices = new int[n];
            }
            return indices;
        }
    }

    private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

    private final InputFile file;
    private final FileMetaData metaData;
    private final ColumnSchema[] columns;
    private final int[] fileColumns;
    private final int batchSize;
    private final int window;
    private final Executor executor;
    private final HardwoodContextImpl context;

    private final AtomicReferenceArray<Batch> ring = new AtomicReferenceArray<>(RING);
    /// Released full-capacity batches waiting for reuse (`RECYCLE` only).
    private final ConcurrentLinkedQueue<Batch> pool = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicInteger tasksInFlight = new AtomicInteger();
    /// Batches handed back: by [#next()] in row order, or by [#sink] as each completes.
    private final AtomicInteger released = new AtomicInteger();
    /// Ordered mode only: the consumer's position.
    private int consumed;
    /// Unordered mode: runs on the pool thread that completes a batch.
    private final Consumer<Batch> sink;
    private volatile int totalBatches = -1;
    private volatile boolean closed;
    private volatile Thread consumerThread;
    private volatile Thread schedulerThread;
    private Batch current;

    /// `file` must be open; it stays owned by the caller.
    public ScatterScan(InputFile file, FileMetaData metaData, List<String> projection, int batchSize,
                       int window, HardwoodContextImpl context) {
        this(file, metaData, projection, batchSize, window, context, null);
    }

    /// With a `sink`, every batch is handed to it on the pool thread that completes it, in no
    /// particular order, and released when it returns; [#awaitCompletion()] waits for the end.
    public ScatterScan(InputFile file, FileMetaData metaData, List<String> projection, int batchSize,
                       int window, HardwoodContextImpl context, Consumer<Batch> sink) {
        this.sink = sink;
        this.file = file;
        this.metaData = metaData;
        FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());
        this.columns = new ColumnSchema[projection.size()];
        this.fileColumns = new int[projection.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = schema.getColumn(projection.get(i));
            fileColumns[i] = columns[i].columnIndex();
            if (columns[i].maxRepetitionLevel() != 0) {
                throw new UnsupportedOperationException("spike: flat columns only");
            }
        }
        this.batchSize = batchSize;
        this.window = window;
        this.context = context;
        this.executor = context.executor();
        this.schedulerThread = Thread.ofVirtual().start(this::schedule);
    }

    /// Returns the next batch in row order, or `null` at the end. The previous batch is released.
    public Batch next() {
        if (current != null) {
            recycle(current);
            current = null;
            consumed++;
            released.incrementAndGet();
            LockSupport.unpark(schedulerThread);
        }
        int seq = consumed;
        consumerThread = Thread.currentThread();
        int spins = 0;
        while (true) {
            Throwable t = error.get();
            if (t != null) {
                throw new RuntimeException(t);
            }
            Batch b = ring.get(seq & (RING - 1));
            if (b != null && b.ready) {
                ring.set(seq & (RING - 1), null);
                current = b;
                return b;
            }
            int total = totalBatches;
            if (total >= 0 && seq >= total) {
                return null;
            }
            if (spins++ < 100) {
                Thread.onSpinWait();
            }
            else {
                LockSupport.parkNanos(PARK_NANOS);
            }
        }
    }

    // ==================== Scheduler ====================

    private final class ChunkCursor {
        final int column;
        final ColumnMetaData meta;
        final ByteBuffer chunk;
        int pos;
        long nextRow;
        Dictionary dictionary;

        ChunkCursor(int column, ColumnChunk cc) throws IOException {
            this.column = column;
            this.meta = cc.metaData();
            this.chunk = file.readRange(cc.chunkStartOffset(), Math.toIntExact(meta.totalCompressedSize()));
        }

        boolean exhausted(long rgRows) {
            return nextRow >= rgRows;
        }
    }

    /// SPIKE instrumentation: `System.nanoTime()` of the first occurrence of each event.
    public static final String[] EVENTS = {"scheduler start", "first dictionary parsed",
            "first data page submitted", "first task start", "first decompress done", "first decode done",
            "first batch complete", "first page codec"};
    public static final AtomicReferenceArray<Object> FIRST = new AtomicReferenceArray<>(EVENTS.length);

    private static void mark(int event) {
        FIRST.compareAndSet(event, null, System.nanoTime());
    }

    private void schedule() {
        mark(0);
        try {
            int batchBase = 0;
            for (RowGroup rg : metaData.rowGroups()) {
                long rgRows = rg.numRows();
                int rgBatches = Math.toIntExact((rgRows + batchSize - 1) / batchSize);
                ChunkCursor[] cursors = new ChunkCursor[columns.length];
                for (int c = 0; c < columns.length; c++) {
                    cursors[c] = new ChunkCursor(c, rg.columns().get(fileColumns[c]));
                }
                while (!closed) {
                    ChunkCursor next = null;
                    for (ChunkCursor cur : cursors) {
                        if (!cur.exhausted(rgRows) && (next == null || cur.nextRow < next.nextRow)) {
                            next = cur;
                        }
                    }
                    if (next == null) {
                        break;
                    }
                    schedulePage(next, rgRows, batchBase, rgBatches);
                }
                batchBase += rgBatches;
            }
            totalBatches = batchBase;
            LockSupport.unpark(consumerThread);
        }
        catch (Throwable t) {
            fail(t);
        }
    }

    private void schedulePage(ChunkCursor cur, long rgRows, int batchBase, int rgBatches) {
        ByteBuffer rest = cur.chunk.slice(cur.pos, cur.chunk.limit() - cur.pos);
        ThriftCompactReader reader = new ThriftCompactReader(rest, 0);
        PageHeader header = PageHeaderReader.read(reader);
        int headerSize = reader.getBytesRead();
        int bodySize = header.compressedPageSize();
        ByteBuffer body = rest.slice(headerSize, bodySize);
        cur.pos += headerSize + bodySize;

        switch (header.type()) {
            case DICTIONARY_PAGE -> {
                cur.dictionary = DictionaryParser.parsePage(header, body, columns[cur.column], cur.meta, context);
                mark(1);
                return;
            }
            case DATA_PAGE -> {
            }
            default -> throw new UnsupportedOperationException("spike: page type " + header.type());
        }

        long firstRow = cur.nextRow;
        int numRows = header.dataPageHeader().numValues();
        cur.nextRow += numRows;

        int firstBatch = batchBase + Math.toIntExact(firstRow / batchSize);
        int lastBatch = batchBase + Math.toIntExact((firstRow + numRows - 1) / batchSize);

        // Deadlock-free: every batch before firstBatch has all its pages submitted already,
        // so the consumer can reach firstBatch without this page.
        while (!closed && firstBatch - released.get() >= window) {
            LockSupport.parkNanos(PARK_NANOS);
        }
        if (lastBatch - released.get() >= RING) {
            throw new IllegalStateException("spike: page spans more batches than the ring holds");
        }
        for (int b = firstBatch; b <= lastBatch; b++) {
            int slot = b & (RING - 1);
            if (ring.get(slot) == null) {
                long rowInRg = (long) (b - batchBase) * batchSize;
                int rows = Math.toIntExact(Math.min(batchSize, rgRows - rowInRg));
                ring.set(slot, newBatch(rows));
            }
        }

        int column = cur.column;
        Dictionary dictionary = cur.dictionary;
        tasksInFlight.incrementAndGet();
        mark(2);
        FIRST.compareAndSet(7, null, cur.meta.codec().toString());
        executor.execute(() -> {
            mark(3);
            try {
                if (error.get() == null && !closed) {
                    decodePage(column, cur.meta, header, body, dictionary, firstRow, numRows, batchBase);
                }
            }
            catch (Throwable t) {
                fail(t);
            }
            finally {
                tasksInFlight.decrementAndGet();
            }
        });
    }

    // ==================== Decode task ====================

    private void decodePage(int column, ColumnMetaData meta, PageHeader header, ByteBuffer body,
                            Dictionary dictionary, long firstRow, int numRows, int batchBase) {
        DataPageHeader dph = header.dataPageHeader();
        int uncompressed = header.uncompressedPageSize();
        byte[] data = context.decompressorFactory().getDecompressor(meta.codec()).decompress(body, uncompressed);
        mark(4);
        ColumnSchema schema = columns[column];
        Scratch scratch = SCRATCH.get();

        int offset = 0;
        int[] defs = null;
        int maxDef = schema.maxDefinitionLevel();
        int nonNull = numRows;
        if (maxDef > 0) {
            int len = ByteBuffer.wrap(data, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
            offset += 4;
            RleBitPackingHybridDecoder dec = new RleBitPackingHybridDecoder(data, offset, len,
                    32 - Integer.numberOfLeadingZeros(maxDef));
            if (!dec.isSingleRleRunOf(maxDef, numRows)) {
                defs = scratch.defs(numRows);
                dec.readInts(defs, 0, numRows);
                nonNull = 0;
                for (int i = 0; i < numRows; i++) {
                    if (defs[i] == maxDef) {
                        nonNull++;
                    }
                }
            }
            offset += len;
        }

        // Value source: PLAIN bytes at `offset`, dictionary indices in scratch, or (binary ids
        // without nulls) a decoder that unpacks straight into each batch segment below.
        int[] indices = null;
        RleBitPackingHybridDecoder directIds = null;
        boolean zeroWidthIds = false;
        Encoding enc = dph.encoding();
        switch (enc) {
            case PLAIN -> {
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                int bitWidth = data[offset++] & 0xFF;
                RleBitPackingHybridDecoder decoder =
                        new RleBitPackingHybridDecoder(data, offset, uncompressed - offset, bitWidth);
                PhysicalType type = schema.type();
                if (DIRECT_IDS && defs == null
                        && (type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY)) {
                    directIds = decoder;
                    zeroWidthIds = bitWidth == 0;
                }
                else {
                    indices = scratch.indices(nonNull);
                    if (bitWidth == 0) {
                        // A zero bit width (single-entry dictionary) decodes nothing, and the
                        // thread's scratch still holds the ids of an earlier page.
                        Arrays.fill(indices, 0, nonNull, 0);
                    }
                    else {
                        decoder.readInts(indices, 0, nonNull);
                    }
                }
            }
            default -> throw new UnsupportedOperationException("spike: encoding " + enc);
        }

        mark(5);
        // Scatter over the batches the page covers. `src` counts non-null values consumed.
        int pageRow = 0;
        int src = 0;
        while (pageRow < numRows) {
            long row = firstRow + pageRow;
            int b = batchBase + Math.toIntExact(row / batchSize);
            int dest = Math.toIntExact(row % batchSize);
            Batch batch = ring.get(b & (RING - 1));
            int len = Math.min(numRows - pageRow, batch.rowCount - dest);
            if (directIds != null) {
                unpackIds(batch, column, directIds, zeroWidthIds, dictionary, maxDef, pageRow, dest, len);
            }
            else {
                src = scatter(batch, column, schema.type(), data, offset, indices, dictionary, defs, maxDef,
                        pageRow, dest, len, src);
            }
            if (batch.pendingRows.addAndGet(-len) == 0) {
                complete(batch, b);
            }
            pageRow += len;
        }
    }

    /// Unpacks the next `len` dictionary ids of a page without nulls straight into `batch` at
    /// `dest`. A zero bit width (single-entry dictionary) decodes nothing, so the ids are written
    /// here: a recycled batch still holds the ids of an earlier batch.
    private static void unpackIds(Batch batch, int column, RleBitPackingHybridDecoder decoder, boolean zeroWidth,
                                  Dictionary dictionary, int maxDef, int pageRow, int dest, int len) {
        long[] validity = batch.validity[column];
        if (validity != null) {
            markValidity(validity, null, maxDef, pageRow, dest, len);
        }
        batch.dictionaries[column] = dictionary;
        int[] out = (int[]) batch.values[column];
        if (zeroWidth) {
            Arrays.fill(out, dest, dest + len, 0);
        }
        else {
            decoder.readInts(out, dest, len);
        }
    }

    /// Writes page rows `[pageRow, pageRow + len)` into `batch` at `dest`; returns the advanced
    /// non-null source position.
    private static int scatter(Batch batch, int column, PhysicalType type, byte[] data, int plainOffset,
                               int[] indices, Dictionary dictionary, int[] defs, int maxDef,
                               int pageRow, int dest, int len, int src) {
        long[] validity = batch.validity[column];
        if (validity != null) {
            markValidity(validity, defs, maxDef, pageRow, dest, len);
        }
        ByteBuffer le = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        switch (type) {
            case INT64 -> {
                long[] out = (long[]) batch.values[column];
                long[] dict = dictionary instanceof Dictionary.LongDictionary d ? d.values() : null;
                if (defs == null) {
                    if (indices == null) {
                        le.position(plainOffset + src * 8).asLongBuffer().get(out, dest, len);
                    }
                    else {
                        for (int i = 0; i < len; i++) {
                            out[dest + i] = dict[indices[src + i]];
                        }
                    }
                    return src + len;
                }
                for (int i = 0; i < len; i++) {
                    if (defs[pageRow + i] == maxDef) {
                        out[dest + i] = indices == null ? le.getLong(plainOffset + src * 8) : dict[indices[src]];
                        src++;
                    }
                }
                return src;
            }
            case DOUBLE -> {
                double[] out = (double[]) batch.values[column];
                double[] dict = dictionary instanceof Dictionary.DoubleDictionary d ? d.values() : null;
                if (defs == null) {
                    if (indices == null) {
                        le.position(plainOffset + src * 8).asDoubleBuffer().get(out, dest, len);
                    }
                    else {
                        for (int i = 0; i < len; i++) {
                            out[dest + i] = dict[indices[src + i]];
                        }
                    }
                    return src + len;
                }
                for (int i = 0; i < len; i++) {
                    if (defs[pageRow + i] == maxDef) {
                        out[dest + i] = indices == null ? le.getDouble(plainOffset + src * 8) : dict[indices[src]];
                        src++;
                    }
                }
                return src;
            }
            case INT32 -> {
                int[] out = (int[]) batch.values[column];
                int[] dict = dictionary instanceof Dictionary.IntDictionary d ? d.values() : null;
                if (defs == null) {
                    if (indices == null) {
                        le.position(plainOffset + src * 4).asIntBuffer().get(out, dest, len);
                    }
                    else {
                        for (int i = 0; i < len; i++) {
                            out[dest + i] = dict[indices[src + i]];
                        }
                    }
                    return src + len;
                }
                for (int i = 0; i < len; i++) {
                    if (defs[pageRow + i] == maxDef) {
                        out[dest + i] = indices == null ? le.getInt(plainOffset + src * 4) : dict[indices[src]];
                        src++;
                    }
                }
                return src;
            }
            case FLOAT -> {
                float[] out = (float[]) batch.values[column];
                float[] dict = dictionary instanceof Dictionary.FloatDictionary d ? d.values() : null;
                if (defs == null) {
                    if (indices == null) {
                        le.position(plainOffset + src * 4).asFloatBuffer().get(out, dest, len);
                    }
                    else {
                        for (int i = 0; i < len; i++) {
                            out[dest + i] = dict[indices[src + i]];
                        }
                    }
                    return src + len;
                }
                for (int i = 0; i < len; i++) {
                    if (defs[pageRow + i] == maxDef) {
                        out[dest + i] = indices == null ? le.getFloat(plainOffset + src * 4) : dict[indices[src]];
                        src++;
                    }
                }
                return src;
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                if (indices == null) {
                    throw new UnsupportedOperationException("spike: binary columns need dictionary pages");
                }
                batch.dictionaries[column] = dictionary;
                int[] out = (int[]) batch.values[column];
                if (defs == null) {
                    System.arraycopy(indices, src, out, dest, len);
                    return src + len;
                }
                for (int i = 0; i < len; i++) {
                    out[dest + i] = defs[pageRow + i] == maxDef ? indices[src++] : -1;
                }
                return src;
            }
            default -> throw new UnsupportedOperationException("spike: " + type);
        }
    }

    /// Sets presence bits for `[dest, dest + len)`. Words at the edges can be shared with a page
    /// decoded on another thread, so every word is merged with an atomic OR.
    private static void markValidity(long[] words, int[] defs, int maxDef, int pageRow, int dest, int len) {
        int end = dest + len;
        int i = dest;
        while (i < end) {
            int word = i >>> 6;
            int wordEnd = Math.min(end, (word + 1) << 6);
            long mask = 0;
            if (defs == null) {
                int from = i & 63;
                int to = wordEnd - (word << 6);
                mask = (to == 64 ? -1L : (1L << to) - 1) & (-1L << from);
            }
            else {
                for (int j = i; j < wordEnd; j++) {
                    if (defs[pageRow + (j - dest)] == maxDef) {
                        mask |= 1L << (j & 63);
                    }
                }
            }
            LONGS.getAndBitwiseOr(words, word, mask);
            i = wordEnd;
        }
    }

    /// A batch for `rows` rows: a recycled one when `RECYCLE` is on and one is free, else a
    /// fresh one, allocated at full batch size under `RECYCLE` so it can be reused for any batch.
    private Batch newBatch(int rows) {
        if (!RECYCLE) {
            return new Batch(rows, columns);
        }
        Batch recycled = pool.poll();
        if (recycled != null) {
            recycled.reset(rows);
            return recycled;
        }
        Batch fresh = new Batch(batchSize, columns);
        fresh.reset(rows);
        return fresh;
    }

    private void recycle(Batch batch) {
        if (RECYCLE) {
            pool.offer(batch);
        }
    }

    private void complete(Batch batch, int b) {
        mark(6);
        if (sink == null) {
            batch.ready = true;
            LockSupport.unpark(consumerThread);
            return;
        }
        sink.accept(batch);
        ring.set(b & (RING - 1), null);
        recycle(batch);
        released.incrementAndGet();
        LockSupport.unpark(schedulerThread);
        LockSupport.unpark(consumerThread);
    }

    /// Unordered mode: waits until the sink has seen every batch.
    public void awaitCompletion() {
        consumerThread = Thread.currentThread();
        while (true) {
            Throwable t = error.get();
            if (t != null) {
                throw new RuntimeException(t);
            }
            int total = totalBatches;
            if (total >= 0 && released.get() >= total) {
                return;
            }
            LockSupport.parkNanos(PARK_NANOS);
        }
    }

    private void fail(Throwable t) {
        error.compareAndSet(null, t);
        LockSupport.unpark(consumerThread);
    }

    @Override
    public void close() {
        closed = true;
        LockSupport.unpark(schedulerThread);
        try {
            schedulerThread.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        while (tasksInFlight.get() > 0) {
            Thread.onSpinWait();
        }
    }
}
