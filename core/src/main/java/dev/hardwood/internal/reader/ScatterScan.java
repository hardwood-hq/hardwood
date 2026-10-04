/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

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
/// Scope: one memory-resident file, flat `INT32`/`INT64`/`FLOAT`/`DOUBLE` columns (`PLAIN` or
/// dictionary) and dictionary-encoded binary columns (exposed as dictionary ids), V1 data pages. No filter, no row cap. Batches never span row groups.
public final class ScatterScan implements AutoCloseable {

    private static final VarHandle LONGS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final int RING = 1 << 14;
    private static final long PARK_NANOS = 1_000_000L;

    /// One row-aligned batch over every projected column.
    public static final class Batch {
        public final int rowCount;
        /// `long[]`, `double[]`, `int[]` or `float[]` per column.
        public final Object[] values;
        /// Presence bitmap per column (set bit = present); `null` for a required column.
        public final long[][] validity;
        /// The [Dictionary] a binary column's ids refer to; batches never span row groups.
        public final Object[] dictionaries;
        final AtomicInteger pendingRows;
        volatile boolean ready;

        Batch(int rowCount, ColumnSchema[] columns) {
            this.rowCount = rowCount;
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

    private final ByteBuffer file;
    private final FileMetaData metaData;
    private final ColumnSchema[] columns;
    private final int[] fileColumns;
    private final int batchSize;
    private final int window;
    private final Executor executor;
    private final HardwoodContextImpl context;

    private final AtomicReferenceArray<Batch> ring = new AtomicReferenceArray<>(RING);
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicInteger tasksInFlight = new AtomicInteger();
    private volatile int consumed;
    private volatile int totalBatches = -1;
    private volatile boolean closed;
    private volatile Thread consumerThread;
    private volatile Thread schedulerThread;
    private Batch current;

    public ScatterScan(ByteBuffer file, FileMetaData metaData, List<String> projection, int batchSize,
                       int window, HardwoodContextImpl context) {
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
            current = null;
            consumed++;
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

        ChunkCursor(int column, ColumnChunk cc) {
            this.column = column;
            this.meta = cc.metaData();
            int start = Math.toIntExact(cc.chunkStartOffset());
            this.chunk = file.slice(start, Math.toIntExact(meta.totalCompressedSize()));
        }

        boolean exhausted(long rgRows) {
            return nextRow >= rgRows;
        }
    }

    private void schedule() {
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
        while (!closed && firstBatch - consumed >= window) {
            LockSupport.parkNanos(PARK_NANOS);
        }
        if (lastBatch - consumed >= RING) {
            throw new IllegalStateException("spike: page spans more batches than the ring holds");
        }
        for (int b = firstBatch; b <= lastBatch; b++) {
            int slot = b & (RING - 1);
            if (ring.get(slot) == null) {
                long rowInRg = (long) (b - batchBase) * batchSize;
                int rows = Math.toIntExact(Math.min(batchSize, rgRows - rowInRg));
                ring.set(slot, new Batch(rows, columns));
            }
        }

        int column = cur.column;
        Dictionary dictionary = cur.dictionary;
        tasksInFlight.incrementAndGet();
        executor.execute(() -> {
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

        // Value source: either PLAIN bytes at `offset`, or dictionary indices in scratch.
        int[] indices = null;
        Encoding enc = dph.encoding();
        switch (enc) {
            case PLAIN -> {
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                int bitWidth = data[offset++] & 0xFF;
                indices = scratch.indices(nonNull);
                new RleBitPackingHybridDecoder(data, offset, uncompressed - offset, bitWidth)
                        .readInts(indices, 0, nonNull);
            }
            default -> throw new UnsupportedOperationException("spike: encoding " + enc);
        }

        // Scatter over the batches the page covers. `src` counts non-null values consumed.
        int pageRow = 0;
        int src = 0;
        while (pageRow < numRows) {
            long row = firstRow + pageRow;
            int b = batchBase + Math.toIntExact(row / batchSize);
            int dest = Math.toIntExact(row % batchSize);
            Batch batch = ring.get(b & (RING - 1));
            int len = Math.min(numRows - pageRow, batch.rowCount - dest);
            src = scatter(batch, column, schema.type(), data, offset, indices, dictionary, defs, maxDef,
                    pageRow, dest, len, src);
            if (batch.pendingRows.addAndGet(-len) == 0) {
                batch.ready = true;
                LockSupport.unpark(consumerThread);
            }
            pageRow += len;
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
