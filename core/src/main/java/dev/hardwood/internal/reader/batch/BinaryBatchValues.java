/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.row.PqInterval;

/// Per-batch values slot for a varlength leaf (`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`
/// / `INT96`).
///
/// Each value is held in one form, fixed by its fill: a value from a dictionary page as its entry
/// `dictIndices[i]` of [#dictionary], every other value as the view `[starts[i], ends[i])` into
/// buffer `bufferIds[i]` of [#buffers], into which its segment copied its bytes ([PlainBytes]). A
/// null value's view is empty. Reading one value's bytes ([#buffer(int)], [#start(int)],
/// [#end(int)] and the accessors that copy, decode, test or compare it) goes through its entry where
/// it has one, and through its view otherwise.
///
/// A loop over every value's bytes reads a [Window] instead: a view for every value, laid out in
/// value order, over the batch's buffers followed by the dictionary's chunks. The thread that
/// completes the cell builds it where the column's consumer has asked for windows before
/// ([ViewDemand], [#finishFill]); otherwise [#window(int)] builds it when asked. The batch never
/// changes once filled: whichever thread reads it sees what the fill wrote.
///
/// Views need not be contiguous or in value order, and several may cover the same bytes. Bytes
/// outside every view are unspecified.
///
/// The batch allocates no buffer larger than [#CHUNK_BYTES] unless one value is, so its bytes never
/// need an array whose size grows with the batch: G1 allocates an array larger than half a heap
/// region as a humongous object, and the smallest region is 1 MB. A dictionary's bytes are the
/// column chunk's, held once for every batch that draws on it, in chunks of their own within the
/// same bound ([Dictionary.ByteArrayDictionary]).
///
/// A batch is filled positionally by the segments of a decoded column's cell ([#prepareForFill],
/// [dev.hardwood.internal.reader.pipeline.BinaryFill],
/// [dev.hardwood.internal.reader.pipeline.NestedFill]): each segment writes its own values' entry
/// ids, views, buffer ids and chunks at its own positions, so the segments are independent of each
/// other and of the order they are written in. The view arrays and the entry ids are installed by
/// the first segment that writes them, and a chunk takes its buffer id ([#addBuffer]), under the
/// batch's monitor.
public final class BinaryBatchValues {

    /// The largest chunk of plain values' bytes: below half of G1's smallest region (1 MB), less an
    /// allowance for the array header.
    public static final int CHUNK_BYTES = 256 * 1024;

    private static final byte[] EMPTY = new byte[0];

    /// Whether a leaf of `type` keeps its values in a batch's [BinaryBatchValues]: a
    /// `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` or `INT96` leaf. Every other leaf keeps them in a
    /// primitive array.
    public static boolean holds(PhysicalType type) {
        return switch (type) {
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> false;
        };
    }

    /// The buffers the views point into, `[0, bufferCount)` in use. Buffer `0` is empty, for the
    /// views of nulls.
    private byte[][] buffers;
    private int bufferCount;
    private int[] bufferIds;
    private int[] starts;
    private int[] ends;

    /// Whether the batch is a recycled holder's, which keeps its full chunks for its next fill
    /// ([#prepareForFill]), and whose fills take full chunks so that they can be reused.
    private boolean keepsChunks;
    /// Full chunks of the last fill, for the next one; `[0, spareCount)` in use.
    private byte[][] spare = new byte[0][];
    private int spareCount;

    /// The window the completing thread built, `null` where it built none ([#finishFill]).
    private Window window;

    /// The column's [ViewDemand] where the batch has dictionary values and no window, raised when
    /// [#window(int)] builds one.
    private ViewDemand viewDemand;

    /// The chunk dictionary backing [#dictIndices], or `null` when no dictionary
    /// page has contributed to this batch (every value then materialises from its
    /// buffer). Holds the per-entry `String` cache that lets [#stringAt] reuse
    /// one instance per dictionary entry per chunk, and is what
    /// `ColumnReader.getBinaryDictionary()` exposes.
    public Dictionary.ByteArrayDictionary dictionary;

    /// Per-value dictionary entry index, meaningful only when [#dictionary] is
    /// non-null. `-1` marks a value that must materialise from its buffer: a
    /// plain-encoded value or a null position. Allocated by the first segment of entry ids
    /// ([#installDictionary]).
    public int[] dictIndices;

    /// Whether every present value has an entry id in [#dictIndices]: set by the fill when every
    /// segment of the batch wrote ids. `false` means a present value may have none, and a reader
    /// that needs to know looks at the ids.
    public boolean idsComplete;

    /// The values this batch can hold. [#starts], [#ends] and [#bufferIds] are allocated at this
    /// size on their first write.
    private final int capacity;

    /// An empty batch slot for `capacity` values.
    public BinaryBatchValues(int capacity) {
        this.buffers = new byte[][] { EMPTY };
        this.bufferCount = 1;
        this.capacity = capacity;
    }

    /// A slot over existing views into one buffer, for a batch that receives no further values.
    public BinaryBatchValues(byte[] bytes, int[] starts, int[] ends) {
        this.buffers = new byte[][] { bytes };
        this.bufferCount = 1;
        this.capacity = starts.length;
        this.starts = starts;
        this.ends = ends;
        this.bufferIds = new int[starts.length];
    }

    /// Set when a batch of the column first builds a [Window] on demand: the column's consumer
    /// reads windows. From then on, the thread that completes a cell with dictionary values builds
    /// its window. One instance per column and read; never lowered.
    public static final class ViewDemand {
        public volatile boolean requested;
    }

    /// The buffers the views point into: value `i` is in `buffers()[bufferIds()[i]]`. For a
    /// reader of every value's view at once, such as a binary matcher, of a batch without
    /// dictionary values, whose views cover every value.
    ///
    /// @throws IllegalStateException if the batch has dictionary values, which have no views of
    ///         their own ([#window(int)] gives them)
    public byte[][] buffers() {
        requireNoDictionary();
        return buffers;
    }

    /// Which of [#buffers()] each value is in; see [#buffers()].
    public int[] bufferIds() {
        requireNoDictionary();
        return bufferIds;
    }

    /// Value `i` starts at `starts()[i]` in its buffer; see [#buffers()].
    public int[] starts() {
        requireNoDictionary();
        return starts;
    }

    /// Value `i` ends at `ends()[i]` in its buffer; see [#buffers()].
    public int[] ends() {
        requireNoDictionary();
        return ends;
    }

    private void requireNoDictionary() {
        if (dictionary != null) {
            throw new IllegalStateException("A batch's dictionary values have no views of their own");
        }
    }

    /// The array value `i` is read from.
    public byte[] buffer(int i) {
        // The view path stays small enough to inline into a caller's loop; the entry path is a call
        return dictionary == null ? buffers[bufferIds[i]] : bufferThroughEntry(i);
    }

    /// Where value `i` starts in [#buffer(int)].
    public int start(int i) {
        return dictionary == null ? starts[i] : startThroughEntry(i);
    }

    /// Where value `i` ends in [#buffer(int)].
    public int end(int i) {
        return dictionary == null ? ends[i] : endThroughEntry(i);
    }

    /// The entry value `i` is read through, or `-1` for a value read through its view.
    private int entryOf(int i) {
        return dictionary == null ? -1 : dictIndices[i];
    }

    private byte[] bufferThroughEntry(int i) {
        int entry = dictIndices[i];
        return entry >= 0 ? dictionary.entryBuffer(entry) : buffers[bufferIds[i]];
    }

    private int startThroughEntry(int i) {
        int entry = dictIndices[i];
        return entry >= 0 ? dictionary.entryStart(entry) : starts[i];
    }

    private int endThroughEntry(int i) {
        int entry = dictIndices[i];
        return entry >= 0 ? dictionary.entryEnd(entry) : ends[i];
    }

    /// The window of the first `count` values: their views in arrays of exactly `count` elements,
    /// value `i` being `[starts[i], ends[i])` of `buffers[bufferIds[i]]`. A batch without dictionary
    /// values hands out its own views; a batch with them hands out the window its completing thread
    /// built, or builds one here, raising the column's [ViewDemand] so that the column's later
    /// batches have theirs built as they complete. The batch is not changed.
    public Window window(int count) {
        if (dictionary == null) {
            if (starts == null) {
                // Every fill of a value installs the view arrays, so only an empty batch has none
                if (count != 0) {
                    throw new IllegalStateException("A batch of " + count + " values has no views");
                }
                return new Window(buffers, new int[0], new int[0], new int[0]);
            }
            return new Window(buffers, trim(bufferIds, count), trim(starts, count), trim(ends, count));
        }
        Window built = window;
        if (built != null) {
            return new Window(built.buffers(), trim(built.bufferIds(), count), trim(built.starts(), count),
                    trim(built.ends(), count));
        }
        ViewDemand demand = viewDemand;
        if (demand != null && !demand.requested) {
            demand.requested = true;
        }
        return buildWindow(count);
    }

    /// Lays out the window of the first `count` values: the views of the values without an entry,
    /// and each dictionary value's view into the dictionary's chunks, which follow the batch's own
    /// buffers.
    private Window buildWindow(int count) {
        Dictionary.ByteArrayDictionary dict = dictionary;
        byte[][] all = Arrays.copyOf(buffers, bufferCount + dict.chunkCount());
        for (int c = 0; c < dict.chunkCount(); c++) {
            all[bufferCount + c] = dict.chunkBytes(c);
        }
        // The values without an entry, plain or null, keep the views their segments wrote
        int[] ids = starts == null ? new int[count] : Arrays.copyOf(bufferIds, count);
        int[] from = starts == null ? new int[count] : Arrays.copyOf(starts, count);
        int[] to = starts == null ? new int[count] : Arrays.copyOf(ends, count);
        dict.writeViews(dictIndices, 0, count, bufferCount, from, to, ids);
        return new Window(all, ids, from, to);
    }

    private static int[] trim(int[] views, int count) {
        return views.length == count ? views : Arrays.copyOf(views, count);
    }

    /// A view of every value of a batch, from [#window(int)].
    public record Window(byte[][] buffers, int[] bufferIds, int[] starts, int[] ends) {
    }

    /// The buffers in use, for tests of how a batch holds its bytes.
    public int bufferCount() {
        return bufferCount;
    }

    /// Whether the view arrays exist yet, for tests that a batch without views allocates none.
    public boolean viewArraysAllocated() {
        return starts != null;
    }

    /// Whether the completing thread built the batch's window, for tests.
    public boolean hasWindow() {
        return window != null;
    }

    /// The values this batch can hold.
    public int capacity() {
        return capacity;
    }

    private void allocateViews() {
        starts = new int[capacity];
        ends = new int[capacity];
        bufferIds = new int[capacity];
    }

    /// Readies a slot for a positional fill: its arrays are kept, its contents and buffers are not.
    /// Runs before any segment writes.
    public void prepareForFill() {
        if (keepsChunks) {
            keepFullChunks();
        }
        Arrays.fill(buffers, 1, bufferCount, null);
        bufferCount = 1;
        dictionary = null;
        idsComplete = false;
        window = null;
        viewDemand = null;
    }

    /// Installs the view arrays for a segment that writes views, unless a segment already has.
    /// A segment reads them with [#fillStarts], [#fillEnds] and [#fillBufferIds] after this.
    public synchronized void installViews() {
        if (starts == null) {
            allocateViews();
        }
    }

    /// The view arrays, for a segment that ran [#installViews].
    public int[] fillStarts() {
        return starts;
    }

    public int[] fillEnds() {
        return ends;
    }

    public int[] fillBufferIds() {
        return bufferIds;
    }

    /// Makes the batch keep its full chunks from one fill to the next: for a recycled holder,
    /// whose batches are read before the holder is refilled and never shared with another batch.
    public void keepChunks() {
        keepsChunks = true;
    }

    boolean keepsChunks() {
        return keepsChunks;
    }

    /// Keeps the full chunks of the last fill as [#spare], and only those: spares an earlier fill
    /// left over and buffers of another size are dropped, so a holder keeps what its last fill
    /// took. The dictionary's chunks are never among the batch's buffers, only a window's.
    private void keepFullChunks() {
        Arrays.fill(spare, 0, spareCount, null);
        spareCount = 0;
        for (int b = 1; b < bufferCount; b++) {
            byte[] buffer = buffers[b];
            if (buffer.length == CHUNK_BYTES) {
                if (spareCount == spare.length) {
                    spare = Arrays.copyOf(spare, Math.max(4, spareCount * 2));
                }
                spare[spareCount++] = buffer;
            }
        }
    }

    /// Adds a full chunk to the batch's buffers, a spare one where the last fill left one, and
    /// returns its id; [#chunk(int)] gives the chunk.
    synchronized int takeChunk() {
        byte[] chunk;
        if (spareCount > 0) {
            chunk = spare[--spareCount];
            spare[spareCount] = null;
        }
        else {
            chunk = new byte[CHUNK_BYTES];
        }
        return addBuffer(chunk);
    }

    /// Buffer `id`, for the fill that added it.
    synchronized byte[] chunk(int id) {
        return buffers[id];
    }

    /// Adds `buffer` to the batch's buffers and returns its id.
    synchronized int addBuffer(byte[] buffer) {
        if (bufferCount == buffers.length) {
            buffers = Arrays.copyOf(buffers, bufferCount * 2);
        }
        buffers[bufferCount] = buffer;
        return bufferCount++;
    }

    /// Adopts `dict` as the dictionary of the batch's entry ids, unless a segment already has, and
    /// returns the ids array the segment writes into.
    ///
    /// @throws IllegalStateException if the batch already holds the ids of another dictionary
    public synchronized int[] installDictionary(Dictionary.ByteArrayDictionary dict) {
        if (dictionary == null) {
            dictionary = dict;
            if (dictIndices == null || dictIndices.length < capacity) {
                dictIndices = new int[capacity];
            }
        }
        else if (dictionary != dict) {
            throw twoDictionaries();
        }
        return dictIndices;
    }

    /// Ends a positional fill of `valueCount` values once every segment is written, on the thread
    /// that completes the cell, before the batch is published: a batch with dictionary values gets
    /// its window built here where `demand` has been raised, and keeps `demand` to raise otherwise.
    public void finishFill(int valueCount, ViewDemand demand) {
        if (dictionary == null) {
            return;
        }
        if (demand.requested) {
            window = buildWindow(valueCount);
        }
        else {
            viewDemand = demand;
        }
    }

    /// The values at `map[0, count)` as a batch of their own, with their dictionary, entry ids and
    /// views, and the kept part of the window where this batch has one. The result shares this
    /// batch's buffers, which no later fill writes: a refill adds chunks of its own.
    ///
    /// @throws IllegalStateException for a batch that keeps its chunks ([#keepChunks]), whose
    ///         next fill writes them again
    BinaryBatchValues compact(int[] map, int count) {
        if (keepsChunks) {
            throw new IllegalStateException("A batch that keeps its chunks for its next fill cannot be compacted");
        }
        BinaryBatchValues out = new BinaryBatchValues(count);
        out.buffers = Arrays.copyOf(buffers, Math.max(bufferCount, 1));
        out.bufferCount = bufferCount;
        out.viewDemand = viewDemand;
        if (starts != null) {
            // A dictionary value's own view is unspecified, and copied as it is
            out.allocateViews();
            gather(bufferIds, starts, ends, map, count, out.bufferIds, out.starts, out.ends);
        }
        if (window != null) {
            int[] ids = new int[count];
            int[] from = new int[count];
            int[] to = new int[count];
            gather(window.bufferIds(), window.starts(), window.ends(), map, count, ids, from, to);
            out.window = new Window(window.buffers(), ids, from, to);
        }
        out.adoptIds(this, map, count);
        return out;
    }

    private static void gather(int[] bufferIds, int[] starts, int[] ends, int[] map, int count, int[] outIds,
                               int[] outStarts, int[] outEnds) {
        for (int i = 0; i < count; i++) {
            int rawIdx = map[i];
            outIds[i] = bufferIds[rawIdx];
            outStarts[i] = starts[rawIdx];
            outEnds[i] = ends[rawIdx];
        }
    }

    /// Takes `raw`'s dictionary and the entry ids of the values at `map[0, count)`.
    private void adoptIds(BinaryBatchValues raw, int[] map, int count) {
        if (raw.dictionary == null) {
            return;
        }
        int[] ids = new int[count];
        for (int i = 0; i < count; i++) {
            ids[i] = raw.dictIndices[map[i]];
        }
        dictionary = raw.dictionary;
        dictIndices = ids;
        idsComplete = raw.idsComplete;
    }

    /// Materialise value `idx` as a fresh `byte[]` copy. Allocates one array
    /// per call — used by convenience accessors and per-row materialisation
    /// paths; hot loops should read [#buffer(int)] / [#start(int)] / [#end(int)] directly.
    public byte[] byteArrayAt(int idx) {
        int entry = entryOf(idx);
        return entry >= 0 ? dictionary.entry(entry) : Arrays.copyOfRange(buffers[bufferIds[idx]], starts[idx], ends[idx]);
    }

    /// Tests value `idx` with `test`, handing it the bytes where they sit.
    public boolean testAt(int idx, BinaryValueTest test) {
        int entry = entryOf(idx);
        return entry >= 0 ? dictionary.testEntry(entry, test) : test.testValue(buffers[bufferIds[idx]], starts[idx], ends[idx]);
    }

    /// Whether value `idx` holds exactly the bytes of `other`, compared where they sit rather than
    /// through a copy.
    public boolean equalsAt(int idx, byte[] other) {
        int entry = entryOf(idx);
        return entry >= 0 ? dictionary.entryEquals(entry, other)
                : Arrays.equals(buffers[bufferIds[idx]], starts[idx], ends[idx], other, 0, other.length);
    }

    /// Decode value `idx` as the single-precision value its `FLOAT16` payload stands for,
    /// reading the two bytes where they sit rather than materialising a `byte[]` for them.
    ///
    /// The float accessors decode here rather than through the generic value conversion
    /// because that path returns `Object` and would box the value they promised unboxed.
    public float float16At(int idx) {
        int start = start(idx);
        return LogicalTypeConverter.bytesToFloat16(buffer(idx), start, end(idx) - start);
    }

    /// Decode value `idx` as the decimal its `DECIMAL` payload stands for, reading the
    /// bytes where they sit rather than materialising a `byte[]` for them.
    ///
    /// The copy `byteArrayAt` makes is dropped again as soon as [BigInteger] has read
    /// it, and whether it survives that is left to escape analysis; reading in place
    /// does not depend on the decision going the right way.
    public BigDecimal decimalAt(int idx, int scale) {
        int start = start(idx);
        return LogicalTypeConverter.bytesToDecimal(buffer(idx), start, end(idx) - start, scale);
    }

    /// Decode value `idx` as the [UUID] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public UUID uuidAt(int idx) {
        int start = start(idx);
        return LogicalTypeConverter.bytesToUuid(buffer(idx), start, end(idx) - start);
    }

    /// Decode value `idx` as the [PqInterval] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public PqInterval intervalAt(int idx) {
        int start = start(idx);
        return LogicalTypeConverter.bytesToInterval(buffer(idx), start, end(idx) - start);
    }

    /// Decode value `idx` as the instant its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload stands
    /// for, reading the bytes where they sit rather than materialising a `byte[]` for them.
    public Instant flba12InstantAt(int idx, LogicalType.TimeUnit unit) {
        int start = start(idx);
        return Flba12Timestamps.toInstant(buffer(idx), start, end(idx) - start, unit);
    }

    /// Decode value `idx` as the instant its legacy `INT96` timestamp stands for, reading the
    /// bytes where they sit rather than materialising a `byte[]` for them.
    public Instant int96InstantAt(int idx) {
        int start = start(idx);
        return LogicalTypeConverter.int96ToInstant(buffer(idx), start, end(idx) - start);
    }

    /// Decode value `idx` as the wall clock its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload
    /// stands for, reading the bytes where they sit.
    public LocalDateTime flba12LocalDateTimeAt(int idx, LogicalType.TimeUnit unit) {
        int start = start(idx);
        return Flba12Timestamps.toLocalDateTime(buffer(idx), start, end(idx) - start, unit);
    }

    /// Materialise value `idx` as a UTF-8 decoded `String`. A dictionary-encoded
    /// value (when [#dictionary] is set and `dictIndices[idx]` is non-negative)
    /// reuses the chunk dictionary's per-entry interned cache, so repeated values
    /// are decoded once per chunk; any other value allocates one `String` per
    /// call from the value's buffer.
    public String stringAt(int idx) {
        Dictionary.ByteArrayDictionary dict = dictionary;
        if (dict != null) {
            int dictIndex = dictIndices[idx];
            if (dictIndex >= 0) {
                return dict.internedString(dictIndex);
            }
        }
        // A value without an entry id, plain or null, had its view written by its segment
        int start = starts[idx];
        int len = ends[idx] - start;
        return new String(buffers[bufferIds[idx]], start, len, StandardCharsets.UTF_8);
    }

    private static IllegalStateException twoDictionaries() {
        return new IllegalStateException("A batch holds values of two column chunks' dictionaries");
    }
}
