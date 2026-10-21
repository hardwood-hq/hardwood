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
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.row.PqInterval;

/// Per-batch values slot for a varlength leaf (`BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY`
/// / `INT96`).
///
/// Value `i` is the view `[starts[i], ends[i])` into buffer `bufferIds[i]` of [#buffers]. Views
/// need not be contiguous or in value order, and several may cover the same bytes: a value from a
/// dictionary page points at its entry in the dictionary's own bytes ([#ensureViews]), while a
/// plain value's bytes are copied by its segment into a chunk of the batch's ([PlainBytes]). A null
/// value's view is empty. Bytes outside every view are unspecified.
///
/// The batch allocates no buffer larger than [#CHUNK_BYTES] unless one value is, so its bytes never
/// need an array whose size grows with the batch: G1 allocates an array larger than half a heap
/// region as a humongous object, and the smallest region is 1 MB. A dictionary's bytes are the
/// column chunk's, held once for every batch that draws on it.
///
/// A batch is filled positionally by the segments of a scheduled column's cell
/// ([#prepareForFill], [BinaryFill], [NestedFill]): each segment writes
/// its own values' entry ids, views, buffer ids and chunks at its own positions, so the segments
/// are independent of each other and of the order they are written in. The view arrays and the
/// entry ids are installed by the first segment that writes them, and a chunk takes its buffer id
/// ([#addBuffer]), under the batch's monitor. The views of dictionary values are not written by the
/// fill: they are built when a reader first needs them ([#ensureViews]), and a reader of the entry
/// ids alone never pays for them.
public final class BinaryBatchValues {

    /// The largest chunk of plain values' bytes: below half of G1's smallest region (1 MB), less an
    /// allowance for the array header.
    static final int CHUNK_BYTES = 256 * 1024;

    private static final byte[] EMPTY = new byte[0];

    /// The buffers the views point into, `[0, bufferCount)` in use. Buffer `0` is empty in a batch
    /// being filled, for the views of nulls.
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

    /// The buffer id of [#dictionary]'s bytes once a view points into them, `-1` until then.
    private int dictionaryBuffer = -1;

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

    /// An empty batch slot for `capacity` values.
    public BinaryBatchValues(int capacity) {
        this.buffers = new byte[][] { EMPTY };
        this.bufferCount = 1;
        this.capacity = capacity;
        this.viewsPending = true;
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

    /// The values this batch can hold. [#starts], [#ends] and [#bufferIds] are allocated at this
    /// size on their first write.
    private int capacity;

    /// Whether a reader must call [#ensureViews] before reading the views: they are not allocated
    /// yet, or dictionary values were recorded without their views. The accessors test this one
    /// flag per value.
    private boolean viewsPending;

    /// End of the range whose dictionary values were recorded without views
    /// ([#finishFill]), `0` when there are none.
    private int deferredEnd;

    /// The column's [ViewDemand], set while dictionary values may be deferred, so that building
    /// their views tells the column to stop deferring.
    ViewDemand viewDemand;

    /// Raised by the first reader that needs the views of deferred dictionary values. From then
    /// on the column builds views as it completes a cell, on the completing thread, so that only the
    /// batches in flight have theirs built on the reading thread. One instance per column.
    static final class ViewDemand {
        volatile boolean requested;
    }

    /// The buffers the views point into: value `i` is in `buffers()[bufferIds()[i]]`. Builds
    /// pending views first.
    public byte[][] buffers() {
        ensureViews();
        return buffers;
    }

    /// Which of [#buffers()] each value is in. Builds pending views first.
    public int[] bufferIds() {
        ensureViews();
        return bufferIds;
    }

    /// The buffer value `i` is in. Builds pending views first.
    public byte[] buffer(int i) {
        ensureViews();
        return buffers[bufferIds[i]];
    }

    /// Value `i` starts at `starts()[i]` in its buffer. Builds pending views first.
    public int[] starts() {
        ensureViews();
        return starts;
    }

    /// Value `i` ends at `ends()[i]` in its buffer. Builds pending views first.
    public int[] ends() {
        ensureViews();
        return ends;
    }

    /// The buffers in use, for tests of how a batch holds its bytes.
    int bufferCount() {
        return bufferCount;
    }

    /// Whether the view arrays exist yet, for tests of their deferred allocation.
    boolean viewArraysAllocated() {
        return starts != null;
    }

    /// Whether views are pending, for tests of the switch to building them as assembled.
    boolean hasPendingViews() {
        return viewsPending;
    }

    /// The values this batch can hold.
    public int capacity() {
        return capacity;
    }

    /// Allocates [#starts], [#ends] and [#bufferIds] on their first write.
    private void viewArrays() {
        if (starts == null) {
            allocateViews();
            viewsPending = deferredEnd > 0;
        }
    }

    private void allocateViews() {
        starts = new int[capacity];
        ends = new int[capacity];
        bufferIds = new int[capacity];
    }

    /// Readies a slot for a positional fill: its arrays are kept, its contents and buffers are not.
    /// Runs before any segment writes.
    void prepareForFill() {
        if (keepsChunks) {
            keepFullChunks();
        }
        Arrays.fill(buffers, 1, bufferCount, null);
        bufferCount = 1;
        dictionaryBuffer = -1;
        dictionary = null;
        idsComplete = false;
        deferredEnd = 0;
        viewDemand = null;
        viewsPending = true;
    }

    /// Installs the view arrays for a segment that writes views, unless a segment already has.
    /// A segment reads them with [#fillStarts], [#fillEnds] and [#fillBufferIds] after this.
    synchronized void installViews() {
        if (starts == null) {
            allocateViews();
        }
    }

    /// The view arrays, for a segment that ran [#installViews].
    int[] fillStarts() {
        return starts;
    }

    int[] fillEnds() {
        return ends;
    }

    int[] fillBufferIds() {
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
    /// left over, the dictionary's bytes and buffers of another size are dropped, so a holder keeps
    /// what its last fill took.
    private void keepFullChunks() {
        Arrays.fill(spare, 0, spareCount, null);
        spareCount = 0;
        for (int b = 1; b < bufferCount; b++) {
            byte[] buffer = buffers[b];
            if (buffer.length == CHUNK_BYTES && b != dictionaryBuffer) {
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
    synchronized int[] installDictionary(Dictionary.ByteArrayDictionary dict) {
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

    /// Ends a positional fill of `valueCount` values, once every segment is written: the
    /// dictionary values' views are built when a reader first needs them, and building them
    /// raises `demand`.
    void finishFill(int valueCount, ViewDemand demand) {
        viewDemand = demand;
        deferredEnd = dictionary != null ? valueCount : 0;
        viewsPending = starts == null || deferredEnd > 0;
    }

    /// Readies the views for reading: allocates them, and builds the views of the dictionary values
    /// recorded without them. Every reader of the views calls this first.
    public void ensureViews() {
        if (viewsPending) {
            buildViews();
        }
    }

    /// Builds the deferred views: each dictionary value points at its entry in the dictionary's
    /// own bytes, which the batch takes as one of its buffers.
    private void buildViews() {
        viewArrays();
        viewsPending = false;
        if (deferredEnd == 0) {
            return;
        }
        if (viewDemand != null) {
            viewDemand.requested = true;
        }
        int end = deferredEnd;
        deferredEnd = 0;
        int[] entryOffsets = dictionary.entryOffsets();
        if (dictionaryBuffer < 0) {
            dictionaryBuffer = addBuffer(dictionary.entryBytes());
        }
        for (int i = 0; i < end; i++) {
            int entry = dictIndices[i];
            if (entry >= 0) {
                bufferIds[i] = dictionaryBuffer;
                starts[i] = entryOffsets[entry];
                ends[i] = entryOffsets[entry + 1];
            }
        }
    }

    /// The values at `map[0, count)` as a batch of their own, with their dictionary and entry
    /// ids. The result shares this batch's buffers, which no later fill writes: a refill adds
    /// chunks of its own. Dictionary values whose views are deferred stay deferred, so a filtered
    /// read builds views only if it reads them.
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
        out.dictionaryBuffer = dictionaryBuffer;
        out.viewArrays();
        int keptDeferredEnd = 0;
        for (int i = 0; i < count; i++) {
            int rawIdx = map[i];
            if (rawIdx < deferredEnd && dictIndices[rawIdx] >= 0) {
                keptDeferredEnd = i + 1;
            }
            else {
                out.bufferIds[i] = bufferIds[rawIdx];
                out.starts[i] = starts[rawIdx];
                out.ends[i] = ends[rawIdx];
            }
        }
        out.adoptIds(this, map, count);
        out.viewDemand = viewDemand;
        out.deferredEnd = keptDeferredEnd;
        out.viewsPending = keptDeferredEnd > 0;
        return out;
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
    /// paths; hot loops should read [#buffers()] / [#bufferIds()] / [#starts()] / [#ends()]
    /// directly.
    public byte[] byteArrayAt(int idx) {
        ensureViews();
        return Arrays.copyOfRange(buffers[bufferIds[idx]], starts[idx], ends[idx]);
    }

    /// Tests value `idx` with `test`, handing it the bytes where they sit.
    public boolean testAt(int idx, BinaryValueTest test) {
        ensureViews();
        return test.testValue(buffers[bufferIds[idx]], starts[idx], ends[idx]);
    }

    /// Whether value `idx` holds exactly the bytes of `other`, compared where they sit rather than
    /// through a copy.
    public boolean equalsAt(int idx, byte[] other) {
        ensureViews();
        return Arrays.equals(buffers[bufferIds[idx]], starts[idx], ends[idx], other, 0, other.length);
    }

    /// Decode value `idx` as the single-precision value its `FLOAT16` payload stands for,
    /// reading the two bytes where they sit rather than materialising a `byte[]` for them.
    ///
    /// The float accessors decode here rather than through the generic value conversion
    /// because that path returns `Object` and would box the value they promised unboxed.
    public float float16At(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToFloat16(buffers[bufferIds[idx]], start, ends[idx] - start);
    }

    /// Decode value `idx` as the decimal its `DECIMAL` payload stands for, reading the
    /// bytes where they sit rather than materialising a `byte[]` for them.
    ///
    /// The copy `byteArrayAt` makes is dropped again as soon as [BigInteger] has read
    /// it, and whether it survives that is left to escape analysis; reading in place
    /// does not depend on the decision going the right way.
    public BigDecimal decimalAt(int idx, int scale) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToDecimal(buffers[bufferIds[idx]], start, ends[idx] - start, scale);
    }

    /// Decode value `idx` as the [UUID] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public UUID uuidAt(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToUuid(buffers[bufferIds[idx]], start, ends[idx] - start);
    }

    /// Decode value `idx` as the [PqInterval] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public PqInterval intervalAt(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToInterval(buffers[bufferIds[idx]], start, ends[idx] - start);
    }

    /// Decode value `idx` as the instant its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload stands
    /// for, reading the bytes where they sit rather than materialising a `byte[]` for them.
    public Instant flba12InstantAt(int idx, LogicalType.TimeUnit unit) {
        ensureViews();
        int start = starts[idx];
        return Flba12Timestamps.toInstant(buffers[bufferIds[idx]], start, ends[idx] - start, unit);
    }

    /// Decode value `idx` as the instant its legacy `INT96` timestamp stands for, reading the
    /// bytes where they sit rather than materialising a `byte[]` for them.
    public Instant int96InstantAt(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.int96ToInstant(buffers[bufferIds[idx]], start, ends[idx] - start);
    }

    /// Decode value `idx` as the wall clock its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload
    /// stands for, reading the bytes where they sit.
    public LocalDateTime flba12LocalDateTimeAt(int idx, LogicalType.TimeUnit unit) {
        ensureViews();
        int start = starts[idx];
        return Flba12Timestamps.toLocalDateTime(buffers[bufferIds[idx]], start, ends[idx] - start, unit);
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
        // A value without an entry id, plain or null, had its view written by its segment,
        // so this reads it without building the deferred views of the batch's dictionary values.
        int start = starts[idx];
        int len = ends[idx] - start;
        return new String(buffers[bufferIds[idx]], start, len, StandardCharsets.UTF_8);
    }

    /// Length in bytes of value `idx`.
    public int lengthAt(int idx) {
        ensureViews();
        return ends[idx] - starts[idx];
    }

    private static IllegalStateException twoDictionaries() {
        return new IllegalStateException("A batch holds values of two column chunks' dictionaries");
    }
}
