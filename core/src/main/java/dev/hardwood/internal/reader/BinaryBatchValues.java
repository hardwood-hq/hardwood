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
/// Value `i` is the view `[starts[i], ends[i])` into `bytes`. Views need not be
/// contiguous or in value order, and several may cover the same bytes: a value from a
/// dictionary page points at its entry in a copy of the dictionary's bytes that the
/// batch holds once ([#viewDictionaryRange]), while a plain value is appended
/// ([#appendAt], [#appendRange]). A null value's view is empty. Bytes outside every view, and bytes
/// at or beyond [#byteCount], are unspecified.
///
/// `bytes` starts empty and grows on demand: a batch whose values all come from one
/// small dictionary holds little more than that dictionary's bytes.
public final class BinaryBatchValues {

    private static final byte[] EMPTY = new byte[0];

    private byte[] bytes;
    private int[] starts;
    private int[] ends;

    /// Bytes of [#bytes] in use; the next appended bytes land here.
    private int byteCount;

    /// Expected bytes per appended value, sizing [#bytes] on the first value append so a
    /// batch of plain values does not grow it many times over.
    private final int bytesPerValueHint;

    /// The dictionary this batch's views draw on, or `null` before its first dictionary value.
    /// A read with a binary column ends its batches at row-group boundaries
    /// ([ColumnWorker#endsBatchesAtRowGroupBoundaries]), so a batch draws on at most one.
    private Dictionary.ByteArrayDictionary viewedDictionary;

    /// Where [#viewedDictionary]'s bytes start in [#bytes] once copied in, `-1` until then.
    private int viewedDictionaryBase;

    /// Bytes of [#viewedDictionary]'s values appended before it is copied in.
    private long appendedDictionaryBytes;

    /// The chunk dictionary backing [#dictIndices], or `null` when no dictionary
    /// page has contributed to this batch (every value then materialises from
    /// [#bytes]). Holds the per-entry `String` cache that lets [#stringAt] reuse
    /// one instance per dictionary entry per chunk, and is what
    /// `ColumnReader.getBinaryDictionary()` exposes.
    public Dictionary.ByteArrayDictionary dictionary;

    /// Per-value dictionary entry index, meaningful only when [#dictionary] is
    /// non-null. `-1` marks a value that must materialise from [#bytes]: a
    /// plain-encoded value or a null position. Allocated lazily
    /// by [#ensureDictionary] when the first dictionary page lands.
    public int[] dictIndices;

    /// An empty batch slot for `capacity` values.
    public BinaryBatchValues(int capacity, int bytesPerValueHint) {
        this(EMPTY, null, null, 0, bytesPerValueHint);
        this.capacity = capacity;
    }

    /// A slot over existing views, for a batch that receives no further values.
    public BinaryBatchValues(byte[] bytes, int[] starts, int[] ends, int byteCount) {
        this(bytes, starts, ends, byteCount, 0);
    }

    private BinaryBatchValues(byte[] bytes, int[] starts, int[] ends, int byteCount, int bytesPerValueHint) {
        this.bytes = bytes;
        this.starts = starts;
        this.ends = ends;
        this.byteCount = byteCount;
        this.bytesPerValueHint = bytesPerValueHint;
        this.capacity = starts != null ? starts.length : 0;
        this.viewsPending = starts == null;
    }

    /// The values this batch can hold. [#starts] and [#ends] are allocated at this size on
    /// their first write.
    private int capacity;

    /// Whether a reader must call [#ensureViews] before reading [#starts] and [#ends]: they are
    /// not allocated yet, or dictionary values were recorded without their views. The accessors
    /// test this one flag per value.
    private boolean viewsPending;

    /// End of the range whose dictionary values were recorded without views
    /// ([#deferDictionaryRange]), `0` when there are none.
    private int deferredEnd;

    /// The column worker's [ViewDemand], set while dictionary values may be deferred, so that
    /// building their views tells the worker to stop deferring.
    ViewDemand viewDemand;

    /// Raised by the first reader that needs the views of deferred dictionary values. From then
    /// on the column's worker builds views as it assembles, on its own thread, so that only the
    /// batches already in flight have theirs built on the reading thread. One instance per worker.
    static final class ViewDemand {
        volatile boolean requested;
    }

    /// The buffer the views point into, valid up to [#byteCount()]. Builds pending views first.
    public byte[] bytes() {
        ensureViews();
        return bytes;
    }

    /// Value `i` starts at `starts()[i]` in [#bytes()]. Builds pending views first.
    public int[] starts() {
        ensureViews();
        return starts;
    }

    /// Value `i` ends at `ends()[i]` in [#bytes()]. Builds pending views first.
    public int[] ends() {
        ensureViews();
        return ends;
    }

    /// Bytes of [#bytes()] in use. Builds pending views first, which may append.
    public int byteCount() {
        ensureViews();
        return byteCount;
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

    /// Allocates [#starts] and [#ends] on their first write.
    private void viewArrays() {
        if (starts == null) {
            starts = new int[capacity];
            ends = new int[capacity];
            viewsPending = deferredEnd > 0;
        }
    }

    /// Readies a reused slot for the next batch: its buffer is kept, its contents are not.
    public void reset() {
        byteCount = 0;
        dictionary = null;
        viewedDictionary = null;
        deferredEnd = 0;
        viewsPending = starts == null;
    }

    /// Records `[destPos, destPos + length)` as dictionary values whose views are built when a
    /// reader first needs them ([#ensureViews]); a null gets its empty view now. A reader that
    /// takes only the entry ids then never pays for the views, nor, if the batch holds no plain
    /// value or null, for their arrays. The ids come from [#recordDictIndices], which the
    /// caller runs for the same range.
    public void deferDictionaryRange(Page.DictionaryByteArrayPage page, int srcPos, int destPos, int length) {
        int[] indices = page.dictIndices();
        for (int i = 0; i < length; i++) {
            if (indices[srcPos + i] < 0) {
                appendEmpty(destPos + i);
            }
        }
        deferredEnd = Math.max(deferredEnd, destPos + length);
        viewsPending = true;
    }

    /// Readies [#starts] and [#ends] for reading: allocates them, and builds the views of the
    /// dictionary values recorded without them. Every reader of the views calls this first.
    public void ensureViews() {
        if (viewsPending) {
            buildViews();
        }
    }

    /// Builds the deferred views by the rule [#viewDictionaryRange] applies as it assembles:
    /// while the deferred values' bytes stay below the dictionary's size they are appended,
    /// otherwise the dictionary is copied in once and the values point into it.
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
        Dictionary.ByteArrayDictionary dict = dictionary;
        int[] entryOffsets = dict.entryOffsets();
        int base = viewBase(dict);
        if (base < 0) {
            long deferredBytes = 0;
            for (int i = 0; i < end; i++) {
                int entry = dictIndices[i];
                if (entry >= 0) {
                    deferredBytes += entryOffsets[entry + 1] - entryOffsets[entry];
                }
            }
            if (appendedDictionaryBytes + deferredBytes < dict.entryBytes().length) {
                appendedDictionaryBytes += deferredBytes;
                for (int i = 0; i < end; i++) {
                    int entry = dictIndices[i];
                    if (entry >= 0) {
                        appendEntry(dict, entry, i);
                    }
                }
                return;
            }
            base = copyIn(0);
        }
        for (int i = 0; i < end; i++) {
            int entry = dictIndices[i];
            if (entry >= 0) {
                starts[i] = base + entryOffsets[entry];
                ends[i] = base + entryOffsets[entry + 1];
            }
        }
    }

    /// The values at `map[0, count)` as a batch of their own, with their dictionary and entry
    /// ids. Consumes this batch: the result takes over its byte buffer, which building the
    /// result's deferred views may append to, so this batch must not be read afterwards.
    /// Dictionary values whose views are deferred stay deferred, so a filtered read builds
    /// views only if it reads them.
    BinaryBatchValues compact(int[] map, int count) {
        BinaryBatchValues out = new BinaryBatchValues(bytes, null, null, byteCount);
        out.capacity = count;
        out.viewArrays();
        int keptDeferredEnd = 0;
        for (int i = 0; i < count; i++) {
            int rawIdx = map[i];
            if (rawIdx < deferredEnd && dictIndices[rawIdx] >= 0) {
                keptDeferredEnd = i + 1;
            }
            else {
                out.starts[i] = starts[rawIdx];
                out.ends[i] = ends[rawIdx];
            }
        }
        out.adoptIds(this, map, count);
        out.viewedDictionary = viewedDictionary;
        out.viewedDictionaryBase = viewedDictionaryBase;
        out.appendedDictionaryBytes = appendedDictionaryBytes;
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
    }

    /// Grows the view arrays (and [#dictIndices], once allocated) to `capacity` values.
    public void growCapacity(int capacity) {
        if (capacity <= this.capacity) {
            return;
        }
        this.capacity = capacity;
        if (starts != null) {
            starts = Arrays.copyOf(starts, capacity);
            ends = Arrays.copyOf(ends, capacity);
        }
        if (dictIndices != null) {
            dictIndices = Arrays.copyOf(dictIndices, capacity);
        }
    }

    /// Materialise value `idx` as a fresh `byte[]` copy. Allocates one array
    /// per call — used by convenience accessors and per-row materialisation
    /// paths; hot loops should read [#bytes()] / [#starts()] / [#ends()] directly.
    public byte[] byteArrayAt(int idx) {
        ensureViews();
        return Arrays.copyOfRange(bytes, starts[idx], ends[idx]);
    }

    /// Decode value `idx` as the single-precision value its `FLOAT16` payload stands for,
    /// reading the two bytes where they sit rather than materialising a `byte[]` for them.
    ///
    /// The float accessors decode here rather than through the generic value conversion
    /// because that path returns `Object` and would box the value they promised unboxed.
    public float float16At(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToFloat16(bytes, start, ends[idx] - start);
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
        return LogicalTypeConverter.bytesToDecimal(bytes, start, ends[idx] - start, scale);
    }

    /// Decode value `idx` as the [UUID] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public UUID uuidAt(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToUuid(bytes, start, ends[idx] - start);
    }

    /// Decode value `idx` as the [PqInterval] its payload stands for, reading the bytes
    /// where they sit rather than materialising a `byte[]` for them.
    public PqInterval intervalAt(int idx) {
        ensureViews();
        int start = starts[idx];
        return LogicalTypeConverter.bytesToInterval(bytes, start, ends[idx] - start);
    }

    /// Decode value `idx` as the instant its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload stands
    /// for, reading the bytes where they sit rather than materialising a `byte[]` for them.
    public Instant flba12InstantAt(int idx, LogicalType.TimeUnit unit) {
        ensureViews();
        int start = starts[idx];
        return Flba12Timestamps.toInstant(bytes, start, ends[idx] - start, unit);
    }

    /// Decode value `idx` as the wall clock its `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` payload
    /// stands for, reading the bytes where they sit.
    public LocalDateTime flba12LocalDateTimeAt(int idx, LogicalType.TimeUnit unit) {
        ensureViews();
        int start = starts[idx];
        return Flba12Timestamps.toLocalDateTime(bytes, start, ends[idx] - start, unit);
    }

    /// Materialise value `idx` as a UTF-8 decoded `String`. A dictionary-encoded
    /// value (when [#dictionary] is set and `dictIndices[idx]` is non-negative)
    /// reuses the chunk dictionary's per-entry interned cache, so repeated values
    /// are decoded once per chunk; any other value allocates one `String` per
    /// call from [#bytes].
    public String stringAt(int idx) {
        Dictionary.ByteArrayDictionary dict = dictionary;
        if (dict != null) {
            int dictIndex = dictIndices[idx];
            if (dictIndex >= 0) {
                return dict.internedString(dictIndex);
            }
        }
        // A value without an entry id, plain or null, had its view written when it was appended,
        // so this reads it without building the deferred views of the batch's dictionary values.
        int start = starts[idx];
        int len = ends[idx] - start;
        return new String(bytes, start, len, StandardCharsets.UTF_8);
    }

    /// Length in bytes of value `idx`.
    public int lengthAt(int idx) {
        ensureViews();
        return ends[idx] - starts[idx];
    }

    /// Appends `len` bytes from `src[srcOffset..)` as value `valueIdx`, growing
    /// [#bytes] if it would otherwise overflow. Throws if the batch's bytes would
    /// overflow `Integer.MAX_VALUE`.
    public void appendAt(int valueIdx, byte[] src, int srcOffset, int len) {
        int start = byteCount;
        ensureBytes((long) start + len, valueIdx, true);
        if (len > 0) {
            System.arraycopy(src, srcOffset, bytes, start, len);
        }
        viewArrays();
        starts[valueIdx] = start;
        ends[valueIdx] = start + len;
        byteCount = start + len;
    }

    /// Records an empty view as value `valueIdx`, as a null value has.
    public void appendEmpty(int valueIdx) {
        viewArrays();
        starts[valueIdx] = byteCount;
        ends[valueIdx] = byteCount;
    }

    /// Appends `values[srcPos, srcPos + length)` as values `[destPos, destPos + length)`,
    /// a `null` giving an empty view. The range form of [#appendAt], keeping the write
    /// position in a local across the range.
    public void appendRange(byte[][] values, int srcPos, int destPos, int length) {
        viewArrays();
        int cursor = byteCount;
        byte[] buffer = bytes;
        for (int i = 0; i < length; i++) {
            byte[] value = values[srcPos + i];
            int dest = destPos + i;
            starts[dest] = cursor;
            if (value != null) {
                int len = value.length;
                if (len > buffer.length - cursor) {
                    ensureBytes((long) cursor + len, dest, true);
                    buffer = bytes;
                }
                System.arraycopy(value, 0, buffer, cursor, len);
                cursor += len;
            }
            ends[dest] = cursor;
        }
        byteCount = cursor;
    }

    /// Sets the views of `[destPos, destPos + length)` to the entries the page's indices
    /// `[srcPos, srcPos + length)` name, `-1` (a null) giving an empty view.
    ///
    /// The values point into one copy of the dictionary's bytes held by this batch. Until
    /// the bytes of the batch's values from a dictionary reach the dictionary's size, those
    /// values are appended instead and the dictionary is not copied in, so a batch holds
    /// at most twice the bytes that appending every value would take.
    public void viewDictionaryRange(Page.DictionaryByteArrayPage page, int srcPos, int destPos, int length) {
        viewArrays();
        Dictionary.ByteArrayDictionary dict = page.dictionary();
        int[] indices = page.dictIndices();
        int[] entryOffsets = dict.entryOffsets();
        int base = viewBase(dict);
        if (base < 0) {
            long rangeBytes = 0;
            for (int i = 0; i < length; i++) {
                int entry = indices[srcPos + i];
                if (entry >= 0) {
                    rangeBytes += entryOffsets[entry + 1] - entryOffsets[entry];
                }
            }
            if (appendedDictionaryBytes + rangeBytes < dict.entryBytes().length) {
                appendedDictionaryBytes += rangeBytes;
                for (int i = 0; i < length; i++) {
                    appendEntry(dict, indices[srcPos + i], destPos + i);
                }
                return;
            }
            base = copyIn(destPos);
        }
        for (int i = 0; i < length; i++) {
            int entry = indices[srcPos + i];
            int dest = destPos + i;
            if (entry < 0) {
                starts[dest] = 0;
                ends[dest] = 0;
            }
            else {
                starts[dest] = base + entryOffsets[entry];
                ends[dest] = base + entryOffsets[entry + 1];
            }
        }
    }

    /// Sets the view of value `destIdx` to the entry the page's index at `srcIdx`
    /// names. The single-value form of [#viewDictionaryRange], for nested assembly.
    public void viewDictionaryValue(Page.DictionaryByteArrayPage page, int srcIdx, int destIdx) {
        viewArrays();
        Dictionary.ByteArrayDictionary dict = page.dictionary();
        int entry = page.dictIndices()[srcIdx];
        if (entry < 0) {
            appendEmpty(destIdx);
            return;
        }
        int[] entryOffsets = dict.entryOffsets();
        int base = viewBase(dict);
        if (base < 0) {
            int len = entryOffsets[entry + 1] - entryOffsets[entry];
            if (appendedDictionaryBytes + len < dict.entryBytes().length) {
                appendedDictionaryBytes += len;
                appendEntry(dict, entry, destIdx);
                return;
            }
            base = copyIn(destIdx);
        }
        starts[destIdx] = base + entryOffsets[entry];
        ends[destIdx] = base + entryOffsets[entry + 1];
    }

    private void appendEntry(Dictionary.ByteArrayDictionary dict, int entry, int destIdx) {
        if (entry < 0) {
            appendEmpty(destIdx);
        }
        else {
            int[] entryOffsets = dict.entryOffsets();
            int from = entryOffsets[entry];
            appendAt(destIdx, dict.entryBytes(), from, entryOffsets[entry + 1] - from);
        }
    }

    /// Returns where `dict`'s bytes start in [#bytes], or `-1` before they are copied in,
    /// adopting `dict` as the batch's [#viewedDictionary] on its first value.
    ///
    /// @throws IllegalStateException if the batch already draws on another dictionary
    private int viewBase(Dictionary.ByteArrayDictionary dict) {
        if (viewedDictionary == null) {
            viewedDictionary = dict;
            viewedDictionaryBase = -1;
            appendedDictionaryBytes = 0;
        }
        else if (viewedDictionary != dict) {
            throw twoDictionaries();
        }
        return viewedDictionaryBase;
    }

    /// Copies [#viewedDictionary] into [#bytes] and returns where it starts; `valueIdx` is the
    /// value that brought it in, named if the copy overflows the buffer.
    private int copyIn(int valueIdx) {
        byte[] entryBytes = viewedDictionary.entryBytes();
        int base = byteCount;
        ensureBytes((long) base + entryBytes.length, valueIdx, false);
        System.arraycopy(entryBytes, 0, bytes, base, entryBytes.length);
        byteCount = base + entryBytes.length;
        viewedDictionaryBase = base;
        return base;
    }

    private static IllegalStateException twoDictionaries() {
        return new IllegalStateException("A batch holds values of two column chunks' dictionaries");
    }

    /// Grows [#bytes] to hold `needed` bytes. A value append sizes a first allocation by
    /// [#bytesPerValueHint]; a dictionary copy takes what it needs.
    private void ensureBytes(long needed, int valueIdx, boolean valueAppend) {
        if (needed > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Binary batch buffer would exceed int32 (~2 GB) at value " + valueIdx
                    + "; reduce the batch size for this column.");
        }
        if (needed > bytes.length) {
            long hint = valueAppend ? (long) bytesPerValueHint * capacity : 0L;
            long newSize = Math.min(Integer.MAX_VALUE, Math.max(Math.max((long) bytes.length * 2L, needed), hint));
            bytes = Arrays.copyOf(bytes, (int) newSize);
        }
    }

    /// Records dictionary entry indices for a contiguous page range
    /// `[srcPos, srcPos + length)` landing at `[destPos, destPos + length)`, so
    /// [#stringAt] can reuse one materialised `String` per entry and
    /// `ColumnReader.getDictionaryIds()` can expose them.
    ///
    /// `pageDictIndices` is `null` for a plain (non-dictionary) page; such
    /// values are recorded as `-1` only once the batch is already on the
    /// dictionary path, since otherwise [#stringAt] reads [#bytes] regardless.
    /// The first dictionary page switches the batch on (see [#ensureDictionary]).
    public void recordDictIndices(int[] pageDictIndices, Dictionary.ByteArrayDictionary pageDict,
                                  int srcPos, int destPos, int length) {
        if (pageDictIndices == null) {
            if (dictionary != null) {
                Arrays.fill(dictIndices, destPos, destPos + length, -1);
            }
            return;
        }
        ensureDictionary(pageDict, destPos);
        System.arraycopy(pageDictIndices, srcPos, dictIndices, destPos, length);
    }

    /// Records the dictionary entry index for a single gathered value at
    /// `destPos`. Used by nested assembly, where kept values are scattered by
    /// the rep/def-level walk rather than copied as a contiguous range. See
    /// [#recordDictIndices] for the range form; the dictionary-switch rules are
    /// identical.
    public void recordDictIndex(int[] pageDictIndices, Dictionary.ByteArrayDictionary pageDict,
                                int srcPos, int destPos) {
        if (pageDictIndices == null) {
            if (dictionary != null) {
                dictIndices[destPos] = -1;
            }
            return;
        }
        // ensureDictionary lazily allocates dictIndices, so it must run before
        // the `dictIndices[destPos]` store target is evaluated — otherwise the
        // store binds the pre-allocation (null) array reference.
        ensureDictionary(pageDict, destPos);
        dictIndices[destPos] = pageDictIndices[srcPos];
    }

    /// Switches the batch onto the dictionary representation on the first
    /// dictionary page that contributes: adopts `pageDict`, allocates
    /// [#dictIndices] (sized to the value capacity), and backfills the plain
    /// prefix `[0, destPos)` with `-1`. A batch of binary values ends at its row
    /// group's end, so every dictionary page of a batch carries the same dictionary.
    ///
    /// @throws IllegalStateException if `pageDict` is not the batch's dictionary
    private void ensureDictionary(Dictionary.ByteArrayDictionary pageDict, int destPos) {
        if (dictionary == null) {
            dictionary = pageDict;
            if (dictIndices == null || dictIndices.length < capacity) {
                dictIndices = new int[capacity];
            }
            Arrays.fill(dictIndices, 0, destPos, -1);
        }
        else if (dictionary != pageDict) {
            throw twoDictionaries();
        }
    }
}
