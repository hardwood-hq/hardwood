/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;
import java.util.function.Supplier;

import dev.hardwood.internal.encoding.BinaryDictionaryEncoder;
import dev.hardwood.internal.encoding.ByteStreamSplitEncoder;
import dev.hardwood.internal.encoding.DeltaByteArrayEncoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.writer.ColumnEncoding;

/// [ValueEncoder] for binary columns. Serves both `BYTE_ARRAY` (variable length, a 4-byte
/// length prefix per value) and `FIXED_LEN_BYTE_ARRAY` (a schema-declared fixed width, no
/// prefix), selected by `typeLength` (`null` for `BYTE_ARRAY`). Values intern through a
/// content-keyed [BinaryDictionaryEncoder], and statistics use unsigned lexicographic order with
/// `BYTE_ARRAY` bound truncation.
final class BinaryValueEncoder extends ValueEncoder {

    /// This chunk's stored values, packed end to end: value `i` occupies
    /// `plainData[plainOffsets[i], plainOffsets[i + 1])`. The bytes are copied rather than
    /// referenced, both because the caller owns its arrays once `writeBatch` returns and because
    /// one buffer per row group costs far less than one object per value.
    private final ByteArrayBuilder plainData = new ByteArrayBuilder();
    private int[] plainOffsets;
    private int plainCount;
    /// What this chunk's present values would occupy `PLAIN`-encoded, accumulated as each is read
    /// rather than recomputed: a variable-width value's size is the one the schema cannot state.
    private long plainValueBits;

    private final byte[][] window;
    private final BinaryDictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final Supplier<BinaryStatistics> statisticsFactory;
    private BinaryStatistics statistics;
    private final Integer typeLength; // null for BYTE_ARRAY, the fixed width for FIXED_LEN_BYTE_ARRAY

    private BinaryColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    BinaryValueEncoder(boolean buildDictionary, Integer typeLength, int startingCapacity,
                       Supplier<BinaryStatistics> statisticsFactory) {
        this.plainOffsets = new int[startingCapacity + 1];
        this.window = new byte[windowCapacity(startingCapacity)][];
        this.dictionary = buildDictionary ? new BinaryDictionaryEncoder() : null;
        // FIXED_LEN_BYTE_ARRAY bounds are written whole and always exact — a fixed width already
        // bounds them — so only BYTE_ARRAY truncates. Integer.MAX_VALUE disables truncation, since
        // no value can be longer than that.
        this.statisticsFactory = statisticsFactory;
        this.statistics = statisticsFactory.get();
        this.typeLength = typeLength;
    }

    private boolean fixedLength() {
        return typeLength != null;
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (BinaryColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private byte[] valueAt(int index) {
        if (index >= windowBase + windowLength) {
            windowBase = index;
            windowLength = Math.min(window.length, size - index);
            source.copyInto(windowBase, window, 0, windowLength);
        }
        return window[index - windowBase];
    }

    @Override
    boolean dictionaryCapable() {
        return dictionary != null;
    }

    @Override
    int intern(int valueIndex) {
        byte[] value = valueAt(valueIndex);
        plainValueBits += (long) (Integer.BYTES + value.length) * Byte.SIZE;
        return dictionary.intern(value);
    }

    @Override
    int dictionarySize() {
        return dictionary.size();
    }

    @Override
    long exactDistinctCount() {
        return dictionary != null ? dictionary.size() : UNKNOWN_DISTINCT_COUNT;
    }

    @Override
    byte[] encodeDictionaryBody() {
        return fixedLength()
                ? PlainEncoder.encodeFixedLenByteArrays(dictionary.values(), 0, dictionary.size(), typeLength)
                : PlainEncoder.encodeByteArrays(dictionary.values(), 0, dictionary.size());
    }

    @Override
    void startChunk() {
        statistics = statisticsFactory.get();
        if (dictionary != null) {
            dictionary.clear();
        }
        plainCount = 0;
        plainValueBits = 0;
        plainData.reset();
    }

    @Override
    void store(int valueIndex) {
        byte[] value = valueAt(valueIndex);
        plainValueBits += (long) (Integer.BYTES + value.length) * Byte.SIZE;
        append(value);
    }

    @Override
    void storeDictionaryValue(int dictionaryIndex) {
        append(dictionary.values()[dictionaryIndex]);
    }

    @Override
    long dictionaryPlainBytes() {
        // The dictionary body is PLAIN: raw bytes for FIXED_LEN_BYTE_ARRAY, each value behind a
        // 4-byte length prefix for BYTE_ARRAY.
        return dictionary.contentBytes() + (fixedLength() ? 0L : (long) Integer.BYTES * dictionary.size());
    }

    @Override
    void dropDictionary() {
        dictionary.clear();
    }

    private void append(byte[] value) {
        if (plainCount + 1 == plainOffsets.length) {
            plainOffsets = Arrays.copyOf(plainOffsets, grownCapacity(plainOffsets.length));
        }
        plainData.write(value, 0, value.length);
        plainOffsets[++plainCount] = plainData.length();
    }

    @Override
    void encodeInto(ByteArrayBuilder out, ColumnEncoding encoding, int from, int count) {
        switch (encoding) {
            case PLAIN -> {
                int length = fixedLength()
                        ? PlainEncoder.fixedWidthLength(count, typeLength)
                        : PlainEncoder.byteArraysLength(plainOffsets, from, count);
                int at = out.reserve(length);
                if (fixedLength()) {
                    PlainEncoder.encodeFixedLenByteArrays(plainData.array(), plainOffsets, from, count,
                            typeLength, out.array(), at);
                }
                else {
                    PlainEncoder.encodeByteArrays(plainData.array(), plainOffsets, from, count, out.array(), at);
                }
            }
            // The values are stored end to end and every one of them is typeLength bytes, so
            // the page's range is already the contiguous run the split reads.
            case BYTE_STREAM_SPLIT -> {
                int width = requireFixedLength(encoding);
                int at = out.reserve(PlainEncoder.fixedWidthLength(count, width));
                ByteStreamSplitEncoder.encode(plainData.array(), plainOffsets[from], count, width,
                        out.array(), at);
            }
            case DELTA_LENGTH_BYTE_ARRAY ->
                out.writeBytes(DeltaLengthByteArrayEncoder.encode(plainData.array(), plainOffsets, from, count));
            case DELTA_BYTE_ARRAY ->
                out.writeBytes(DeltaByteArrayEncoder.encode(plainData.array(), plainOffsets, from, count));
            default -> throw unsupported(encoding, physicalType());
        }
    }

    private PhysicalType physicalType() {
        return fixedLength() ? PhysicalType.FIXED_LEN_BYTE_ARRAY : PhysicalType.BYTE_ARRAY;
    }

    private int requireFixedLength(ColumnEncoding encoding) {
        if (!fixedLength()) {
            throw unsupported(encoding, PhysicalType.BYTE_ARRAY);
        }
        return typeLength;
    }

    @Override
    void stat(int valueIndex) {
        statistics.accept(valueAt(valueIndex));
    }

    @Override
    void statNull() {
        statistics.acceptNull();
    }

    @Override
    Statistics statistics() {
        return statistics.toStatistics();
    }

    @Override
    long uniformValueBits() {
        return fixedLength() ? (long) typeLength * Byte.SIZE : VARIABLE_VALUE_BITS;
    }

    /// The packed store's own offsets, which is where a variable-width value's size comes from
    /// once the batch it arrived in is gone.
    @Override
    int[] storedValueOffsets() {
        return fixedLength() ? null : plainOffsets;
    }

    @Override
    long plainValueBits(long presentValues) {
        // A fixed width is arithmetic; a variable one is the total accumulated as the values
        // were read, each behind the 4-byte length prefix `PLAIN` puts in front of it.
        return fixedLength() ? presentValues * typeLength * Byte.SIZE : plainValueBits;
    }

    @Override
    long retainedBytes() {
        // The packed value bytes and one offset per stored value, plus the dictionary where the
        // chunk is still interning into one.
        return plainData.length() + (long) plainCount * Integer.BYTES
                + (dictionary == null ? 0 : dictionary.retainedBytes());
    }

    @Override
    long maxRetainedBytesPerValue() {
        // A fixed width is the schema's to state; a variable one is read from the batch.
        return fixedLength()
                ? Math.max(typeLength, INDEX_BYTES + BINARY_DICTIONARY_BYTES_PER_ENTRY + typeLength)
                : VARIABLE_RETAINED_BYTES;
    }

    @Override
    long contentBytes(long presentValues) {
        // The values without the length prefix each carries in plainValueBits.
        return fixedLength()
                ? presentValues * typeLength
                : plainValueBits / Byte.SIZE - presentValues * Integer.BYTES;
    }

    @Override
    long contentBytesFor(ColumnSource source, int leafFrom, int leafCount) {
        if (fixedLength()) {
            return (long) leafCount * typeLength;
        }
        BinaryColumnSource binary = (BinaryColumnSource) source;
        long bytes = 0;
        for (int i = leafFrom; i < leafFrom + leafCount; i++) {
            bytes += binary.valueBytesAt(i);
        }
        return bytes;
    }

    /// What a variable-width value retains beyond its own bytes: an offset in the store, an index
    /// in the chunk's index stream, and the fixed part of a dictionary entry.
    static long variableValueOverheadBytes() {
        return Integer.BYTES + INDEX_BYTES + BINARY_DICTIONARY_BYTES_PER_ENTRY;
    }
}
