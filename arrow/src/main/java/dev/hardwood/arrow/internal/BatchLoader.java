/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.arrow.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;

import dev.hardwood.Validity;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Copies the current batch of a [ColumnReaders] into Arrow buffers and wraps them as an
/// [ArrowRecordBatch], the unit an [org.apache.arrow.vector.ipc.ArrowReader] loads into its
/// root.
///
/// Every buffer is allocated at its exact size from the caller's allocator and filled once.
/// A fixed-width column whose Java layout is Arrow's is copied in bulk; the others are
/// converted value by value, writing nothing at a null position, which is left zero.
public final class BatchLoader {

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    /// The ends of the 64-bit nanosecond range, as whole seconds and a nanosecond remainder.
    private static final long MIN_SECONDS = Math.floorDiv(Long.MIN_VALUE, NANOS_PER_SECOND);
    private static final long MIN_REMAINDER = Math.floorMod(Long.MIN_VALUE, NANOS_PER_SECOND);
    private static final long MAX_SECONDS = Long.MAX_VALUE / NANOS_PER_SECOND;
    private static final long MAX_REMAINDER = Long.MAX_VALUE % NANOS_PER_SECOND;
    private static final int INT96_WIDTH = 12;
    private static final int DECIMAL_128_WIDTH = 16;

    /// The views of a batch's binary values: their total length, whether they lie one after
    /// another in value order, and where the first present value starts.
    ///
    /// @param totalLength the bytes the present values hold, as a `long` since the views of
    ///        a dictionary-backed batch can name the same bytes many times over
    /// @param contiguous whether every present value starts where the previous one ended,
    ///        so the values are one run of `totalLength` bytes from `firstStart`
    /// @param firstStart the start of the first present value, or `0` when none is present
    record Views(long totalLength, boolean contiguous, int firstStart) {
    }

    private BatchLoader() {
    }

    /// Loads the current batch of `columns` as a record batch of `plan`'s fields.
    ///
    /// The returned batch holds the only references to its buffers; closing it releases them.
    /// On failure every buffer allocated so far is released before the exception propagates.
    public static ArrowRecordBatch load(ArrowPlan plan, ColumnReaders columns, BufferAllocator allocator) {
        int rows = columns.getRecordCount();
        List<ArrowFieldNode> nodes = new ArrayList<>();
        List<ArrowBuf> buffers = new ArrayList<>();
        try {
            for (ArrowPlan.Node root : plan.roots()) {
                loadNode(root, rows, columns, allocator, nodes, buffers);
            }
            // The batch retains every buffer; the references this method holds are released below.
            return new ArrowRecordBatch(rows, nodes, buffers);
        }
        finally {
            for (ArrowBuf buffer : buffers) {
                buffer.close();
            }
        }
    }

    /// Appends the node and buffers of `node` and its descendants, in the depth-first order
    /// Arrow's `VectorLoader` consumes them. `count` is the number of items at the node's
    /// scope: the batch's records at the top, a list's entries below it.
    private static void loadNode(ArrowPlan.Node node, int count, ColumnReaders columns, BufferAllocator allocator,
            List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        switch (node.shape()) {
            case LEAF -> loadLeaf(node, columns.getColumnReader(node.position()), count, allocator, nodes, buffers);
            case STRUCT -> {
                Validity validity = node.hasLayer()
                        ? columns.getColumnReader(node.position()).getLayerValidity(node.layer())
                        : Validity.NO_NULLS;
                nodes.add(new ArrowFieldNode(count, validity.nullCount(count)));
                buffers.add(validityBuffer(validity, count, allocator));
                for (ArrowPlan.Node child : node.children()) {
                    loadNode(child, count, columns, allocator, nodes, buffers);
                }
            }
            case LIST -> {
                int entries = loadOffsets(node, count, columns, allocator, nodes, buffers);
                loadNode(node.children().get(0), entries, columns, allocator, nodes, buffers);
            }
            case MAP -> {
                int entries = loadOffsets(node, count, columns, allocator, nodes, buffers);
                // The entries struct Arrow puts between a map and its key and value is always present.
                nodes.add(new ArrowFieldNode(entries, 0));
                buffers.add(validityBuffer(Validity.NO_NULLS, entries, allocator));
                loadNode(node.children().get(0), entries, columns, allocator, nodes, buffers);
                loadNode(node.children().get(1), entries, columns, allocator, nodes, buffers);
            }
        }
    }

    /// The validity and offsets of a list or map, from its layer on its anchor leaf, and the
    /// number of entries the offsets end at.
    private static int loadOffsets(ArrowPlan.Node node, int count, ColumnReaders columns, BufferAllocator allocator,
            List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        ColumnReader anchor = columns.getColumnReader(node.position());
        Validity validity = anchor.getLayerValidity(node.layer());
        int[] offsets = anchor.getLayerOffsets(node.layer());
        if (offsets.length != count + 1) {
            throw new IllegalStateException("Column '" + node.field().getName() + "' reports " + (offsets.length - 1)
                    + " items at layer " + node.layer() + " in a scope of " + count);
        }
        nodes.add(new ArrowFieldNode(count, validity.nullCount(count)));
        buffers.add(validityBuffer(validity, count, allocator));
        buffers.add(ints(offsets, count + 1, allocator));
        return offsets[count];
    }

    private static void loadLeaf(ArrowPlan.Node entry, ColumnReader reader, int count, BufferAllocator allocator,
            List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        ColumnSchema column = reader.getColumnSchema();
        int values = reader.getValueCount();
        if (values != count) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' holds " + values
                    + " values in a scope of " + count);
        }
        Validity validity = reader.getLeafValidity();
        if (entry.kind() == ScalarKind.NULL) {
            requireAllNull(column, validity, count);
            nodes.add(new ArrowFieldNode(count, count));
            return;
        }
        nodes.add(new ArrowFieldNode(count, validity.nullCount(count)));
        buffers.add(validityBuffer(validity, count, allocator));
        switch (entry.kind()) {
            case BOOLEAN -> buffers.add(booleans(reader.getBooleans(), validity, count, allocator));
            case INT32 -> buffers.add(ints(reader.getInts(), count, allocator));
            case INT8 -> buffers.add(narrowed(reader, column, validity, count, allocator, 1,
                    Byte.MIN_VALUE, Byte.MAX_VALUE));
            case INT16 -> buffers.add(narrowed(reader, column, validity, count, allocator, 2,
                    Short.MIN_VALUE, Short.MAX_VALUE));
            case UINT8 -> buffers.add(narrowed(reader, column, validity, count, allocator, 1, 0, 0xFF));
            case UINT16 -> buffers.add(narrowed(reader, column, validity, count, allocator, 2, 0, 0xFFFF));
            case INT64 -> buffers.add(longs(reader.getLongs(), count, allocator));
            case FLOAT -> buffers.add(floats(reader.getFloats(), count, allocator));
            case DOUBLE -> buffers.add(doubles(reader.getDoubles(), count, allocator));
            case DECIMAL_INT32 -> buffers.add(decimalLongs(widened(reader.getInts(), count), validity, count, allocator));
            case DECIMAL_INT64 -> buffers.add(decimalLongs(reader.getLongs(), validity, count, allocator));
            case DECIMAL_BYTES -> buffers.add(decimalBytes(reader, entry, column, validity, count, allocator));
            case INT96_TIMESTAMP -> buffers.add(int96Timestamps(reader, column, validity, count, allocator));
            case BINARY -> binary(reader, column, validity, count, allocator, buffers);
            case FIXED_BINARY -> buffers.add(fixedBinary(reader, column, validity, count, allocator));
            case NULL -> throw new IllegalStateException("NULL columns carry no buffers");
        }
    }

    // ==================== Validity ====================

    /// A `NULL`-annotated column holds no value; one that does is malformed, and the row path
    /// refuses it, so the loader does too rather than reporting the value as null.
    static void requireAllNull(ColumnSchema column, Validity validity, int count) {
        int present = validity.nextNotNull(0, count);
        if (present >= 0) {
            throw new ParquetReadException("Column '" + column.fieldPath()
                    + "' is annotated NULL but holds a value at row " + present);
        }
    }

    /// Arrow's validity bitmap has the layout of the reader's: set bit means present, bit `i`
    /// of byte `i / 8`. The words are written little-endian; bits at or past `count` are
    /// cleared, since the reader leaves them undefined. An all-present scope gets an all-set
    /// bitmap of its own rather than the empty buffer Arrow would accept: `VectorLoader`
    /// releases a vector's old bitmap before allocating a replacement for an empty one, so an
    /// allocation that failed there would leave the vector over a released buffer. Allocating
    /// here keeps every allocation in the phase whose failure releases everything.
    private static ArrowBuf validityBuffer(Validity validity, int count, BufferAllocator allocator) {
        int bytes = (count + 7) >>> 3;
        ArrowBuf buffer = allocator.buffer(bytes);
        try {
            long[] words = validity.words();
            if (words == null) {
                byte[] allPresent = new byte[bytes];
                Arrays.fill(allPresent, (byte) 0xFF);
                buffer.setBytes(0, allPresent, 0, bytes);
            }
            else if (bytes > 0) {
                ByteBuffer view = buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN);
                int fullWords = bytes >>> 3;
                for (int w = 0; w < fullWords; w++) {
                    view.putLong(w << 3, words[w]);
                }
                int tailBytes = bytes & 7;
                for (int k = 0; k < tailBytes; k++) {
                    view.put((fullWords << 3) + k, (byte) (words[fullWords] >>> (k << 3)));
                }
            }
            int tailBits = count & 7;
            if (tailBits != 0) {
                int last = bytes - 1;
                buffer.setByte(last, buffer.getByte(last) & ((1 << tailBits) - 1));
            }
            buffer.writerIndex(bytes);
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    // ==================== Fixed-width values ====================

    private static ArrowBuf booleans(boolean[] values, Validity validity, int count, BufferAllocator allocator) {
        int bytes = (count + 7) >>> 3;
        byte[] bits = new byte[bytes];
        for (int i = 0; i < count; i++) {
            if (values[i] && validity.isNotNull(i)) {
                bits[i >>> 3] |= (byte) (1 << (i & 7));
            }
        }
        ArrowBuf buffer = allocator.buffer(bytes);
        buffer.setBytes(0, bits, 0, bytes);
        buffer.writerIndex(bytes);
        return buffer;
    }

    private static ArrowBuf ints(int[] values, int count, BufferAllocator allocator) {
        int bytes = Math.multiplyExact(count, Integer.BYTES);
        ArrowBuf buffer = allocator.buffer(bytes);
        if (bytes > 0) {
            buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().put(values, 0, count);
        }
        buffer.writerIndex(bytes);
        return buffer;
    }

    private static ArrowBuf longs(long[] values, int count, BufferAllocator allocator) {
        int bytes = Math.multiplyExact(count, Long.BYTES);
        ArrowBuf buffer = allocator.buffer(bytes);
        if (bytes > 0) {
            buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer().put(values, 0, count);
        }
        buffer.writerIndex(bytes);
        return buffer;
    }

    private static ArrowBuf floats(float[] values, int count, BufferAllocator allocator) {
        int bytes = Math.multiplyExact(count, Float.BYTES);
        ArrowBuf buffer = allocator.buffer(bytes);
        if (bytes > 0) {
            buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(values, 0, count);
        }
        buffer.writerIndex(bytes);
        return buffer;
    }

    private static ArrowBuf doubles(double[] values, int count, BufferAllocator allocator) {
        int bytes = Math.multiplyExact(count, Double.BYTES);
        ArrowBuf buffer = allocator.buffer(bytes);
        if (bytes > 0) {
            buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer().put(values, 0, count);
        }
        buffer.writerIndex(bytes);
        return buffer;
    }

    /// A zeroed buffer of `count * width` bytes, so a position no value is written at reads
    /// as zero.
    private static ArrowBuf zeroed(int count, int width, BufferAllocator allocator) {
        int bytes = Math.multiplyExact(count, width);
        ArrowBuf buffer = allocator.buffer(bytes);
        buffer.setZero(0, bytes);
        buffer.writerIndex(bytes);
        return buffer;
    }

    private static ArrowBuf narrowed(ColumnReader reader, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator, int width, int min, int max) {
        int[] values = reader.getInts();
        ArrowBuf buffer = zeroed(count, width, allocator);
        try {
            ByteBuffer view = count == 0 ? null
                    : buffer.nioBuffer(0, count * width).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < count; i++) {
                if (validity.isNull(i)) {
                    continue;
                }
                int value = values[i];
                requireInRange(column, i, value, min, max);
                if (width == 1) {
                    view.put(i, (byte) value);
                }
                else {
                    view.putShort(i << 1, (short) value);
                }
            }
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    private static long[] widened(int[] values, int count) {
        long[] longs = new long[count];
        for (int i = 0; i < count; i++) {
            longs[i] = values[i];
        }
        return longs;
    }

    /// Refuses a value outside the range its `INT(8)` or `INT(16)` annotation declares, which a
    /// writer that checked its annotations would not have stored.
    static void requireInRange(ColumnSchema column, int row, int value, int min, int max) {
        if (value < min || value > max) {
            throw new ParquetReadException("Column '" + column.fieldPath() + "' is annotated " + column.logicalType()
                    + " but holds " + value + " at row " + row + ", outside [" + min + ", " + max + "]");
        }
    }

    private static ArrowBuf decimalLongs(long[] values, Validity validity, int count, BufferAllocator allocator) {
        ArrowBuf buffer = zeroed(count, DECIMAL_128_WIDTH, allocator);
        for (int i = 0; i < count; i++) {
            if (validity.isNotNull(i)) {
                putDecimal128(buffer, i, values[i]);
            }
        }
        return buffer;
    }

    /// A 16-byte little-endian two's-complement decimal: the low word, then the sign extension.
    private static void putDecimal128(ArrowBuf buffer, int index, long unscaled) {
        long offset = (long) index * DECIMAL_128_WIDTH;
        buffer.setLong(offset, unscaled);
        buffer.setLong(offset + Long.BYTES, unscaled < 0 ? -1L : 0L);
    }

    private static ArrowBuf decimalBytes(ColumnReader reader, ArrowPlan.Node entry, ColumnSchema column,
            Validity validity, int count, BufferAllocator allocator) {
        int width = ((ArrowType.Decimal) entry.field().getType()).getBitWidth() / Byte.SIZE;
        byte[] bytes = reader.getBinaryValues();
        int[] starts = reader.getBinaryStarts();
        int[] ends = reader.getBinaryEnds();
        ArrowBuf buffer = zeroed(count, width, allocator);
        try {
            byte[] littleEndian = new byte[width];
            for (int i = 0; i < count; i++) {
                if (validity.isNull(i)) {
                    continue;
                }
                int start = starts[i];
                int end = ends[i];
                int length = end - start;
                if (length > width) {
                    throw new ParquetReadException("Column '" + column.fieldPath() + "' is annotated "
                            + column.logicalType() + " but holds a " + length + "-byte value at row " + i
                            + ", wider than the " + width + " bytes its precision allows");
                }
                byte sign = length > 0 && bytes[start] < 0 ? (byte) 0xFF : 0;
                for (int k = 0; k < length; k++) {
                    littleEndian[k] = bytes[end - 1 - k];
                }
                Arrays.fill(littleEndian, length, width, sign);
                buffer.setBytes((long) i * width, littleEndian, 0, width);
            }
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    private static ArrowBuf int96Timestamps(ColumnReader reader, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator) {
        byte[] bytes = reader.getBinaryValues();
        int[] starts = reader.getBinaryStarts();
        int[] ends = reader.getBinaryEnds();
        ByteBuffer source = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        ArrowBuf buffer = zeroed(count, Long.BYTES, allocator);
        try {
            for (int i = 0; i < count; i++) {
                if (validity.isNull(i)) {
                    continue;
                }
                int start = starts[i];
                requireWidth(column, i, ends[i] - start, INT96_WIDTH);
                long nanosOfDay = source.getLong(start);
                int julianDay = source.getInt(start + Long.BYTES);
                buffer.setLong((long) i * Long.BYTES, epochNanos(column, i, julianDay, nanosOfDay));
            }
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    /// Nanoseconds since the epoch of a Julian day and the nanoseconds into it, or a refusal
    /// when the instant lies outside the 584-year span a 64-bit nanosecond count covers.
    ///
    /// The instant is split into whole seconds and a nanosecond remainder and compared with the
    /// range's two ends in that form, since both ends fall inside a second: the product of the
    /// seconds and a billion overflows for the first representable second, while the count it
    /// produces, taken with the remainder, fits. Once the instant is known to be in range, the
    /// wrapping multiplication yields the exact count.
    static long epochNanos(ColumnSchema column, int row, int julianDay, long nanosOfDay) {
        long epochDay = julianDay - LogicalTypeConverter.JULIAN_EPOCH_OFFSET_DAYS;
        long seconds = epochDay * SECONDS_PER_DAY + Math.floorDiv(nanosOfDay, NANOS_PER_SECOND);
        long remainder = Math.floorMod(nanosOfDay, NANOS_PER_SECOND);
        boolean belowRange = seconds < MIN_SECONDS || (seconds == MIN_SECONDS && remainder < MIN_REMAINDER);
        boolean aboveRange = seconds > MAX_SECONDS || (seconds == MAX_SECONDS && remainder > MAX_REMAINDER);
        if (belowRange || aboveRange) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath() + "' holds an INT96 timestamp at row "
                    + row + " (Julian day " + julianDay + ", " + nanosOfDay
                    + " nanoseconds into it) outside the range of an Arrow timestamp in nanoseconds");
        }
        return seconds * NANOS_PER_SECOND + remainder;
    }

    private static ArrowBuf fixedBinary(ColumnReader reader, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator) {
        int width = column.typeLength();
        byte[] bytes = reader.getBinaryValues();
        int[] starts = reader.getBinaryStarts();
        int[] ends = reader.getBinaryEnds();
        ArrowBuf buffer = zeroed(count, width, allocator);
        try {
            for (int i = 0; i < count; i++) {
                if (validity.isNull(i)) {
                    continue;
                }
                int start = starts[i];
                requireWidth(column, i, ends[i] - start, width);
                buffer.setBytes((long) i * width, bytes, start, width);
            }
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    private static void requireWidth(ColumnSchema column, int row, int length, int width) {
        if (length != width) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' holds a " + length
                    + "-byte value at row " + row + " where every value is " + width + " bytes wide");
        }
    }

    // ==================== Variable-length values ====================

    private static void binary(ColumnReader reader, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator, List<ArrowBuf> buffers) {
        byte[] bytes = reader.getBinaryValues();
        int[] starts = reader.getBinaryStarts();
        int[] ends = reader.getBinaryEnds();
        Views views = views(starts, ends, validity, count);
        int dataLength = requireAddressable(views.totalLength(), column);
        int offsetsLength = Math.multiplyExact(count + 1, Integer.BYTES);
        ArrowBuf offsets = allocator.buffer(offsetsLength);
        buffers.add(offsets);
        ArrowBuf data = allocator.buffer(dataLength);
        buffers.add(data);
        IntBuffer offsetView = offsets.nioBuffer(0, offsetsLength).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        if (views.contiguous() && dataLength > 0) {
            data.setBytes(0, bytes, views.firstStart(), dataLength);
        }
        int position = 0;
        offsetView.put(0, 0);
        for (int i = 0; i < count; i++) {
            if (validity.isNotNull(i)) {
                int length = ends[i] - starts[i];
                if (!views.contiguous() && length > 0) {
                    data.setBytes(position, bytes, starts[i], length);
                }
                position += length;
            }
            offsetView.put(i + 1, position);
        }
        offsets.writerIndex(offsetsLength);
        data.writerIndex(dataLength);
    }

    /// Measures the present values' views: the bytes they hold in total, and whether they
    /// follow one another in value order, as the views of a batch no dictionary reached do.
    static Views views(int[] starts, int[] ends, Validity validity, int count) {
        long total = 0;
        boolean contiguous = true;
        int firstStart = 0;
        int expectedStart = -1;
        for (int i = 0; i < count; i++) {
            if (validity.isNull(i)) {
                continue;
            }
            int start = starts[i];
            if (expectedStart < 0) {
                firstStart = start;
            }
            else if (start != expectedStart) {
                contiguous = false;
            }
            expectedStart = ends[i];
            total += ends[i] - start;
        }
        return new Views(total, contiguous, firstStart);
    }

    /// The length as an `int`, or a refusal: Arrow's `Binary` and `Utf8` offsets are 32-bit,
    /// so one batch's values must fit in a buffer of at most `Integer.MAX_VALUE` bytes.
    static int requireAddressable(long totalLength, ColumnSchema column) {
        if (totalLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' holds " + totalLength
                    + " bytes of binary values in one batch, more than the " + Integer.MAX_VALUE
                    + " bytes an Arrow buffer with 32-bit offsets addresses: read with a smaller batch size");
        }
        return (int) totalLength;
    }
}
