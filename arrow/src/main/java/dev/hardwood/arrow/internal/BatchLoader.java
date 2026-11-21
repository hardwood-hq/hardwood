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
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;

import dev.hardwood.Validity;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.reader.BinaryValues;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Copies the current batch of a [ColumnReaders] into an [ArrowRecordBatch] backed by exact-sized
/// off-heap buffers from the caller's [BufferAllocator].
public final class BatchLoader {

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final long MIN_SECONDS = Math.floorDiv(Long.MIN_VALUE, NANOS_PER_SECOND);
    private static final long MIN_REMAINDER = Math.floorMod(Long.MIN_VALUE, NANOS_PER_SECOND);
    private static final long MAX_SECONDS = Long.MAX_VALUE / NANOS_PER_SECOND;
    private static final long MAX_REMAINDER = Long.MAX_VALUE % NANOS_PER_SECOND;
    private static final int INT96_WIDTH = 12;
    private static final int DECIMAL_128_WIDTH = 16;

    record Views(long totalLength, boolean contiguous, byte[] singleBuffer, int firstStart) {
    }

    private BatchLoader() {
    }

    /// Loads the current batch of `columns` into an [ArrowRecordBatch]. Releases every allocated
    /// buffer if any step fails.
    public static ArrowRecordBatch load(ArrowPlan plan, ColumnReaders columns, BufferAllocator allocator) {
        int rows = columns.getRecordCount();
        List<ArrowFieldNode> nodes = new ArrayList<>();
        List<ArrowBuf> buffers = new ArrayList<>();
        try {
            for (ArrowPlan.Node root : plan.roots()) {
                loadNode(root, rows, columns, allocator, nodes, buffers);
            }
            return new ArrowRecordBatch(rows, nodes, buffers);
        }
        finally {
            for (ArrowBuf buffer : buffers) {
                buffer.close();
            }
        }
    }

    // ==================== Record batch walk ====================

    private static void loadNode(ArrowPlan.Node node, int count, ColumnReaders columns, BufferAllocator allocator,
            List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        switch (node.shape()) {
            case LEAF -> loadLeaf(node, columns.getColumnReader(node.position()), count, allocator, nodes, buffers);
            case STRUCT -> {
                Validity validity = node.hasLayer() && node.field().isNullable()
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
                nodes.add(new ArrowFieldNode(entries, 0));
                buffers.add(validityBuffer(Validity.NO_NULLS, entries, allocator));
                loadNode(node.children().get(0), entries, columns, allocator, nodes, buffers);
                loadNode(node.children().get(1), entries, columns, allocator, nodes, buffers);
            }
        }
    }

    private static int loadOffsets(ArrowPlan.Node node, int count, ColumnReaders columns, BufferAllocator allocator,
            List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
        ColumnReader anchor = columns.getColumnReader(node.position());
        Validity arrowValidity = node.field().isNullable()
                ? anchor.getLayerValidity(node.layer())
                : Validity.NO_NULLS;
        int[] offsets = anchor.getLayerOffsets(node.layer());
        if (offsets.length != count + 1) {
            throw new IllegalStateException("Column '" + node.field().getName() + "' reports " + (offsets.length - 1)
                    + " items at layer " + node.layer() + " in a scope of " + count);
        }
        nodes.add(new ArrowFieldNode(count, arrowValidity.nullCount(count)));
        buffers.add(validityBuffer(arrowValidity, count, allocator));
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
        Validity sourceValidity = reader.getLeafValidity();
        if (entry.kind() == ScalarKind.NULL) {
            requireAllNull(column, sourceValidity, count);
            nodes.add(new ArrowFieldNode(count, count));
            return;
        }
        // A REQUIRED child of an OPTIONAL struct is non-nullable in Arrow: emit nullCount=0 and an
        // all-set bitmap, while sourceValidity still guards parent-null placeholder slots.
        Validity arrowValidity = entry.field().isNullable() ? sourceValidity : Validity.NO_NULLS;
        nodes.add(new ArrowFieldNode(count, arrowValidity.nullCount(count)));
        buffers.add(validityBuffer(arrowValidity, count, allocator));
        switch (entry.kind()) {
            case BOOLEAN -> buffers.add(booleans(reader.getBooleans(), sourceValidity, count, allocator));
            case INT32 -> buffers.add(ints(reader.getInts(), count, allocator));
            case INT8 -> buffers.add(narrowed(reader.getInts(), column, sourceValidity, count, allocator, 1,
                    Byte.MIN_VALUE, Byte.MAX_VALUE));
            case INT16 -> buffers.add(narrowed(reader.getInts(), column, sourceValidity, count, allocator, 2,
                    Short.MIN_VALUE, Short.MAX_VALUE));
            case UINT8 -> buffers.add(narrowed(reader.getInts(), column, sourceValidity, count, allocator, 1, 0, 0xFF));
            case UINT16 -> buffers.add(narrowed(reader.getInts(), column, sourceValidity, count, allocator, 2, 0, 0xFFFF));
            case INT64 -> buffers.add(longs(reader.getLongs(), count, allocator));
            case FLOAT -> buffers.add(floats(reader.getFloats(), count, allocator));
            case DOUBLE -> buffers.add(doubles(reader.getDoubles(), count, allocator));
            case DECIMAL_INT32 -> buffers.add(decimal128(reader.getInts(), null, sourceValidity, count, allocator));
            case DECIMAL_INT64 -> buffers.add(decimal128(null, reader.getLongs(), sourceValidity, count, allocator));
            case DECIMAL_BYTES -> buffers.add(decimalBytes(reader.getBinaryValues(), entry, column, sourceValidity,
                    count, allocator));
            case INT96_TIMESTAMP -> buffers.add(int96Timestamps(reader.getBinaryValues(), column, sourceValidity,
                    count, allocator));
            case BINARY -> binary(reader.getBinaryValues(), column, sourceValidity, count, allocator, buffers);
            case FIXED_BINARY -> buffers.add(fixedBinary(reader.getBinaryValues(), column, sourceValidity, count,
                    allocator));
            case NULL -> throw new IllegalStateException("NULL columns carry no buffers");
        }
    }

    // ==================== Buffer allocation ====================

    private static ArrowBuf fillBuffer(int bytes, BufferAllocator allocator, Consumer<ByteBuffer> fill) {
        ArrowBuf buffer = allocator.buffer(bytes);
        try {
            if (bytes > 0) {
                fill.accept(buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN));
            }
            buffer.writerIndex(bytes);
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    private static ArrowBuf fillZeroed(int count, int width, BufferAllocator allocator, Consumer<ByteBuffer> fill) {
        int bytes = Math.multiplyExact(count, width);
        ArrowBuf buffer = allocator.buffer(bytes);
        try {
            if (bytes > 0) {
                buffer.setZero(0, bytes);
                fill.accept(buffer.nioBuffer(0, bytes).order(ByteOrder.LITTLE_ENDIAN));
            }
            buffer.writerIndex(bytes);
            return buffer;
        }
        catch (RuntimeException e) {
            buffer.close();
            throw e;
        }
    }

    // ==================== Validity ====================

    static void requireAllNull(ColumnSchema column, Validity validity, int count) {
        int present = validity.nextNotNull(0, count);
        if (present >= 0) {
            throw new ParquetReadException("Column '" + column.fieldPath()
                    + "' is annotated UNKNOWN but holds a value at row " + present);
        }
    }

    /// Copies the validity bitmap (or fills all-set words for `Validity.NO_NULLS`) directly into
    /// off-heap memory and clears tail bits past `count`.
    private static ArrowBuf validityBuffer(Validity validity, int count, BufferAllocator allocator) {
        int bytes = (count + 7) >>> 3;
        return fillBuffer(bytes, allocator, view -> {
            long[] words = validity.words();
            int fullWords = bytes >>> 3;
            int tailBytes = bytes & 7;
            if (words == null) {
                for (int w = 0; w < fullWords; w++) {
                    view.putLong(w << 3, -1L);
                }
                for (int k = 0; k < tailBytes; k++) {
                    view.put((fullWords << 3) + k, (byte) 0xFF);
                }
            }
            else {
                if (fullWords > 0) {
                    view.asLongBuffer().put(words, 0, fullWords);
                }
                for (int k = 0; k < tailBytes; k++) {
                    view.put((fullWords << 3) + k, (byte) (words[fullWords] >>> (k << 3)));
                }
            }
            int tailBits = count & 7;
            if (tailBits != 0) {
                int last = bytes - 1;
                view.put(last, (byte) (view.get(last) & ((1 << tailBits) - 1)));
            }
        });
    }

    // ==================== Fixed-width values ====================

    private static ArrowBuf ints(int[] values, int count, BufferAllocator allocator) {
        return fillBuffer(Math.multiplyExact(count, Integer.BYTES), allocator,
                view -> view.asIntBuffer().put(values, 0, count));
    }

    private static ArrowBuf longs(long[] values, int count, BufferAllocator allocator) {
        return fillBuffer(Math.multiplyExact(count, Long.BYTES), allocator,
                view -> view.asLongBuffer().put(values, 0, count));
    }

    private static ArrowBuf floats(float[] values, int count, BufferAllocator allocator) {
        return fillBuffer(Math.multiplyExact(count, Float.BYTES), allocator,
                view -> view.asFloatBuffer().put(values, 0, count));
    }

    private static ArrowBuf doubles(double[] values, int count, BufferAllocator allocator) {
        return fillBuffer(Math.multiplyExact(count, Double.BYTES), allocator,
                view -> view.asDoubleBuffer().put(values, 0, count));
    }

    private static ArrowBuf booleans(boolean[] values, Validity validity, int count, BufferAllocator allocator) {
        int bytes = (count + 7) >>> 3;
        return fillBuffer(bytes, allocator, view -> {
            boolean hasNulls = validity.hasNulls();
            for (int b = 0; b < bytes; b++) {
                int base = b << 3;
                int limit = Math.min(8, count - base);
                int mask = 0;
                for (int bit = 0; bit < limit; bit++) {
                    int idx = base + bit;
                    if (values[idx] && (!hasNulls || validity.isNotNull(idx))) {
                        mask |= 1 << bit;
                    }
                }
                view.put(b, (byte) mask);
            }
        });
    }

    private static ArrowBuf narrowed(int[] values, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator, int width, int min, int max) {
        return fillZeroed(count, width, allocator, view -> {
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < count; i++) {
                if (hasNulls && validity.isNull(i)) {
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
        });
    }

    static void requireInRange(ColumnSchema column, int row, int value, int min, int max) {
        if (value < min || value > max) {
            throw new ParquetReadException("Column '" + column.fieldPath() + "' is annotated " + column.logicalType()
                    + " but holds " + value + " at row " + row + ", outside [" + min + ", " + max + "]");
        }
    }

    private static ArrowBuf decimal128(int[] intValues, long[] longValues, Validity validity, int count,
            BufferAllocator allocator) {
        return fillZeroed(count, DECIMAL_128_WIDTH, allocator, view -> {
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < count; i++) {
                if (!hasNulls || validity.isNotNull(i)) {
                    long unscaled = intValues != null ? intValues[i] : longValues[i];
                    int offset = i * DECIMAL_128_WIDTH;
                    view.putLong(offset, unscaled);
                    view.putLong(offset + Long.BYTES, unscaled < 0 ? -1L : 0L);
                }
            }
        });
    }

    private static ArrowBuf decimalBytes(BinaryValues values, ArrowPlan.Node entry, ColumnSchema column,
            Validity validity, int count, BufferAllocator allocator) {
        int width = ((ArrowType.Decimal) entry.field().getType()).getBitWidth() / Byte.SIZE;
        return fillZeroed(count, width, allocator, view -> {
            byte[] littleEndian = new byte[width];
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < count; i++) {
                if (hasNulls && validity.isNull(i)) {
                    continue;
                }
                byte[] bytes = values.buffer(i);
                int start = values.start(i);
                int end = values.end(i);
                int length = end - start;
                if (length > width) {
                    throw new ParquetReadException("Column '" + column.fieldPath() + "' is annotated "
                            + column.logicalType() + " but holds a " + length + "-byte value at row " + i
                            + ", wider than the " + width + "-byte Arrow decimal");
                }
                byte sign = length > 0 && bytes[start] < 0 ? (byte) 0xFF : 0;
                for (int k = 0; k < length; k++) {
                    littleEndian[k] = bytes[end - 1 - k];
                }
                Arrays.fill(littleEndian, length, width, sign);
                view.put(i * width, littleEndian, 0, width);
            }
        });
    }

    private static ArrowBuf int96Timestamps(BinaryValues values, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator) {
        return fillZeroed(count, Long.BYTES, allocator, dest -> {
            boolean hasNulls = validity.hasNulls();
            byte[] lastBuffer = null;
            ByteBuffer source = null;
            for (int i = 0; i < count; i++) {
                if (hasNulls && validity.isNull(i)) {
                    continue;
                }
                byte[] bytes = values.buffer(i);
                if (bytes != lastBuffer) {
                    lastBuffer = bytes;
                    source = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
                }
                int start = values.start(i);
                requireWidth(column, i, values.end(i) - start, INT96_WIDTH);
                long nanosOfDay = source.getLong(start);
                int julianDay = source.getInt(start + Long.BYTES);
                dest.putLong(i * Long.BYTES, epochNanos(column, i, julianDay, nanosOfDay));
            }
        });
    }

    static long epochNanos(ColumnSchema column, int row, int julianDay, long nanosOfDay) {
        long epochDay = julianDay - LogicalTypeConverter.JULIAN_EPOCH_OFFSET_DAYS;
        long seconds = epochDay * SECONDS_PER_DAY + Math.floorDiv(nanosOfDay, NANOS_PER_SECOND);
        long remainder = Math.floorMod(nanosOfDay, NANOS_PER_SECOND);
        boolean belowRange = seconds < MIN_SECONDS || (seconds == MIN_SECONDS && remainder < MIN_REMAINDER);
        boolean aboveRange = seconds > MAX_SECONDS || (seconds == MAX_SECONDS && remainder > MAX_REMAINDER);
        if (belowRange || aboveRange) {
            throw new UnsupportedOperationException("Column '" + column.fieldPath() + "' holds an INT96 timestamp at "
                    + "row " + row + " (Julian day " + julianDay + ", " + nanosOfDay
                    + " nanoseconds into it) outside the range of an Arrow timestamp in nanoseconds");
        }
        return seconds * NANOS_PER_SECOND + remainder;
    }

    private static ArrowBuf fixedBinary(BinaryValues values, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator) {
        int width = column.typeLength();
        return fillZeroed(count, width, allocator, dest -> {
            boolean hasNulls = validity.hasNulls();
            for (int i = 0; i < count; i++) {
                if (hasNulls && validity.isNull(i)) {
                    continue;
                }
                int start = values.start(i);
                requireWidth(column, i, values.end(i) - start, width);
                dest.put(i * width, values.buffer(i), start, width);
            }
        });
    }

    private static void requireWidth(ColumnSchema column, int row, int length, int width) {
        if (length != width) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' holds a " + length
                    + "-byte value at row " + row + " where every value is " + width + " bytes wide");
        }
    }

    // ==================== Variable-length values ====================

    private static void binary(BinaryValues values, ColumnSchema column, Validity validity, int count,
            BufferAllocator allocator, List<ArrowBuf> buffers) {
        Views views = views(values::buffer, values::start, values::end, validity, count);
        int dataLength = requireAddressable(views.totalLength(), column);
        int offsetsLength = Math.multiplyExact(count + 1, Integer.BYTES);
        ArrowBuf offsets = allocator.buffer(offsetsLength);
        buffers.add(offsets);
        ArrowBuf data = allocator.buffer(dataLength);
        buffers.add(data);
        IntBuffer offsetView = offsets.nioBuffer(0, offsetsLength).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        ByteBuffer dataView = dataLength == 0 ? null : data.nioBuffer(0, dataLength);
        if (views.contiguous() && dataLength > 0) {
            dataView.put(0, views.singleBuffer(), views.firstStart(), dataLength);
        }
        boolean hasNulls = validity.hasNulls();
        int position = 0;
        offsetView.put(0, 0);
        for (int i = 0; i < count; i++) {
            if (!hasNulls || validity.isNotNull(i)) {
                int start = values.start(i);
                int length = values.end(i) - start;
                if (!views.contiguous() && length > 0) {
                    dataView.put(position, values.buffer(i), start, length);
                }
                position += length;
            }
            offsetView.put(i + 1, position);
        }
        offsets.writerIndex(offsetsLength);
        data.writerIndex(dataLength);
    }

    static Views views(IntFunction<byte[]> buffers, IntUnaryOperator starts, IntUnaryOperator ends,
            Validity validity, int count) {
        long total = 0;
        boolean contiguous = true;
        byte[] singleBuffer = null;
        int firstStart = 0;
        int expectedStart = -1;
        boolean hasNulls = validity.hasNulls();
        for (int i = 0; i < count; i++) {
            if (hasNulls && validity.isNull(i)) {
                continue;
            }
            byte[] buf = buffers.apply(i);
            int start = starts.applyAsInt(i);
            if (expectedStart < 0) {
                singleBuffer = buf;
                firstStart = start;
            }
            else if (buf != singleBuffer || start != expectedStart) {
                contiguous = false;
            }
            expectedStart = ends.applyAsInt(i);
            total += expectedStart - start;
        }
        return new Views(total, contiguous, singleBuffer, firstStart);
    }

    static int requireAddressable(long totalLength, ColumnSchema column) {
        if (totalLength > Integer.MAX_VALUE) {
            throw new IllegalStateException("Column '" + column.fieldPath() + "' holds " + totalLength
                    + " bytes of binary values in one batch, more than the " + Integer.MAX_VALUE
                    + " bytes an Arrow buffer with 32-bit offsets addresses: read with a smaller batch size");
        }
        return (int) totalLength;
    }
}
