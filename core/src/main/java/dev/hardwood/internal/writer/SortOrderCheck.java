/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.List;

import dev.hardwood.Validity;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.metadata.SortingColumnIndex;
import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.internal.schema.AnnotationKind;
import dev.hardwood.internal.schema.AnnotationPairings;
import dev.hardwood.internal.schema.ByteColumnOrder;
import dev.hardwood.metadata.SortingColumn;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Holds a row group's records to the sort order its sorting columns declare, failing the write
/// at the first record that sorts before the one ahead of it.
///
/// Records are compared lexicographically over the sorting columns, one column at a time across
/// a slice: the first column compares every record with its predecessor, and each later column
/// only the records the columns before it left tied. So a sort whose leading column is mostly
/// distinct costs one comparison per record, whatever the number of columns.
///
/// A sorting column repeats nowhere on its path, so its value for a record sits at the record's
/// own position in its source and its nulls are the shredder's [RecordShredder#presence]. Each
/// column compares in its column order, the one its statistics use, with a `NaN` sorting after
/// every number and `-0.0` equal to `0.0`.
///
/// The previous record is carried across slices and batches and dropped when a row group is cut,
/// since the order is declared per row group.
public final class SortOrderCheck {

    /// Values read from a source at a time.
    private static final int CHUNK = 1024;

    private final List<SortingColumn> declared;
    private final Key[] keys;
    /// Per record of the slice, whether it equals its predecessor on every column compared so far.
    private boolean[] tied = new boolean[CHUNK];
    private boolean hasPrevious;
    private boolean checkedSinceCut;
    private int rowGroupOrdinal;

    /// @param schema the file schema
    /// @param declared the sorting columns as configured
    /// @param resolved the same columns resolved against `schema`, in declared order
    public SortOrderCheck(FileSchema schema, List<SortingColumn> declared, List<SortingColumnIndex> resolved) {
        this.declared = List.copyOf(declared);
        this.keys = new Key[resolved.size()];
        for (int k = 0; k < keys.length; k++) {
            int column = resolved.get(k).columnIndex();
            keys[k] = Key.forColumn(schema.getColumn(column), column, declared.get(k));
        }
    }

    /// Checks records `[from, from + count)` of the batch `shredder` is bound to, which become
    /// records `[rowOffset, rowOffset + count)` of the open row group, against the record before
    /// each.
    ///
    /// @throws IllegalArgumentException at the first record that sorts before its predecessor
    void check(RecordShredder shredder, ColumnSource[] sources, int from, int count, int rowOffset) {
        if (count == 0) {
            return;
        }
        if (tied.length < count) {
            tied = new boolean[count];
        }
        // Each column searches only the rows ahead of the earliest violation found so far, so the
        // row reported is the first out of order whichever column it differs on.
        int limit = count;
        Key violated = null;
        for (int k = 0; k < keys.length; k++) {
            Key key = keys[k];
            int violation = checkColumn(key, k == 0, sources[key.column], shredder.presence(key.column), from, limit);
            if (violation >= 0) {
                limit = violation;
                violated = key;
            }
        }
        if (violated != null) {
            throw outOfOrder(violated, rowOffset + limit);
        }
        for (Key key : keys) {
            key.endSlice();
        }
        hasPrevious = true;
        checkedSinceCut = true;
    }

    /// Drops the previous record, the open row group having been cut.
    void startRowGroup() {
        hasPrevious = false;
        if (checkedSinceCut) {
            rowGroupOrdinal++;
            checkedSinceCut = false;
        }
    }

    /// Compares records `[from, from + count)` on one column, the leading one against every
    /// predecessor and a later one only where the columns before it tied, and returns the
    /// slice-relative index of the first record that sorts before its predecessor, or `-1`.
    private int checkColumn(Key key, boolean leading, ColumnSource source, Validity presence, int from, int count) {
        for (int base = 0; base < count; base += CHUNK) {
            int n = Math.min(CHUNK, count - base);
            key.load(source, from + base, n);
            for (int i = 0; i < n; i++) {
                int at = base + i;
                boolean isNull = presence.hasNulls() && presence.isNull(from + at);
                if (at == 0 && !hasPrevious) {
                    tied[0] = false;
                }
                else if (leading || tied[at]) {
                    int order = key.compareToPrevious(isNull, i);
                    if (order < 0) {
                        return at;
                    }
                    tied[at] = order == 0;
                }
                key.keep(isNull, i);
            }
        }
        return -1;
    }

    private IllegalArgumentException outOfOrder(Key key, int row) {
        return new IllegalArgumentException("Row " + row + " of row group " + rowGroupOrdinal + " sorts before row "
                + (row - 1) + " on column '" + key.declared.columnPath() + "', against the declared sorting columns "
                + declared);
    }

    /// One sorting column: its last value, and a chunk of the values that follow it.
    private abstract static sealed class Key {

        final int column;
        final SortingColumn declared;
        private boolean previousNull;

        Key(int column, SortingColumn declared) {
            this.column = column;
            this.declared = declared;
        }

        static Key forColumn(ColumnSchema schema, int column, SortingColumn declared) {
            boolean unsigned = AnnotationKind.ordersUnsigned(schema.logicalType());
            return switch (schema.type()) {
                case INT32 -> new IntKey(column, declared, unsigned);
                case INT64 -> new LongKey(column, declared, unsigned);
                case FLOAT -> new FloatKey(column, declared);
                case DOUBLE -> new DoubleKey(column, declared);
                case BOOLEAN -> new BooleanKey(column, declared);
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new BinaryKey(column, declared,
                        AnnotationPairings.byteColumnOrder(schema.type(), schema.logicalType()));
                default -> throw new IllegalStateException(
                        "Sorting column " + declared.columnPath() + " is " + schema.type()
                                + ", which the writer does not write");
            };
        }

        /// Copies `n` of the source's values from `from` into this key's chunk.
        abstract void load(ColumnSource source, int from, int n);

        /// The sign of present values `i` of the chunk against the previous value, ascending.
        abstract int compareValue(int i);

        /// Makes value `i` of the chunk the previous value.
        abstract void keepValue(int i);

        /// Detaches the previous value from the batch, whose arrays the caller may reuse once the
        /// batch is written.
        void endSlice() {
        }

        /// Where the record whose value is chunk entry `i` (or null) sorts against the previous
        /// record under this column: negative before it, which is out of order.
        final int compareToPrevious(boolean isNull, int i) {
            if (isNull || previousNull) {
                if (isNull == previousNull) {
                    return 0;
                }
                // A null after a value is in order where nulls sort last, and the reverse.
                return isNull == declared.nullsFirst() ? -1 : 1;
            }
            int order = compareValue(i);
            return declared.descending() ? -order : order;
        }

        final void keep(boolean isNull, int i) {
            previousNull = isNull;
            if (!isNull) {
                keepValue(i);
            }
        }
    }

    private static final class IntKey extends Key {
        private final boolean unsigned;
        private final int[] values = new int[CHUNK];
        private int previous;

        IntKey(int column, SortingColumn declared, boolean unsigned) {
            super(column, declared);
            this.unsigned = unsigned;
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((IntColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            return unsigned ? Integer.compareUnsigned(values[i], previous) : Integer.compare(values[i], previous);
        }

        @Override
        void keepValue(int i) {
            previous = values[i];
        }
    }

    private static final class LongKey extends Key {
        private final boolean unsigned;
        private final long[] values = new long[CHUNK];
        private long previous;

        LongKey(int column, SortingColumn declared, boolean unsigned) {
            super(column, declared);
            this.unsigned = unsigned;
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((LongColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            return unsigned ? Long.compareUnsigned(values[i], previous) : Long.compare(values[i], previous);
        }

        @Override
        void keepValue(int i) {
            previous = values[i];
        }
    }

    private static final class FloatKey extends Key {
        private final float[] values = new float[CHUNK];
        private float previous;

        FloatKey(int column, SortingColumn declared) {
            super(column, declared);
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((FloatColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            // Adding 0.0f turns -0.0f into 0.0f, which Float.compare would otherwise order apart;
            // NaN stays NaN, which Float.compare puts after every number.
            return Float.compare(values[i] + 0.0f, previous + 0.0f);
        }

        @Override
        void keepValue(int i) {
            previous = values[i];
        }
    }

    private static final class DoubleKey extends Key {
        private final double[] values = new double[CHUNK];
        private double previous;

        DoubleKey(int column, SortingColumn declared) {
            super(column, declared);
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((DoubleColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            // As FloatKey: -0.0 equal to 0.0, NaN after every number.
            return Double.compare(values[i] + 0.0, previous + 0.0);
        }

        @Override
        void keepValue(int i) {
            previous = values[i];
        }
    }

    private static final class BooleanKey extends Key {
        private final boolean[] values = new boolean[CHUNK];
        private boolean previous;

        BooleanKey(int column, SortingColumn declared) {
            super(column, declared);
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((BooleanColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            return Boolean.compare(values[i], previous);
        }

        @Override
        void keepValue(int i) {
            previous = values[i];
        }
    }

    private static final class BinaryKey extends Key {
        private final ByteColumnOrder order;
        private final byte[][] values = new byte[CHUNK][];
        private byte[] previous;

        BinaryKey(int column, SortingColumn declared, ByteColumnOrder order) {
            super(column, declared);
            this.order = order;
        }

        @Override
        void load(ColumnSource source, int from, int n) {
            ((BinaryColumnSource) source).copyInto(from, values, 0, n);
        }

        @Override
        int compareValue(int i) {
            byte[] value = values[i];
            if (order == ByteColumnOrder.HALF_FLOAT) {
                // As FloatKey: -0.0 equal to 0.0, NaN after every number.
                return Float.compare(LogicalTypeConverter.float16At(value, 0) + 0.0f,
                        LogicalTypeConverter.float16At(previous, 0) + 0.0f);
            }
            return BinaryComparator.compare(value, 0, value.length, previous, order);
        }

        @Override
        void keepValue(int i) {
            // A reference into the caller's batch, which is not modified while the slice is
            // checked; endSlice copies the last one out.
            previous = values[i];
        }

        @Override
        void endSlice() {
            if (previous != null) {
                previous = previous.clone();
            }
        }
    }
}
