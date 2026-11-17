/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.memory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// What decoding outside a read holds, held to a limit before any of it is allocated: the
/// metadata tools' dictionaries and pages, and the dictionaries a filter parses to prune row
/// groups ([#standalone]). A read charges its decodes to its context's [ReadAheadBuffer] instead.
///
/// Each column counts the largest dictionary and the largest page it has met, a page's batches
/// with, for a column that keeps its pages, their levels and offsets; the decoding counts the
/// largest task buffers once. The file's declared sizes decide them, so a file can ask for any
/// amount; a decode whose charge would take the total past the limit fails with a
/// [ParquetReadException] before the allocation. Nothing is released.
public final class WorkingSet {

    public static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";

    /// The share of the maximum heap a read's working set may take without the override: all of
    /// it, since the charges err high and a read whose working set exceeds the heap could not run.
    private static final int DEFAULT_HEAP_PERCENT = 100;

    private final long limit;
    private final AtomicLongArray dictionaries;
    private final AtomicLongArray pages;
    /// The largest buffers a task has needed, in a one-element array to share [#raise].
    private final AtomicLongArray task = new AtomicLongArray(1);
    private final AtomicLong total = new AtomicLong();

    /// A working set of `columns` decoded columns, held to [#maxWorkingSetBytes].
    public WorkingSet(int columns) {
        this(columns, maxWorkingSetBytes());
    }

    public WorkingSet(int columns, long limit) {
        this.limit = limit;
        this.dictionaries = new AtomicLongArray(columns);
        this.pages = new AtomicLongArray(columns);
    }

    /// A working set of its own for one column's dictionaries and pages, for code that decodes
    /// outside a read: the metadata tools and the dictionaries a filter parses to prune row
    /// groups.
    public static Column standalone(ColumnSchema schema) {
        return new WorkingSet(1).column(0, schema);
    }

    /// The handle through which the decoded column at `index` charges its dictionaries and
    /// pages.
    public Column column(int index, ColumnSchema schema) {
        return new Column(this, index, schema);
    }

    /// The working-set limit of a read: [#DEFAULT_HEAP_PERCENT] of the maximum heap unless
    /// overridden through [#MAX_WORKING_SET_BYTES_PROPERTY].
    ///
    /// Read for each read rather than cached in a static final, so that the override holds
    /// whenever it is set. A value that is not a positive byte count is rejected rather than
    /// replaced by the default: an override that does not take effect leaves the reader refusing
    /// the very file it was set for.

    static long maxWorkingSetBytes() {
        return InternalProperty.longValue(MAX_WORKING_SET_BYTES_PROPERTY,
                Runtime.getRuntime().maxMemory() / 100 * DEFAULT_HEAP_PERCENT, 1, "a positive byte count");
    }

    /// Charges a page or dictionary page needing `needed` bytes: raises `slots`' entry at `slot`
    /// to `held`, the task buffers to `taskBytes`, and the total with them; fails if the total then
    /// exceeds the limit.
    private void charge(AtomicLongArray slots, int slot, long held, long taskBytes, long needed, String what) {
        long added = raise(slots, slot, held) + raise(task, 0, taskBytes);
        if (added == 0) {
            return;
        }
        long total = this.total.addAndGet(added);
        if (total > limit) {
            throw new ParquetReadException(what + " needs " + needed + " bytes to decode, which takes "
                    + "the read's working set to " + total + " bytes, more than its limit of " + limit + " ("
                    + MAX_WORKING_SET_BYTES_PROPERTY + ")");
        }
    }

    /// Raises `slots`' entry at `slot` to `bytes` if it is lower, and returns by how much.
    private static long raise(AtomicLongArray slots, int slot, long bytes) {
        long previous;
        do {
            previous = slots.get(slot);
            if (bytes <= previous) {
                return 0;
            }
        }
        while (!slots.compareAndSet(slot, previous, bytes));
        return bytes - previous;
    }

    /// One decoded column of a [WorkingSet].
    public static final class Column implements DecodeCharges {

        /// The `int`s a nested batch keeps per value besides the value: its definition and
        /// repetition level and its offsets.
        private static final int NESTED_BATCH_INTS = 3;

        private final WorkingSet set;
        private final int index;
        private final ColumnSchema schema;
        private Column(WorkingSet set, int index, ColumnSchema schema) {
            this.set = set;
            this.index = index;
            this.schema = schema;
        }

        /// Charges a dictionary page before it is decompressed and parsed: the entries the column
        /// holds, as many bytes as the page plus an offset per entry for a binary column, beside
        /// those of the other chunks its batch holds, and the page's decompressed bytes, which the
        /// parsing task holds.
        ///
        /// @throws ParquetReadException if the read's working set would exceed its limit
        @Override
        public void dictionary(int numValues, int uncompressedSize) {
            long entries = dictionaryEntries(schema, numValues, uncompressedSize);
            set.charge(set.dictionaries, index, entries, uncompressedSize,
                    entries + uncompressedSize, "Dictionary page");
        }

        /// Charges a data page before it is decompressed and decoded: the batches its task fills,
        /// a value width per value and, for a binary column, its bytes and views; for a nested
        /// column also the levels and offsets each batch keeps; and the buffers its task takes
        /// ([#pageBuffers]), which the task holds, or the column where it keeps them until its
        /// cells fill.
        ///
        /// @param uncompressedSize the page's uncompressed size, which a binary column's batches copy
        /// @param decodedBytes the bytes its task decompresses or copies the page into; `0` for a
        ///        page whose values are read where they lie
        /// @param encoding the encoding of the page's values
        /// @param kept whether the column keeps the page's bytes and levels past its task
        /// @throws ParquetReadException if the read's working set would exceed its limit
        @Override
        public void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept) {
            page(numValues, uncompressedSize, decodedBytes, encoding, kept, 0);
        }

        @Override
        public void reconstructed(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding,
                                  boolean kept, long valueBytes, DecodeBuffers buffers) {
            page(numValues, uncompressedSize, decodedBytes, encoding, kept, valueBytes);
        }

        /// Charges a data page as [#page(int, int, long, Encoding, boolean)] does, with
        /// `valueBytes` of values its decode reconstructs beyond its own bytes, as
        /// `DELTA_BYTE_ARRAY` does: held by the task, or the column that keeps the page, and copied
        /// into the batches.
        ///
        /// @throws ParquetReadException if the read's working set would exceed its limit
        public void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept,
                         long valueBytes) {
            long batches = batches(numValues, uncompressedSize, kept) + valueBytes;
            long buffers = pageBuffers(schema, numValues, decodedBytes, encoding) + valueBytes;
            if (kept) {
                set.charge(set.pages, index, batches + buffers, 0, batches + buffers, "Data page");
            }
            else {
                set.charge(set.pages, index, batches, buffers, batches + buffers, "Data page");
            }
        }

        /// The bytes the entries of `dictionary`, parsed for `schema`'s column, take.
        public static long entries(ColumnSchema schema, Dictionary dictionary) {
            return dictionary instanceof Dictionary.ByteArrayDictionary binary
                    ? binary.byteCount() + Integer.BYTES * ((long) dictionary.size() + binary.chunkCount())
                    : (long) dictionary.size() * valueWidth(schema.type());
        }

        /// What a page of `schema`'s column, which keeps its pages' bytes and levels, takes while the
        /// column keeps it, with every byte of the page decompressed: its batches with their levels
        /// and offsets, and its buffers.
        public static long keptPage(ColumnSchema schema, int numValues, int uncompressedSize, Encoding encoding) {
            return batchBytes(schema, numValues, uncompressedSize) + (long) numValues * Integer.BYTES * NESTED_BATCH_INTS
                    + pageBuffers(schema, numValues, uncompressedSize, encoding);
        }

        /// The bytes the entries of a dictionary page of `schema`'s column take, from its header:
        /// as many as the page and, for a binary column, an offset per entry.
        public static long dictionaryEntries(ColumnSchema schema, int numValues, int uncompressedSize) {
            return uncompressedSize + (isNumeric(schema.type()) ? 0 : Integer.BYTES * (numValues + 1L));
        }

        /// The bytes a page's batches hold: a value width per value and, for a binary column, its
        /// bytes; with the levels and offsets each batch keeps where the column keeps the page.
        private long batches(int numValues, int uncompressedSize, boolean kept) {
            long batches = batchBytes(schema, numValues, uncompressedSize);
            if (kept) {
                batches += (long) numValues * Integer.BYTES * NESTED_BATCH_INTS;
            }
            return batches;
        }

        /// The bytes the batches of a flat page of `schema`'s column hold once it is decoded: a value
        /// width per value and, for a binary column, the page's bytes.
        public static long batchBytes(ColumnSchema schema, int numValues, int uncompressedSize) {
            long batches = (long) numValues * valueWidth(schema.type());
            if (!isNumeric(schema.type())) {
                batches += uncompressedSize;
            }
            return batches;
        }

        /// The buffers the task decoding a data page of `schema`'s column takes: the `decodedBytes`
        /// it decompresses or copies the page into, an `int` per value for each level array, and
        /// the `int`s per value its encoding decodes through ([#scratchInts]).
        public static long pageBuffers(ColumnSchema schema, int numValues, long decodedBytes, Encoding encoding) {
            int intArrays = (schema.maxDefinitionLevel() > 0 ? 1 : 0) + (schema.maxRepetitionLevel() > 0 ? 1 : 0)
                    + scratchInts(schema.type(), encoding);
            return decodedBytes + (long) numValues * Integer.BYTES * intArrays;
        }

        /// The `int` arrays of one entry per value a page's values decode through: a binary page's
        /// offsets, with the lengths of `DELTA_LENGTH_BYTE_ARRAY` and the prefix and suffix lengths
        /// of `DELTA_BYTE_ARRAY`; none for numeric values read in place, nor for ids (a
        /// dictionary's, `RLE` booleans), which a fill decodes a chunk at a time.
        static int scratchInts(PhysicalType type, Encoding encoding) {
            return switch (encoding) {
                case DELTA_LENGTH_BYTE_ARRAY -> 2;
                case DELTA_BYTE_ARRAY -> 3;
                case PLAIN, BYTE_STREAM_SPLIT -> isNumeric(type) ? 0 : 1;
                case PLAIN_DICTIONARY, RLE_DICTIONARY, RLE, DELTA_BINARY_PACKED, ALP, BIT_PACKED, UNKNOWN -> 0;
            };
        }

        /// Whether `schema`'s column holds numeric values, which a batch holds by value width alone.
        public static boolean numeric(ColumnSchema schema) {
            return isNumeric(schema.type());
        }

        private static boolean isNumeric(PhysicalType type) {
            return !BinaryBatchValues.holds(type);
        }

        /// The bytes a batch holds per value: the value's own for a numeric column; for a binary
        /// one its view (start, end and buffer) and its dictionary id.
        private static int valueWidth(PhysicalType type) {
            return switch (type) {
                case BOOLEAN -> 1;
                case INT32, FLOAT -> Integer.BYTES;
                case INT64, DOUBLE -> Long.BYTES;
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> 4 * Integer.BYTES;
            };
        }
    }
}
