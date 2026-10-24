/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.memory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// The memory a read must hold to produce its next batch, held to a limit before any of it is
/// allocated.
///
/// The working set has two parts. Each decoded column holds a dictionary's entries and a data
/// page's batches, which live until the consumer has read them, and a nested column also the
/// page's decompressed bytes and levels, which it keeps until its cells fill. The task decoding a
/// page holds the page's decompressed bytes and its level and dictionary-id scratch while it runs.
/// The file's declared sizes decide both, so a file can ask for any amount; a read whose working
/// set would exceed the limit fails with a [ParquetReadException] before the allocation.
///
/// Each column counts the dictionaries one batch holds and the largest page it has met in the read,
/// and the read the largest task buffers any page has needed; the working set is their sum. A
/// batch holds the dictionary of every chunk it has rows of, as its pages decode with it: one for
/// a batch within a row group, more for one that spans row groups. A column counts the most any of
/// its batches holds, from where the read's batch plan places the chunks' rows. The task
/// buffers count once: a read needs one task at a time, and decoding pages on several threads at
/// once is the reader's choice, as reading ahead is, not the file's. The sum does not depend on
/// how far apart the columns' fetch steps run, nor on how far a column decodes ahead of its
/// consumer, so whether a read of every page fails depends on
/// the file and the limit alone, not on timing, the context's threads or other reads. A read that
/// stops early, at a row limit or a closed reader, has also charged whatever pages its columns
/// decoded ahead of where it stopped, which depends on timing. Nothing is released: a read that
/// has passed a large page stays charged for it.
///
/// The limit is per read and nothing waits for it. Working-set memory is released only as the
/// consumer reads, so a read that waited for another's could wait for a consumer blocked on
/// itself.
///
/// The tasks of a read that run beside the one its working set counts hold their buffers by the
/// reader's choice, and the context's [ReadAheadBuffer] holds them ([#admit]): a task whose
/// buffers exceed the retention size starts as the read's one task while no other such task of
/// the read runs, beside it only while its buffers fit the buffer, and otherwise once a task ends.
/// A task within the retention size starts at once, as a context keeps that much per thread
/// anyway.
public final class WorkingSet {

    /// The admission of a task [#admit] admitted as the read's one task its working set counts.
    public static final long OWN_TASK = -1;
    /// The admission of a task [#admitTask] did not admit.
    private static final long REFUSED = -2;

    static final String MAX_WORKING_SET_BYTES_PROPERTY = "hardwood.internal.maxWorkingSetBytes";

    /// The share of the maximum heap a read's working set may take without the override: all of
    /// it, since the charges err high and a read whose working set exceeds the heap could not run.
    private static final int DEFAULT_HEAP_PERCENT = 100;

    private final long limit;
    private final AtomicLongArray dictionaries;
    /// Per column, the dictionaries its current batch may hold.
    private final DictionaryWindow[] windows;
    private final AtomicLongArray pages;
    /// The largest buffers a task has needed, in a one-element array to share [#raise].
    private final AtomicLongArray task = new AtomicLongArray(1);
    private final AtomicLong total = new AtomicLong();
    /// Holds the buffers of the read's tasks beside its one task; `null` outside a read, whose
    /// tasks are not admitted.
    private final ReadAheadBuffer readAhead;
    /// Raised while a task of the read whose buffers exceed the retention size runs as its one task.
    private final AtomicBoolean ownTask = new AtomicBoolean();

    /// A working set of `columns` decoded columns, held to [#maxWorkingSetBytes], whose tasks
    /// beside its one task `readAhead` holds.
    public WorkingSet(int columns, ReadAheadBuffer readAhead) {
        this(columns, maxWorkingSetBytes(), readAhead);
    }

    public WorkingSet(int columns, long limit, ReadAheadBuffer readAhead) {
        this.limit = limit;
        this.readAhead = readAhead;
        this.dictionaries = new AtomicLongArray(columns);
        this.windows = new DictionaryWindow[columns];
        for (int i = 0; i < columns; i++) {
            windows[i] = new DictionaryWindow();
        }
        this.pages = new AtomicLongArray(columns);
    }

    /// A working set of its own for one column's dictionaries and pages, for code that decodes
    /// outside a read: the metadata tools and the dictionaries a filter parses to prune row
    /// groups.
    public static Column standalone(ColumnSchema schema) {
        return new WorkingSet(1, null).column(0, schema);
    }

    /// The handle through which the decoded column at `index` charges its dictionaries and
    /// pages.
    public Column column(int index, ColumnSchema schema) {
        return new Column(this, index, schema);
    }

    /// Tells the column at `index` that its pages now come from a row group the read's batch plan
    /// places, which holds the rows of the chunk whose dictionary it charges next: the batch
    /// holding the row group's first planned row starts at the read's row `batchStartRow`, and the
    /// row group's planned rows end before the read's row `endRow`. Called by the column's fetch
    /// step as it enters the row group.
    public void enter(int index, long batchStartRow, long endRow) {
        windows[index].enter(batchStartRow, endRow);
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

    /// What [#admit] came to: the task admitted, with the admission to hand to [#taskEnded] when
    /// its buffers are released ([#OWN_TASK], the bytes charged to the read-ahead buffer, or `0`),
    /// or refused, with what it waits for: a task ending or read-ahead released after it was
    /// refused, met at once if one has. A refused task always has one to wait for: it was refused
    /// only while the read's own task ran.
    ///
    /// @param admission the admission of an admitted task
    /// @param refused what a refused task waits for, `null` for an admitted one
    public record Admission(long admission, Need refused) {

        private static final Admission UNCHARGED = new Admission(0, null);

        public boolean admitted() {
            return refused == null;
        }
    }

    /// Admits a task of the read whose buffers take `bytes`, to start now: at once, uncharged, if
    /// they are within the retention size ([DecodeBuffers#RETAINED_BYTES]) or the task runs
    /// outside a read; as the read's one task while no other larger one runs; beside it while
    /// they fit the context's read-ahead buffer, which they are charged to.
    public Admission admit(long bytes) {
        long seen = readAhead == null ? 0 : readAhead.releases();
        long admission = admitTask(bytes);
        if (admission == REFUSED) {
            return new Admission(0, readAhead.releaseAfter(seen));
        }
        return admission == 0 ? Admission.UNCHARGED : new Admission(admission, null);
    }

    /// The admission of a task whose buffers take `bytes` ([#admit]), or [#REFUSED].
    private long admitTask(long bytes) {
        if (readAhead == null || bytes <= DecodeBuffers.RETAINED_BYTES) {
            return 0;
        }
        if (ownTask.compareAndSet(false, true)) {
            return OWN_TASK;
        }
        return readAhead.tryCharge(bytes) ? bytes : REFUSED;
    }

    /// Releases what [#admit] admitted a task under, which runs what waits for a task to end.
    public void taskEnded(long admission) {
        if (admission == OWN_TASK) {
            ownTask.set(false);
            readAhead.wake();
        }
        else if (admission > 0) {
            readAhead.release(admission);
        }
    }

    /// Drops `retry` if it still waits for a task to end.
    public void cancelWait(Runnable retry) {
        if (readAhead != null) {
            readAhead.cancel(retry);
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

    /// Reads and parses a dictionary page for [Column#parseDictionary].
    @FunctionalInterface
    public interface DictionaryRead {

        /// The dictionary.
        Dictionary read() throws IOException;
    }

    /// What [Column#parseDictionary] came to: the dictionary parsed, or, with no dictionary, what
    /// the parse waits for.
    public record DictionaryParse(Dictionary dictionary, Need pending) {
    }

    /// One decoded column of a [WorkingSet].
    public static final class Column {

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

        /// Reads and parses a dictionary page whose header declares `uncompressedSize` bytes, once
        /// its parse is admitted beside the read's other tasks ([WorkingSet#admit]), and
        /// releases the admission when `read` returns. `read` parses the page, whose bytes the
        /// caller has read.
        ///
        /// @return the dictionary `read` returned, or what the parse waits for: a task of the read
        ///         ending
        public DictionaryParse parseDictionary(int uncompressedSize, DictionaryRead read) throws IOException {
            Admission admitted = set.admit(uncompressedSize);
            if (!admitted.admitted()) {
                return new DictionaryParse(null, admitted.refused());
            }
            try {
                return new DictionaryParse(read.read(), null);
            }
            finally {
                set.taskEnded(admitted.admission());
            }
        }


        /// Charges a dictionary page before it is decompressed and parsed: the entries the column
        /// holds, as many bytes as the page plus an offset per entry for a binary column, beside
        /// those of the other chunks its batch holds, and the page's decompressed bytes, which the
        /// parsing task holds.
        ///
        /// @throws ParquetReadException if the read's working set would exceed its limit
        public void dictionary(int numValues, int uncompressedSize) {
            long entries = uncompressedSize;
            if (!isNumeric(schema.type())) {
                entries += Integer.BYTES * (numValues + 1L);
            }
            set.charge(set.dictionaries, index, set.windows[index].add(entries), uncompressedSize,
                    entries + uncompressedSize, "Dictionary page");
        }

        /// Charges the entries of a dictionary parsed before the column reached its chunk, as
        /// pruning parses one, beside those of the other chunks its batch holds.
        ///
        /// @throws ParquetReadException if the read's working set would exceed its limit
        public void dictionary(Dictionary dictionary) {
            long entries = dictionary instanceof Dictionary.ByteArrayDictionary binary
                    ? binary.entryBytes().length + Integer.BYTES * (dictionary.size() + 1L)
                    : (long) dictionary.size() * valueWidth(schema.type());
            set.charge(set.dictionaries, index, set.windows[index].add(entries), 0, entries, "Dictionary page");
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
        public void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept) {
            page(numValues, uncompressedSize, decodedBytes, encoding, kept, 0);
        }

        /// Charges a data page as [#page(int, int, long, Encoding, boolean)] does, with
        /// `valueBytes` of values its decode reconstructs beyond its own bytes, as
        /// `DELTA_BYTE_ARRAY` does: held by the task, or the column that keeps the page, and copied
        /// into the batches.
        ///
        /// @throws ParquetReadException if the read's working set would exceed its limit
        public void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept,
                         long valueBytes) {
            long batches = (long) numValues * valueWidth(schema.type()) + valueBytes;
            if (!isNumeric(schema.type())) {
                batches += uncompressedSize;
            }
            long buffers = pageBuffers(schema, numValues, decodedBytes, encoding) + valueBytes;
            if (kept) {
                batches += (long) numValues * Integer.BYTES * NESTED_BATCH_INTS;
                set.charge(set.pages, index, batches + buffers, 0, batches + buffers, "Data page");
            }
            else {
                set.charge(set.pages, index, batches, buffers, batches + buffers, "Data page");
            }
        }

        /// The buffers the task decoding a data page of `schema`'s column takes: the `decodedBytes`
        /// it decompresses or copies the page into, an `int` per value for each level array, and
        /// the `int`s per value its encoding decodes through ([#scratchInts]).
        public static long pageBuffers(ColumnSchema schema, int numValues, long decodedBytes, Encoding encoding) {
            int intArrays = (schema.maxDefinitionLevel() > 0 ? 1 : 0) + (schema.maxRepetitionLevel() > 0 ? 1 : 0)
                    + scratchInts(schema.type(), encoding);
            return decodedBytes + (long) numValues * Integer.BYTES * intArrays;
        }

        /// The `int` arrays of one entry per value a page's values decode through: the ids of a
        /// dictionary-encoded page or of `RLE` booleans; a binary page's offsets, with the lengths
        /// of `DELTA_LENGTH_BYTE_ARRAY` and the prefix and suffix lengths of `DELTA_BYTE_ARRAY`;
        /// none for numeric values read in place.
        static int scratchInts(PhysicalType type, Encoding encoding) {
            return switch (encoding) {
                case PLAIN_DICTIONARY, RLE_DICTIONARY, RLE -> 1;
                case DELTA_LENGTH_BYTE_ARRAY -> 2;
                case DELTA_BYTE_ARRAY -> 3;
                case PLAIN, BYTE_STREAM_SPLIT -> isNumeric(type) ? 0 : 1;
                case DELTA_BINARY_PACKED, ALP, BIT_PACKED, UNKNOWN -> 0;
            };
        }

        private static boolean isNumeric(PhysicalType type) {
            return switch (type) {
                case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
            };
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

    /// The dictionaries a column's current batch may hold: those of the chunks whose rows end after
    /// the batch holding the current chunk's first row starts. Row groups are entered and charged
    /// in the read's row order, on the column's fetch steps.
    private static final class DictionaryWindow {

        /// A dictionary charged, held while a batch holds rows before `endRow`.
        private record Held(long endRow, long bytes) {
        }

        private final ArrayDeque<Held> held = new ArrayDeque<>();
        private long heldBytes;
        /// Whether the column's pages come from a row group of a read's batch plan; outside one,
        /// each dictionary counts alone.
        private boolean planned;
        /// The read's row the batch holding the current row group's first planned row starts at.
        private long batchStartRow;
        /// The read's row the current row group's planned rows end before.
        private long endRow;

        synchronized void enter(long batchStartRow, long endRow) {
            this.planned = true;
            this.batchStartRow = batchStartRow;
            this.endRow = endRow;
        }

        /// Adds the current chunk's dictionary of `bytes`, and returns the bytes of the
        /// dictionaries the batch holding the chunk's first row holds with it.
        synchronized long add(long bytes) {
            if (!planned) {
                return bytes;
            }
            while (!held.isEmpty() && held.peekFirst().endRow() <= batchStartRow) {
                heldBytes -= held.pollFirst().bytes();
            }
            held.addLast(new Held(endRow, bytes));
            heldBytes += bytes;
            return heldBytes;
        }
    }
}
