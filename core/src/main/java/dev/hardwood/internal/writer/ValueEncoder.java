/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.writer.ColumnEncoding;

/// The per-physical-type half of a column chunk's value section: the typed value read window
/// over the batch source, the typed `PLAIN` buffer, the (optional) dictionary, and the typed
/// statistics. [ColumnChunkBuffer] owns the type-agnostic half — repetition / definition level
/// streams, page sealing, compression, CRC, and the dictionary-index stream — and drives this
/// encoder value by value as the [RecordShredder] streams a record range in.
///
/// The shredder emits only source positions, so every type-specific line lives here behind a
/// type-agnostic shredder and page sealer. A concrete encoder reads a present value through its
/// window at the shredder's `valueIndex`, feeds it to the dictionary or the `PLAIN` buffer, and
/// extends the statistics; an absent slot only advances the null count.
abstract class ValueEncoder {

    /// Selects the encoder for a column's physical type. `encoding` is the column's resolved
    /// policy, which builds a dictionary only under [ColumnEncoding#AUTO];
    /// `statisticsTruncationLength` bounds `BYTE_ARRAY` `min` / `max` bounds.
    static ValueEncoder forColumn(ColumnSchema column, ColumnEncoding encoding,
                                  int statisticsTruncationLength, int startingCapacity) {
        PhysicalType type = column.type();
        boolean unsigned = isUnsigned(column);
        // Only AUTO decides between a dictionary and something else, so only AUTO needs one
        // built: a column under a named policy pays neither the interning nor the index array.
        boolean dictionary = encoding == ColumnEncoding.AUTO;
        return switch (type) {
            case INT32 -> new IntValueEncoder(dictionary, unsigned, startingCapacity);
            case INT64 -> new LongValueEncoder(dictionary, unsigned, startingCapacity);
            case FLOAT -> new FloatValueEncoder(dictionary, startingCapacity);
            case DOUBLE -> new DoubleValueEncoder(dictionary, startingCapacity);
            case BOOLEAN -> new BooleanValueEncoder(startingCapacity);
            case BYTE_ARRAY -> new BinaryValueEncoder(dictionary, null, startingCapacity,
                    () -> BinaryStatistics.forColumn(column, statisticsTruncationLength));
            case FIXED_LEN_BYTE_ARRAY -> new BinaryValueEncoder(dictionary,
                    requireTypeLength(column), startingCapacity,
                    () -> BinaryStatistics.forColumn(column, statisticsTruncationLength));
            default -> throw new IllegalArgumentException(
                    "Writer does not support physical type " + type + " for column " + column.name());
        };
    }

    /// Rejects an encoding this physical type cannot carry. Configuration validation has already
    /// excluded every such pair at writer creation, so reaching here is a writer defect rather
    /// than a caller's mistake.
    static IllegalStateException unsupported(ColumnEncoding encoding, PhysicalType type) {
        return new IllegalStateException(
                "Encoding " + encoding + " is not applicable to a " + type + " column");
    }

    /// The capacity a filled value store grows to: half again as much, so a row group's worth of
    /// values is reached in a bounded number of copies without doubling the overshoot at the
    /// sizes a row group reaches.
    static int grownCapacity(int current) {
        int grown = current + (current >> 1) + 8;
        if (grown < 0 || grown > RowGroupBuffer.MAX_STORE_CAPACITY) {
            if (current >= RowGroupBuffer.MAX_STORE_CAPACITY) {
                throw new IllegalStateException(
                        "A column chunk cannot hold more than " + RowGroupBuffer.MAX_STORE_CAPACITY + " values");
            }
            return RowGroupBuffer.MAX_STORE_CAPACITY;
        }
        return grown;
    }

    /// The capacity a column's value store starts at: an equal share of the row group's byte
    /// budget, counted in values of this column's own width.
    ///
    /// A store that starts far below what a row group will hold reaches it by copying itself
    /// repeatedly, and geometric growth leaves behind about twice the store's final size in
    /// garbage — for a row group sized in tens of megabytes, the writer's largest source of it.
    /// Starting from the budget removes most of those copies, and it bounds the eager allocation
    /// by the number the caller already set: every column starting at its share means the whole
    /// schema starts at one row group's worth, however many columns it has.
    ///
    /// Clamped at both ends. The floor keeps a column of a very wide schema, or of a very small
    /// target, from starting at a capacity it would immediately outgrow; the ceiling keeps a
    /// narrow schema with a large target from allocating a row group's worth for a file that may
    /// hold ten records.
    /// @param budgetBytesPerColumn the row group's byte target divided among its columns
    /// @param eagerBytesPerValue what one value costs across the column's eager buffers
    static int startingCapacity(long budgetBytesPerColumn, long eagerBytesPerValue) {
        long values = budgetBytesPerColumn / Math.max(1, eagerBytesPerValue);
        return (int) Math.clamp(values, MIN_BUFFER_VALUES, MAX_BUFFER_VALUES);
    }

    /// What one value of a column's starting capacity costs across every buffer the column
    /// allocates before a record arrives.
    ///
    /// The value store is one of four. A column that may build a dictionary keeps an index beside
    /// each value, every column reads its source through a window, and a levelled column keeps a
    /// byte per entry per level stream — so a capacity derived from the store alone reserves
    /// several times the share it was given, and the more columns a schema has the further the
    /// total lands from the target. The window is charged its full width although it is capped at
    /// a slice, which errs towards a smaller capacity.
    ///
    /// @param column the column
    /// @param dictionaryCapable whether this column may intern, which is [ColumnEncoding#AUTO]
    ///        over a type that has a dictionary
    /// @param levelBytesPerEntry a byte for each level stream the column has
    static long eagerBytesPerValue(ColumnSchema column, boolean dictionaryCapable, int levelBytesPerEntry) {
        long store = storeBytesPerValue(column);
        // A binary column reads through a window of references rather than of values.
        long window = switch (column.type()) {
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> REFERENCE_BYTES;
            default -> store;
        };
        return store + window + levelBytesPerEntry + (dictionaryCapable ? INDEX_BYTES : 0);
    }

    /// A reference in a window of `byte[]`, under compressed oops.
    private static final long REFERENCE_BYTES = 4;

    /// What one value occupies in a column's store *before a record arrives*, which is what the
    /// store term above counts.
    ///
    /// Both binary forms are sized by the offset they keep per value rather than by their content.
    /// A `BYTE_ARRAY`'s content is not the schema's to state at all, and a
    /// `FIXED_LEN_BYTE_ARRAY`'s is — but neither is allocated up front: both hold their bytes in
    /// one packed buffer that starts at 32 bytes and grows, so charging a fixed width for content
    /// that has not been allocated would start the column far below the share it was given and
    /// deny it the regrowth this capacity exists to save.
    static long storeBytesPerValue(ColumnSchema column) {
        return switch (column.type()) {
            case BOOLEAN -> 1;
            case INT32, FLOAT -> Integer.BYTES;
            case INT64, DOUBLE -> Long.BYTES;
            case BYTE_ARRAY -> Integer.BYTES;
            case FIXED_LEN_BYTE_ARRAY -> Integer.BYTES;
            case INT96 -> Long.BYTES;
        };
    }

    /// The capacity of the fixed window a column reads its source through.
    ///
    /// The window is scratch, not storage: it is filled from the batch in bulk and read value by
    /// value, so what it wants is to span the records appended between two fills — a slice — and
    /// nothing more. Sized to the slice, a column fills it exactly once per slice.
    ///
    /// It is held to the column's starting capacity as well, so that a schema wide enough for the
    /// per-column buffers to dominate does not pay a slice-sized window on every column. A window
    /// below the slice is refilled more than once per slice and costs nothing else.
    static int windowCapacity(int startingCapacity) {
        return Math.min(WINDOW_VALUES, startingCapacity);
    }

    private static final int WINDOW_VALUES = 4096;

    /// Low enough that a schema of hundreds of columns still starts near its target rather than
    /// at a per-column constant multiplied by the column count, and high enough that a column
    /// which does fill reaches a useful size in a few growths.
    private static final int MIN_BUFFER_VALUES = 512;
    private static final int MAX_BUFFER_VALUES = 1 << 16;

    /// Whether the column's statistics compare unsigned: only the `UINT_*` annotations do.
    private static boolean isUnsigned(ColumnSchema column) {
        return column.logicalType() instanceof LogicalType.IntType integer && !integer.isSigned();
    }

    private static int requireTypeLength(ColumnSchema column) {
        if (column.typeLength() == null) {
            throw new IllegalArgumentException(
                    "FIXED_LEN_BYTE_ARRAY column " + column.name() + " has no type length");
        }
        return column.typeLength();
    }

    /// Rebinds to a new batch's source and resets the value read window. The dictionary and
    /// statistics persist across the batches of one column chunk.
    abstract void reset(ColumnSource source);

    /// Starts a new column chunk: the dictionary and statistics begin again, and the value store
    /// is emptied while keeping the capacity it grew to, so the next row group reuses the buffers
    /// this one sized rather than allocating and regrowing its own.
    abstract void startChunk();

    /// Whether this type is dictionary-encodable at all (`false` for `BOOLEAN`).
    abstract boolean dictionaryCapable();

    /// Interns the present value at `valueIndex`, returning its dictionary index and assigning a
    /// new one in first-seen order if it is unseen. Only called while the chunk still has a
    /// dictionary, which requires [#dictionaryCapable].
    abstract int intern(int valueIndex);

    /// The bytes the dictionary body would occupy if written, which is one half of what a chunk's
    /// encoding is chosen on and the measure the analysis cap bounds.
    abstract long dictionaryPlainBytes();

    /// Appends the dictionary's value at `dictionaryIndex` to the value store, the operation that
    /// turns interned values back into stored ones when a chunk gives up its dictionary.
    abstract void storeDictionaryValue(int dictionaryIndex);

    /// Releases the dictionary's entries, keeping the capacity it reached for the next chunk.
    abstract void dropDictionary();

    /// The number of distinct dictionary values assigned so far.
    abstract int dictionarySize();

    /// The exact number of distinct present values this chunk holds, or [#UNKNOWN_DISTINCT_COUNT]
    /// where the encoder cannot state it — a column encoding without a dictionary has not counted
    /// them. Only meaningful while the chunk still has whatever structure the count comes from.
    abstract long exactDistinctCount();

    /// Stands for a cardinality an encoder cannot state, which keeps `distinct_count` out of the
    /// statistics rather than guessing at it.
    static final long UNKNOWN_DISTINCT_COUNT = -1;

    /// The bits every stored value of this column occupies `PLAIN`-encoded, or
    /// [#VARIABLE_VALUE_BITS] where they differ. Read once per chunk when its pages are cut, so
    /// the common column costs no per-value call there.
    abstract long uniformValueBits();

    /// Stands for a column whose stored values do not all occupy the same width, which is
    /// `BYTE_ARRAY` and nothing else.
    static final long VARIABLE_VALUE_BITS = -1;

    /// Where a chunk's stored values end, one entry per value plus a trailing bound, so value
    /// `i` occupies `offsets[i + 1] - offsets[i]` bytes. `null` where every value is the same
    /// width and [#uniformValueBits] answers instead.
    ///
    /// Handed over as the array rather than answered per value: cutting a page reads one of these
    /// for every value of the chunk, and a call per value there is a virtual call per value.
    int[] storedValueOffsets() {
        return null;
    }

    /// The `PLAIN`-encoded dictionary body — the distinct values in index order.
    abstract byte[] encodeDictionaryBody();

    /// Copies the present value at `valueIndex` into this chunk's value store, appending it after
    /// the values stored before it. The store holds every value the chunk did not intern, for the
    /// whole row group, because a page's bytes are only produced once the row group is complete
    /// and the batch the value arrived in is gone by then.
    abstract void store(int valueIndex);

    /// Encodes `count` stored values starting at `from` — the value range of one page — with
    /// `encoding`, appending them to `out`. `encoding` is this column's resolved policy and never
    /// [ColumnEncoding#AUTO]: a chunk that kept its dictionary writes an index stream instead and
    /// never reaches here.
    ///
    /// Each page encodes its range standalone, carrying whatever header or baseline its encoding
    /// needs, because a reader may seek to any page and must decode it without the page before.
    ///
    /// The section goes into the page body directly rather than into an array the caller copies
    /// in. Where the encoded length follows from the count — every encoding but the delta family,
    /// whose length is a property of the values — the room is reserved in `out` and filled in
    /// place, so a page's value bytes are produced exactly once.
    abstract void encodeInto(ByteArrayBuilder out, ColumnEncoding encoding, int from, int count);

    /// Extends the chunk statistics with the present value at `valueIndex`.
    abstract void stat(int valueIndex);

    /// Extends the chunk statistics with an absent (null) slot.
    abstract void statNull();

    /// The accumulated chunk statistics.
    abstract Statistics statistics();

    /// The most one present value can retain: its width in the value store, or a dictionary index
    /// and a whole new dictionary entry, whichever is larger — a value being in one or the other
    /// and the chunk's encoding not being settled while it accumulates. [#VARIABLE_RETAINED_BYTES]
    /// where the width is not the schema's to state, which is `BYTE_ARRAY` and nothing else.
    ///
    /// A bound rather than a cost. What a value actually retains depends on whether its
    /// dictionary has seen it before, which is not knowable without hashing it, so this is what
    /// the writer sizes a slice with — never what it cuts a row group on.
    abstract long maxRetainedBytesPerValue();

    /// Stands for a value whose width has to be read from the batch.
    static final long VARIABLE_RETAINED_BYTES = -1;

    /// The bytes this chunk's packed content store would hold were every one of its
    /// `presentValues` present values stored rather than interned, which is what it holds once the
    /// chunk gives up its dictionary. `0` for a type whose values are not packed as bytes, whose
    /// store is bounded by its value count.
    long contentBytes(long presentValues) {
        return 0;
    }

    /// What the values at `[leafFrom, leafFrom + leafCount)` of `source` would add to
    /// [#contentBytes], charging every slot as present.
    long contentBytesFor(ColumnSource source, int leafFrom, int leafCount) {
        return 0;
    }

    /// What a new entry costs each dictionary, mirroring the tables charged in `retainedBytes()`.
    static final long INT_DICTIONARY_BYTES_PER_ENTRY = Integer.BYTES + 2L * (Integer.BYTES + Integer.BYTES);
    static final long LONG_DICTIONARY_BYTES_PER_ENTRY = Long.BYTES + 2L * (Long.BYTES + Integer.BYTES);
    /// A binary entry's fixed part: the `byte[]` header, the value array's reference, and the
    /// table slots. Its content is read from the batch and added to this.
    static final long BINARY_DICTIONARY_BYTES_PER_ENTRY = 16 + 4 + 2L * (Integer.BYTES + Integer.BYTES);

    /// The bytes an index costs in the chunk's index stream, which a value pays on top of its
    /// dictionary entry while the chunk is still interning.
    static final long INDEX_BYTES = Integer.BYTES;

    /// The bytes this encoder retains for the chunk it is accumulating: its value store and, where
    /// it has one, its dictionary. Charged from what the chunk holds rather than from what its
    /// buffers have grown to, since the stores keep their capacity across chunks and a reset chunk
    /// holds nothing.
    abstract long retainedBytes();

    /// What this chunk's present values would occupy `PLAIN`-encoded, which is the half of the
    /// encoding comparison the dictionary is weighed against.
    ///
    /// Answered for the whole chunk rather than accumulated a value at a time: for every width the
    /// schema fixes it is arithmetic on the count, and only a variable-width `BYTE_ARRAY` has to
    /// carry a running total — which it can add to as it reads each value anyway.
    ///
    /// @param presentValues how many present values the chunk holds
    abstract long plainValueBits(long presentValues);
}
