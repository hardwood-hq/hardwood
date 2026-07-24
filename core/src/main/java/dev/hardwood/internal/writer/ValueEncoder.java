/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;

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

    /// [#intern] returns this when interning a new value would overflow the dictionary limit,
    /// signalling the caller to fall back to `PLAIN`.
    static final int DICTIONARY_OVERFLOW = Integer.MIN_VALUE;

    /// Selects the encoder for a column's physical type. `pageValues` sizes the per-page value
    /// buffer and the read window; `enableDictionary` requests dictionary encoding where the
    /// type supports it.
    static ValueEncoder forColumn(ColumnSchema column, int pageValues, boolean enableDictionary) {
        PhysicalType type = column.type();
        return switch (type) {
            case INT32 -> new IntValueEncoder(pageValues, enableDictionary);
            case INT64 -> new LongValueEncoder(pageValues, enableDictionary);
            case FLOAT -> new FloatValueEncoder(pageValues, enableDictionary);
            case DOUBLE -> new DoubleValueEncoder(pageValues, enableDictionary);
            case BOOLEAN -> new BooleanValueEncoder(pageValues);
            default -> throw new IllegalArgumentException(
                    "Writer does not support physical type " + type + " for column " + column.name());
        };
    }

    /// Rebinds to a new batch's source and resets the value read window. The dictionary and
    /// statistics persist across the batches of one column chunk.
    abstract void reset(ColumnSource source);

    /// Whether this type is dictionary-encodable at all (`false` for `BOOLEAN`).
    abstract boolean dictionaryCapable();

    /// Interns the present value at `valueIndex` for the current dictionary page, returning its
    /// dictionary index (assigning a new one in first-seen order if unseen), or
    /// [#DICTIONARY_OVERFLOW] when assigning a new value would push the dictionary body past
    /// `dictionaryLimitBytes`. Only called when [#dictionaryCapable] and the chunk is still
    /// dictionary-active.
    abstract int intern(int valueIndex, long dictionaryLimitBytes);

    /// The number of distinct dictionary values assigned so far.
    abstract int dictionarySize();

    /// The `PLAIN`-encoded dictionary body — the distinct values in index order.
    abstract byte[] encodeDictionaryBody();

    /// Buffers the present value at `valueIndex` into the `PLAIN` value buffer at page-relative
    /// slot `slot` (the count of present values sealed into the current page so far).
    abstract void appendPlain(int slot, int valueIndex);

    /// `PLAIN`-encodes the first `count` buffered plain values (the current page's present
    /// values).
    abstract byte[] encodePlain(int count);

    /// Extends the chunk statistics with the present value at `valueIndex`.
    abstract void stat(int valueIndex);

    /// Extends the chunk statistics with an absent (null) slot.
    abstract void statNull();

    /// The accumulated chunk statistics.
    abstract Statistics statistics();
}
